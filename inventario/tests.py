from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone
from decimal import Decimal
from openpyxl import load_workbook
from io import BytesIO

from inventario.models import AjusteInventario, AlmacenSyncRun, ExistenciaInsumo, MovimientoInventario
from maestros.models import Insumo, InsumoAlias, PointPendingMatch, UnidadMedida
from recetas.models import LineaReceta, Receta


class InventarioAliasesPendingTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_inv",
            email="admin_inv@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

    def test_pending_persisted_hide_and_restore_visibility(self):
        run = AlmacenSyncRun.objects.create(
            source=AlmacenSyncRun.SOURCE_DRIVE,
            status=AlmacenSyncRun.STATUS_OK,
            started_at=timezone.now(),
            matched=10,
            unmatched=2,
            pending_preview=[
                {
                    "source": "inventario",
                    "row": 8,
                    "nombre_origen": "Harina pastelera 25kg",
                    "nombre_normalizado": "harina pastelera 25kg",
                    "sugerencia": "Harina pastelera",
                    "score": 92.0,
                }
            ],
        )

        response = self.client.get(reverse("inventario:aliases_catalog"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["pending_visible_count"], 1)
        self.assertEqual(response.context["pending_source"], "persisted")

        self.client.post(
            reverse("inventario:aliases_catalog"),
            {"action": "clear_pending", "hide_run_id": str(run.id)},
        )
        response_hidden = self.client.get(reverse("inventario:aliases_catalog"))
        self.assertEqual(response_hidden.status_code, 200)
        self.assertEqual(response_hidden.context["pending_visible_count"], 0)
        self.assertEqual(response_hidden.context["hidden_run_id"], run.id)
        self.assertIsNotNone(response_hidden.context["hidden_pending_run"])

        self.client.post(
            reverse("inventario:aliases_catalog"),
            {"action": "reset_hidden_pending"},
        )
        response_restored = self.client.get(reverse("inventario:aliases_catalog"))
        self.assertEqual(response_restored.status_code, 200)
        self.assertEqual(response_restored.context["pending_visible_count"], 1)
        self.assertEqual(response_restored.context["pending_source"], "persisted")

    def test_load_pending_run_moves_preview_to_session(self):
        run = AlmacenSyncRun.objects.create(
            source=AlmacenSyncRun.SOURCE_SCHEDULED,
            status=AlmacenSyncRun.STATUS_OK,
            started_at=timezone.now(),
            matched=20,
            unmatched=1,
            pending_preview=[
                {
                    "source": "inventario",
                    "row": 4,
                    "nombre_origen": "Mantequilla barra",
                    "nombre_normalizado": "mantequilla barra",
                    "sugerencia": "Mantequilla",
                    "score": 88.0,
                }
            ],
        )

        response = self.client.post(
            reverse("inventario:aliases_catalog"),
            {"action": "load_pending_run", "run_id": str(run.id)},
        )
        self.assertEqual(response.status_code, 302)
        session_preview = self.client.session.get("inventario_pending_preview")
        self.assertIsInstance(session_preview, list)
        self.assertEqual(len(session_preview), 1)

        response_after = self.client.get(reverse("inventario:aliases_catalog"))
        self.assertEqual(response_after.status_code, 200)
        self.assertEqual(response_after.context["pending_source"], "session")
        self.assertEqual(response_after.context["pending_visible_count"], 1)

    def test_export_cross_pending_csv(self):
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="P-100",
            point_nombre="Mantequilla Barra",
            fuzzy_score=88.5,
            fuzzy_sugerencia="Mantequilla",
        )
        receta = Receta.objects.create(nombre="Receta Test Export", hash_contenido="hash-export-001")
        LineaReceta.objects.create(
            receta=receta,
            posicion=1,
            insumo=None,
            insumo_texto="Mantequilla Barra",
            cantidad=1,
            unidad=None,
            unidad_texto="kg",
            costo_unitario_snapshot=0,
            match_status=LineaReceta.STATUS_REJECTED,
        )

        response = self.client.get(
            reverse("inventario:aliases_catalog"),
            {"export": "cross_pending_csv"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/csv", response["Content-Type"])
        body = response.content.decode("utf-8")
        self.assertIn("nombre_muestra", body)
        self.assertIn("Mantequilla Barra", body)

    def test_export_cross_pending_xlsx(self):
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="P-101",
            point_nombre="Mantequilla Barra",
            fuzzy_score=88.5,
            fuzzy_sugerencia="Mantequilla",
        )
        receta = Receta.objects.create(nombre="Receta Test Export XLSX", hash_contenido="hash-export-xlsx-001")
        LineaReceta.objects.create(
            receta=receta,
            posicion=1,
            insumo=None,
            insumo_texto="Mantequilla Barra",
            cantidad=1,
            unidad=None,
            unidad_texto="kg",
            costo_unitario_snapshot=0,
            match_status=LineaReceta.STATUS_REJECTED,
        )

        response = self.client.get(
            reverse("inventario:aliases_catalog"),
            {"export": "cross_pending_xlsx"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            response["Content-Type"],
        )
        wb = load_workbook(BytesIO(response.content), data_only=True)
        ws = wb.active
        headers = [ws.cell(row=1, column=i).value for i in range(1, 10)]
        self.assertEqual(
            headers,
            [
                "nombre_muestra",
                "nombre_normalizado",
                "point_count",
                "almacen_count",
                "receta_count",
                "fuentes_activas",
                "total_count",
                "sugerencia",
                "score_max",
            ],
        )
        self.assertEqual(ws.cell(row=2, column=1).value, "Mantequilla Barra")

    def test_export_cross_pending_csv_with_filters(self):
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="P-200",
            point_nombre="Mantequilla Barra",
            fuzzy_score=95.0,
            fuzzy_sugerencia="Mantequilla",
        )
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="P-201",
            point_nombre="Azucar Morena",
            fuzzy_score=72.0,
            fuzzy_sugerencia="Azucar",
        )
        receta = Receta.objects.create(nombre="Receta Test Filtros", hash_contenido="hash-export-002")
        LineaReceta.objects.create(
            receta=receta,
            posicion=1,
            insumo=None,
            insumo_texto="Mantequilla Barra",
            cantidad=1,
            unidad=None,
            unidad_texto="kg",
            costo_unitario_snapshot=0,
            match_status=LineaReceta.STATUS_REJECTED,
        )
        LineaReceta.objects.create(
            receta=receta,
            posicion=2,
            insumo=None,
            insumo_texto="Azucar Morena",
            cantidad=1,
            unidad=None,
            unidad_texto="kg",
            costo_unitario_snapshot=0,
            match_status=LineaReceta.STATUS_REJECTED,
        )

        response = self.client.get(
            reverse("inventario:aliases_catalog"),
            {
                "export": "cross_pending_csv",
                "cross_q": "mantequilla",
                "cross_min_sources": "2",
                "cross_score_min": "90",
                "cross_only_suggested": "1",
            },
        )
        self.assertEqual(response.status_code, 200)
        body = response.content.decode("utf-8")
        self.assertIn("Mantequilla Barra", body)
        self.assertNotIn("Azucar Morena", body)

    def test_export_cross_pending_csv_with_point_tipo_filter(self):
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="P-TIPO-INS-001",
            point_nombre="Insumo only row",
            fuzzy_score=80.0,
            fuzzy_sugerencia="Harina",
        )
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_PRODUCTO,
            point_codigo="P-TIPO-PROD-001",
            point_nombre="Producto only row",
            fuzzy_score=86.0,
            fuzzy_sugerencia="Receta sugerida",
        )

        response = self.client.get(
            reverse("inventario:aliases_catalog"),
            {
                "export": "cross_pending_csv",
                "cross_point_tipo": "PRODUCTO",
                "cross_min_sources": "1",
            },
        )
        self.assertEqual(response.status_code, 200)
        body = response.content.decode("utf-8")
        self.assertIn("Producto only row", body)
        self.assertNotIn("Insumo only row", body)

    def test_auto_apply_suggestions_creates_alias_and_cleans_pending(self):
        unidad = UnidadMedida.objects.create(codigo="kg", nombre="Kilogramo", tipo=UnidadMedida.TIPO_MASA)
        insumo = Insumo.objects.create(nombre="Harina Pastelera", unidad_base=unidad)
        run = AlmacenSyncRun.objects.create(
            source=AlmacenSyncRun.SOURCE_DRIVE,
            status=AlmacenSyncRun.STATUS_OK,
            started_at=timezone.now(),
            matched=11,
            unmatched=1,
            pending_preview=[
                {
                    "source": "inventario",
                    "row": 9,
                    "nombre_origen": "Harina pastelera 25kg",
                    "nombre_normalizado": "harina pastelera 25kg",
                    "sugerencia": "Harina Pastelera",
                    "score": 95.0,
                }
            ],
        )

        response = self.client.post(
            reverse("inventario:aliases_catalog"),
            {
                "action": "auto_apply_suggestions",
                "auto_min_score": "90",
                "auto_min_sources": "1",
                "auto_max_rows": "50",
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertTrue(
            InsumoAlias.objects.filter(nombre_normalizado="harina pastelera 25kg", insumo=insumo).exists()
        )

        run.refresh_from_db()
        self.assertEqual(run.pending_preview, [])

    def test_auto_apply_suggestions_min_sources_gate(self):
        unidad = UnidadMedida.objects.create(codigo="kg", nombre="Kilogramo", tipo=UnidadMedida.TIPO_MASA)
        insumo = Insumo.objects.create(nombre="Azucar Glass", unidad_base=unidad)
        AlmacenSyncRun.objects.create(
            source=AlmacenSyncRun.SOURCE_DRIVE,
            status=AlmacenSyncRun.STATUS_OK,
            started_at=timezone.now(),
            matched=11,
            unmatched=1,
            pending_preview=[
                {
                    "source": "inventario",
                    "row": 5,
                    "nombre_origen": "Azucar glass fina",
                    "nombre_normalizado": "azucar glass fina",
                    "sugerencia": "Azucar Glass",
                    "score": 96.0,
                }
            ],
        )

        # Con 2 fuentes mínimas no debe crear alias (solo existe en almacén).
        response = self.client.post(
            reverse("inventario:aliases_catalog"),
            {
                "action": "auto_apply_suggestions",
                "auto_min_score": "90",
                "auto_min_sources": "2",
                "auto_max_rows": "50",
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertFalse(
            InsumoAlias.objects.filter(nombre_normalizado="azucar glass fina", insumo=insumo).exists()
        )

        # Agregamos pendiente en Point para activar 2 fuentes y sí debe crear alias.
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="P-XYZ",
            point_nombre="Azucar glass fina",
            fuzzy_score=96.0,
            fuzzy_sugerencia="Azucar Glass",
        )
        response = self.client.post(
            reverse("inventario:aliases_catalog"),
            {
                "action": "auto_apply_suggestions",
                "auto_min_score": "90",
                "auto_min_sources": "2",
                "auto_max_rows": "50",
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertTrue(
            InsumoAlias.objects.filter(nombre_normalizado="azucar glass fina", insumo=insumo).exists()
        )

    def test_auto_apply_suggestions_respects_cross_filters(self):
        unidad = UnidadMedida.objects.create(codigo="kg", nombre="Kilogramo", tipo=UnidadMedida.TIPO_MASA)
        insumo_mantequilla = Insumo.objects.create(nombre="Mantequilla", unidad_base=unidad)
        insumo_azucar = Insumo.objects.create(nombre="Azucar Morena", unidad_base=unidad)

        AlmacenSyncRun.objects.create(
            source=AlmacenSyncRun.SOURCE_DRIVE,
            status=AlmacenSyncRun.STATUS_OK,
            started_at=timezone.now(),
            matched=20,
            unmatched=2,
            pending_preview=[
                {
                    "source": "inventario",
                    "row": 7,
                    "nombre_origen": "Mantequilla barra",
                    "nombre_normalizado": "mantequilla barra",
                    "sugerencia": "Mantequilla",
                    "score": 95.0,
                },
                {
                    "source": "inventario",
                    "row": 8,
                    "nombre_origen": "Azucar morena premium",
                    "nombre_normalizado": "azucar morena premium",
                    "sugerencia": "Azucar Morena",
                    "score": 96.0,
                },
            ],
        )
        # Damos 2 fuentes activas a ambas filas para evitar filtros por fuente.
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="PM-001",
            point_nombre="Mantequilla barra",
            fuzzy_score=95.0,
            fuzzy_sugerencia="Mantequilla",
        )
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="PM-002",
            point_nombre="Azucar morena premium",
            fuzzy_score=96.0,
            fuzzy_sugerencia="Azucar Morena",
        )

        response = self.client.post(
            reverse("inventario:aliases_catalog"),
            {
                "action": "auto_apply_suggestions",
                "auto_min_score": "90",
                "auto_min_sources": "1",
                "auto_max_rows": "50",
                "cross_q": "mantequilla",
                "cross_min_sources": "1",
                "cross_score_min": "0",
                "cross_only_suggested": "1",
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertTrue(
            InsumoAlias.objects.filter(nombre_normalizado="mantequilla barra", insumo=insumo_mantequilla).exists()
        )
        self.assertFalse(
            InsumoAlias.objects.filter(nombre_normalizado="azucar morena premium", insumo=insumo_azucar).exists()
        )

    def test_auto_apply_suggestions_redirect_keeps_cross_filters(self):
        response = self.client.post(
            reverse("inventario:aliases_catalog"),
            {
                "action": "auto_apply_suggestions",
                "auto_min_score": "90",
                "auto_min_sources": "2",
                "auto_max_rows": "50",
                "next_q": "harina",
                "cross_q": "mantequilla",
                "cross_source": "POINT",
                "cross_min_sources": "2",
                "cross_score_min": "90",
                "cross_point_tipo": "PRODUCTO",
                "cross_limit": "50",
                "cross_offset": "100",
                "cross_sort_by": "score_max",
                "cross_sort_dir": "asc",
                "cross_only_suggested": "1",
            },
        )
        self.assertEqual(response.status_code, 302)
        location = response["Location"]
        self.assertIn("q=harina", location)
        self.assertIn("cross_q=mantequilla", location)
        self.assertIn("cross_source=POINT", location)
        self.assertIn("cross_min_sources=2", location)
        self.assertIn("cross_score_min=90.0", location)
        self.assertIn("cross_point_tipo=PRODUCTO", location)
        self.assertIn("cross_limit=50", location)
        self.assertIn("cross_offset=100", location)
        self.assertIn("cross_sort_by=score_max", location)
        self.assertIn("cross_sort_dir=asc", location)
        self.assertIn("cross_only_suggested=1", location)

    def test_cross_unified_view_pagination_and_sort(self):
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="PT-CROSS-PAG-01",
            point_nombre="C Name",
            fuzzy_score=85.0,
            fuzzy_sugerencia="Sugerencia C",
        )
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="PT-CROSS-PAG-02",
            point_nombre="A Name",
            fuzzy_score=85.0,
            fuzzy_sugerencia="Sugerencia A",
        )
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="PT-CROSS-PAG-03",
            point_nombre="B Name",
            fuzzy_score=85.0,
            fuzzy_sugerencia="Sugerencia B",
        )

        response = self.client.get(
            reverse("inventario:aliases_catalog"),
            {
                "cross_point_tipo": "INSUMO",
                "cross_min_sources": "1",
                "cross_limit": "1",
                "cross_offset": "1",
                "cross_sort_by": "nombre_muestra",
                "cross_sort_dir": "asc",
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["cross_limit"], 1)
        self.assertEqual(response.context["cross_offset"], 1)
        self.assertEqual(response.context["cross_sort_by"], "nombre_muestra")
        self.assertEqual(response.context["cross_sort_dir"], "asc")
        self.assertEqual(response.context["cross_returned_count"], 1)
        self.assertTrue(response.context["cross_has_prev"])
        self.assertTrue(response.context["cross_has_next"])
        rows = response.context["cross_unified_rows"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["nombre_muestra"], "B Name")

    def test_cross_unified_view_source_filter(self):
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="PT-CROSS-SRC-01",
            point_nombre="Solo Point",
            fuzzy_score=90.0,
            fuzzy_sugerencia="Sugerencia Point",
        )
        receta = Receta.objects.create(nombre="Receta Solo Source", hash_contenido="hash-cross-source-001")
        LineaReceta.objects.create(
            receta=receta,
            posicion=1,
            insumo=None,
            insumo_texto="Solo Receta",
            cantidad=1,
            unidad=None,
            unidad_texto="kg",
            costo_unitario_snapshot=0,
            match_status=LineaReceta.STATUS_REJECTED,
        )

        response_point = self.client.get(
            reverse("inventario:aliases_catalog"),
            {
                "cross_source": "POINT",
                "cross_point_tipo": "INSUMO",
                "cross_min_sources": "1",
                "cross_limit": "50",
            },
        )
        self.assertEqual(response_point.status_code, 200)
        rows_point = response_point.context["cross_unified_rows"]
        self.assertGreaterEqual(len(rows_point), 1)
        self.assertTrue(all(int(row.get("point_count") or 0) > 0 for row in rows_point))

        response_recetas = self.client.get(
            reverse("inventario:aliases_catalog"),
            {
                "cross_source": "RECETAS",
                "cross_point_tipo": "INSUMO",
                "cross_min_sources": "1",
                "cross_limit": "50",
            },
        )
        self.assertEqual(response_recetas.status_code, 200)
        rows_recetas = response_recetas.context["cross_unified_rows"]
        self.assertGreaterEqual(len(rows_recetas), 1)
        self.assertTrue(all(int(row.get("receta_count") or 0) > 0 for row in rows_recetas))

    def test_bulk_reassign_resolves_and_cleans_pending(self):
        unidad = UnidadMedida.objects.create(codigo="kg", nombre="Kilogramo", tipo=UnidadMedida.TIPO_MASA)
        insumo_a = Insumo.objects.create(nombre="Harina A", unidad_base=unidad)
        insumo_b = Insumo.objects.create(nombre="Harina B", unidad_base=unidad)
        alias = InsumoAlias.objects.create(nombre="Harina pastelera 25kg", insumo=insumo_a)
        run = AlmacenSyncRun.objects.create(
            source=AlmacenSyncRun.SOURCE_DRIVE,
            status=AlmacenSyncRun.STATUS_OK,
            started_at=timezone.now(),
            matched=7,
            unmatched=1,
            pending_preview=[
                {
                    "source": "inventario",
                    "row": 14,
                    "nombre_origen": "Harina pastelera 25kg",
                    "nombre_normalizado": "harina pastelera 25kg",
                    "sugerencia": "Harina B",
                    "score": 92.0,
                }
            ],
        )

        response = self.client.post(
            reverse("inventario:aliases_catalog"),
            {
                "action": "bulk_reassign",
                "insumo_id": str(insumo_b.id),
                "alias_ids": [str(alias.id)],
            },
        )
        self.assertEqual(response.status_code, 302)

        alias.refresh_from_db()
        self.assertEqual(alias.insumo_id, insumo_b.id)
        run.refresh_from_db()
        self.assertEqual(run.pending_preview, [])

    def test_bulk_import_aliases_creates_and_cleans_pending(self):
        unidad = UnidadMedida.objects.create(codigo="kg", nombre="Kilogramo", tipo=UnidadMedida.TIPO_MASA)
        insumo = Insumo.objects.create(nombre="Harina Pastelera", unidad_base=unidad)
        run = AlmacenSyncRun.objects.create(
            source=AlmacenSyncRun.SOURCE_DRIVE,
            status=AlmacenSyncRun.STATUS_OK,
            started_at=timezone.now(),
            matched=5,
            unmatched=1,
            pending_preview=[
                {
                    "source": "inventario",
                    "row": 6,
                    "nombre_origen": "Harina pastelera 25kg",
                    "nombre_normalizado": "harina pastelera 25kg",
                    "sugerencia": "Harina Pastelera",
                    "score": 95.0,
                }
            ],
        )
        payload = "alias,insumo\nHarina pastelera 25kg,Harina Pastelera\n"
        archivo = SimpleUploadedFile("aliases.csv", payload.encode("utf-8"), content_type="text/csv")

        response = self.client.post(
            reverse("inventario:aliases_catalog"),
            {
                "action": "import_bulk",
                "score_min": "90",
                "archivo_aliases": archivo,
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertTrue(
            InsumoAlias.objects.filter(nombre_normalizado="harina pastelera 25kg", insumo=insumo).exists()
        )
        run.refresh_from_db()
        self.assertEqual(run.pending_preview, [])

    def test_bulk_import_aliases_stores_unresolved_preview(self):
        payload = "alias,insumo\nFresa natural premium,No Existe En Catalogo\n"
        archivo = SimpleUploadedFile("aliases.csv", payload.encode("utf-8"), content_type="text/csv")

        response = self.client.post(
            reverse("inventario:aliases_catalog"),
            {
                "action": "import_bulk",
                "score_min": "95",
                "archivo_aliases": archivo,
            },
        )
        self.assertEqual(response.status_code, 302)

        preview = self.client.session.get("inventario_alias_import_preview")
        stats = self.client.session.get("inventario_alias_import_stats")
        self.assertIsInstance(preview, list)
        self.assertEqual(len(preview), 1)
        self.assertEqual(preview[0]["alias"], "Fresa natural premium")
        self.assertEqual(stats["unresolved"], 1)

    def test_alias_import_preview_export_csv_and_xlsx(self):
        session = self.client.session
        session["inventario_alias_import_preview"] = [
            {
                "row": 2,
                "alias": "Fresa natural premium",
                "insumo_archivo": "No Existe En Catalogo",
                "sugerencia": "Fresa Fresca",
                "score": 88.5,
                "method": "FUZZY",
                "motivo": "Insumo no resuelto (score<90.0).",
            }
        ]
        session.save()

        response_csv = self.client.get(reverse("inventario:aliases_catalog"), {"export": "alias_import_preview_csv"})
        self.assertEqual(response_csv.status_code, 200)
        self.assertIn("text/csv", response_csv["Content-Type"])
        body_csv = response_csv.content.decode("utf-8")
        self.assertIn("row,alias,insumo_archivo,sugerencia,score,method,motivo", body_csv)
        self.assertIn("Fresa natural premium", body_csv)

        response_xlsx = self.client.get(reverse("inventario:aliases_catalog"), {"export": "alias_import_preview_xlsx"})
        self.assertEqual(response_xlsx.status_code, 200)
        self.assertIn(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            response_xlsx["Content-Type"],
        )
        wb = load_workbook(BytesIO(response_xlsx.content), data_only=True)
        ws = wb.active
        headers = [ws.cell(row=1, column=i).value for i in range(1, 8)]
        self.assertEqual(headers, ["row", "alias", "insumo_archivo", "sugerencia", "score", "method", "motivo"])
        self.assertEqual(ws.cell(row=2, column=2).value, "Fresa natural premium")

    def test_alias_template_export_csv_and_xlsx(self):
        response_csv = self.client.get(reverse("inventario:aliases_catalog"), {"export": "alias_template_csv"})
        self.assertEqual(response_csv.status_code, 200)
        self.assertIn("text/csv", response_csv["Content-Type"])
        self.assertIn("alias,insumo", response_csv.content.decode("utf-8"))

        response_xlsx = self.client.get(reverse("inventario:aliases_catalog"), {"export": "alias_template_xlsx"})
        self.assertEqual(response_xlsx.status_code, 200)
        self.assertIn(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            response_xlsx["Content-Type"],
        )

    def test_apply_suggestion_creates_alias_and_cleans_pending(self):
        unidad = UnidadMedida.objects.create(codigo="kg", nombre="Kilogramo", tipo=UnidadMedida.TIPO_MASA)
        insumo = Insumo.objects.create(nombre="Mantequilla", unidad_base=unidad)
        run = AlmacenSyncRun.objects.create(
            source=AlmacenSyncRun.SOURCE_DRIVE,
            status=AlmacenSyncRun.STATUS_OK,
            started_at=timezone.now(),
            matched=12,
            unmatched=1,
            pending_preview=[
                {
                    "source": "inventario",
                    "row": 3,
                    "nombre_origen": "Mantequilla barra",
                    "nombre_normalizado": "mantequilla barra",
                    "sugerencia": "Mantequilla",
                    "score": 95.0,
                }
            ],
        )

        response = self.client.post(
            reverse("inventario:aliases_catalog"),
            {
                "action": "apply_suggestion",
                "alias_name": "Mantequilla barra",
                "suggestion": "Mantequilla",
                "score_min": "90",
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertTrue(
            InsumoAlias.objects.filter(nombre_normalizado="mantequilla barra", insumo=insumo).exists()
        )
        run.refresh_from_db()
        self.assertEqual(run.pending_preview, [])

    def test_aliases_catalog_export_csv_and_xlsx(self):
        unidad = UnidadMedida.objects.create(codigo="kg", nombre="Kilogramo", tipo=UnidadMedida.TIPO_MASA)
        insumo = Insumo.objects.create(nombre="Harina", unidad_base=unidad)
        InsumoAlias.objects.create(nombre="Harina pastelera 25kg", insumo=insumo)

        response_csv = self.client.get(reverse("inventario:aliases_catalog"), {"export": "aliases_csv"})
        self.assertEqual(response_csv.status_code, 200)
        self.assertIn("text/csv", response_csv["Content-Type"])
        body = response_csv.content.decode("utf-8")
        self.assertIn("alias,normalizado,insumo_oficial", body)
        self.assertIn("Harina pastelera 25kg", body)

        response_xlsx = self.client.get(reverse("inventario:aliases_catalog"), {"export": "aliases_xlsx"})
        self.assertEqual(response_xlsx.status_code, 200)
        self.assertIn(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            response_xlsx["Content-Type"],
        )
        wb = load_workbook(BytesIO(response_xlsx.content), data_only=True)
        ws = wb.active
        headers = [ws.cell(row=1, column=1).value, ws.cell(row=1, column=2).value, ws.cell(row=1, column=3).value]
        self.assertEqual(headers, ["alias", "normalizado", "insumo_oficial"])


class InventarioAjustesApprovalTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.admin = user_model.objects.create_user(
            username="admin_aprueba_inv",
            email="admin_aprueba_inv@example.com",
            password="test12345",
        )
        self.almacen = user_model.objects.create_user(
            username="almacen_inv",
            email="almacen_inv@example.com",
            password="test12345",
        )
        admin_group, _ = Group.objects.get_or_create(name="ADMIN")
        almacen_group, _ = Group.objects.get_or_create(name="ALMACEN")
        self.admin.groups.add(admin_group)
        self.almacen.groups.add(almacen_group)

        self.unidad = UnidadMedida.objects.create(codigo="kg", nombre="Kilogramo", tipo=UnidadMedida.TIPO_MASA)
        self.insumo = Insumo.objects.create(nombre="Azucar Ajuste", unidad_base=self.unidad, activo=True)
        self.existencia = ExistenciaInsumo.objects.create(insumo=self.insumo, stock_actual=Decimal("10"))

    def test_almacen_registra_ajuste_queda_pendiente(self):
        self.client.force_login(self.almacen)
        response = self.client.post(
            reverse("inventario:ajustes"),
            {
                "action": "create",
                "insumo_id": self.insumo.id,
                "cantidad_sistema": "10",
                "cantidad_fisica": "8",
                "motivo": "Conteo semanal",
                "create_and_apply": "1",
            },
        )
        self.assertEqual(response.status_code, 302)

        ajuste = AjusteInventario.objects.latest("id")
        self.assertEqual(ajuste.estatus, AjusteInventario.STATUS_PENDIENTE)
        self.assertEqual(ajuste.solicitado_por_id, self.almacen.id)
        self.existencia.refresh_from_db()
        self.assertEqual(self.existencia.stock_actual, Decimal("10"))
        self.assertFalse(MovimientoInventario.objects.filter(referencia=ajuste.folio).exists())

    def test_admin_aprueba_y_aplica_ajuste(self):
        ajuste = AjusteInventario.objects.create(
            insumo=self.insumo,
            cantidad_sistema=Decimal("10"),
            cantidad_fisica=Decimal("8"),
            motivo="Conteo mensual",
            estatus=AjusteInventario.STATUS_PENDIENTE,
            solicitado_por=self.almacen,
        )

        self.client.force_login(self.admin)
        response = self.client.post(
            reverse("inventario:ajustes"),
            {
                "action": "approve",
                "ajuste_id": ajuste.id,
                "comentario_revision": "Aprobado por diferencias de merma",
            },
        )
        self.assertEqual(response.status_code, 302)

        ajuste.refresh_from_db()
        self.assertEqual(ajuste.estatus, AjusteInventario.STATUS_APLICADO)
        self.assertEqual(ajuste.aprobado_por_id, self.admin.id)
        self.assertIsNotNone(ajuste.aprobado_en)
        self.assertIsNotNone(ajuste.aplicado_en)

        self.existencia.refresh_from_db()
        self.assertEqual(self.existencia.stock_actual, Decimal("8"))
        movimiento = MovimientoInventario.objects.filter(referencia=ajuste.folio).first()
        self.assertIsNotNone(movimiento)
        self.assertEqual(movimiento.cantidad, Decimal("2"))
        self.assertEqual(movimiento.tipo, MovimientoInventario.TIPO_SALIDA)

    def test_admin_rechaza_ajuste_sin_afectar_stock(self):
        ajuste = AjusteInventario.objects.create(
            insumo=self.insumo,
            cantidad_sistema=Decimal("10"),
            cantidad_fisica=Decimal("7"),
            motivo="Conteo de cierre",
            estatus=AjusteInventario.STATUS_PENDIENTE,
            solicitado_por=self.almacen,
        )

        self.client.force_login(self.admin)
        response = self.client.post(
            reverse("inventario:ajustes"),
            {
                "action": "reject",
                "ajuste_id": ajuste.id,
                "comentario_revision": "Falta evidencia de conteo físico",
            },
        )
        self.assertEqual(response.status_code, 302)

        ajuste.refresh_from_db()
        self.assertEqual(ajuste.estatus, AjusteInventario.STATUS_RECHAZADO)
        self.assertEqual(ajuste.aprobado_por_id, self.admin.id)
        self.existencia.refresh_from_db()
        self.assertEqual(self.existencia.stock_actual, Decimal("10"))
        self.assertFalse(MovimientoInventario.objects.filter(referencia=ajuste.folio).exists())

    def test_almacen_no_puede_aprobar_ajuste(self):
        ajuste = AjusteInventario.objects.create(
            insumo=self.insumo,
            cantidad_sistema=Decimal("10"),
            cantidad_fisica=Decimal("9"),
            motivo="Conteo rápido",
            estatus=AjusteInventario.STATUS_PENDIENTE,
            solicitado_por=self.almacen,
        )
        self.client.force_login(self.almacen)
        response = self.client.post(
            reverse("inventario:ajustes"),
            {"action": "approve", "ajuste_id": ajuste.id},
        )
        self.assertEqual(response.status_code, 403)
