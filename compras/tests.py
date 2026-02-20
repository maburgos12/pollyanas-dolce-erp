from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from compras.models import OrdenCompra, SolicitudCompra
from inventario.models import MovimientoInventario
from maestros.models import CostoInsumo, Insumo, Proveedor, UnidadMedida
from recetas.models import LineaReceta, PlanProduccion, PlanProduccionItem, Receta


class ComprasFase2FiltersTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_test",
            email="admin_test@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

        self.proveedor = Proveedor.objects.create(nombre="Proveedor Test", activo=True)
        self.unidad_kg = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        self.unidad_lt = UnidadMedida.objects.create(
            codigo="lt",
            nombre="Litro",
            tipo=UnidadMedida.TIPO_VOLUMEN,
            factor_to_base=Decimal("1000"),
        )

        self.insumo_masa_blank = Insumo.objects.create(
            nombre="Harina sin categoria",
            categoria="",
            unidad_base=self.unidad_kg,
            proveedor_principal=self.proveedor,
            activo=True,
        )
        self.insumo_masa_explicit = Insumo.objects.create(
            nombre="Mantequilla categoria masa",
            categoria="Masa",
            unidad_base=self.unidad_kg,
            proveedor_principal=self.proveedor,
            activo=True,
        )
        self.insumo_volumen = Insumo.objects.create(
            nombre="Leche sin categoria",
            categoria="",
            unidad_base=self.unidad_lt,
            proveedor_principal=self.proveedor,
            activo=True,
        )

        CostoInsumo.objects.create(
            insumo=self.insumo_masa_blank,
            proveedor=self.proveedor,
            costo_unitario=Decimal("10"),
            source_hash="cost-harina-1",
        )
        CostoInsumo.objects.create(
            insumo=self.insumo_masa_explicit,
            proveedor=self.proveedor,
            costo_unitario=Decimal("5"),
            source_hash="cost-mantequilla-1",
        )
        CostoInsumo.objects.create(
            insumo=self.insumo_volumen,
            proveedor=self.proveedor,
            costo_unitario=Decimal("7"),
            source_hash="cost-leche-1",
        )

        self.periodo_mes = "2026-02"
        self.fecha_base = date(2026, 2, 10)
        self.solicitud_masa_blank = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="admin",
            insumo=self.insumo_masa_blank,
            proveedor_sugerido=self.proveedor,
            cantidad=Decimal("2"),
            fecha_requerida=self.fecha_base,
            estatus=SolicitudCompra.STATUS_APROBADA,
        )
        self.solicitud_masa_explicit = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="admin",
            insumo=self.insumo_masa_explicit,
            proveedor_sugerido=self.proveedor,
            cantidad=Decimal("1"),
            fecha_requerida=self.fecha_base,
            estatus=SolicitudCompra.STATUS_APROBADA,
        )
        self.solicitud_volumen = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="admin",
            insumo=self.insumo_volumen,
            proveedor_sugerido=self.proveedor,
            cantidad=Decimal("3"),
            fecha_requerida=self.fecha_base,
            estatus=SolicitudCompra.STATUS_APROBADA,
        )

        OrdenCompra.objects.create(
            solicitud=self.solicitud_masa_blank,
            proveedor=self.proveedor,
            fecha_emision=self.fecha_base,
            monto_estimado=Decimal("30"),
            estatus=OrdenCompra.STATUS_ENVIADA,
        )
        OrdenCompra.objects.create(
            solicitud=self.solicitud_volumen,
            proveedor=self.proveedor,
            fecha_emision=self.fecha_base,
            monto_estimado=Decimal("100"),
            estatus=OrdenCompra.STATUS_ENVIADA,
        )

        self.receta_plan = Receta.objects.create(
            nombre="Base prueba plan",
            hash_contenido="test-hash-plan-001",
        )
        LineaReceta.objects.create(
            receta=self.receta_plan,
            posicion=1,
            insumo=self.insumo_masa_blank,
            insumo_texto="Harina",
            cantidad=Decimal("2"),
            unidad=self.unidad_kg,
            unidad_texto="kg",
            costo_unitario_snapshot=Decimal("10"),
            match_method=LineaReceta.MATCH_EXACT,
            match_score=100,
            match_status=LineaReceta.STATUS_AUTO,
        )
        self.plan = PlanProduccion.objects.create(
            nombre="Plan Febrero Test",
            fecha_produccion=self.fecha_base,
        )
        PlanProduccionItem.objects.create(
            plan=self.plan,
            receta=self.receta_plan,
            cantidad=Decimal("1"),
        )
        MovimientoInventario.objects.create(
            fecha=timezone.make_aware(datetime(2026, 2, 10, 11, 0, 0)),
            tipo=MovimientoInventario.TIPO_CONSUMO,
            insumo=self.insumo_masa_blank,
            cantidad=Decimal("3"),
            referencia=f"PLAN_PRODUCCION:{self.plan.id}",
        )

    def test_resumen_api_aplica_filtro_categoria(self):
        url = reverse("compras:solicitudes_resumen_api")
        response = self.client.get(
            url,
            {
                "periodo_tipo": "mes",
                "periodo_mes": self.periodo_mes,
                "categoria": "Masa",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()

        self.assertEqual(payload["filters"]["categoria"], "Masa")
        self.assertEqual(payload["totals"]["solicitudes_count"], 2)
        self.assertAlmostEqual(payload["totals"]["presupuesto_estimado_total"], 25.0, places=2)
        self.assertAlmostEqual(payload["totals"]["presupuesto_ejecutado_total"], 30.0, places=2)

        categorias = {row["categoria"]: row for row in payload["top_categorias"]}
        self.assertIn("Masa", categorias)
        self.assertAlmostEqual(categorias["Masa"]["estimado"], 25.0, places=2)
        self.assertAlmostEqual(categorias["Masa"]["ejecutado"], 30.0, places=2)

    def test_solicitudes_view_context_preserva_categoria(self):
        url = reverse("compras:solicitudes")
        response = self.client.get(
            url,
            {
                "periodo_tipo": "mes",
                "periodo_mes": self.periodo_mes,
                "categoria": "Masa",
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["categoria_filter"], "Masa")
        self.assertEqual(len(response.context["solicitudes"]), 2)
        self.assertIn("categoria=Masa", response.context["current_query"])

    def test_consumo_vs_plan_api_retorna_totales(self):
        url = reverse("compras:solicitudes_consumo_vs_plan_api")
        response = self.client.get(
            url,
            {
                "periodo_tipo": "mes",
                "periodo_mes": self.periodo_mes,
                "categoria": "Masa",
                "source": "all",
                "consumo_ref": "all",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()

        self.assertEqual(payload["filters"]["categoria"], "Masa")
        self.assertEqual(payload["filters"]["consumo_ref"], "all")
        self.assertAlmostEqual(payload["totals"]["plan_qty_total"], 2.0, places=2)
        self.assertAlmostEqual(payload["totals"]["consumo_real_qty_total"], 3.0, places=2)
        self.assertAlmostEqual(payload["totals"]["plan_cost_total"], 20.0, places=2)
        self.assertAlmostEqual(payload["totals"]["consumo_real_cost_total"], 30.0, places=2)
        self.assertAlmostEqual(payload["totals"]["variacion_cost_total"], 10.0, places=2)
        self.assertIsNotNone(payload["totals"]["cobertura_pct"])
        self.assertEqual(payload["totals"]["sin_costo_count"], 0)
        self.assertEqual(payload["totals"]["semaforo_rojo_count"], 1)
        self.assertEqual(payload["rows"][0]["semaforo"], "ROJO")
        self.assertFalse(payload["rows"][0]["sin_costo"])

    def test_consumo_vs_plan_api_filtra_movimientos_solo_con_referencia_plan(self):
        MovimientoInventario.objects.create(
            fecha=timezone.make_aware(datetime(2026, 2, 10, 12, 0, 0)),
            tipo=MovimientoInventario.TIPO_CONSUMO,
            insumo=self.insumo_masa_blank,
            cantidad=Decimal("2"),
            referencia="SALIDA_MANUAL",
        )
        url = reverse("compras:solicitudes_consumo_vs_plan_api")
        payload_all = self.client.get(
            url,
            {
                "periodo_tipo": "mes",
                "periodo_mes": self.periodo_mes,
                "categoria": "Masa",
                "source": "all",
                "consumo_ref": "all",
            },
        ).json()
        payload_plan_ref = self.client.get(
            url,
            {
                "periodo_tipo": "mes",
                "periodo_mes": self.periodo_mes,
                "categoria": "Masa",
                "source": "all",
                "consumo_ref": "plan_ref",
            },
        ).json()

        self.assertAlmostEqual(payload_all["totals"]["consumo_real_qty_total"], 5.0, places=2)
        self.assertAlmostEqual(payload_plan_ref["totals"]["consumo_real_qty_total"], 3.0, places=2)
        self.assertEqual(payload_plan_ref["filters"]["consumo_ref"], "plan_ref")

    def test_consumo_vs_plan_marca_alerta_sin_costo_unitario(self):
        insumo_sin_costo = Insumo.objects.create(
            nombre="Insumo interno sin costo",
            categoria="Masa",
            unidad_base=self.unidad_kg,
            proveedor_principal=self.proveedor,
            activo=True,
        )
        receta_sin_costo = Receta.objects.create(
            nombre="Receta sin costo",
            hash_contenido="test-hash-plan-002",
        )
        LineaReceta.objects.create(
            receta=receta_sin_costo,
            posicion=1,
            insumo=insumo_sin_costo,
            insumo_texto="Insumo interno sin costo",
            cantidad=Decimal("1"),
            unidad=self.unidad_kg,
            unidad_texto="kg",
            costo_unitario_snapshot=Decimal("0"),
            match_method=LineaReceta.MATCH_EXACT,
            match_score=100,
            match_status=LineaReceta.STATUS_AUTO,
        )
        plan_sin_costo = PlanProduccion.objects.create(
            nombre="Plan Sin Costo",
            fecha_produccion=self.fecha_base,
        )
        PlanProduccionItem.objects.create(
            plan=plan_sin_costo,
            receta=receta_sin_costo,
            cantidad=Decimal("1"),
        )

        url = reverse("compras:solicitudes_consumo_vs_plan_api")
        payload = self.client.get(
            url,
            {
                "periodo_tipo": "mes",
                "periodo_mes": self.periodo_mes,
                "categoria": "Masa",
                "source": "all",
                "consumo_ref": "all",
            },
        ).json()

        self.assertGreaterEqual(payload["totals"]["sin_costo_count"], 1)
        row = next(r for r in payload["rows"] if r["insumo"] == "Insumo interno sin costo")
        self.assertTrue(row["sin_costo"])
        self.assertEqual(row["alerta"], "Sin costo unitario")


class ComprasSolicitudesImportPreviewTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_import",
            email="admin_import@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

        self.proveedor = Proveedor.objects.create(nombre="Proveedor Import", activo=True)
        self.unidad = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        self.insumo_harina = Insumo.objects.create(
            nombre="Harina Import",
            categoria="Masa",
            unidad_base=self.unidad,
            proveedor_principal=self.proveedor,
            activo=True,
        )
        self.insumo_azucar = Insumo.objects.create(
            nombre="Azucar Import",
            categoria="Masa",
            unidad_base=self.unidad,
            proveedor_principal=self.proveedor,
            activo=True,
        )

    def test_import_preview_confirma_edicion_y_descarte(self):
        csv_content = (
            "insumo,cantidad,area,solicitante,fecha_requerida,estatus\n"
            "Harina Import,2,Compras,ana,2026-02-20,BORRADOR\n"
            "Azucar con typo,3,Compras,luis,2026-02-21,BORRADOR\n"
            "Harina Import,1,Compras,maria,2026-02-22,BORRADOR\n"
        )
        archivo = SimpleUploadedFile("solicitudes.csv", csv_content.encode("utf-8"), content_type="text/csv")

        with patch(
            "compras.views.match_insumo",
            side_effect=[
                (self.insumo_harina, 100.0, "exact"),
                (self.insumo_azucar, 60.0, "fuzzy"),
                (self.insumo_harina, 100.0, "exact"),
            ],
        ):
            response = self.client.post(
                reverse("compras:solicitudes_importar"),
                {
                    "archivo": archivo,
                    "periodo_tipo": "mes",
                    "periodo_mes": "2026-02",
                    "area": "Compras",
                    "solicitante": "admin_import",
                    "estatus": SolicitudCompra.STATUS_BORRADOR,
                    "score_min": "90",
                    "evitar_duplicados": "on",
                },
            )

        self.assertEqual(response.status_code, 302)
        preview_payload = self.client.session.get("compras_solicitudes_import_preview")
        self.assertIsNotNone(preview_payload)
        rows = preview_payload["rows"]
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[1]["insumo_id"], "")

        confirmar_data = {}
        for row in rows:
            row_id = row["row_id"]
            confirmar_data[f"row_{row_id}_insumo_id"] = row["insumo_id"]
            confirmar_data[f"row_{row_id}_cantidad"] = row["cantidad"]
            confirmar_data[f"row_{row_id}_fecha_requerida"] = row["fecha_requerida"]
            confirmar_data[f"row_{row_id}_area"] = row["area"]
            confirmar_data[f"row_{row_id}_solicitante"] = row["solicitante"]
            confirmar_data[f"row_{row_id}_proveedor_id"] = row["proveedor_id"]
            confirmar_data[f"row_{row_id}_estatus"] = row["estatus"]

        confirmar_data[f"row_{rows[0]['row_id']}_include"] = "on"
        confirmar_data[f"row_{rows[1]['row_id']}_include"] = "on"
        confirmar_data[f"row_{rows[1]['row_id']}_insumo_id"] = str(self.insumo_azucar.id)

        response_confirm = self.client.post(
            reverse("compras:solicitudes_importar_confirmar"),
            confirmar_data,
        )
        self.assertEqual(response_confirm.status_code, 302)

        solicitudes = list(SolicitudCompra.objects.order_by("id"))
        self.assertEqual(len(solicitudes), 2)
        self.assertEqual({s.insumo_id for s in solicitudes}, {self.insumo_harina.id, self.insumo_azucar.id})
        self.assertIsNone(self.client.session.get("compras_solicitudes_import_preview"))

    def test_eliminar_solicitud_permitida_si_no_tiene_oc_activa(self):
        solicitud = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="admin_import",
            insumo=self.insumo_harina,
            proveedor_sugerido=self.proveedor,
            cantidad=Decimal("1"),
            fecha_requerida=date(2026, 2, 20),
            estatus=SolicitudCompra.STATUS_BORRADOR,
        )

        response = self.client.post(
            reverse("compras:solicitud_eliminar", args=[solicitud.id]),
            {"return_query": "source=manual"},
        )
        self.assertEqual(response.status_code, 302)
        self.assertFalse(SolicitudCompra.objects.filter(id=solicitud.id).exists())

    def test_eliminar_solicitud_bloqueada_si_tiene_oc_activa(self):
        solicitud = SolicitudCompra.objects.create(
            area="Compras",
            solicitante="admin_import",
            insumo=self.insumo_harina,
            proveedor_sugerido=self.proveedor,
            cantidad=Decimal("1"),
            fecha_requerida=date(2026, 2, 20),
            estatus=SolicitudCompra.STATUS_APROBADA,
        )
        OrdenCompra.objects.create(
            solicitud=solicitud,
            proveedor=self.proveedor,
            fecha_emision=date(2026, 2, 20),
            monto_estimado=Decimal("10"),
            estatus=OrdenCompra.STATUS_ENVIADA,
        )

        response = self.client.post(
            reverse("compras:solicitud_eliminar", args=[solicitud.id]),
            {"return_query": "source=manual"},
        )
        self.assertEqual(response.status_code, 302)
        self.assertTrue(SolicitudCompra.objects.filter(id=solicitud.id).exists())
