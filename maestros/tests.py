from io import BytesIO
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from openpyxl import load_workbook

from maestros.models import Insumo, InsumoAlias, PointPendingMatch, Proveedor, UnidadMedida


class PointPendingReviewTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_maestros",
            email="admin_maestros@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

        self.unidad = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        self.proveedor = Proveedor.objects.create(nombre="Proveedor Test", activo=True)
        self.insumo_harina = Insumo.objects.create(
            nombre="Harina Pastelera",
            categoria="Masa",
            unidad_base=self.unidad,
            proveedor_principal=self.proveedor,
            activo=True,
        )

    def test_point_pending_export_csv_applies_filters(self):
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="PT-INS-001",
            point_nombre="Harina Point",
            fuzzy_sugerencia="Harina Pastelera",
            fuzzy_score=95.0,
            method="FUZZY",
        )
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="PT-INS-002",
            point_nombre="Azucar Point",
            fuzzy_sugerencia="Azucar",
            fuzzy_score=40.0,
            method="FUZZY",
        )
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_PROVEEDOR,
            point_codigo="PT-PROV-001",
            point_nombre="Proveedor Point",
            fuzzy_sugerencia="Proveedor Test",
            fuzzy_score=99.0,
            method="EXACT",
        )

        response = self.client.get(
            reverse("maestros:point_pending_review"),
            {
                "tipo": "INSUMO",
                "q": "Harina",
                "score_min": "90",
                "export": "csv",
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/csv", response["Content-Type"])
        body = response.content.decode("utf-8")
        self.assertIn("PT-INS-001", body)
        self.assertNotIn("PT-INS-002", body)
        self.assertNotIn("PT-PROV-001", body)
        self.assertIn("INSUMO,Harina,90.0,1", body)

    def test_point_pending_export_xlsx_includes_headers(self):
        PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="PT-INS-003",
            point_nombre="Mantequilla Point",
            fuzzy_sugerencia="Mantequilla",
            fuzzy_score=92.5,
            method="FUZZY",
        )

        response = self.client.get(
            reverse("maestros:point_pending_review"),
            {"tipo": "INSUMO", "export": "xlsx"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            response["Content-Type"],
        )

        wb = load_workbook(BytesIO(response.content), data_only=True)
        ws = wb.active
        headers = [ws.cell(row=4, column=i).value for i in range(1, 9)]
        self.assertEqual(
            headers,
            [
                "id",
                "tipo",
                "codigo_point",
                "nombre_point",
                "sugerencia",
                "score",
                "metodo",
                "creado_en",
            ],
        )
        self.assertEqual(ws.cell(row=5, column=3).value, "PT-INS-003")

    def test_auto_resolve_sugerencias_insumos_updates_mapping_and_alias(self):
        pending = PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="PT-INS-010",
            point_nombre="Harina Point Oficial",
            fuzzy_sugerencia="Harina Pastelera",
            fuzzy_score=97.0,
            method="FUZZY",
        )

        response = self.client.post(
            reverse("maestros:point_pending_review"),
            {
                "tipo": "INSUMO",
                "action": "resolve_sugerencias_insumos",
                "pending_ids": [str(pending.id)],
                "auto_score_min": "90",
                "create_aliases": "on",
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertFalse(PointPendingMatch.objects.filter(id=pending.id).exists())

        self.insumo_harina.refresh_from_db()
        self.assertEqual(self.insumo_harina.codigo_point, "PT-INS-010")
        self.assertEqual(self.insumo_harina.nombre_point, "Harina Point Oficial")

        alias_norm = "harina point oficial"
        self.assertTrue(
            InsumoAlias.objects.filter(nombre_normalizado=alias_norm, insumo=self.insumo_harina).exists()
        )

    def test_auto_resolve_sugerencias_uses_current_filter_when_no_selection(self):
        pending_ok = PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="PT-INS-020",
            point_nombre="Harina Point Filtrada",
            fuzzy_sugerencia="Harina Pastelera",
            fuzzy_score=95.0,
            method="FUZZY",
        )
        pending_other = PointPendingMatch.objects.create(
            tipo=PointPendingMatch.TIPO_INSUMO,
            point_codigo="PT-INS-021",
            point_nombre="Azucar Point",
            fuzzy_sugerencia="Azucar",
            fuzzy_score=99.0,
            method="FUZZY",
        )

        response = self.client.post(
            reverse("maestros:point_pending_review"),
            {
                "tipo": "INSUMO",
                "action": "resolve_sugerencias_insumos",
                "q": "Harina",
                "score_min": "90",
                "auto_score_min": "90",
                "create_aliases": "on",
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertFalse(PointPendingMatch.objects.filter(id=pending_ok.id).exists())
        self.assertTrue(PointPendingMatch.objects.filter(id=pending_other.id).exists())
