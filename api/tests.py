from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from inventario.models import ExistenciaInsumo
from maestros.models import Insumo, UnidadMedida
from recetas.models import LineaReceta, PlanProduccion, PlanProduccionItem, Receta
from recetas.utils.costeo_versionado import asegurar_version_costeo


class RecetasCosteoApiTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_api_costeo",
            email="admin_api_costeo@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

        self.unidad = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        self.insumo = Insumo.objects.create(nombre="Azucar API", unidad_base=self.unidad, activo=True)
        self.receta = Receta.objects.create(
            nombre="Receta API",
            sheet_name="Insumos API",
            hash_contenido="hash-api-001",
            rendimiento_cantidad=Decimal("5"),
            rendimiento_unidad=self.unidad,
        )
        self.linea = LineaReceta.objects.create(
            receta=self.receta,
            posicion=1,
            insumo=self.insumo,
            insumo_texto="Azucar API",
            cantidad=Decimal("2"),
            unidad=self.unidad,
            unidad_texto="kg",
            costo_unitario_snapshot=Decimal("3"),
            match_status=LineaReceta.STATUS_AUTO,
            match_score=100,
            match_method=LineaReceta.MATCH_EXACT,
        )
        asegurar_version_costeo(self.receta, fuente="TEST_API")

    def test_endpoint_versiones(self):
        url = reverse("api_receta_versiones", args=[self.receta.id])
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["receta_id"], self.receta.id)
        self.assertGreaterEqual(payload["total"], 1)
        self.assertIn("items", payload)
        self.assertIn("costo_total", payload["items"][0])

    def test_endpoint_costo_historico_con_comparativo(self):
        self.linea.costo_unitario_snapshot = Decimal("4")
        self.linea.save(update_fields=["costo_unitario_snapshot"])
        asegurar_version_costeo(self.receta, fuente="TEST_API")

        url = reverse("api_receta_costo_historico", args=[self.receta.id])
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["receta_id"], self.receta.id)
        self.assertGreaterEqual(len(payload["puntos"]), 2)
        self.assertIn("comparativo", payload)
        self.assertIn("delta_total", payload["comparativo"])

    def test_endpoint_mrp_calcular_requerimientos_por_plan(self):
        plan = PlanProduccion.objects.create(nombre="Plan API", fecha_produccion=date(2026, 2, 21))
        PlanProduccionItem.objects.create(plan=plan, receta=self.receta, cantidad=Decimal("3"))
        ExistenciaInsumo.objects.create(insumo=self.insumo, stock_actual=Decimal("2"))

        url = reverse("api_mrp_calcular_requerimientos")
        resp = self.client.post(url, {"plan_id": plan.id})
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["source"], "plan")
        self.assertEqual(payload["plan_id"], plan.id)
        self.assertGreaterEqual(payload["totales"]["insumos"], 1)
        self.assertGreaterEqual(payload["totales"]["alertas_capacidad"], 1)
        self.assertEqual(payload["items"][0]["insumo"], self.insumo.nombre)
