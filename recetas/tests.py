from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from compras.models import OrdenCompra, SolicitudCompra
from maestros.models import Insumo, Proveedor, UnidadMedida
from recetas.models import LineaReceta, PlanProduccion, PlanProduccionItem, Receta


class MatchingPendientesAutocompleteTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_match",
            email="admin_match@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

        unidad = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        self.insumo_1 = Insumo.objects.create(nombre="Harina Pastelera", unidad_base=unidad, activo=True)
        self.insumo_2 = Insumo.objects.create(nombre="Harina Integral", unidad_base=unidad, activo=True)
        Insumo.objects.create(nombre="Mantequilla", unidad_base=unidad, activo=True)

        receta = Receta.objects.create(nombre="Receta Test Match", hash_contenido="hash-match-test-001")
        LineaReceta.objects.create(
            receta=receta,
            posicion=1,
            insumo=None,
            insumo_texto="Harina",
            cantidad=Decimal("1"),
            unidad=unidad,
            unidad_texto="kg",
            costo_unitario_snapshot=Decimal("0"),
            match_status=LineaReceta.STATUS_NEEDS_REVIEW,
            match_score=85,
            match_method="FUZZY",
        )

    def test_matching_pendientes_view_loads(self):
        response = self.client.get(reverse("recetas:matching_pendientes"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Matching pendientes")

    def test_matching_pendientes_export_csv(self):
        response = self.client.get(reverse("recetas:matching_pendientes"), {"export": "csv", "q": "Harina"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/csv", response["Content-Type"])
        body = response.content.decode("utf-8")
        self.assertIn("receta,posicion,ingrediente,metodo,score,insumo_ligado", body)
        self.assertIn("Receta Test Match", body)

    def test_matching_insumos_search_filters_by_query(self):
        response = self.client.get(
            reverse("recetas:matching_insumos_search"),
            {"q": "harina", "limit": "10"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        names = [x["nombre"] for x in payload["results"]]
        self.assertIn(self.insumo_1.nombre, names)
        self.assertIn(self.insumo_2.nombre, names)
        self.assertNotIn("Mantequilla", names)

    def test_matching_insumos_search_enforces_limit(self):
        response = self.client.get(
            reverse("recetas:matching_insumos_search"),
            {"q": "harina", "limit": "1"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["count"], 1)
        self.assertEqual(len(payload["results"]), 1)


class PlanProduccionSolicitudesModeTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_superuser(
            username="admin_plan",
            email="admin_plan@example.com",
            password="test12345",
        )
        self.client.force_login(self.user)

        self.unidad = UnidadMedida.objects.create(
            codigo="kg",
            nombre="Kilogramo",
            tipo=UnidadMedida.TIPO_MASA,
            factor_to_base=Decimal("1000"),
        )
        self.proveedor = Proveedor.objects.create(nombre="Proveedor Plan", activo=True)
        self.insumo = Insumo.objects.create(
            nombre="Harina Plan",
            unidad_base=self.unidad,
            proveedor_principal=self.proveedor,
            activo=True,
        )
        self.receta = Receta.objects.create(nombre="Receta Plan", hash_contenido="hash-plan-001")
        LineaReceta.objects.create(
            receta=self.receta,
            posicion=1,
            insumo=self.insumo,
            insumo_texto="Harina Plan",
            cantidad=Decimal("2"),
            unidad=self.unidad,
            unidad_texto="kg",
            costo_unitario_snapshot=Decimal("5"),
            match_status=LineaReceta.STATUS_AUTO,
            match_score=100,
            match_method=LineaReceta.MATCH_EXACT,
        )
        self.plan = PlanProduccion.objects.create(nombre="Plan Test", fecha_produccion=date(2026, 2, 20))
        PlanProduccionItem.objects.create(plan=self.plan, receta=self.receta, cantidad=Decimal("1"))

    def test_generar_solicitudes_accumulate_mode_updates_existing(self):
        url = reverse("recetas:plan_produccion_generar_solicitudes", args=[self.plan.id])

        response_1 = self.client.post(
            url,
            {"next_view": "plan", "replace_prev": "1", "auto_create_oc": "1"},
        )
        self.assertEqual(response_1.status_code, 302)

        response_2 = self.client.post(
            url,
            {"next_view": "plan", "replace_prev": "0", "auto_create_oc": "1"},
        )
        self.assertEqual(response_2.status_code, 302)

        solicitudes = SolicitudCompra.objects.filter(
            area=f"PLAN_PRODUCCION:{self.plan.id}",
            estatus=SolicitudCompra.STATUS_BORRADOR,
            insumo=self.insumo,
        )
        self.assertEqual(solicitudes.count(), 1)
        self.assertEqual(solicitudes.first().cantidad, Decimal("4"))

        ocs = OrdenCompra.objects.filter(
            referencia=f"PLAN_PRODUCCION:{self.plan.id}",
            estatus=OrdenCompra.STATUS_BORRADOR,
            solicitud__isnull=True,
            proveedor=self.proveedor,
        )
        self.assertEqual(ocs.count(), 1)
        self.assertEqual(ocs.first().monto_estimado, Decimal("20"))
