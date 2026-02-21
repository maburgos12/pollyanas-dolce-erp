from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from maestros.models import Insumo, UnidadMedida
from recetas.models import LineaReceta, Receta


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
