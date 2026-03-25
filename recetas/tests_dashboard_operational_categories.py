from django.test import SimpleTestCase

from recetas.views import _point_operational_category


class DashboardOperationalCategoryTests(SimpleTestCase):
    def test_maps_pastel_mediano_from_name(self):
        self.assertEqual(
            _point_operational_category(category="", family="Pastel", item_name="Pastel de Fresas Con Crema Mediano"),
            "Pastel Mediano",
        )

    def test_maps_pay_grande_from_name(self):
        self.assertEqual(
            _point_operational_category(category="", family="Pay", item_name="Pay de Queso Grande"),
            "Pay Grande",
        )

    def test_maps_individual_from_cheesecake(self):
        self.assertEqual(
            _point_operational_category(category="", family="", item_name="Cheesecake Lotus Individual"),
            "Individual",
        )
