from decimal import Decimal
from types import SimpleNamespace

from django.test import SimpleTestCase

from inventario.canonical_point_inventory import (
    InventoryLocation,
    display_quantity,
    require_inventory_location,
)


class CanonicalPointInventoryContractTests(SimpleTestCase):
    def test_location_is_required(self):
        with self.assertRaisesMessage(ValueError, "ubicación de inventario es obligatoria"):
            require_inventory_location(None)

    def test_only_business_locations_are_accepted(self):
        self.assertEqual(require_inventory_location("ALMACEN"), InventoryLocation.ALMACEN)
        self.assertEqual(require_inventory_location("CEDIS"), InventoryLocation.CEDIS)
        with self.assertRaises(ValueError):
            require_inventory_location("CFP")

    def test_base_units_are_presented_as_kg_liters_or_pieces(self):
        self.assertEqual(
            display_quantity(Decimal("169669.245"), SimpleNamespace(codigo="g")),
            (Decimal("169.669245"), "kg"),
        )
        self.assertEqual(
            display_quantity(Decimal("11217.150"), SimpleNamespace(codigo="ml")),
            (Decimal("11.21715"), "L"),
        )
        self.assertEqual(
            display_quantity(Decimal("7"), SimpleNamespace(codigo="pza")),
            (Decimal("7"), "pza"),
        )
