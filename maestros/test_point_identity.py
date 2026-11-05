from decimal import Decimal

from django.test import TestCase

from maestros.models import CostoInsumo, Insumo, Proveedor
from maestros.utils.canonical_catalog import canonical_insumo_by_id, canonical_member_ids


class PointCatalogIdentityTests(TestCase):
    def test_distinct_point_codes_do_not_share_members_or_cost(self):
        caja = Insumo.objects.create(nombre="CAJA CH", codigo_point="075")
        rollos = Insumo.objects.create(nombre="Rollos Termicos", codigo_point="RTERMICOS")
        Insumo.objects.filter(pk=caja.pk).update(nombre_normalizado=rollos.nombre_normalizado)
        proveedor = Proveedor.objects.create(nombre="Proveedor identidad")
        CostoInsumo.objects.create(insumo=rollos, proveedor=proveedor, costo_unitario=Decimal("50"))

        self.assertEqual(canonical_insumo_by_id(caja.id).id, caja.id)
        self.assertEqual(canonical_insumo_by_id(rollos.id).id, rollos.id)
        self.assertEqual(canonical_member_ids(caja), [caja.id])
        from maestros.utils.canonical_catalog import latest_costo_canonico
        self.assertIsNone(latest_costo_canonico(caja))

    def test_partial_name_save_updates_normalized_name(self):
        insumo = Insumo.objects.create(nombre="Rollos Termicos")
        insumo.nombre = "CAJA CH"
        insumo.save(update_fields=["nombre"])
        insumo.refresh_from_db()
        self.assertEqual(insumo.nombre_normalizado, "caja ch")
