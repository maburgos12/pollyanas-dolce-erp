from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from pos_bridge.models import PointProduct
from pos_bridge.services.product_count_units import COUNT_UNIT_KEY, product_count_unit


class ProductCountUnitSyncTests(TestCase):
    def setUp(self):
        self.product = PointProduct.objects.create(external_id='119',sku='0119',name='Bollo',metadata={'keep':True})
        self.rows = [{'PK_Producto':119,'Codigo':'0119','FK_Unidad':5,'Unidad':'PZA'}]

    def run_sync(self, dry=False):
        with patch('pos_bridge.management.commands.sync_product_count_units.PointHttpSessionClient') as client:
            c = client.return_value.__enter__.return_value
            c.get_units.return_value = [{'ID_Unidad':5,'Abreviacion':'PZA'}]
            c.get_all_products.return_value = self.rows
            call_command('sync_product_count_units', dry_run=dry,stdout=StringIO())

    def test_sync_keeps_metadata_and_attaches_exact_identity_provenance(self):
        self.run_sync()
        self.product.refresh_from_db()
        self.assertTrue(self.product.metadata['keep'])
        self.assertEqual(product_count_unit(self.product)[0], 'PZA')
        self.assertEqual(self.product.metadata[COUNT_UNIT_KEY]['point_unit_id'],5)

    def test_dry_run_performs_no_writes(self):
        before = self.product.metadata
        self.run_sync(dry=True)
        self.product.refresh_from_db()
        self.assertEqual(self.product.metadata,before)

    def test_unit_id_is_resolved_from_official_units_when_label_absent(self):
        del self.rows[0]['Unidad']
        self.run_sync()
        self.product.refresh_from_db()
        self.assertEqual(product_count_unit(self.product)[0], 'PZA')

    def test_invalid_catalog_rolls_back_without_erasing_old_metadata(self):
        for rows in ([{'Codigo':'0119','FK_Unidad':999}], []):
            self.rows = rows
            with self.assertRaises(CommandError):
                self.run_sync()
            self.product.refresh_from_db()
            self.assertEqual(self.product.metadata,{'keep':True})

    def test_ambiguous_codes_do_not_block_unique_products_or_keep_old_unit_evidence(self):
        self.run_sync()
        other=PointProduct.objects.create(external_id='unique',sku='UNIQUE',name='Único',metadata={'keep':True})
        self.rows += [{'Codigo':'0119','Unidad':'KG','FK_Unidad':3}, {'Codigo':'UNIQUE','Unidad':'LT','FK_Unidad':18}]
        self.run_sync(dry=True)
        self.product.refresh_from_db()
        self.assertEqual(product_count_unit(self.product)[0],'PZA')
        self.run_sync()
        self.product.refresh_from_db();other.refresh_from_db()
        self.assertEqual(self.product.metadata,{'keep':True})
        self.assertEqual(product_count_unit(self.product)[0],'')
        self.assertEqual(product_count_unit(other)[0],'LT')
        self.assertTrue(other.metadata['keep'])

    def test_duplicate_codes_are_ambiguous_even_when_units_agree(self):
        other=PointProduct.objects.create(external_id='unique',sku='UNIQUE',name='Único')
        self.rows = self.rows*2 + [{'Codigo':'UNIQUE','Unidad':'PZA','FK_Unidad':5}]
        self.run_sync()
        self.product.refresh_from_db()
        self.assertNotIn(COUNT_UNIT_KEY,self.product.metadata)
        other.refresh_from_db()
        self.assertEqual(product_count_unit(other)[0],'PZA')
