"""Developer metadata stays reproducible and never queries operational rows."""
import io
import json
from pathlib import Path
import subprocess
import tempfile
from types import ModuleType
from unittest.mock import patch

from django.core.management import call_command
from django.db import models
from django.test import SimpleTestCase, override_settings
from django.test.utils import isolate_apps
from django.urls import include, path

from orquestacion.services.erp_system_map import build_system_map


class SystemMapTests(SimpleTestCase):
    def test_declared_models_fk_m2m_and_tools_have_source_references(self):
        result=build_system_map()
        indexed={row['modelo']:row for row in result['models']}
        user=indexed['auth.User']
        groups=next(row for row in user['many_to_many'] if row['field']=='groups')
        self.assertEqual(groups['target'],'auth.Group')
        self.assertEqual(groups['through'],'auth.User_groups')
        self.assertTrue(groups['through_auto_created'])
        self.assertTrue(indexed['compras.OrdenCompra']['relaciones'])
        self.assertEqual(len(indexed['compras.OrdenCompra']['source']['sha256']),64)
        tool=next(row for row in result['tools'] if row['key']=='erp.get_sales_summary')
        self.assertEqual(tool['runtime_projection'],'business_read_bridge')
        self.assertEqual(tool['schema_source']['path'],'api/ai_gateway_business_reads.py')
        for name in ('handler','access_check','schema_source'):
            self.assertEqual(len(tool[name]['sha256']),64)
        self.assertTrue(any(row['label']=='rrhh' for row in result['apps']))
        self.assertFalse(result['coverage']['semantic_completeness_proven'])

    def test_recursive_namespaced_routes_preserve_declared_callback(self):
        conf=ModuleType('map_test_urls')
        conf.urlpatterns=[path('nested/',include(([path('leaf/',lambda request:None,name='leaf')],'nested'),namespace='inner'))]
        with override_settings(ROOT_URLCONF=conf): result=build_system_map()
        route=next(row for row in result['routes'] if row['name']=='inner:leaf')
        self.assertEqual(route['pattern'],'nested/leaf/')
        self.assertEqual(route['verification'],'declared_resolved')
        self.assertIn('symbol',route['callback'])

    @isolate_apps('orquestacion')
    def test_model_defaults_and_row_values_are_never_materialized(self):
        class PrivateFixture(models.Model):
            token=models.CharField(max_length=90,default='secret-default-do-not-export')
            class Meta: app_label='orquestacion'
        with patch('orquestacion.services.erp_system_map.apps.get_models',return_value=[PrivateFixture]):
            result=build_system_map()
        serialized=json.dumps(result)
        self.assertNotIn('secret-default-do-not-export',serialized)
        self.assertNotIn('"default"',serialized)
        self.assertFalse(result['operational_rows_read'])
        self.assertEqual(result['call_relationships']['inferred'],[])
        self.assertEqual(result['call_relationships']['unresolved'],[])
        self.assertEqual(result['call_relationships']['static_scan'],'not_performed')

    def test_content_and_tree_hash_track_bytes_not_commit_or_generated_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            subprocess.run(['git','init','-q',tmp],check=True)
            source=root/'reader.py'; source.write_text('value = 1\n')
            (root/'.env').write_text('TOKEN=secret-env-do-not-export')
            (root/'generated-system-map.json').write_text('{"generated":1}')
            subprocess.run(['git','-C',tmp,'add','.'],check=True)
            subprocess.run(['git','-C',tmp,'-c','user.name=Metadata test','-c','user.email=test@example.invalid','commit','-qm','fixture'],check=True)
            with override_settings(BASE_DIR=root):
                first=build_system_map()
                self.assertEqual(first,build_system_map())
                with patch('orquestacion.services.erp_system_map._source_commit',return_value='different-head'):
                    next_head=build_system_map()
                self.assertEqual(first['content_hash'],next_head['content_hash'])
                self.assertEqual(first['source_tree_hash'],next_head['source_tree_hash'])
                (root/'generated-system-map.json').write_text('{"generated":2}')
                self.assertEqual(first['source_tree_hash'],build_system_map()['source_tree_hash'])
                source.write_text('value = 2\n')
                changed=build_system_map()
                self.assertNotEqual(first['source_tree_hash'],changed['source_tree_hash'])
                self.assertNotEqual(first['content_hash'],changed['content_hash'])
            self.assertEqual(first['coverage']['tracked_code_files'],1)
            self.assertNotIn('secret-env-do-not-export',json.dumps(first))

    def test_command_emits_only_json_without_querying_database(self):
        output=io.StringIO()
        call_command('build_erp_system_map',stdout=output)
        result=json.loads(output.getvalue())
        self.assertEqual(result['schema_version'],1)
        self.assertFalse(result['operational_rows_read'])
        self.assertEqual(len(result['source_tree_hash']),64)

    def test_source_references_reject_private_paths_and_symlinks(self):
        from orquestacion.services.erp_system_map import _source_inventory, _source_ref
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            private = root / 'private_media'
            private.mkdir()
            secret = private / 'reader.py'
            secret.write_text('private evidence must not be hashed\n')
            link = root / 'reader_link.py'
            link.symlink_to(secret)
            directory_link = root / 'linked'
            directory_link.symlink_to(private, target_is_directory=True)
            for filename in (secret, link, directory_link / 'reader.py'):
                with self.subTest(filename=str(filename)), patch(
                    'orquestacion.services.erp_system_map.inspect.getsourcefile', return_value=str(filename)
                ):
                    result = _source_ref(lambda: None, root)
                    self.assertIsNone(result['path'])
                    self.assertIsNone(result['sha256'])
                    self.assertTrue(result['external_or_unresolved'])
                tracked = filename.relative_to(root).as_posix().encode() + b'\0'
                with patch('orquestacion.services.erp_system_map.subprocess.run', return_value=subprocess.CompletedProcess([], 0, tracked)):
                    self.assertEqual(_source_inventory(root)['files'], [])
            public = root / 'public_reader.py'
            public.write_text('value = 1\n')
            with patch('orquestacion.services.erp_system_map.inspect.getsourcefile', return_value=str(public)):
                result = _source_ref(lambda: None, root)
            self.assertEqual(result['path'], 'public_reader.py')
            self.assertEqual(len(result['sha256']), 64)
            self.assertFalse(result['external_or_unresolved'])
