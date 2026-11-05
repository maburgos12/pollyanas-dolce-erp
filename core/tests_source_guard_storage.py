import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.core.checks import run_checks
from django.test import TestCase, override_settings

from core.checks import _iter_assignable_source_files
from core.hallmark_ui_audit import BASELINE_PATH


class SourceGuardStorageTests(TestCase):
    def test_checks_no_entran_al_nas_y_detectan_asignacion_inactiva(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / BASELINE_PATH).parent.mkdir()
            (root / BASELINE_PATH).write_text('{"accepted_issues": []}')
            expected = []
            for name in ('views.py', 'api_views.py', 'serializers.py', 'forms.py'):
                path = root / 'app' / name
                path.parent.mkdir(exist_ok=True)
                path.write_text('User.objects.all()')
                expected.append(path)
            for parent in (root, root / 'app'):
                private = parent / 'storage/nas/templates/views.py'
                private.parent.mkdir(parents=True)
                private.touch()
            original = os.scandir

            def scandir(path):
                path = Path(path)
                if path.is_relative_to(root):
                    self.assertNotIn('storage', path.relative_to(root).parts)
                return original(path)

            with override_settings(BASE_DIR=root), patch('os.scandir', side_effect=scandir):
                self.assertEqual(sorted(_iter_assignable_source_files(root)), sorted(expected))
                issues = run_checks()
            self.assertEqual(sum(issue.id == 'rrhh.E901' for issue in issues), 4)
