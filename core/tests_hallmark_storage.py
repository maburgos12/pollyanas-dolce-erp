import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.test import SimpleTestCase

from core.hallmark_ui_audit import SKIP_DIR_PARTS, iter_ui_files


class HallmarkStorageTraversalTests(SimpleTestCase):
    def test_no_entra_a_datos_excluidos_y_conserva_archivos_de_ui(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            expected = [root / 'app/templates/app/pagina.html', root / 'static/css/app.css']
            for path in expected + [root / 'otro.html', root / 'app/codigo.py']:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            for name in SKIP_DIR_PARTS:
                for parent in (root, root / 'app'):
                    path = parent / name / 'remoto/templates/privado.html'
                    path.parent.mkdir(parents=True)
                    path.touch()
            original = os.scandir

            def scandir(path):
                self.assertFalse(set(Path(path).relative_to(root).parts) & SKIP_DIR_PARTS)
                return original(path)

            with patch('os.scandir', side_effect=scandir):
                self.assertEqual(iter_ui_files(root), sorted(expected))
