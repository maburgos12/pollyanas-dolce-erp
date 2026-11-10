from pathlib import Path
from tempfile import TemporaryDirectory
import sys
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).parents[1]))
from carga_point_alerts import deliver


class CargaPointAlertTests(unittest.TestCase):
    def test_reintenta_envio_fallido_y_deduplica_envio_aceptado(self):
        alerta = dict(id=41, ruta_id=12, folio="RUT-TEST", recarga_id=42, inicio="10/11 12:03",
                      sync_posterior=False)
        sender = Mock(side_effect=[OSError("offline"), 123])
        with TemporaryDirectory() as directory:
            path = Path(directory) / "delivery.json"
            with self.assertRaises(OSError):
                deliver([alerta], path, sender)
            self.assertFalse(path.exists())
            self.assertEqual(deliver([alerta], path, sender), {41: 123})
            self.assertEqual(deliver([alerta], path, sender), {})
            self.assertEqual(sender.call_count, 2)
            message = sender.call_args.args[0]
            for detail in ("RUT-TEST", "41", "Recarga CEDIS 42", "/logistica/rutas/12/"):
                self.assertIn(detail, message)
