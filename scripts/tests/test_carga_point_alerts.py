from pathlib import Path
from tempfile import TemporaryDirectory
import json
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


    def test_reintentos_del_mismo_bloqueo_generan_una_sola_alerta_por_tramo(self):
        base = dict(ruta_id=12, folio="RUT-TEST", recarga_id=42, inicio="23/11 12:00",
                    sync_posterior=False, estado="SIN_CARGA", clave="12:42:2026-11-23:SIN_CARGA")
        sender = Mock(return_value=321)
        with TemporaryDirectory() as directory:
            path = Path(directory) / "delivery.json"
            self.assertEqual(deliver([{**base, "id": 41}, {**base, "id": 42}], path, sender), {41: 321})
            self.assertEqual(deliver([{**base, "id": 43}], path, sender), {})
            sender.assert_called_once()
            self.assertIn("sin carga disponible", sender.call_args.args[0])

    def test_recibo_previo_por_job_se_conserva_al_agrupar_por_tramo(self):
        alerta = dict(id=41, ruta_id=12, folio="RUT-TEST", recarga_id=None,
                      inicio="23/11 12:00", sync_posterior=False, estado="ERROR",
                      clave="12:0:2026-11-23:ERROR")
        sender = Mock()
        with TemporaryDirectory() as directory:
            path = Path(directory) / "delivery.json"
            path.write_text(json.dumps({"41": 321}))
            self.assertEqual(deliver([alerta], path, sender), {})
            self.assertEqual(json.loads(path.read_text()), {alerta["clave"]: 321})
            sender.assert_not_called()
