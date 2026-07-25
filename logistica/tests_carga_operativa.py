from io import StringIO
from importlib import import_module
from types import SimpleNamespace

from django.core.management import call_command
from django.db import migrations
from django.test import SimpleTestCase, TestCase
from django.utils import timezone

from .carga_operativa import ClasificacionLineaPoint, clasificar_linea_point


class MigracionCargaOperativaTests(SimpleTestCase):
    def test_confirma_limpieza_antes_de_crear_indice_parcial(self):
        migration_module = import_module(
            "logistica.migrations.0041_rutacarga_point_activa_unica"
        )

        self.assertFalse(migration_module.Migration.atomic)
        run_python = next(
            operation
            for operation in migration_module.Migration.operations
            if isinstance(operation, migrations.RunPython)
        )
        self.assertTrue(run_python.atomic)


class ClasificacionCargaOperativaTests(SimpleTestCase):
    def point_line(self, *, sent_at=None, is_enviado=None, requested="0", sent="0"):
        transfer = {}
        if is_enviado is not None:
            transfer["isEnviado"] = is_enviado
        return SimpleNamespace(
            sent_at=sent_at,
            requested_quantity=requested,
            sent_quantity=sent,
            raw_payload={"transfer": transfer},
        )

    def test_sent_at_es_evidencia_operativa_de_enviado(self):
        linea = self.point_line(sent_at=timezone.now(), is_enviado=False)

        self.assertEqual(
            clasificar_linea_point(linea),
            ClasificacionLineaPoint.ENVIADA,
        )

    def test_is_enviado_true_es_evidencia_operativa_aunque_el_total_sea_cero(self):
        linea = self.point_line(is_enviado=True, requested="7", sent="0")

        self.assertEqual(
            clasificar_linea_point(linea),
            ClasificacionLineaPoint.ENVIADA,
        )

    def test_cantidades_sin_transicion_enviado_son_solo_auditoria(self):
        linea = self.point_line(is_enviado=False, requested="7", sent="5")

        self.assertEqual(
            clasificar_linea_point(linea),
            ClasificacionLineaPoint.AUDITORIA_SOLICITUD,
        )


class LimpiarCargaOperativaCommandTests(TestCase):
    def test_por_defecto_solo_audita_y_exige_bandera_para_ejecutar(self):
        salida = StringIO()

        call_command("limpiar_carga_operativa", stdout=salida)

        self.assertIn("MODO: AUDITORIA", salida.getvalue())
        self.assertIn("No se modificaron datos", salida.getvalue())
