from datetime import datetime

from django.core.exceptions import ValidationError
from django.test import TestCase
from django.utils import timezone

from .models import Empleado, EventoHikCloud


class ReservaCodigoHikTests(TestCase):
    def setUp(self):
        self.propietario = Empleado.objects.create(codigo="355", nombre="Propietario histórico")
        self.otro = Empleado.objects.create(codigo="356", nombre="Otra persona")
        EventoHikCloud.objects.create(
            fuente="hikconnect_cloud",
            event_id="historia-355",
            payload_hash="a" * 64,
            payload={"employee_external_id": "355"},
            codigo_externo="355",
            ocurrido_en=timezone.make_aware(datetime(2026, 9, 10, 8, 0)),
            tipo_evento="check_in",
            empleado=self.propietario,
            estado=EventoHikCloud.ESTADO_ACEPTADO,
            projection_status="applied",
        )

    def test_codigo_con_historial_de_otra_persona_no_se_reasigna(self):
        Empleado.objects.filter(pk=self.propietario.pk).update(codigo="357")
        self.otro.codigo = "355"

        with self.assertRaisesMessage(
            ValidationError,
            "Este código Hik conserva historial de otra persona",
        ):
            self.otro.save(update_fields=["codigo", "updated_at"])

        self.otro.refresh_from_db()
        self.assertEqual(self.otro.codigo, "356")

    def test_guardar_otro_campo_no_bloquea_colision_historica_existente(self):
        Empleado.objects.filter(pk=self.propietario.pk).update(codigo="357")
        Empleado.objects.filter(pk=self.otro.pk).update(codigo="355")
        self.otro.refresh_from_db()
        self.otro.nombre = "Nombre actualizado"

        self.otro.save(update_fields=["nombre", "nombre_normalizado", "updated_at"])

        self.otro.refresh_from_db()
        self.assertEqual(self.otro.nombre, "Nombre actualizado")
        self.assertEqual(self.otro.codigo, "355")

    def test_propietario_historico_puede_restaurar_su_codigo(self):
        Empleado.objects.filter(pk=self.propietario.pk).update(codigo="357")
        self.propietario.refresh_from_db()
        self.propietario.codigo = "355"

        self.propietario.save(update_fields=["codigo", "updated_at"])

        self.propietario.refresh_from_db()
        self.assertEqual(self.propietario.codigo, "355")

    def test_codigo_sin_historial_se_puede_asignar(self):
        self.otro.codigo = "999"

        self.otro.save(update_fields=["codigo", "updated_at"])

        self.otro.refresh_from_db()
        self.assertEqual(self.otro.codigo, "999")
