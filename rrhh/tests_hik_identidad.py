import json
from datetime import date, datetime
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory

from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase
from django.utils import timezone

from core.models import AuditLog

from .models import (
    AsistenciaEmpleado,
    Empleado,
    EmpleadoBaja,
    EventoHikCloud,
    HoraExtra,
)
from .services_extra_conciliacion import NOTA_EXTRA_AUTOMATICA


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


class ReconciliarIdentidadHikTests(TestCase):
    def setUp(self):
        self.destino = Empleado.objects.create(codigo="355", nombre="Johan", activo=True)
        self.origen = Empleado.objects.create(codigo="356", nombre="Angélica", activo=True)
        self._evento(
            event_id="historia-destino-355",
            codigo="355",
            empleado=self.destino,
            ocurrido_en=datetime(2026, 9, 15, 8, 0),
        )
        self._evento(
            event_id="historia-origen-356",
            codigo="356",
            empleado=self.origen,
            ocurrido_en=datetime(2026, 9, 10, 8, 0),
        )
        EmpleadoBaja.objects.create(
            empleado=self.origen,
            nombre=self.origen.nombre,
            fecha_ingreso=self.origen.fecha_ingreso,
            fecha_baja=date(2026, 9, 10),
        )
        Empleado.objects.filter(pk=self.origen.pk).update(codigo="HIK-TEMP-TEST")
        Empleado.objects.filter(pk=self.destino.pk).update(codigo="356")
        Empleado.objects.filter(pk=self.origen.pk).update(codigo="355")
        self.destino.refresh_from_db()
        self.origen.refresh_from_db()
        self.receipt = self._evento(
            event_id="marca-mal-asignada-355",
            codigo="355",
            empleado=self.origen,
            ocurrido_en=datetime(2026, 9, 17, 18, 30),
        )
        self.asistencia = AsistenciaEmpleado.objects.create(
            empleado=self.origen,
            fecha=date(2026, 9, 17),
            entrada=timezone.make_aware(datetime(2026, 9, 17, 8, 0)),
            salida=timezone.make_aware(datetime(2026, 9, 17, 18, 30)),
            fuente=AsistenciaEmpleado.FUENTE_HIKCONNECT_API,
        )
        self.extra = HoraExtra.objects.create(
            empleado=self.origen,
            asistencia=self.asistencia,
            fecha=self.asistencia.fecha,
            horas="0.50",
            notas=f"{NOTA_EXTRA_AUTOMATICA} Tiempo posterior a la salida programada.",
        )

    def _evento(self, *, event_id, codigo, empleado, ocurrido_en):
        return EventoHikCloud.objects.create(
            fuente="hikconnect_cloud",
            event_id=event_id,
            payload_hash=(event_id + "x" * 64)[:64],
            payload={"employee_external_id": codigo},
            codigo_externo=codigo,
            ocurrido_en=timezone.make_aware(ocurrido_en),
            tipo_evento="check_out",
            empleado=empleado,
            estado=EventoHikCloud.ESTADO_ACEPTADO,
            projection_status="applied",
            effects_status="completed",
        )

    def _command_args(self):
        return {
            "codigo_afectado": "355",
            "codigo_origen": "356",
            "empleado_origen_id": self.origen.id,
            "empleado_destino_id": self.destino.id,
            "desde": "2026-09-17",
            "hasta": "2026-09-23",
        }

    def test_dry_run_no_modifica_datos(self):
        output = StringIO()

        call_command("reconciliar_identidad_hik", stdout=output, **self._command_args())

        preview = json.loads(output.getvalue())
        self.assertEqual(preview["modo"], "dry-run")
        self.assertEqual(preview["conteos"]["recibos"], 1)
        self.assertEqual(preview["conteos"]["asistencias"], 1)
        self.origen.refresh_from_db()
        self.destino.refresh_from_db()
        self.assertEqual(self.origen.codigo, "355")
        self.assertEqual(self.destino.codigo, "356")
        self.assertEqual(EventoHikCloud.objects.get(pk=self.receipt.pk).empleado_id, self.origen.id)
        self.assertFalse(AuditLog.objects.filter(model="rrhh.IdentidadHik").exists())

    def test_apply_mueve_registros_con_respaldo_y_auditoria(self):
        output = StringIO()

        with TemporaryDirectory() as backup_dir:
            call_command(
                "reconciliar_identidad_hik",
                stdout=output,
                apply=True,
                backup_dir=backup_dir,
                **self._command_args(),
            )
            result = json.loads(output.getvalue())
            backup = Path(result["respaldo"])
            self.assertTrue(backup.exists())
            self.assertEqual(len(result["respaldo_sha256"]), 64)
            snapshot = json.loads(backup.read_text(encoding="utf-8"))
            self.assertEqual(snapshot["conteos"]["recibos"], 1)

        self.origen.refresh_from_db()
        self.destino.refresh_from_db()
        self.asistencia.refresh_from_db()
        self.extra.refresh_from_db()
        self.receipt.refresh_from_db()
        self.assertEqual(self.origen.codigo, "356")
        self.assertEqual(self.destino.codigo, "355")
        self.assertEqual(self.receipt.empleado_id, self.destino.id)
        self.assertEqual(self.asistencia.empleado_id, self.destino.id)
        self.assertEqual(self.extra.empleado_id, self.destino.id)
        audit = AuditLog.objects.get(model="rrhh.IdentidadHik")
        self.assertEqual(audit.payload["conteos"]["recibos"], 1)
        self.assertEqual(audit.payload["respaldo_sha256"], result["respaldo_sha256"])

    def test_asistencia_destino_en_misma_fecha_aborta(self):
        AsistenciaEmpleado.objects.create(
            empleado=self.destino,
            fecha=self.asistencia.fecha,
            fuente=AsistenciaEmpleado.FUENTE_HIKCONNECT_API,
        )

        with self.assertRaisesMessage(CommandError, "asistencia del destino"):
            call_command("reconciliar_identidad_hik", stdout=StringIO(), **self._command_args())

        self.assertEqual(EventoHikCloud.objects.get(pk=self.receipt.pk).empleado_id, self.origen.id)

    def test_extra_autorizada_aborta(self):
        self.extra.estado = HoraExtra.ESTADO_AUTORIZADO
        self.extra.save(update_fields=["estado"])

        with self.assertRaisesMessage(CommandError, "hora extra protegida"):
            call_command("reconciliar_identidad_hik", stdout=StringIO(), **self._command_args())

    def test_extra_con_ajuste_humano_aborta(self):
        self.extra.ajuste_autorizacion = {"motivo": "Corrección de supervisión"}
        self.extra.save(update_fields=["ajuste_autorizacion"])

        with self.assertRaisesMessage(CommandError, "hora extra protegida"):
            call_command("reconciliar_identidad_hik", stdout=StringIO(), **self._command_args())

    def test_asistencia_no_hik_aborta(self):
        self.asistencia.fuente = AsistenciaEmpleado.FUENTE_MANUAL
        self.asistencia.save(update_fields=["fuente"])

        with self.assertRaisesMessage(CommandError, "asistencia no proviene de Hik"):
            call_command("reconciliar_identidad_hik", stdout=StringIO(), **self._command_args())
