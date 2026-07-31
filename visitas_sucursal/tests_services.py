from datetime import date
from decimal import Decimal
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from core.models import AuditLog, Sucursal
from logistica.models import PuntoLogistico

from .models import ChecklistVisita, VisitaSucursal
from .services import (
    AuditoriaVisitaError,
    crear_borrador_extraordinario,
    ejecutar_auditoria,
    programaciones_pendientes,
)


class AuditoriaSucursalServiceTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user("auditor", password="pass")
        self.sucursal = Sucursal.objects.create(codigo="PAY", nombre="Payán", activa=True)
        self.punto = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Payán",
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=80,
        )

    def _visita(self, *, fecha_programada=date(2026, 8, 10), estatus=VisitaSucursal.ESTATUS_PROGRAMADA):
        visita = VisitaSucursal.objects.create(
            sucursal=self.sucursal,
            fecha_programada=fecha_programada,
            estatus=estatus,
            creado_por=self.user,
        )
        ChecklistVisita.objects.create(
            visita=visita,
            categoria="Orden y limpieza",
            titulo="Pisos",
            orden=1,
        )
        return visita

    def test_ejecuta_fuera_de_fecha_sin_mover_programacion(self):
        visita = self._visita()

        resultado = ejecutar_auditoria(
            visita_id=visita.id,
            sucursal=self.sucursal,
            user=self.user,
            latitud=Decimal("25.570000"),
            longitud=Decimal("-108.470000"),
            precision_m=Decimal("12.50"),
            respuestas={visita.checklist.get().id: (ChecklistVisita.RESPUESTA_SI, "Correcto")},
            observaciones="Visita ejecutada dos días después.",
            fecha_real=date(2026, 8, 12),
            realizada_en=timezone.now(),
        )

        resultado.refresh_from_db()
        self.assertEqual(resultado.fecha_programada, date(2026, 8, 10))
        self.assertEqual(resultado.fecha_real, date(2026, 8, 12))
        self.assertEqual(resultado.desviacion_dias, 2)
        self.assertEqual(resultado.estatus, VisitaSucursal.ESTATUS_REALIZADA)
        self.assertEqual(resultado.realizada_por, self.user)
        self.assertEqual(resultado.gps_distancia_sucursal_m, 0)
        self.assertEqual(resultado.gps_radio_geocerca_m, 80)
        self.assertTrue(resultado.gps_dentro_geocerca)
        self.assertTrue(
            AuditLog.objects.filter(
                action="visita_sucursal_realizada",
                model="VisitaSucursal",
                object_id=str(resultado.id),
            ).exists()
        )

    def test_precision_mayor_a_cien_no_cierra_ni_modifica_checklist(self):
        visita = self._visita()
        item = visita.checklist.get()

        with self.assertRaisesMessage(AuditoriaVisitaError, "precisión"):
            ejecutar_auditoria(
                visita_id=visita.id,
                sucursal=self.sucursal,
                user=self.user,
                latitud=Decimal("25.570000"),
                longitud=Decimal("-108.470000"),
                precision_m=Decimal("100.01"),
                respuestas={item.id: (ChecklistVisita.RESPUESTA_SI, "No debe persistir")},
            )

        visita.refresh_from_db()
        item.refresh_from_db()
        self.assertEqual(visita.estatus, VisitaSucursal.ESTATUS_PROGRAMADA)
        self.assertEqual(item.respuesta, ChecklistVisita.RESPUESTA_PENDIENTE)

    def test_ubicacion_fuera_de_geocerca_no_cierra(self):
        visita = self._visita()

        with self.assertRaisesMessage(AuditoriaVisitaError, "fuera de la geocerca"):
            ejecutar_auditoria(
                visita_id=visita.id,
                sucursal=self.sucursal,
                user=self.user,
                latitud=Decimal("25.580000"),
                longitud=Decimal("-108.470000"),
                precision_m=Decimal("20"),
                respuestas={},
            )

        visita.refresh_from_db()
        self.assertEqual(visita.estatus, VisitaSucursal.ESTATUS_PROGRAMADA)

    def test_visita_realizada_no_puede_ejecutarse_otra_vez(self):
        visita = self._visita(estatus=VisitaSucursal.ESTATUS_REALIZADA)

        with self.assertRaisesMessage(AuditoriaVisitaError, "ya no está pendiente"):
            ejecutar_auditoria(
                visita_id=visita.id,
                sucursal=self.sucursal,
                user=self.user,
                latitud=Decimal("25.570000"),
                longitud=Decimal("-108.470000"),
                precision_m=Decimal("20"),
                respuestas={},
            )

    def test_extraordinaria_es_idempotente_y_no_aparece_como_programacion(self):
        token = uuid4()

        primera = crear_borrador_extraordinario(
            user=self.user,
            sucursal=self.sucursal,
            motivo=VisitaSucursal.MOTIVO_QUEJA,
            detalle="Queja por atención.",
            clave_idempotencia=token,
        )
        segunda = crear_borrador_extraordinario(
            user=self.user,
            sucursal=self.sucursal,
            motivo=VisitaSucursal.MOTIVO_QUEJA,
            detalle="Queja por atención.",
            clave_idempotencia=token,
        )

        self.assertEqual(primera.id, segunda.id)
        self.assertEqual(primera.estatus, VisitaSucursal.ESTATUS_BORRADOR)
        self.assertIsNone(primera.fecha_programada)
        self.assertGreater(primera.checklist.count(), 10)
        self.assertNotIn(primera, list(programaciones_pendientes(self.sucursal)))

    def test_clave_idempotente_no_puede_reutilizarse_en_otra_sucursal(self):
        token = uuid4()
        otra = Sucursal.objects.create(codigo="CTR", nombre="Centro", activa=True)
        crear_borrador_extraordinario(
            user=self.user,
            sucursal=self.sucursal,
            motivo=VisitaSucursal.MOTIVO_QUEJA,
            detalle="Queja por atención.",
            clave_idempotencia=token,
        )

        with self.assertRaisesMessage(AuditoriaVisitaError, "no corresponde"):
            crear_borrador_extraordinario(
                user=self.user,
                sucursal=otra,
                motivo=VisitaSucursal.MOTIVO_OTRO,
                detalle="Otra causa.",
                clave_idempotencia=token,
            )
