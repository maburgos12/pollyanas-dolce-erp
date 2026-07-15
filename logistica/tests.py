import json
import importlib
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time
from decimal import Decimal
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from django.conf import settings
from django.apps import apps as django_apps
from django.contrib.auth.models import Group, User
from django.contrib import admin
from django.core.exceptions import PermissionDenied, ValidationError
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.management import call_command
from django.db import IntegrityError, OperationalError, close_old_connections, transaction
from django.test import SimpleTestCase, TestCase, TransactionTestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from core.access import ACCESS_MANAGE, ACCESS_VIEW
from core.email_rendering import render_email_to_string
from core.models import Notificacion, Sucursal, UserModuleAccess
from crm.models import Cliente, PedidoCliente
from api.logistica_serializers import ParadaRutaSerializer, RutaCargaChecklistSerializer
from logistica.models import (
    BitacoraSalidaLlegada,
    CargaCombustibleUnidad,
    EntregaRuta,
    EventoRuta,
    ParadaEntregaEvidencia,
    ParadaRuta,
    PuntoLogistico,
    Repartidor,
    RutaCargaChecklist,
    RutaCargaChecklistLinea,
    RutaEntrega,
    UbicacionRuta,
    Unidad,
)
from logistica.services_combustible_auditoria import auditar_carga_combustible
from logistica.services_carga_ruta import (
    autorizar_diferencia_checklist_carga,
    cerrar_ruta_con_diferencia_autorizada,
    checklist_bloquea_salida,
    lineas_tramo_operativo_actual,
    marcar_lineas_checklist_superadas_historicas,
    obtener_checklist_carga_detallado,
    registrar_recarga_cedis,
    ruta_tiene_movimiento_point_nuevo,
    sincronizar_checklist_carga_desde_point,
    sincronizar_recepcion_desde_point,
    validar_linea_carga,
)
from logistica.services_google_roads import snap_gps_path_to_roads
from logistica.services_entregas import (
    EntregaIdempotenciaConflicto,
    confirmar_entrega_parada,
    revisar_entrega_excepcional,
)
from logistica.services_rutas_control import (
    distancia_metros,
    registrar_ubicacion_ruta,
    resumen_control_rutas,
    ruta_es_operativa_hoy,
)
from logistica.services_tiempos_ruta import resumen_tiempos_ruta
from logistica.tasks import _emails_de_grupo, detectar_gps_perdido_rutas
from api.logistica_views import _can_operate_pwa
from pos_bridge.models import PointBranch, PointSyncJob, PointTransferLine
from recetas.models import Receta, SolicitudReabastoCedis, SolicitudReabastoCedisLinea
from rentabilidad.models_rentabilidad import SucursalRentabilidad
from reportes.models import FactProduccionDiaria
from rrhh.models import Empleado


class LogisticaEntregaDomainTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="repartidor.entregas", password="pass123")
        self.user.groups.add(Group.objects.get_or_create(name="repartidor")[0])
        self.sucursal = Sucursal.objects.create(codigo="DOM-LOG", nombre="Dominio Logistica", activa=True)
        self.unidad = Unidad.objects.create(codigo="DOM-01", descripcion="Unidad dominio", sucursal=self.sucursal)
        self.repartidor = Repartidor.objects.create(user=self.user, sucursal=self.sucursal, unidad_asignada=self.unidad)
        self.ruta = RutaEntrega.objects.create(
            nombre="Ruta dominio entregas",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )
        self.punto = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Sucursal dominio",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )
        self.parada = ParadaRuta.objects.create(ruta=self.ruta, punto=self.punto, orden=1)
        self.jefe = User.objects.create_user(username="jefe.entregas", password="pass123")
        UserModuleAccess.objects.create(
            user=self.jefe,
            module="logistica.rutas",
            access=ACCESS_MANAGE,
            updated_by=self.jefe,
        )

    def _confirmar(self, **overrides):
        payload = {
            "ruta": self.ruta,
            "parada": self.parada,
            "actor": self.user,
            "entrega_estado": ParadaRuta.ENTREGA_ENTREGADA,
            "motivo": "GPS sin señal",
            "client_event_id": "entrega-excepcional-1",
            "ubicacion": {
                "causa": "GPS_SIN_SENAL",
                "client_timestamp": "2026-07-10T12:00:00-07:00",
                "client_version": "pwa-v60",
            },
            "origen": "PWA",
        }
        payload.update(overrides)
        return confirmar_entrega_parada(**payload)

    def _registrar_geocerca_real(self):
        BitacoraSalidaLlegada.objects.create(
            repartidor=self.repartidor,
            unidad=self.unidad,
            km_salida=1000,
            nivel_gas_salida="lleno",
            foto_tablero_salida=SimpleUploadedFile("tablero.gif", b"gif", content_type="image/gif"),
        )
        registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.570000",
                "longitud": "-108.470000",
                "precision_metros": "8.00",
                "timestamp_dispositivo": timezone.now(),
                "tracking_origen": "automatico_pwa",
            },
        )
        return EventoRuta.objects.get(
            ruta=self.ruta,
            parada=self.parada,
            tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            metadata__origen_servicio="registrar_ubicacion_ruta",
        )

    def test_sin_geocerca_registra_excepcion_sin_fabricar_visita(self):
        resultado = self._confirmar()

        self.parada.refresh_from_db()
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_ENTREGADA)
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertIsNone(self.parada.hora_llegada_real)
        self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_PENDIENTE)
        self.assertEqual(self.parada.revision_entrega_causa, "GPS_SIN_SENAL")
        self.assertTrue(resultado.requiere_revision)
        self.assertEqual(
            EventoRuta.objects.filter(parada=self.parada, tipo=EventoRuta.TIPO_ENTREGA_EXCEPCIONAL).count(),
            1,
        )

    def test_geocerca_real_no_requiere_revision(self):
        self._registrar_geocerca_real()

        resultado = self._confirmar(
            motivo="Entrega confirmada",
            client_event_id="entrega-geocerca-1",
            ubicacion={"causa": "DENTRO_GEOFENCE"},
        )

        self.parada.refresh_from_db()
        self.assertFalse(resultado.requiere_revision)
        self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_NO_REQUERIDA)
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertIsNone(self.parada.hora_llegada_real)
        self.assertEqual(EventoRuta.objects.filter(parada=self.parada, tipo=EventoRuta.TIPO_ENTREGA).count(), 1)

    def test_muestra_gps_confiable_conserva_llegada_legacy_y_crea_evento_nuevo(self):
        legacy = EventoRuta.objects.create(
            ruta=self.ruta,
            parada=self.parada,
            tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            severidad=EventoRuta.SEVERIDAD_OK,
            descripcion="Llegada legacy sin procedencia verificable.",
            latitud=self.parada.latitud_geocerca,
            longitud=self.parada.longitud_geocerca,
            distancia_metros=0,
            creado_por=self.user,
        )

        llegada = self._registrar_geocerca_real()
        resultado = self._confirmar(client_event_id="entrega-tras-llegada-legacy")

        self.parada.refresh_from_db()
        self.assertFalse(resultado.requiere_revision)
        self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_NO_REQUERIDA)
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertIsNone(self.parada.hora_llegada_real)
        self.assertEqual(
            EventoRuta.objects.filter(parada=self.parada, tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE).count(),
            2,
        )
        legacy.refresh_from_db()
        self.assertIsNone(legacy.ubicacion_id)
        self.assertEqual(llegada.metadata["origen_servicio"], "registrar_ubicacion_ruta")
        self.assertIsNotNone(llegada.ubicacion_id)

    def test_una_muestra_confiable_no_fabrica_permanencia_desde_eventos_legacy(self):
        llegada_antigua = EventoRuta.objects.create(
            ruta=self.ruta,
            parada=self.parada,
            tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            severidad=EventoRuta.SEVERIDAD_OK,
            descripcion="Llegada legacy antigua.",
            latitud=self.parada.latitud_geocerca,
            longitud=self.parada.longitud_geocerca,
            distancia_metros=0,
            creado_por=self.user,
        )
        EventoRuta.objects.filter(pk=llegada_antigua.pk).update(
            creado_en=timezone.now() - timezone.timedelta(minutes=10),
        )
        EventoRuta.objects.create(
            ruta=self.ruta,
            parada=self.parada,
            tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            severidad=EventoRuta.SEVERIDAD_OK,
            descripcion="Llegada legacy reciente.",
            latitud=self.parada.latitud_geocerca,
            longitud=self.parada.longitud_geocerca,
            distancia_metros=0,
            creado_por=self.user,
        )

        self._registrar_geocerca_real()

        self.parada.refresh_from_db()
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertIsNone(self.parada.hora_llegada_real)

    def test_retry_identico_es_idempotente(self):
        primero = self._confirmar()
        segundo = self._confirmar()

        self.assertEqual(primero.evento.id, segundo.evento.id)
        self.assertEqual(primero.evidencia.id, segundo.evidencia.id)
        self.assertEqual(
            EventoRuta.objects.filter(parada=self.parada, tipo=EventoRuta.TIPO_ENTREGA_EXCEPCIONAL).count(),
            1,
        )
        self.assertEqual(
            ParadaEntregaEvidencia.objects.filter(parada=self.parada, client_event_id="entrega-excepcional-1").count(),
            1,
        )

    def test_retry_identico_devuelve_original_aunque_ruta_ya_este_completada(self):
        primero = self._confirmar()
        self.ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
        self.ruta.save(update_fields=["estatus", "updated_at"])

        segundo = self._confirmar()

        self.assertTrue(segundo.idempotente)
        self.assertEqual(segundo.evento.id, primero.evento.id)
        self.assertEqual(segundo.evidencia.id, primero.evidencia.id)

    def test_evento_geocerca_fabricado_no_acredita_presencia(self):
        ubicacion = UbicacionRuta.objects.create(
            ruta=self.ruta,
            repartidor=self.repartidor,
            unidad=self.unidad,
            latitud="25.570000",
            longitud="-108.470000",
            precision_metros="8.00",
        )
        EventoRuta.objects.create(
            ruta=self.ruta,
            parada=self.parada,
            ubicacion=ubicacion,
            tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            severidad=EventoRuta.SEVERIDAD_OK,
            descripcion="Evento parecido construido fuera del servicio GPS.",
            latitud=ubicacion.latitud,
            longitud=ubicacion.longitud,
            distancia_metros=0,
            creado_por=self.user,
        )

        resultado = self._confirmar(client_event_id="geocerca-fabricada")

        self.parada.refresh_from_db()
        self.assertTrue(resultado.requiere_revision)
        self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_PENDIENTE)

    def test_retry_con_payload_distinto_es_conflicto(self):
        self._confirmar()

        with self.assertRaises(EntregaIdempotenciaConflicto):
            self._confirmar(entrega_estado=ParadaRuta.ENTREGA_NO_ENTREGADA)

        self.parada.refresh_from_db()
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_ENTREGADA)

    def test_point_no_es_actor_autorizado(self):
        point_user = User.objects.create_user(username="point.sync")

        with self.assertRaises(PermissionDenied):
            self._confirmar(actor=point_user, client_event_id="point-no-autorizado")

        self.parada.refresh_from_db()
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)

    def test_otro_repartidor_no_puede_confirmar(self):
        otro = User.objects.create_user(username="otro.repartidor")
        otro.groups.add(Group.objects.get_or_create(name="repartidor")[0])
        Repartidor.objects.create(user=otro, sucursal=self.sucursal)

        with self.assertRaises(PermissionDenied):
            self._confirmar(actor=otro, client_event_id="otro-repartidor")

    def test_cedis_no_admite_confirmacion_de_entrega(self):
        self.punto.tipo = PuntoLogistico.TIPO_CEDIS
        self.punto.save(update_fields=["tipo"])

        with self.assertRaises(ValidationError):
            self._confirmar(client_event_id="cedis-no-entrega")

    def test_autorizar_excepcion_no_fabrica_visita(self):
        self._confirmar()

        revisar_entrega_excepcional(
            parada=self.parada,
            actor=self.jefe,
            decision=ParadaRuta.REVISION_AUTORIZADA,
            motivo="Evidencia operativa suficiente",
        )

        self.parada.refresh_from_db()
        self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_AUTORIZADA)
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertIsNone(self.parada.hora_llegada_real)
        self.assertEqual(EventoRuta.objects.filter(parada=self.parada, tipo=EventoRuta.TIPO_ENTREGA_AUTORIZADA).count(), 1)

    def test_rechazar_excepcion_conserva_entrega_y_no_fabrica_visita(self):
        self._confirmar()

        revisar_entrega_excepcional(
            parada=self.parada,
            actor=self.jefe,
            decision=ParadaRuta.REVISION_RECHAZADA,
            motivo="Evidencia insuficiente",
        )

        self.parada.refresh_from_db()
        self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_RECHAZADA)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_ENTREGADA)
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertIsNone(self.parada.hora_llegada_real)
        self.assertEqual(EventoRuta.objects.filter(parada=self.parada, tipo=EventoRuta.TIPO_ENTREGA_RECHAZADA).count(), 1)

    def test_repartidor_no_puede_resolver_revision(self):
        self._confirmar()

        with self.assertRaises(PermissionDenied):
            revisar_entrega_excepcional(
                parada=self.parada,
                actor=self.user,
                decision=ParadaRuta.REVISION_AUTORIZADA,
                motivo="Yo mismo autorizo",
            )


class LogisticaAuditoriaEntregaTests(TestCase):
    def setUp(self):
        self.repartidor_user = User.objects.create_user(username="auditoria.repartidor", password="pass123")
        self.sucursal = Sucursal.objects.create(codigo="AUD-LOG", nombre="Auditoría Logística", activa=True)
        self.unidad = Unidad.objects.create(codigo="AUD-01", descripcion="Unidad auditoría", sucursal=self.sucursal)
        self.repartidor = Repartidor.objects.create(
            user=self.repartidor_user,
            sucursal=self.sucursal,
            unidad_asignada=self.unidad,
        )
        self.ruta = RutaEntrega.objects.create(
            nombre="Ruta auditoría entregas",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )
        self.punto = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Sucursal auditoría",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )

    def _parada(self, orden):
        return ParadaRuta.objects.create(ruta=self.ruta, punto=self.punto, orden=orden)

    def _evento(self, parada, tipo, *, metadata=None, creado_por=None):
        return EventoRuta.objects.create(
            ruta=self.ruta,
            parada=parada,
            tipo=tipo,
            severidad=EventoRuta.SEVERIDAD_OK,
            descripcion="Fixture válida antes de corrupción de auditoría.",
            metadata=metadata or {},
            creado_por=creado_por or self.repartidor_user,
        )

    def _geocerca_valida(self, parada):
        ubicacion = UbicacionRuta.objects.create(
            ruta=self.ruta,
            repartidor=self.repartidor,
            unidad=self.unidad,
            latitud=parada.latitud_geocerca,
            longitud=parada.longitud_geocerca,
            precision_metros="8.00",
            timestamp_dispositivo=timezone.now(),
        )
        return EventoRuta.objects.create(
            ruta=self.ruta,
            parada=parada,
            tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            severidad=EventoRuta.SEVERIDAD_OK,
            descripcion="Llegada GPS confiable antes de corrupción.",
            ubicacion=ubicacion,
            latitud=ubicacion.latitud,
            longitud=ubicacion.longitud,
            distancia_metros=0,
            metadata={
                "origen_servicio": "registrar_ubicacion_ruta",
                "ubicacion_confiable": True,
                "ruta_id": self.ruta.id,
                "repartidor_id": self.repartidor.id,
                "unidad_id": self.unidad.id,
            },
            creado_por=self.repartidor_user,
        )

    def test_detecta_reglas_idempotentes_sin_reescribir_hechos_operativos(self):
        from logistica.services_auditoria_entregas import auditar_entregas_ruta

        sin_geocerca = self._parada(1)
        visitada_sin_gps = self._parada(2)
        actor_indebido = self._parada(3)
        horas_admin = self._parada(4)
        geocerca_invalida = self._parada(5)
        confirmacion_duplicada = self._parada(6)
        revision_sin_alerta = self._parada(7)
        ahora = timezone.now()

        # La corrupción de auditoría evita servicios de dominio deliberadamente.
        ParadaRuta.objects.filter(pk=sin_geocerca.pk).update(
            entrega_estado=ParadaRuta.ENTREGA_ENTREGADA,
            revision_entrega_estado=ParadaRuta.REVISION_NO_REQUERIDA,
        )
        ParadaRuta.objects.filter(pk=visitada_sin_gps.pk).update(
            estado=ParadaRuta.ESTADO_VISITADA,
            hora_llegada_real=ahora,
        )
        usuario_sync = User.objects.create_user(username="point.sync.auditoria")
        self._evento(actor_indebido, EventoRuta.TIPO_ENTREGA)
        ParadaRuta.objects.filter(pk=actor_indebido.pk).update(
            entrega_estado=ParadaRuta.ENTREGA_ENTREGADA,
            entrega_confirmada_por=usuario_sync,
            entrega_confirmada_en=ahora,
            revision_entrega_estado=ParadaRuta.REVISION_AUTORIZADA,
        )
        self._evento(horas_admin, EventoRuta.TIPO_ENTREGA, creado_por=self.repartidor_user)
        self._evento(
            horas_admin,
            EventoRuta.TIPO_INCIDENCIA_MANUAL,
            metadata={
                "origen": "point_transfer",
                "campos_derivados": ["hora_llegada_real", "hora_salida_real"],
            },
        )
        ParadaRuta.objects.filter(pk=horas_admin.pk).update(
            hora_llegada_real=ahora,
            hora_salida_real=ahora + timezone.timedelta(minutes=5),
        )
        geocerca = self._geocerca_valida(geocerca_invalida)
        EventoRuta.objects.filter(pk=geocerca.pk).update(distancia_metros=9999)
        primera = self._evento(confirmacion_duplicada, EventoRuta.TIPO_ENTREGA)
        segunda = self._evento(revision_sin_alerta, EventoRuta.TIPO_ENTREGA_EXCEPCIONAL)
        EventoRuta.objects.filter(pk=segunda.pk).update(parada=confirmacion_duplicada)
        ParadaRuta.objects.filter(pk=revision_sin_alerta.pk).update(
            revision_entrega_estado=ParadaRuta.REVISION_PENDIENTE,
            revision_entrega_causa="GPS_SIN_SENAL",
        )

        hechos_antes = list(
            ParadaRuta.objects.filter(ruta=self.ruta).order_by("pk").values(
                "pk", "estado", "entrega_estado", "hora_llegada_real", "hora_salida_real",
                "revision_entrega_estado", "revision_entrega_revisada_por_id",
                "revision_entrega_revisada_en", "revision_entrega_resolucion",
            )
        )
        eventos_operativos_antes = set(EventoRuta.objects.values_list("pk", flat=True))

        primero = auditar_entregas_ruta(ruta_id=self.ruta.pk)
        segundo = auditar_entregas_ruta(ruta_id=self.ruta.pk)

        alertas = EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_INCONSISTENCIA_ENTREGA)
        reglas = set(alertas.values_list("metadata__regla", flat=True))
        self.assertEqual(
            reglas,
            {
                "ENTREGADA_SIN_GEOFENCE_O_REVISION",
                "VISITADA_SIN_GPS_CONFIABLE",
                "ENTREGA_ACTOR_INDEBIDO",
                "HORAS_DERIVADAS_FUENTE_ADMIN_POINT",
                "LLEGADA_GEOFENCE_INVALIDA",
                "CONFIRMACION_DUPLICADA_O_INCOMPATIBLE",
                "REVISION_PENDIENTE_SIN_ALERTA",
            },
        )
        self.assertEqual(alertas.count(), 7)
        self.assertEqual(primero["alertas_creadas"], 7)
        self.assertEqual(segundo["alertas_creadas"], 0)
        self.assertEqual(hechos_antes, list(ParadaRuta.objects.filter(ruta=self.ruta).order_by("pk").values(*hechos_antes[0].keys())))
        self.assertTrue(eventos_operativos_antes.issubset(set(EventoRuta.objects.values_list("pk", flat=True))))
        self.assertEqual(primera.parada_id, confirmacion_duplicada.pk)

    def test_dry_run_y_comando_diagnostico_no_crean_alertas(self):
        parada = self._parada(1)
        ParadaRuta.objects.filter(pk=parada.pk).update(
            entrega_estado=ParadaRuta.ENTREGA_ENTREGADA,
            revision_entrega_estado=ParadaRuta.REVISION_NO_REQUERIDA,
        )
        salida = StringIO()

        call_command("auditar_entregas_ruta", "--ruta-id", str(self.ruta.pk), stdout=salida)

        self.assertIn("ENTREGADA_SIN_GEOFENCE_O_REVISION", salida.getvalue())
        self.assertIn("dry-run", salida.getvalue().lower())
        self.assertFalse(EventoRuta.objects.filter(tipo=EventoRuta.TIPO_INCONSISTENCIA_ENTREGA).exists())

    def test_estados_finales_sin_geocerca_generan_alerta_determinista_y_resoluble(self):
        from logistica.services_auditoria_entregas import auditar_entregas_ruta
        from logistica.services_entregas import resolver_alerta_historica

        estados = [
            ParadaRuta.ENTREGA_ENTREGADA,
            ParadaRuta.ENTREGA_CON_DIFERENCIA,
            ParadaRuta.ENTREGA_NO_ENTREGADA,
        ]
        for orden, estado in enumerate(estados, start=1):
            parada = self._parada(orden)
            ParadaRuta.objects.filter(pk=parada.pk).update(
                entrega_estado=estado,
                revision_entrega_estado=ParadaRuta.REVISION_NO_REQUERIDA,
            )

        primero = auditar_entregas_ruta(ruta_id=self.ruta.pk)
        segundo = auditar_entregas_ruta(ruta_id=self.ruta.pk)
        alertas = list(
            EventoRuta.objects.filter(
                ruta=self.ruta,
                metadata__regla="ENTREGADA_SIN_GEOFENCE_O_REVISION",
            ).order_by("parada_id")
        )

        self.assertEqual(primero["alertas_creadas"], 3)
        self.assertEqual(segundo["alertas_creadas"], 0)
        self.assertEqual([evento.metadata["hecho"] for evento in alertas], estados)
        self.assertEqual(len({evento.clave_auditoria for evento in alertas}), 3)
        admin = User.objects.create_user(username="admin.auditoria.estados")
        UserModuleAccess.objects.create(
            user=admin, module="logistica.rutas", access=ACCESS_MANAGE, updated_by=admin,
        )
        for evento in alertas:
            resolver_alerta_historica(
                evento=evento,
                actor=admin,
                motivo="Hecho histórico revisado.",
            )
        self.assertFalse(
            EventoRuta.objects.filter(
                pk__in=[evento.pk for evento in alertas],
                revision_alerta_estado=EventoRuta.REVISION_ALERTA_PENDIENTE,
            ).exists()
        )

    def test_comando_exige_alcance_y_escritura_explicitamente(self):
        from django.core.management.base import CommandError

        parada = self._parada(1)
        ParadaRuta.objects.filter(pk=parada.pk).update(
            entrega_estado=ParadaRuta.ENTREGA_ENTREGADA,
            revision_entrega_estado=ParadaRuta.REVISION_NO_REQUERIDA,
        )

        with self.assertRaises(CommandError):
            call_command("auditar_entregas_ruta")
        call_command("auditar_entregas_ruta", "--ruta-id", self.ruta.pk)
        self.assertFalse(EventoRuta.objects.filter(clave_auditoria__isnull=False).exists())
        call_command("auditar_entregas_ruta", "--ruta-id", self.ruta.pk, "--crear-alertas")
        self.assertEqual(EventoRuta.objects.filter(clave_auditoria__isnull=False).count(), 1)

    def test_cedis_visitada_con_recarga_valida_no_exige_gps(self):
        from logistica.services_auditoria_entregas import auditar_entregas_ruta

        cedis = PuntoLogistico.objects.create(
            nombre="CEDIS auditoría",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.571000",
            longitud="-108.471000",
            radio_geocerca_metros=120,
        )
        parada = ParadaRuta.objects.create(ruta=self.ruta, punto=cedis, orden=1)
        ahora = timezone.now()
        ParadaRuta.objects.filter(pk=parada.pk).update(
            estado=ParadaRuta.ESTADO_VISITADA,
            hora_llegada_real=ahora,
            hora_salida_real=ahora,
        )
        self._evento(
            parada,
            EventoRuta.TIPO_RECARGA_CEDIS,
            metadata={"tipo": "recarga_cedis", "numero": 1},
        )

        resultado = auditar_entregas_ruta(ruta_id=self.ruta.pk, dry_run=True)

        self.assertEqual(resultado["hallazgos"], [])

    def test_cedis_visitada_sin_recarga_valida_si_se_reporta(self):
        from logistica.services_auditoria_entregas import auditar_entregas_ruta

        cedis = PuntoLogistico.objects.create(
            nombre="CEDIS inconsistente",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.572000",
            longitud="-108.472000",
            radio_geocerca_metros=120,
        )
        parada = ParadaRuta.objects.create(ruta=self.ruta, punto=cedis, orden=1)
        ParadaRuta.objects.filter(pk=parada.pk).update(estado=ParadaRuta.ESTADO_VISITADA)

        resultado = auditar_entregas_ruta(ruta_id=self.ruta.pk, dry_run=True)

        self.assertIn("VISITADA_SIN_GPS_CONFIABLE", {item["regla"] for item in resultado["hallazgos"]})

    def test_texto_libre_point_o_administracion_no_infiere_horas(self):
        from logistica.services_auditoria_entregas import auditar_entregas_ruta

        parada = self._parada(1)
        ahora = timezone.now()
        ParadaRuta.objects.filter(pk=parada.pk).update(
            hora_llegada_real=ahora,
            hora_salida_real=ahora,
            entrega_notas="Administración revisó Point; no es procedencia horaria.",
        )
        self._evento(
            parada,
            EventoRuta.TIPO_INCIDENCIA_MANUAL,
            metadata={"comentario": "sync de Point revisado por admin"},
        )

        resultado = auditar_entregas_ruta(ruta_id=self.ruta.pk, dry_run=True)

        self.assertNotIn(
            "HORAS_DERIVADAS_FUENTE_ADMIN_POINT",
            {item["regla"] for item in resultado["hallazgos"]},
        )

    def test_actor_se_valida_con_procedencia_inmutable_no_permisos_o_asignacion_actual(self):
        from logistica.services_auditoria_entregas import auditar_entregas_ruta

        parada = self._parada(1)
        evento = self._evento(
            parada,
            EventoRuta.TIPO_ENTREGA,
            metadata={"origen": "servicio_entregas", "entrega_estado": ParadaRuta.ENTREGA_ENTREGADA},
        )
        evidencia = ParadaEntregaEvidencia.objects.create(
            ruta=self.ruta,
            parada=parada,
            capturado_por=self.repartidor_user,
            client_event_id="actor-inmutable",
            metadata={"evento_id": evento.id, "origen": "servicio_entregas"},
        )
        ParadaRuta.objects.filter(pk=parada.pk).update(
            entrega_estado=ParadaRuta.ENTREGA_ENTREGADA,
            entrega_confirmada_por=self.repartidor_user,
            entrega_confirmada_en=timezone.now(),
            revision_entrega_estado=ParadaRuta.REVISION_AUTORIZADA,
        )
        nuevo = User.objects.create_user(username="repartidor.nuevo.auditoria")
        nuevo_repartidor = Repartidor.objects.create(user=nuevo, sucursal=self.sucursal)
        RutaEntrega.objects.filter(pk=self.ruta.pk).update(repartidor=nuevo_repartidor)
        self.repartidor_user.is_active = False
        self.repartidor_user.save(update_fields=["is_active"])

        resultado = auditar_entregas_ruta(ruta_id=self.ruta.pk, dry_run=True)

        self.assertNotIn("ENTREGA_ACTOR_INDEBIDO", {item["regla"] for item in resultado["hallazgos"]})
        self.assertEqual(evidencia.capturado_por_id, self.repartidor_user.id)

    def test_auditoria_dry_run_tiene_presupuesto_constante_de_queries(self):
        from logistica.services_auditoria_entregas import auditar_entregas_ruta

        for numero in range(3):
            ruta = RutaEntrega.objects.create(
                nombre=f"Ruta query budget {numero}",
                fecha_ruta=timezone.localdate(),
                estatus=RutaEntrega.ESTATUS_PLANEADA,
                repartidor=self.repartidor,
                unidad_operativa=self.unidad,
            )
            for orden in range(1, 5):
                ParadaRuta.objects.create(ruta=ruta, punto=self.punto, orden=orden)

        with self.assertNumQueries(4):
            auditar_entregas_ruta(fecha=timezone.localdate(), dry_run=True)

    def test_adopta_clave_legacy_duplicada_deterministicamente_sin_tercera_alerta(self):
        from logistica.services_auditoria_entregas import auditar_entregas_ruta

        parada = self._parada(1)
        ParadaRuta.objects.filter(pk=parada.pk).update(
            entrega_estado=ParadaRuta.ENTREGA_ENTREGADA,
            revision_entrega_estado=ParadaRuta.REVISION_NO_REQUERIDA,
        )
        clave = f"ENTREGADA_SIN_GEOFENCE_O_REVISION:{self.ruta.id}:{parada.id}:ENTREGADA"
        primero = self._evento(
            parada,
            EventoRuta.TIPO_INCONSISTENCIA_ENTREGA,
            metadata={"clave": clave, "regla": "ENTREGADA_SIN_GEOFENCE_O_REVISION"},
        )
        segundo = self._evento(
            parada,
            EventoRuta.TIPO_INCONSISTENCIA_ENTREGA,
            metadata={"clave": clave, "regla": "ENTREGADA_SIN_GEOFENCE_O_REVISION"},
        )

        resultado = auditar_entregas_ruta(ruta_id=self.ruta.pk)

        primero.refresh_from_db()
        segundo.refresh_from_db()
        self.assertEqual(resultado["alertas_creadas"], 0)
        self.assertLessEqual(len(primero.clave_auditoria), 255)
        self.assertRegex(
            primero.clave_auditoria,
            rf"^ENTREGADA_SIN_GEOFENCE_O_REVISION:{self.ruta.id}:{parada.id}:[0-9a-f]{{64}}$",
        )
        self.assertIsNone(segundo.clave_auditoria)
        self.assertEqual(EventoRuta.objects.filter(parada=parada).count(), 2)

    def test_confirmaciones_masivas_producen_clave_acotada_y_una_alerta(self):
        from logistica.services_auditoria_entregas import auditar_entregas_ruta

        parada = self._parada(1)
        for numero in range(180):
            self._evento(
                parada,
                EventoRuta.TIPO_ENTREGA if numero % 2 == 0 else EventoRuta.TIPO_ENTREGA_EXCEPCIONAL,
            )

        primero = auditar_entregas_ruta(ruta_id=self.ruta.pk)
        segundo = auditar_entregas_ruta(ruta_id=self.ruta.pk)

        alerta = EventoRuta.objects.get(
            parada=parada,
            metadata__regla="CONFIRMACION_DUPLICADA_O_INCOMPATIBLE",
        )
        self.assertLessEqual(len(alerta.clave_auditoria), 255)
        self.assertRegex(
            alerta.clave_auditoria,
            rf"^CONFIRMACION_DUPLICADA_O_INCOMPATIBLE:{self.ruta.id}:{parada.id}:[0-9a-f]{{64}}$",
        )
        self.assertEqual(primero["alertas_creadas"], 1)
        self.assertEqual(segundo["alertas_creadas"], 0)

    def test_backfill_normaliza_clave_legacy_larga_y_adopta_duplicado_mas_antiguo(self):
        parada = self._parada(1)
        hecho = "eventos-" + "-".join(str(numero) for numero in range(500))
        metadata = {
            "regla": "CONFIRMACION_DUPLICADA_O_INCOMPATIBLE",
            "ruta_id": self.ruta.id,
            "parada_id": parada.id,
            "hecho": hecho,
            "clave": f"CONFIRMACION_DUPLICADA_O_INCOMPATIBLE:{self.ruta.id}:{parada.id}:{hecho}",
        }
        primero = self._evento(parada, EventoRuta.TIPO_INCONSISTENCIA_ENTREGA, metadata=metadata)
        segundo = self._evento(parada, EventoRuta.TIPO_INCONSISTENCIA_ENTREGA, metadata=metadata)
        migration = importlib.import_module("logistica.migrations.0034_eventoruta_clave_auditoria")

        migration.adoptar_claves_legacy(django_apps, None)

        primero.refresh_from_db()
        segundo.refresh_from_db()
        self.assertLessEqual(len(primero.clave_auditoria), 255)
        self.assertRegex(
            primero.clave_auditoria,
            rf"^CONFIRMACION_DUPLICADA_O_INCOMPATIBLE:{self.ruta.id}:{parada.id}:[0-9a-f]{{64}}$",
        )
        self.assertIsNone(segundo.clave_auditoria)

    def test_tarea_celery_ejecuta_auditoria_segura(self):
        from logistica.tasks import auditar_entregas_ruta_task

        parada = self._parada(1)
        ParadaRuta.objects.filter(pk=parada.pk).update(
            revision_entrega_estado=ParadaRuta.REVISION_PENDIENTE,
            revision_entrega_causa="GPS_SIN_SENAL",
        )

        resultado = auditar_entregas_ruta_task(ruta_id=self.ruta.pk)

        parada.refresh_from_db()
        self.assertEqual(resultado["alertas_creadas"], 1)
        self.assertEqual(parada.revision_entrega_estado, ParadaRuta.REVISION_PENDIENTE)

    def test_tarea_valida_fecha_y_configura_reintentos_de_bd(self):
        from logistica.tasks import auditar_entregas_ruta_task

        with self.assertRaises(ValueError):
            auditar_entregas_ruta_task(ruta_id=self.ruta.pk, fecha="fecha-invalida")
        self.assertEqual(auditar_entregas_ruta_task.max_retries, 3)

    def test_periodicidad_es_opt_in_y_permanece_apagada_por_defecto(self):
        self.assertFalse(settings.LOGISTICA_AUDITORIA_ENTREGAS_BEAT_ENABLED)
        self.assertNotIn("logistica-auditar-entregas-ruta", settings.CELERY_BEAT_SCHEDULE)


class LogisticaAuditoriaEntregaConcurrencyTests(TransactionTestCase):
    def setUp(self):
        user = User.objects.create_user(username="auditoria.concurrente")
        sucursal = Sucursal.objects.create(codigo="AUD-CON", nombre="Auditoría concurrente", activa=True)
        unidad = Unidad.objects.create(codigo="AUD-CON", descripcion="Unidad concurrente", sucursal=sucursal)
        repartidor = Repartidor.objects.create(user=user, sucursal=sucursal, unidad_asignada=unidad)
        self.ruta = RutaEntrega.objects.create(
            nombre="Ruta auditoría concurrente",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
            repartidor=repartidor,
            unidad_operativa=unidad,
        )
        punto = PuntoLogistico.objects.create(
            sucursal=sucursal,
            nombre="Punto concurrente",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )
        parada = ParadaRuta.objects.create(ruta=self.ruta, punto=punto, orden=1)
        ParadaRuta.objects.filter(pk=parada.pk).update(
            entrega_estado=ParadaRuta.ENTREGA_ENTREGADA,
            revision_entrega_estado=ParadaRuta.REVISION_NO_REQUERIDA,
        )

    def test_dos_auditores_concurrentes_crean_una_sola_alerta(self):
        from logistica.services_auditoria_entregas import auditar_entregas_ruta

        def ejecutar():
            close_old_connections()
            try:
                return auditar_entregas_ruta(ruta_id=self.ruta.pk)["alertas_creadas"]
            finally:
                close_old_connections()

        with ThreadPoolExecutor(max_workers=2) as pool:
            creadas = list(pool.map(lambda _: ejecutar(), range(2)))

        self.assertEqual(sum(creadas), 1)
        self.assertEqual(EventoRuta.objects.exclude(clave_auditoria__isnull=True).count(), 1)


class LogisticaEntregaApiStabilizationTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="repartidor.api.entregas", password="pass123")
        self.user.groups.add(Group.objects.get_or_create(name="repartidor")[0])
        self.sucursal = Sucursal.objects.create(codigo="API-LOG", nombre="API Logistica", activa=True)
        self.unidad = Unidad.objects.create(codigo="API-01", descripcion="Unidad API", sucursal=self.sucursal)
        self.repartidor = Repartidor.objects.create(user=self.user, sucursal=self.sucursal, unidad_asignada=self.unidad)
        self.ruta = RutaEntrega.objects.create(
            nombre="Ruta API entregas",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )
        self.punto = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Sucursal API",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )
        self.parada = ParadaRuta.objects.create(ruta=self.ruta, punto=self.punto, orden=1)
        self.url = reverse(
            "api_logistica_ruta_parada_entrega",
            kwargs={"ruta_id": self.ruta.id, "parada_id": self.parada.id},
        )
        self.client.force_login(self.user)

    def _payload(self, **overrides):
        payload = {
            "entrega_estado": ParadaRuta.ENTREGA_ENTREGADA,
            "notas": "GPS sin señal; entrega recibida.",
            "client_event_id": "api-entrega-excepcional-1",
            "client_context": {
                "causa": "GPS_SIN_SENAL",
                "client_timestamp": timezone.now().isoformat(),
                "client_version": "pwa-v60",
            },
            "evidencias": [{"tipo": "CONFIRMACION", "comentario": "Entrega completa."}],
        }
        payload.update(overrides)
        return payload

    def _registrar_geocerca_real(self):
        BitacoraSalidaLlegada.objects.create(
            repartidor=self.repartidor,
            unidad=self.unidad,
            km_salida=1000,
            nivel_gas_salida="lleno",
            foto_tablero_salida=SimpleUploadedFile("tablero.gif", b"gif", content_type="image/gif"),
        )
        registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.570000",
                "longitud": "-108.470000",
                "precision_metros": "8.00",
                "timestamp_dispositivo": timezone.now(),
                "tracking_origen": "automatico_pwa",
            },
        )

    def test_sin_geocerca_responde_200_registra_revision_y_warning_sin_visita(self):
        response = self.client.post(self.url, json.dumps(self._payload()), content_type="application/json")

        self.parada.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["requiere_revision"])
        self.assertIn("revisada por tu jefe", response.json()["warning"])
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertIsNone(self.parada.hora_llegada_real)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_ENTREGADA)
        self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_PENDIENTE)
        self.assertEqual(EventoRuta.objects.filter(parada=self.parada, tipo=EventoRuta.TIPO_ENTREGA_EXCEPCIONAL).count(), 1)
        self.assertFalse(EventoRuta.objects.filter(parada=self.parada, tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE).exists())

    def test_usuario_dual_jefe_repartidor_conserva_origen_pwa_en_endpoint(self):
        UserModuleAccess.objects.create(
            user=self.user, module="logistica.rutas", access=ACCESS_MANAGE, updated_by=self.user,
        )
        response = self.client.post(
            self.url,
            json.dumps(self._payload(client_event_id="dual-role-pwa-1")),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        evento = EventoRuta.objects.get(parada=self.parada, tipo=EventoRuta.TIPO_ENTREGA_EXCEPCIONAL)
        self.assertEqual(evento.metadata["origen_confirmacion"], "PWA")
        self.parada.refresh_from_db()
        self.assertEqual(self.parada.revision_entrega_causa, "GPS_SIN_SENAL")

    def test_con_geocerca_confiable_confirma_normal_sin_revision(self):
        self._registrar_geocerca_real()

        response = self.client.post(
            self.url,
            json.dumps(self._payload(client_event_id="api-entrega-geocerca-1", client_context={})),
            content_type="application/json",
        )

        self.parada.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["requiere_revision"])
        self.assertEqual(response.json()["warning"], "")
        self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_NO_REQUERIDA)
        self.assertEqual(EventoRuta.objects.filter(parada=self.parada, tipo=EventoRuta.TIPO_ENTREGA).count(), 1)

    def test_excepcion_exige_motivo_y_rechaza_contexto_vacio_sin_contrato_legacy(self):
        sin_motivo = self.client.post(self.url, json.dumps(self._payload(notas="")), content_type="application/json")
        sin_contexto = self.client.post(
            self.url,
            json.dumps(self._payload(client_event_id="api-sin-contexto", client_context={})),
            content_type="application/json",
        )

        self.assertEqual(sin_motivo.status_code, 400)
        self.assertEqual(sin_contexto.status_code, 400)

    def test_confirmacion_exige_client_event_id(self):
        response = self.client.post(
            self.url,
            json.dumps(self._payload(client_event_id="")),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("client_event_id", response.json())

    @override_settings(LOGISTICA_PWA_V59_COMPAT_UNTIL="2099-07-17T23:59:59-07:00")
    def test_replay_v59_dentro_de_ventana_acepta_tres_estados_y_es_idempotente(self):
        casos = [
            ParadaRuta.ENTREGA_ENTREGADA,
            ParadaRuta.ENTREGA_CON_DIFERENCIA,
            ParadaRuta.ENTREGA_NO_ENTREGADA,
        ]
        for index, entrega_estado in enumerate(casos, start=1):
            with self.subTest(entrega_estado=entrega_estado):
                parada = self.parada if index == 1 else ParadaRuta.objects.create(
                    ruta=self.ruta, punto=self.punto, orden=index,
                )
                url = reverse(
                    "api_logistica_ruta_parada_entrega",
                    kwargs={"ruta_id": self.ruta.id, "parada_id": parada.id},
                )
                queue_id = f"legacy-queue-{index}"
                payload = {
                    "entrega_estado": entrega_estado,
                    "notas": "Confirmación recuperada de cola offline v59.",
                    "client_event_id": f"offline-v59-{queue_id}",
                    "client_context": {
                        "causa": "GPS_SIN_SENAL",
                        "client_timestamp": timezone.now().isoformat(),
                        "client_version": "pwa-v59-offline",
                    },
                    "evidencias": [],
                }
                headers = {"HTTP_X_LOGISTICA_OFFLINE_QUEUE_ID": queue_id}
                primero = self.client.post(url, json.dumps(payload), content_type="application/json", **headers)
                retry = self.client.post(url, json.dumps(payload), content_type="application/json", **headers)

                self.assertEqual(primero.status_code, 200, primero.content)
                self.assertEqual(retry.status_code, 200, retry.content)
                self.assertEqual(retry.json(), primero.json())
                parada.refresh_from_db()
                self.assertEqual(parada.entrega_estado, entrega_estado)
                self.assertEqual(parada.revision_entrega_estado, ParadaRuta.REVISION_PENDIENTE)
                self.assertEqual(parada.revision_entrega_causa, "CLIENTE_LEGACY")
                self.assertEqual(
                    ParadaEntregaEvidencia.objects.filter(
                        parada=parada, client_event_id=f"offline-v59-{queue_id}"
                    ).count(),
                    1,
                )

    @override_settings(LOGISTICA_PWA_V59_COMPAT_UNTIL="2099-07-17T23:59:59-07:00")
    def test_replay_v59_posterior_a_resolucion_no_sobrescribe_revision(self):
        queue_id = "legacy-queue-resuelto"
        payload = {
            "entrega_estado": ParadaRuta.ENTREGA_NO_ENTREGADA,
            "notas": "Sucursal cerrada; cola offline v59.",
            "client_event_id": f"offline-v59-{queue_id}",
            "client_context": {
                "causa": "GPS_SIN_SENAL",
                "client_timestamp": timezone.now().isoformat(),
                "client_version": "pwa-v59-offline",
            },
            "evidencias": [],
        }
        headers = {"HTTP_X_LOGISTICA_OFFLINE_QUEUE_ID": queue_id}
        primero = self.client.post(self.url, json.dumps(payload), content_type="application/json", **headers)
        jefe = User.objects.create_user(username="jefe.v59.resuelto")
        UserModuleAccess.objects.create(
            user=jefe, module="logistica.rutas", access=ACCESS_MANAGE, updated_by=jefe,
        )
        revisar_entrega_excepcional(
            parada=self.parada,
            actor=jefe,
            decision=ParadaRuta.REVISION_AUTORIZADA,
            motivo="Incidencia validada por teléfono.",
        )

        retry = self.client.post(self.url, json.dumps(payload), content_type="application/json", **headers)

        self.assertEqual(retry.status_code, 200)
        self.assertEqual(retry.json(), primero.json())
        self.parada.refresh_from_db()
        self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_AUTORIZADA)

    @override_settings(LOGISTICA_PWA_V59_COMPAT_UNTIL="2000-01-01T00:00:00-07:00")
    def test_replay_v59_vencido_rechaza_evidencia_incompleta(self):
        queue_id = "legacy-expirado"
        payload = {
            "entrega_estado": ParadaRuta.ENTREGA_ENTREGADA,
            "notas": "Cola offline v59 expirada.",
            "client_event_id": f"offline-v59-{queue_id}",
            "client_context": {
                "causa": "GPS_SIN_SENAL",
                "client_timestamp": timezone.now().isoformat(),
                "client_version": "pwa-v59-offline",
            },
            "evidencias": [{"tipo": "CONFIRMACION", "comentario": "Evidencia que no extiende la ventana."}],
        }
        response = self.client.post(
            self.url, json.dumps(payload), content_type="application/json",
            HTTP_X_LOGISTICA_OFFLINE_QUEUE_ID=queue_id,
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)

    @override_settings(LOGISTICA_PWA_V59_COMPAT_UNTIL="2099-07-17T23:59:59-07:00")
    def test_replay_v59_rechaza_spoof_de_header_id_version_o_causa(self):
        casos = [
            ("queue-real", "offline-v59-queue-distinto", "pwa-v59-offline", "GPS_SIN_SENAL"),
            ("queue-real", "offline-v59-queue-real", "pwa-v60", "GPS_SIN_SENAL"),
            ("queue-real", "offline-v59-queue-real", "pwa-v59-offline", "FUERA_DE_RADIO"),
            ("queue con espacios", "offline-v59-queue con espacios", "pwa-v59-offline", "GPS_SIN_SENAL"),
        ]
        for index, (header, event_id, version, causa) in enumerate(casos):
            with self.subTest(index=index):
                payload = {
                    "entrega_estado": ParadaRuta.ENTREGA_ENTREGADA,
                    "notas": "Intento de replay.",
                    "client_event_id": event_id,
                    "client_context": {
                        "causa": causa,
                        "client_timestamp": timezone.now().isoformat(),
                        "client_version": version,
                    },
                    "evidencias": [{"tipo": "CONFIRMACION", "comentario": "Evidencia spoof."}],
                }
                response = self.client.post(
                    self.url, json.dumps(payload), content_type="application/json",
                    HTTP_X_LOGISTICA_OFFLINE_QUEUE_ID=header,
                )
                self.assertEqual(response.status_code, 400, response.content)

    @override_settings(LOGISTICA_PWA_V59_COMPAT_UNTIL="2099-07-17T23:59:59-07:00")
    def test_replay_v59_con_geocerca_siempre_queda_cliente_legacy_pendiente(self):
        self._registrar_geocerca_real()
        queue_id = "legacy-con-geocerca"
        payload = {
            "entrega_estado": ParadaRuta.ENTREGA_ENTREGADA,
            "notas": "Replay offline anterior.",
            "client_event_id": f"offline-v59-{queue_id}",
            "client_context": {
                "causa": "GPS_SIN_SENAL",
                "client_timestamp": timezone.now().isoformat(),
                "client_version": "pwa-v59-offline",
            },
            "evidencias": [],
        }
        response = self.client.post(
            self.url, json.dumps(payload), content_type="application/json",
            HTTP_X_LOGISTICA_OFFLINE_QUEUE_ID=queue_id,
        )

        self.assertEqual(response.status_code, 200, response.content)
        self.parada.refresh_from_db()
        self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_PENDIENTE)
        self.assertEqual(self.parada.revision_entrega_causa, "CLIENTE_LEGACY")

    @override_settings(LOGISTICA_PWA_V59_COMPAT_UNTIL="fecha-invalida")
    def test_check_reporta_configuracion_v59_malformada(self):
        from logistica.checks import logistica_v59_compat_window

        self.assertEqual([item.id for item in logistica_v59_compat_window(None)], ["logistica.E911"])

    @override_settings(LOGISTICA_PWA_V59_COMPAT_UNTIL="2000-01-01")
    def test_check_advierte_ventana_v59_vencida(self):
        from logistica.checks import logistica_v59_compat_window

        self.assertEqual([item.id for item in logistica_v59_compat_window(None)], ["logistica.W911"])

    @override_settings(LOGISTICA_PWA_V59_COMPAT_UNTIL="")
    def test_ventana_v59_puede_deshabilitarse_inmediatamente(self):
        from logistica.checks import logistica_v59_compat_window

        self.assertEqual(logistica_v59_compat_window(None), [])

    @override_settings(LOGISTICA_PWA_V59_COMPAT_UNTIL="fecha-invalida")
    def test_configuracion_v59_malformada_falla_cerrado_en_api(self):
        queue_id = "legacy-config-invalida"
        payload = {
            "entrega_estado": ParadaRuta.ENTREGA_ENTREGADA,
            "notas": "Replay con configuración inválida.",
            "client_event_id": f"offline-v59-{queue_id}",
            "client_context": {
                "causa": "GPS_SIN_SENAL",
                "client_timestamp": timezone.now().isoformat(),
                "client_version": "pwa-v59-offline",
            },
            "evidencias": [],
        }
        response = self.client.post(
            self.url, json.dumps(payload), content_type="application/json",
            HTTP_X_LOGISTICA_OFFLINE_QUEUE_ID=queue_id,
        )

        self.assertEqual(response.status_code, 400)

    def test_helper_js_reproduce_cola_v59_real_y_no_toca_payload_v60(self):
        helper = Path(__file__).resolve().parent / "static" / "logistica" / "pwa" / "offline_queue_compat.js"
        script = r'''
const fs = require("fs");
const vm = require("vm");
const context = { console };
vm.createContext(context);
vm.runInContext(fs.readFileSync(process.argv[1], "utf8"), context);
const prepare = context.PDLogisticaOfflineQueue.prepareReplay;
const states = ["ENTREGADA", "CON_DIFERENCIA", "NO_ENTREGADA"];
for (const [index, state] of states.entries()) {
  const item = {
    id: `queue-${index}`,
    path: "/rutas/1/paradas/2/entrega/",
    queued_at: "2026-07-10T12:00:00.000Z",
    headers: {"Content-Type": "application/json"},
    body: {kind: "text", value: JSON.stringify({entrega_estado: state, evidencias: []})}
  };
  const first = prepare(item);
  const second = prepare(item);
  if (JSON.stringify(first) !== JSON.stringify(second)) throw new Error("replay no determinista");
  const payload = JSON.parse(first.body.value);
  if (payload.client_event_id !== `offline-v59-queue-${index}`) throw new Error("id incorrecto");
  if (payload.client_context.client_version !== "pwa-v59-offline") throw new Error("contexto incorrecto");
  if (first.headers["X-Logistica-Offline-Queue-Id"] !== `queue-${index}`) throw new Error("header incorrecto");
}
const v60 = {id:"q-new", path:"/rutas/1/paradas/2/entrega/", queued_at:"2026-07-10T12:00:00Z", headers:{}, body:{kind:"text", value:JSON.stringify({client_event_id:"v60-id", client_context:{client_version:"pwa-v60"}, evidencias:[]})}};
if (JSON.stringify(prepare(v60)) !== JSON.stringify(v60)) throw new Error("payload v60 modificado");
'''
        resultado = subprocess.run(
            ["node", "-e", script, str(helper)], capture_output=True, text=True, check=False,
        )
        self.assertEqual(resultado.returncode, 0, resultado.stderr)

    def test_client_context_rechaza_tipos_claves_y_tamano_no_aprobados(self):
        invalidos = [
            [],
            "GPS sin señal",
            123,
            {"causa": "GPS_SIN_SENAL", "correo_cliente": "persona@example.com"},
            {"causa": "X" * 201},
        ]
        for index, contexto in enumerate(invalidos):
            with self.subTest(contexto=contexto):
                response = self.client.post(
                    self.url,
                    json.dumps(self._payload(client_event_id=f"contexto-invalido-{index}", client_context=contexto)),
                    content_type="application/json",
                )
                self.assertEqual(response.status_code, 400)

    def test_client_context_rechaza_limites_numericos_y_timestamp_invalido(self):
        invalidos = [
            {"causa": "FUERA_DE_RADIO", "latitud": "999999999999999999999"},
            {"causa": "FUERA_DE_RADIO", "longitud": "181"},
            {"causa": "GPS_SIN_SENAL", "precision_metros": "-1"},
            {"causa": "FUERA_DE_RADIO", "distancia_metros": -1},
            {"causa": "GPS_SIN_SENAL", "client_timestamp": "ayer"},
            {"causa": "CORREO_CLIENTE"},
        ]
        for index, contexto in enumerate(invalidos):
            contexto.setdefault("client_timestamp", timezone.now().isoformat())
            contexto.setdefault("client_version", "pwa-v60")
            response = self.client.post(
                self.url,
                json.dumps(self._payload(client_event_id=f"limite-contexto-{index}", client_context=contexto)),
                content_type="application/json",
            )
            self.assertEqual(response.status_code, 400, contexto)

    def test_integrity_error_no_relacionado_no_se_reporta_como_colision_idempotente(self):
        with patch("api.logistica_views.confirmar_entrega_parada", side_effect=IntegrityError("otra restricción")):
            with self.assertRaises(IntegrityError):
                self.client.post(self.url, json.dumps(self._payload()), content_type="application/json")

    def test_colision_client_event_id_secundario_devuelve_409_y_revierte(self):
        ParadaEntregaEvidencia.objects.create(
            ruta=self.ruta,
            parada=self.parada,
            capturado_por=self.user,
            client_event_id="secundario-ocupado",
            comentario="Evento anterior",
        )
        payload = self._payload(
            client_event_id="confirmacion-nueva",
            evidencias=[
                {"comentario": "Primaria"},
                {"comentario": "Secundaria", "client_event_id": "secundario-ocupado"},
            ],
        )

        response = self.client.post(self.url, json.dumps(payload), content_type="application/json")

        self.parada.refresh_from_db()
        self.assertEqual(response.status_code, 409)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)
        self.assertFalse(ParadaEntregaEvidencia.objects.filter(client_event_id="confirmacion-nueva").exists())

    def test_serializer_coleccion_geocerca_tiene_presupuesto_constante_de_queries(self):
        for orden in range(2, 7):
            punto = PuntoLogistico.objects.create(
                sucursal=self.sucursal,
                nombre=f"Sucursal API {orden}",
                tipo=PuntoLogistico.TIPO_SUCURSAL,
                latitud="25.570000",
                longitud="-108.470000",
                radio_geocerca_metros=120,
            )
            ParadaRuta.objects.create(ruta=self.ruta, punto=punto, orden=orden)
        paradas = self.ruta.paradas.select_related(
            "ruta", "punto", "punto__sucursal", "entrega_confirmada_por", "revision_entrega_revisada_por"
        ).order_by("orden")

        with self.assertNumQueries(2):
            data = ParadaRutaSerializer(paradas, many=True).data

        self.assertEqual(len(data), 6)

    def test_retry_exacto_devuelve_original_y_payload_divergente_da_409(self):
        payload = self._payload()
        primero = self.client.post(self.url, json.dumps(payload), content_type="application/json")
        retry = self.client.post(self.url, json.dumps(payload), content_type="application/json")
        conflicto = self.client.post(
            self.url,
            json.dumps({**payload, "notas": "Otro contenido"}),
            content_type="application/json",
        )

        self.assertEqual(primero.status_code, 200)
        self.assertEqual(retry.status_code, 200)
        self.assertEqual(retry.json(), primero.json())
        self.assertEqual(conflicto.status_code, 409)
        self.assertEqual(EventoRuta.objects.filter(parada=self.parada, tipo=EventoRuta.TIPO_ENTREGA_EXCEPCIONAL).count(), 1)

    def test_retry_exacto_devuelve_snapshot_original_despues_de_completar_ruta(self):
        payload = self._payload(client_event_id="api-retry-ruta-completada")
        primero = self.client.post(self.url, json.dumps(payload), content_type="application/json")
        self.ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
        self.ruta.save(update_fields=["estatus", "updated_at"])

        retry = self.client.post(self.url, json.dumps(payload), content_type="application/json")
        conflicto = self.client.post(
            self.url,
            json.dumps({**payload, "notas": "Payload divergente tras cierre"}),
            content_type="application/json",
        )

        self.assertEqual(primero.status_code, 200)
        self.assertEqual(retry.status_code, 200)
        self.assertEqual(retry.json(), primero.json())
        self.assertEqual(conflicto.status_code, 409)

    def test_retry_exacto_devuelve_snapshot_original_despues_de_revision(self):
        payload = self._payload(client_event_id="api-retry-revision-autorizada")
        primero = self.client.post(self.url, json.dumps(payload), content_type="application/json")
        jefe = User.objects.create_user(username="jefe.retry.entregas", password="pass123")
        UserModuleAccess.objects.create(user=jefe, module="logistica.rutas", access=ACCESS_MANAGE, updated_by=jefe)
        revisar_entrega_excepcional(
            parada=self.parada,
            actor=jefe,
            decision=ParadaRuta.REVISION_AUTORIZADA,
            motivo="Evidencia revisada",
        )

        retry = self.client.post(self.url, json.dumps(payload), content_type="application/json")

        self.assertEqual(primero.status_code, 200)
        self.assertEqual(retry.status_code, 200)
        self.assertEqual(retry.json(), primero.json())

    def test_retry_exacto_devuelve_snapshot_original_despues_de_rechazo(self):
        payload = self._payload(client_event_id="api-retry-revision-rechazada")
        primero = self.client.post(self.url, json.dumps(payload), content_type="application/json")
        jefe = User.objects.create_user(username="jefe.retry.rechazo", password="pass123")
        UserModuleAccess.objects.create(user=jefe, module="logistica.rutas", access=ACCESS_MANAGE, updated_by=jefe)
        revisar_entrega_excepcional(
            parada=self.parada,
            actor=jefe,
            decision=ParadaRuta.REVISION_RECHAZADA,
            motivo="Evidencia insuficiente",
        )

        retry = self.client.post(self.url, json.dumps(payload), content_type="application/json")

        self.assertEqual(primero.status_code, 200)
        self.assertEqual(retry.status_code, 200)
        self.assertEqual(retry.json(), primero.json())

    def test_fallo_guardando_snapshot_revierte_entrega_evento_y_evidencia(self):
        with patch("api.logistica_views.guardar_respuesta_idempotente", side_effect=RuntimeError("fallo snapshot")):
            with self.assertRaises(RuntimeError):
                self.client.post(self.url, json.dumps(self._payload()), content_type="application/json")

        self.parada.refresh_from_db()
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)
        self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_NO_REQUERIDA)
        self.assertFalse(ParadaEntregaEvidencia.objects.filter(parada=self.parada).exists())
        self.assertFalse(EventoRuta.objects.filter(parada=self.parada, tipo=EventoRuta.TIPO_ENTREGA_EXCEPCIONAL).exists())

    def test_retry_reconstruye_snapshot_faltante_sin_usar_estado_mutable(self):
        payload = self._payload(client_event_id="api-retry-snapshot-faltante")
        primero = self.client.post(self.url, json.dumps(payload), content_type="application/json")
        evidencia = ParadaEntregaEvidencia.objects.get(client_event_id="api-retry-snapshot-faltante")
        metadata = dict(evidencia.metadata)
        metadata.pop("respuesta_api")
        evidencia.metadata = metadata
        evidencia.save(update_fields=["metadata"])
        jefe = User.objects.create_user(username="jefe.retry.snapshot.faltante", password="pass123")
        UserModuleAccess.objects.create(user=jefe, module="logistica.rutas", access=ACCESS_MANAGE, updated_by=jefe)
        revisar_entrega_excepcional(
            parada=self.parada,
            actor=jefe,
            decision=ParadaRuta.REVISION_AUTORIZADA,
            motivo="Revisión posterior",
        )
        self.ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
        self.ruta.save(update_fields=["estatus", "updated_at"])

        retry = self.client.post(self.url, json.dumps(payload), content_type="application/json")

        self.assertEqual(retry.status_code, 200)
        self.assertEqual(retry.json(), primero.json())

    def test_retry_sin_respuesta_api_usa_snapshot_inmutable_y_excluye_evidencia_posterior(self):
        payload = self._payload(client_event_id="api-retry-snapshot-inmutable")
        primero = self.client.post(self.url, json.dumps(payload), content_type="application/json")
        evidencia = ParadaEntregaEvidencia.objects.get(client_event_id="api-retry-snapshot-inmutable")
        self.assertIn("snapshot_dominio", evidencia.metadata)
        self.assertEqual(evidencia.metadata["evidencia_ids"], [evidencia.id])
        metadata = dict(evidencia.metadata)
        metadata.pop("respuesta_api")
        evidencia.metadata = metadata
        evidencia.save(update_fields=["metadata"])

        ParadaEntregaEvidencia.objects.create(
            ruta=self.ruta,
            parada=self.parada,
            tipo=ParadaEntregaEvidencia.TIPO_INCIDENCIA,
            comentario="Evidencia administrativa posterior",
            capturado_por=self.user,
            client_event_id="evidencia-posterior",
            metadata={"origen": "point_posterior"},
        )
        jefe = User.objects.create_user(username="jefe.retry.inmutable", password="pass123")
        UserModuleAccess.objects.create(user=jefe, module="logistica.rutas", access=ACCESS_MANAGE, updated_by=jefe)
        revisar_entrega_excepcional(
            parada=self.parada,
            actor=jefe,
            decision=ParadaRuta.REVISION_RECHAZADA,
            motivo="Revisión posterior al hecho",
        )
        self.ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
        self.ruta.save(update_fields=["estatus", "updated_at"])

        retry = self.client.post(self.url, json.dumps(payload), content_type="application/json")

        self.assertEqual(retry.status_code, 200)
        self.assertEqual(retry.json(), primero.json())
        self.assertEqual([row["id"] for row in retry.json()["evidencias"]], [evidencia.id])

    def test_retry_legacy_sin_snapshot_declara_recuperacion_limitada(self):
        payload = self._payload(client_event_id="api-retry-legacy-sin-snapshot")
        self.client.post(self.url, json.dumps(payload), content_type="application/json")
        evidencia = ParadaEntregaEvidencia.objects.get(client_event_id="api-retry-legacy-sin-snapshot")
        metadata = dict(evidencia.metadata)
        metadata.pop("respuesta_api")
        metadata.pop("snapshot_dominio")
        metadata.pop("evidencia_ids")
        evidencia.metadata = metadata
        evidencia.save(update_fields=["metadata"])
        jefe = User.objects.create_user(username="jefe.retry.legacy", password="pass123")
        UserModuleAccess.objects.create(user=jefe, module="logistica.rutas", access=ACCESS_MANAGE, updated_by=jefe)
        revisar_entrega_excepcional(
            parada=self.parada,
            actor=jefe,
            decision=ParadaRuta.REVISION_AUTORIZADA,
            motivo="Cambio posterior no recuperable",
        )

        retry = self.client.post(self.url, json.dumps(payload), content_type="application/json")

        self.assertEqual(retry.status_code, 200)
        self.assertTrue(retry.json()["replay_recuperado"])
        self.assertEqual(retry.json()["parada"]["revision_entrega_estado"], ParadaRuta.REVISION_PENDIENTE)
        self.assertIsNone(retry.json()["parada"]["revision_entrega_revisada_por"])
        self.assertEqual([row["id"] for row in retry.json()["evidencias"]], [evidencia.id])

    def test_ajuste_erp_no_fabrica_visita_hora_ni_geocerca(self):
        jefe = User.objects.create_user(username="jefe.api.entregas", password="pass123")
        UserModuleAccess.objects.create(user=jefe, module="logistica.rutas", access=ACCESS_MANAGE, updated_by=jefe)
        self.client.force_login(jefe)

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {
                "action": "ajustar_entrega_manual",
                "parada_id": self.parada.id,
                "entrega_estado": ParadaRuta.ENTREGA_ENTREGADA,
                "nota_entrega_manual": "Confirmación telefónica de sucursal.",
            },
        )

        self.parada.refresh_from_db()
        self.assertEqual(response.status_code, 302)
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertIsNone(self.parada.hora_llegada_real)
        self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_PENDIENTE)
        self.assertFalse(EventoRuta.objects.filter(parada=self.parada, tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE).exists())

    def test_admin_hace_readonly_todos_los_campos_fisicos_entrega_y_revision(self):
        model_admin = admin.site._registry[ParadaRuta]
        readonly = set(model_admin.get_readonly_fields(None, self.parada))
        esperados = {
            "estado", "hora_llegada_real", "hora_salida_real", "distancia_llegada_metros",
            "entrega_estado", "entrega_confirmada_en", "entrega_confirmada_por", "entrega_notas",
            "revision_entrega_estado", "revision_entrega_causa", "revision_entrega_datos",
            "revision_entrega_revisada_por", "revision_entrega_revisada_en", "revision_entrega_resolucion",
        }
        self.assertTrue(esperados.issubset(readonly), esperados - readonly)

    def test_serializer_expone_revision(self):
        data = ParadaRutaSerializer(self.parada).data
        self.assertIn("revision_entrega_estado", data)
        self.assertIn("revision_entrega_causa", data)
        self.assertIn("revision_entrega_datos", data)

    def test_serializer_expone_geocerca_confiable_con_misma_regla_de_dominio(self):
        self._registrar_geocerca_real()

        data = ParadaRutaSerializer(self.parada).data

        self.parada.refresh_from_db()
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertTrue(data["geocerca_confiable"])

    def test_serializer_no_confunde_visita_legacy_con_geocerca_confiable(self):
        self.parada.estado = ParadaRuta.ESTADO_VISITADA
        self.parada.hora_llegada_real = timezone.now()
        self.parada.save(update_fields=["estado", "hora_llegada_real", "actualizado_en"])

        data = ParadaRutaSerializer(self.parada).data

        self.assertFalse(data["geocerca_confiable"])

    def test_serializer_expone_revisor_id_y_nombre(self):
        self.client.post(self.url, json.dumps(self._payload()), content_type="application/json")
        jefe = User.objects.create_user(first_name="Laura", last_name="Jefa", username="laura.jefa")
        UserModuleAccess.objects.create(user=jefe, module="logistica.rutas", access=ACCESS_MANAGE, updated_by=jefe)
        revisar_entrega_excepcional(
            parada=self.parada,
            actor=jefe,
            decision=ParadaRuta.REVISION_RECHAZADA,
            motivo="Falta evidencia",
        )

        data = ParadaRutaSerializer(ParadaRuta.objects.get(pk=self.parada.pk)).data

        self.assertEqual(data["revision_entrega_revisada_por"], jefe.id)
        self.assertEqual(data["revision_entrega_revisada_por_nombre"], "Laura Jefa")

    def test_pwa_avisa_pide_motivo_y_envia_contexto_idempotente(self):
        pwa_html = (Path(__file__).resolve().parent / "templates" / "logistica" / "pwa.html").read_text(encoding="utf-8")

        self.assertIn("La entrega se registrará, pero será revisada por tu jefe.", pwa_html)
        self.assertIn("Explica por qué confirmas la entrega sin geocerca", pwa_html)
        self.assertIn("client_event_id: clientEventId", pwa_html)
        self.assertIn("client_timestamp: new Date().toISOString()", pwa_html)
        self.assertIn("data.warning", pwa_html)
        self.assertIn("parada?.geocerca_confiable !== true", pwa_html)
        self.assertNotIn('parada?.estado !== "VISITADA"', pwa_html)

    def test_guardrail_exige_version_recarga_point_en_registro_y_service_worker(self):
        from logistica.checks import REQUIRED_SERVICE_WORKER_MARKERS, REQUIRED_TEMPLATE_MARKERS

        self.assertEqual(
            set(REQUIRED_TEMPLATE_MARKERS),
            {"route-control-v65-recarga-point"},
        )
        self.assertIn("pollyanas-logistica-pwa-v65-recarga-point", REQUIRED_SERVICE_WORKER_MARKERS)
        self.assertNotIn("route-control-v57", REQUIRED_TEMPLATE_MARKERS)


class LogisticaRevisionEntregaTests(TestCase):
    def setUp(self):
        self.repartidor_user = User.objects.create_user(username="repartidor.revision", password="pass123")
        self.jefe = User.objects.create_user(
            username="jefe.revision",
            first_name="Laura",
            last_name="Jefa",
            password="pass123",
        )
        self.consulta = User.objects.create_user(username="consulta.revision", password="pass123")
        UserModuleAccess.objects.create(
            user=self.jefe,
            module="logistica.rutas",
            access=ACCESS_MANAGE,
            updated_by=self.jefe,
        )
        UserModuleAccess.objects.create(
            user=self.consulta,
            module="logistica.rutas",
            access=ACCESS_VIEW,
            updated_by=self.jefe,
        )
        UserModuleAccess.objects.create(
            user=self.repartidor_user,
            module="logistica.rutas",
            access=ACCESS_VIEW,
            updated_by=self.jefe,
        )
        self.sucursal = Sucursal.objects.create(codigo="REV-LOG", nombre="Sucursal revisión", activa=True)
        self.unidad = Unidad.objects.create(codigo="REV-01", descripcion="Unidad revisión", sucursal=self.sucursal)
        self.repartidor = Repartidor.objects.create(
            user=self.repartidor_user,
            sucursal=self.sucursal,
            unidad_asignada=self.unidad,
        )
        self.repartidor_user.is_staff = True
        self.repartidor_user.save(update_fields=["is_staff"])
        self.ruta = RutaEntrega.objects.create(
            nombre="Ruta revisión de entregas",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )
        self.punto = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Sucursal revisión",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )
        self.parada = ParadaRuta.objects.create(ruta=self.ruta, punto=self.punto, orden=1)
        confirmar_entrega_parada(
            ruta=self.ruta,
            parada=self.parada,
            actor=self.repartidor_user,
            entrega_estado=ParadaRuta.ENTREGA_ENTREGADA,
            motivo="GPS sin señal; entrega confirmada con foto.",
            client_event_id="revision-ui-1",
            ubicacion={
                "causa": "GPS_SIN_SENAL",
                "latitud": "25.571000",
                "longitud": "-108.471000",
                "precision_metros": "45.50",
                "distancia_metros": 184,
                "client_timestamp": timezone.now().isoformat(),
                "client_version": "pwa-v60",
            },
            evidencias=[{"comentario": "Foto de mostrador", "tipo": ParadaEntregaEvidencia.TIPO_FOTO_ENTREGA}],
            origen="PWA",
        )
        self.url = reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id})

    def test_ruta_detail_muestra_cola_pendiente_y_evidencia_operativa(self):
        self.client.force_login(self.jefe)
        self.parada.refresh_from_db()
        hora_entrega_local = timezone.localtime(self.parada.entrega_confirmada_en).strftime("%Y-%m-%d %H:%M")

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["revisiones_pendientes_count"], 1)
        self.assertTrue(response.context["cierre_administrativo_pendiente"])
        self.assertContains(response, "Cierre administrativo pendiente")
        self.assertContains(response, "GPS_SIN_SENAL")
        self.assertContains(response, "184")
        self.assertContains(response, "25.571000")
        self.assertContains(response, "45.50")
        self.assertContains(response, "repartidor.revision")
        self.assertContains(response, "GPS sin señal; entrega confirmada con foto.")
        self.assertContains(response, "Foto de mostrador")
        self.assertContains(response, "Hora de entrega")
        self.assertContains(response, hora_entrega_local)
        self.assertContains(response, "Estado físico")
        self.assertContains(response, self.parada.get_estado_display())
        self.assertContains(response, "Hecho de entrega")
        self.assertContains(response, self.parada.get_entrega_estado_display())
        self.assertContains(response, "Revisión administrativa")
        self.assertContains(response, "Autorizar")
        self.assertContains(response, "Rechazar")

    def test_formulario_revision_asocia_label_visible_con_motivo_requerido(self):
        self.client.force_login(self.jefe)

        response = self.client.get(self.url)

        field_id = f"motivo-revision-{self.parada.id}"
        self.assertContains(response, f'for="{field_id}"')
        self.assertContains(response, "Motivo de la resolución")
        self.assertContains(response, f'id="{field_id}"')
        self.assertContains(response, 'name="motivo_revision"')
        self.assertContains(response, "required")
        self.assertContains(response, f'id="revision-entrega-{self.parada.id}"')
        self.assertContains(response, "data-async-action")
        self.assertContains(response, f'value="revision-entrega-{self.parada.id}"')

    def test_jefe_autoriza_con_motivo_y_conserva_hecho_fisico_y_evidencia(self):
        self.client.force_login(self.jefe)
        evidencia = self.parada.evidencias_entrega.get()

        response = self.client.post(
            self.url,
            {
                "action": "revisar_entrega",
                "parada_id": self.parada.id,
                "decision": "AUTORIZADA",
                "motivo_revision": "Foto y llamada verificadas.",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.parada.refresh_from_db()
        self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_AUTORIZADA)
        self.assertEqual(self.parada.revision_entrega_revisada_por, self.jefe)
        self.assertEqual(self.parada.revision_entrega_resolucion, "Foto y llamada verificadas.")
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertIsNone(self.parada.hora_llegada_real)
        self.assertTrue(ParadaEntregaEvidencia.objects.filter(pk=evidencia.pk).exists())

    def test_revision_json_actualiza_solo_la_fila_y_devuelve_toast(self):
        self.client.force_login(self.jefe)

        response = self.client.post(
            self.url,
            {
                "action": "revisar_entrega",
                "parada_id": self.parada.id,
                "decision": "AUTORIZADA",
                "motivo_revision": "Foto y llamada verificadas.",
                "context_anchor": f"revision-entrega-{self.parada.id}",
            },
            HTTP_ACCEPT="application/json",
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["target"], f"#revision-entrega-{self.parada.id}")
        self.assertEqual(payload["toast"]["type"], "success")
        self.assertIn("Revisión de entrega registrada", payload["toast"]["message"])
        self.assertIn(f'id="revision-entrega-{self.parada.id}"', payload["html"])
        self.assertIn("Autorizada", payload["html"])

    def test_revision_json_error_conserva_estado_y_permite_reintento(self):
        self.client.force_login(self.jefe)

        response = self.client.post(
            self.url,
            {
                "action": "revisar_entrega",
                "parada_id": self.parada.id,
                "decision": "AUTORIZADA",
                "motivo_revision": "   ",
            },
            HTTP_ACCEPT="application/json",
        )

        self.assertEqual(response.status_code, 422)
        payload = response.json()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["target"], f"#revision-entrega-{self.parada.id}")
        self.assertEqual(payload["toast"]["type"], "error")
        self.assertTrue(payload["toast"]["persistent"])
        self.parada.refresh_from_db()
        self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_PENDIENTE)

    def test_revision_html_fallback_regresa_a_la_revision_afectada(self):
        self.client.force_login(self.jefe)

        response = self.client.post(
            self.url,
            {
                "action": "revisar_entrega",
                "parada_id": self.parada.id,
                "decision": "RECHAZADA",
                "motivo_revision": "La evidencia no identifica la sucursal.",
                "context_anchor": f"revision-entrega-{self.parada.id}",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response["Location"].endswith(f"#revision-entrega-{self.parada.id}"))

    def test_jefe_rechaza_en_ruta_completada_y_la_resolucion_sigue_visible(self):
        self.ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
        self.ruta.save(update_fields=["estatus", "updated_at"])
        self.client.force_login(self.jefe)

        response = self.client.post(
            self.url,
            {
                "action": "revisar_entrega",
                "parada_id": self.parada.id,
                "decision": "RECHAZADA",
                "motivo_revision": "La evidencia no identifica la sucursal.",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.parada.refresh_from_db()
        self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_RECHAZADA)
        page = self.client.get(self.url)
        self.assertContains(page, "Rechazada")
        self.assertContains(page, "Laura Jefa")
        self.assertContains(page, "La evidencia no identifica la sucursal.")
        self.assertTrue(page.context["cierre_administrativo_pendiente"])

    def test_motivo_es_obligatorio_y_revision_permanece_pendiente(self):
        self.client.force_login(self.jefe)

        response = self.client.post(
            self.url,
            {"action": "revisar_entrega", "parada_id": self.parada.id, "decision": "AUTORIZADA", "motivo_revision": "   "},
        )

        self.assertEqual(response.status_code, 302)
        self.parada.refresh_from_db()
        self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_PENDIENTE)

    def test_repartidor_y_usuario_de_consulta_no_pueden_resolver(self):
        for usuario in (self.repartidor_user, self.consulta):
            with self.subTest(usuario=usuario.username):
                self.client.force_login(usuario)
                response = self.client.post(
                    self.url,
                    {
                        "action": "revisar_entrega",
                        "parada_id": self.parada.id,
                        "decision": "AUTORIZADA",
                        "motivo_revision": "Intento sin permiso",
                    },
                )
                self.assertEqual(response.status_code, 403)
        self.parada.refresh_from_db()
        self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_PENDIENTE)

    def test_cierre_operativo_permite_revision_pendiente_y_la_contabiliza(self):
        self.client.force_login(self.jefe)

        response = self.client.post(
            self.url,
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_COMPLETADA},
            follow=True,
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_COMPLETADA)
        self.assertEqual(response.context["revisiones_pendientes_count"], 1)
        self.assertContains(response, "Cierre administrativo pendiente")

    def test_jefe_corrige_rechazo_con_motivo_y_evidencia_sin_borrar_entrega(self):
        evidencia = self.parada.evidencias_entrega.get()
        revisar_entrega_excepcional(
            parada=self.parada,
            actor=self.jefe,
            decision=ParadaRuta.REVISION_RECHAZADA,
            motivo="La foto no identifica el local.",
        )

        resultado = revisar_entrega_excepcional(
            parada=self.parada,
            actor=self.jefe,
            decision=ParadaRuta.REVISION_CORREGIDA,
            motivo="Se verificó llamada con la encargada y folio 443.",
        )

        self.parada.refresh_from_db()
        self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_CORREGIDA)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_ENTREGADA)
        self.assertTrue(ParadaEntregaEvidencia.objects.filter(pk=evidencia.pk).exists())
        self.assertEqual(resultado.evento.metadata["decision"], ParadaRuta.REVISION_CORREGIDA)
        self.assertEqual(resultado.evento.tipo, EventoRuta.TIPO_ENTREGA_CORREGIDA)
        correccion = self.parada.evidencias_entrega.get(metadata__evento_correccion_id=resultado.evento.id)
        self.assertEqual(correccion.capturado_por, self.jefe)
        self.assertTrue(correccion.metadata["preserva_evidencia_original"])
        repetida = revisar_entrega_excepcional(
            parada=self.parada, actor=self.jefe, decision=ParadaRuta.REVISION_CORREGIDA,
            motivo="Se verificó llamada con la encargada y folio 443.",
        )
        self.assertTrue(repetida.idempotente)
        self.assertEqual(EventoRuta.objects.filter(parada=self.parada, tipo=EventoRuta.TIPO_ENTREGA_CORREGIDA).count(), 1)
        self.client.force_login(self.jefe)
        page = self.client.get(self.url)
        self.assertFalse(page.context["cierre_administrativo_pendiente"])

    def test_bandeja_global_incluye_pendiente_rechazada_y_alerta_historica_no_requerida(self):
        historica = ParadaRuta.objects.create(ruta=self.ruta, punto=self.punto, orden=2)
        EventoRuta.objects.create(
            ruta=self.ruta,
            parada=historica,
            tipo=EventoRuta.TIPO_INCONSISTENCIA_ENTREGA,
            severidad=EventoRuta.SEVERIDAD_ALERTA,
            descripcion="Entrega histórica sin geocerca.",
            clave_auditoria=f"hist-{historica.id}",
            metadata={"regla": "ENTREGADA_SIN_GEOFENCE_O_REVISION"},
        )
        self.client.force_login(self.jefe)

        response = self.client.get(reverse("logistica:revisiones_entrega"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, self.ruta.folio)
        self.assertContains(response, "GPS_SIN_SENAL")
        self.assertContains(response, "Entrega histórica sin geocerca")
        self.assertContains(response, "repartidor.revision")
        self.assertTrue(Notificacion.objects.filter(usuario=self.jefe, url="/logistica/rutas/revisiones/").exists())
        control = self.client.get(reverse("logistica:control_rutas"))
        self.assertContains(control, "Abrir bandeja de revisiones")
        self.assertGreaterEqual(control.context["revisiones_globales_count"], 2)

    def test_jefe_resuelve_alerta_historica_con_motivo_y_sale_del_conteo(self):
        historica = ParadaRuta.objects.create(ruta=self.ruta, punto=self.punto, orden=2)
        alerta = EventoRuta.objects.create(
            ruta=self.ruta, parada=historica,
            tipo=EventoRuta.TIPO_INCONSISTENCIA_ENTREGA,
            severidad=EventoRuta.SEVERIDAD_ALERTA,
            descripcion="Entrega histórica sin geocerca.",
            clave_auditoria=f"resolver-{historica.id}",
            metadata={"regla": "ENTREGADA_SIN_GEOFENCE_O_REVISION"},
        )
        self.client.force_login(self.jefe)
        response = self.client.post(reverse("logistica:revisiones_entrega"), {
            "evento_id": alerta.id,
            "motivo_revision": "Se verificó con encargada y evidencia física.",
        })
        self.assertEqual(response.status_code, 302)
        alerta.refresh_from_db()
        self.assertEqual(alerta.revision_alerta_estado, EventoRuta.REVISION_ALERTA_RESUELTA)
        self.assertEqual(alerta.revision_alerta_resuelta_por, self.jefe)
        self.assertIsNotNone(alerta.revision_alerta_resuelta_en)
        bandeja = self.client.get(reverse("logistica:revisiones_entrega"))
        self.assertNotContains(bandeja, "Entrega histórica sin geocerca.")


class LogisticaEntregaContratoFinalTests(LogisticaEntregaDomainTests):
    def test_servicio_exige_origen_estructurado(self):
        with self.assertRaisesMessage(ValidationError, "origen"):
            confirmar_entrega_parada(
                ruta=self.ruta, parada=self.parada, actor=self.user,
                entrega_estado=ParadaRuta.ENTREGA_ENTREGADA, motivo="Prueba",
                client_event_id="sin-origen", ubicacion={},
            )

    def test_ajuste_admin_siempre_requiere_revision_aun_con_geocerca(self):
        self._registrar_geocerca_real()
        resultado = self._confirmar(
            origen="AJUSTE_ADMIN",
            ubicacion={
                "causa": "AJUSTE_ADMINISTRATIVO",
                "client_timestamp": timezone.now().isoformat(),
                "client_version": "erp-ruta-detail",
            },
        )
        self.parada.refresh_from_db()
        self.assertTrue(resultado.requiere_revision)
        self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_PENDIENTE)
        self.assertEqual(resultado.evento.metadata["origen_confirmacion"], "AJUSTE_ADMIN")

    def test_excepcion_pwa_exige_contexto_completo_y_causa_permitida(self):
        with self.assertRaises(ValidationError):
            self._confirmar(origen="PWA", ubicacion={"causa": "INVENTADA"})

    def test_servicio_no_infiere_cliente_legacy_por_contexto_vacio(self):
        with self.assertRaises(ValidationError):
            self._confirmar(
                origen="PWA",
                client_event_id="cola-v59-evidencia-1",
                ubicacion={},
                evidencias=[{"client_event_id": "cola-v59-evidencia-1", "comentario": "cola offline"}],
            )

    def test_gps_nuevo_no_reescribe_evento_legacy(self):
        legacy_actor = User.objects.create_user(username="legacy.actor")
        legacy_fecha = timezone.now() - timezone.timedelta(hours=2)
        legacy = EventoRuta.objects.create(
            ruta=self.ruta,
            parada=self.parada,
            tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            descripcion="Legacy",
            creado_por=legacy_actor,
        )
        EventoRuta.objects.filter(pk=legacy.pk).update(creado_en=legacy_fecha)
        self._registrar_geocerca_real()
        legacy.refresh_from_db()
        self.assertEqual(legacy.creado_por, legacy_actor)
        self.assertEqual(legacy.creado_en, legacy_fecha)
        self.assertEqual(EventoRuta.objects.filter(parada=self.parada, tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE).count(), 2)


class LogisticaGuardEscritoresTests(TestCase):
    def test_guard_detecta_asignacion_y_update_directos(self):
        from logistica.checks import critical_parada_writes_in_source
        source = "parada.entrega_estado = 'ENTREGADA'\nParadaRuta.objects.filter(pk=1).update(estado='VISITADA')"
        self.assertEqual(len(critical_parada_writes_in_source(source, "logistica/views_nueva.py")), 2)

    def test_guard_detecta_setattr_bulk_update_payload_y_sql_directo(self):
        from logistica.checks import critical_parada_writes_in_source
        source = """
setattr(parada, 'entrega_estado', valor)
ParadaRuta.objects.bulk_update(paradas, ['estado', 'hora_llegada_real'])
ParadaRuta.objects.filter(pk=1).update(**{'revision_entrega_estado': 'AUTORIZADA'})
cursor.execute('UPDATE logistica_paradaruta SET entrega_estado = %s', ['ENTREGADA'])
"""
        fields = {field for _, field in critical_parada_writes_in_source(source, "api/nuevo.py")}
        self.assertTrue({"entrega_estado", "estado", "hora_llegada_real", "revision_entrega_estado"}.issubset(fields))

    @patch("logistica.tasks.auditar_entregas_ruta")
    def test_auditor_programado_cubre_hoy_y_dia_operativo_anterior(self, auditar):
        from logistica.tasks import auditar_entregas_ruta_task
        auditar.side_effect = [
            {"rutas_revisadas": 1, "paradas_revisadas": 2, "hallazgos": [], "alertas_creadas": 0, "dry_run": False},
            {"rutas_revisadas": 3, "paradas_revisadas": 4, "hallazgos": [], "alertas_creadas": 1, "dry_run": False},
        ]
        result = auditar_entregas_ruta_task.run()
        self.assertEqual(auditar.call_count, 2)
        self.assertEqual(result["rutas_revisadas"], 4)
        self.assertEqual(result["alertas_creadas"], 1)

    @patch("logistica.tasks.timezone.localdate", return_value=date(2026, 7, 10))
    @patch("logistica.tasks.auditar_entregas_ruta")
    def test_auditor_recupera_todos_los_dias_perdidos_desde_cursor(self, auditar, _localdate):
        from logistica.models import AuditoriaEntregaCursor
        from logistica.tasks import auditar_entregas_ruta_task
        AuditoriaEntregaCursor.objects.create(ultima_fecha_exitosa=date(2026, 7, 5))
        auditar.return_value = {
            "rutas_revisadas": 0, "paradas_revisadas": 0, "hallazgos": [],
            "alertas_creadas": 0, "dry_run": False,
        }
        resultado = auditar_entregas_ruta_task.run()
        self.assertEqual(resultado["ventana_fechas"], [
            "2026-07-06", "2026-07-07", "2026-07-08", "2026-07-09", "2026-07-10",
        ])
        self.assertEqual(AuditoriaEntregaCursor.objects.get().ultima_fecha_exitosa, date(2026, 7, 10))

    @patch("logistica.tasks.timezone.localdate", return_value=date(2026, 7, 10))
    @patch("logistica.tasks.auditar_entregas_ruta")
    def test_auditor_reanuda_en_primer_dia_no_exitoso(self, auditar, _localdate):
        from logistica.models import AuditoriaEntregaCursor
        from logistica.tasks import auditar_entregas_ruta_task
        cursor = AuditoriaEntregaCursor.objects.create(ultima_fecha_exitosa=date(2026, 7, 7))
        ok = {"rutas_revisadas": 0, "paradas_revisadas": 0, "hallazgos": [], "alertas_creadas": 0, "dry_run": False}
        auditar.side_effect = [ok, RuntimeError("falla del 9")]
        with self.assertRaises(RuntimeError):
            auditar_entregas_ruta_task.run()
        cursor.refresh_from_db()
        self.assertEqual(cursor.ultima_fecha_exitosa, date(2026, 7, 8))


class LogisticaReglasAdyacentesStabilizationTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="reglas.adyacentes", password="pass123")
        self.user.groups.add(Group.objects.get_or_create(name="repartidor")[0])
        self.sucursal = Sucursal.objects.create(codigo="ADY-LOG", nombre="Reglas adyacentes", activa=True)
        self.unidad = Unidad.objects.create(codigo="ADY-01", descripcion="Unidad reglas", sucursal=self.sucursal)
        self.repartidor = Repartidor.objects.create(
            user=self.user,
            sucursal=self.sucursal,
            unidad_asignada=self.unidad,
        )
        self.bitacora = BitacoraSalidaLlegada.objects.create(
            repartidor=self.repartidor,
            unidad=self.unidad,
            km_salida=1000,
            nivel_gas_salida="lleno",
            foto_tablero_salida=SimpleUploadedFile("tablero.gif", b"gif", content_type="image/gif"),
        )
        self.punto = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Sucursal reglas",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )

    def _crear_ruta(self, *, fecha=None, estatus=RutaEntrega.ESTATUS_PLANEADA):
        ruta = RutaEntrega.objects.create(
            nombre=f"Ruta reglas {RutaEntrega.objects.count() + 1}",
            fecha_ruta=fecha or timezone.localdate(),
            estatus=estatus,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
            bitacora_salida=self.bitacora if estatus == RutaEntrega.ESTATUS_EN_RUTA else None,
            hora_inicio_real=timezone.now() if estatus == RutaEntrega.ESTATUS_EN_RUTA else None,
        )
        parada = ParadaRuta.objects.create(ruta=ruta, punto=self.punto, orden=1)
        return ruta, parada

    def _crear_transferencia(self, *, requested="5.000", sent="0.000", source_hash="ady-zero"):
        origin = PointBranch.objects.create(external_id=f"CEDIS-{source_hash}", name="CEDIS")
        destination = PointBranch.objects.create(
            external_id=f"SUC-{source_hash}",
            name=self.sucursal.nombre,
            erp_branch=self.sucursal,
        )
        return PointTransferLine.objects.create(
            origin_branch=origin,
            destination_branch=destination,
            erp_destination_branch=self.sucursal,
            transfer_external_id=f"T-{source_hash}",
            detail_external_id=f"D-{source_hash}",
            source_hash=source_hash,
            registered_at=timezone.now(),
            sent_at=timezone.now(),
            item_name="Pastel enviado cero",
            item_code="ZERO-01",
            unit="pz",
            requested_quantity=requested,
            sent_quantity=sent,
            received_quantity="0.000",
            is_open=True,
        )

    def test_solicitado_positivo_enviado_cero_permanece_visible_y_resuelto(self):
        ruta, _ = self._crear_ruta()
        transferencia = self._crear_transferencia()

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        linea = resumen.checklist.lineas.get(source_hash=transferencia.source_hash)
        self.assertEqual(linea.cantidad_solicitada, Decimal("5.000"))
        self.assertEqual(linea.cantidad_enviada_esperada, Decimal("0.000"))
        self.assertEqual(linea.cantidad_cargada, Decimal("0.000"))
        self.assertEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED)
        self.assertIsNone(linea.validado_por)
        self.assertEqual(linea.client_event_id, "")
        self.assertFalse(linea.evidencias_entrega.exists())
        self.assertIsNone(checklist_bloquea_salida(ruta))

    def _linea_validada_con_cuatro(self, *, source_hash):
        ruta, _ = self._crear_ruta()
        transferencia = self._crear_transferencia(sent="4.000", source_hash=source_hash)
        sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)
        linea = RutaCargaChecklistLinea.objects.get(source_hash=transferencia.source_hash)
        linea.cantidad_cargada = Decimal("4.000")
        linea.estatus = RutaCargaChecklistLinea.ESTATUS_CARGADA
        linea.validado_por = self.user
        linea.validado_en = timezone.now()
        linea.save(update_fields=["cantidad_cargada", "estatus", "validado_por", "validado_en", "actualizado_en"])
        checklist = linea.checklist
        checklist.estatus = RutaCargaChecklist.ESTATUS_CONFIRMADA
        checklist.confirmado_por = self.user
        checklist.confirmado_en = timezone.now()
        checklist.save(update_fields=["estatus", "confirmado_por", "confirmado_en", "actualizado_en"])
        return ruta, checklist, linea, transferencia

    def test_point_cambia_de_cuatro_a_dos_preserva_captura_y_marca_diferencia(self):
        ruta, checklist, linea, transferencia = self._linea_validada_con_cuatro(source_hash="ady-change-4-2")

        transferencia.sent_quantity = Decimal("2.000")
        transferencia.save(update_fields=["sent_quantity", "updated_at"])
        sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)
        linea.refresh_from_db()
        checklist.refresh_from_db()
        self.assertEqual(linea.cantidad_enviada_esperada, Decimal("2.000"))
        self.assertEqual(linea.cantidad_cargada, Decimal("4.000"))
        self.assertEqual(linea.validado_por, self.user)
        self.assertEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_SOBRANTE)
        self.assertEqual(checklist.estatus, RutaCargaChecklist.ESTATUS_CON_INCIDENCIA)
        self.assertIsNotNone(checklist_bloquea_salida(ruta))

    def test_point_cambia_de_cuatro_a_cero_preserva_captura_y_marca_diferencia(self):
        ruta, checklist, linea, transferencia = self._linea_validada_con_cuatro(source_hash="ady-change-4-0")

        transferencia.sent_quantity = Decimal("0.000")
        transferencia.save(update_fields=["sent_quantity", "updated_at"])
        sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)
        linea.refresh_from_db()
        checklist.refresh_from_db()
        self.assertEqual(linea.cantidad_enviada_esperada, Decimal("0.000"))
        self.assertEqual(linea.cantidad_cargada, Decimal("4.000"))
        self.assertEqual(linea.validado_por, self.user)
        self.assertEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_SOBRANTE)
        self.assertEqual(checklist.estatus, RutaCargaChecklist.ESTATUS_CON_INCIDENCIA)
        self.assertIsNotNone(checklist_bloquea_salida(ruta))

    def test_point_permanece_en_cuatro_conserva_captura_y_cargada(self):
        ruta, checklist, linea, transferencia = self._linea_validada_con_cuatro(source_hash="ady-change-4-4")

        sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        linea.refresh_from_db()
        checklist.refresh_from_db()
        self.assertEqual(linea.cantidad_enviada_esperada, Decimal("4.000"))
        self.assertEqual(linea.cantidad_cargada, Decimal("4.000"))
        self.assertEqual(linea.validado_por, self.user)
        self.assertEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_CARGADA)
        self.assertEqual(checklist.estatus, RutaCargaChecklist.ESTATUS_CONFIRMADA)
        self.assertIsNone(checklist_bloquea_salida(ruta))

    def _diferencia_point_autorizada(self, *, source_hash):
        ruta, checklist, linea, transferencia = self._linea_validada_con_cuatro(source_hash=source_hash)
        transferencia.sent_quantity = Decimal("2.000")
        transferencia.save(update_fields=["sent_quantity", "updated_at"])
        sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)
        autorizar_diferencia_checklist_carga(
            ruta=ruta,
            user=self.user,
            autorizado=True,
            notas="Diferencia Point revisada.",
        )
        checklist.refresh_from_db()
        self.assertEqual(checklist.motivo_override, "Diferencia Point revisada.")
        self.assertIsNone(checklist_bloquea_salida(ruta))
        return ruta, checklist, linea, transferencia

    def test_resync_point_identico_preserva_autorizacion_de_diferencia(self):
        ruta, checklist, linea, _ = self._diferencia_point_autorizada(source_hash="ady-auth-identical")

        sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        checklist.refresh_from_db()
        linea.refresh_from_db()
        ruta.refresh_from_db()
        self.assertEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_SOBRANTE)
        self.assertEqual(checklist.motivo_override, "Diferencia Point revisada.")
        self.assertIsNone(checklist_bloquea_salida(ruta))

    def test_resync_point_con_metadata_irrelevante_preserva_autorizacion(self):
        ruta, checklist, _, transferencia = self._diferencia_point_autorizada(source_hash="ady-auth-metadata")
        transferencia.raw_payload = {"sync_note": "metadata actualizada sin cambio de cantidades"}
        transferencia.save(update_fields=["raw_payload", "updated_at"])

        sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        checklist.refresh_from_db()
        ruta.refresh_from_db()
        self.assertEqual(checklist.motivo_override, "Diferencia Point revisada.")
        self.assertIsNone(checklist_bloquea_salida(ruta))

    def test_resync_point_con_cambio_real_invalida_autorizacion_y_bloquea(self):
        ruta, checklist, linea, transferencia = self._diferencia_point_autorizada(source_hash="ady-auth-change")
        transferencia.sent_quantity = Decimal("1.000")
        transferencia.save(update_fields=["sent_quantity", "updated_at"])

        sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        checklist.refresh_from_db()
        linea.refresh_from_db()
        ruta.refresh_from_db()
        self.assertEqual(linea.cantidad_enviada_esperada, Decimal("1.000"))
        self.assertEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_SOBRANTE)
        self.assertEqual(checklist.motivo_override, "")
        self.assertIsNotNone(checklist_bloquea_salida(ruta))

    def test_ruta_nocturna_reconocida_por_api_acepta_tracking_y_vencida_no(self):
        ayer = timezone.localdate() - timezone.timedelta(days=1)
        with patch("api.logistica_views.timezone.now") as now_mock:
            now_mock.return_value = timezone.make_aware(datetime.combine(ayer, time(hour=23)))
            ruta, _ = self._crear_ruta(fecha=ayer, estatus=RutaEntrega.ESTATUS_EN_RUTA)
        self.client.force_login(self.user)

        activa = self.client.get(reverse("api_logistica_ruta_activa"))
        self.assertEqual(activa.status_code, 200)
        self.assertEqual(activa.data["ruta"]["id"], ruta.id)
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=ruta,
            payload={"latitud": "25.570000", "longitud": "-108.470000", "precision_metros": "10"},
        )
        self.assertEqual(ubicacion.ruta, ruta)

        ruta.created_at = timezone.make_aware(datetime.combine(ayer, time(hour=20)))
        ruta.save(update_fields=["created_at"])
        with self.assertRaisesMessage(ValidationError, "día operativo actual"):
            registrar_ubicacion_ruta(
                user=self.user,
                ruta=ruta,
                payload={"latitud": "25.570000", "longitud": "-108.470000"},
            )

    def test_ruta_nocturna_en_ruta_gana_a_ruta_de_hoy_planeada_y_solo_ella_acepta_tracking(self):
        ayer = timezone.localdate() - timezone.timedelta(days=1)
        ruta_nocturna, _ = self._crear_ruta(fecha=ayer, estatus=RutaEntrega.ESTATUS_EN_RUTA)
        RutaEntrega.objects.filter(pk=ruta_nocturna.pk).update(
            created_at=timezone.make_aware(datetime.combine(ayer, time(hour=23)))
        )
        ruta_nocturna.refresh_from_db()
        ruta_planeada, _ = self._crear_ruta(fecha=timezone.localdate(), estatus=RutaEntrega.ESTATUS_PLANEADA)
        self.client.force_login(self.user)

        activa = self.client.get(reverse("api_logistica_ruta_activa"))

        self.assertEqual(activa.status_code, 200)
        self.assertEqual(activa.data["ruta"]["id"], ruta_nocturna.id)
        self.assertTrue(ruta_es_operativa_hoy(ruta_nocturna))
        self.assertFalse(ruta_es_operativa_hoy(ruta_planeada))
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=ruta_nocturna,
            payload={"latitud": "25.570000", "longitud": "-108.470000"},
        )
        self.assertEqual(ubicacion.ruta, ruta_nocturna)

    def test_ruta_en_ruta_de_hoy_gana_a_nocturna_planeada_y_solo_ella_es_operativa(self):
        ayer = timezone.localdate() - timezone.timedelta(days=1)
        ruta_nocturna, _ = self._crear_ruta(fecha=ayer, estatus=RutaEntrega.ESTATUS_PLANEADA)
        RutaEntrega.objects.filter(pk=ruta_nocturna.pk).update(
            created_at=timezone.make_aware(datetime.combine(ayer, time(hour=23)))
        )
        ruta_nocturna.refresh_from_db()
        ruta_hoy, _ = self._crear_ruta(fecha=timezone.localdate(), estatus=RutaEntrega.ESTATUS_EN_RUTA)
        self.client.force_login(self.user)

        activa = self.client.get(reverse("api_logistica_ruta_activa"))

        self.assertEqual(activa.data["ruta"]["id"], ruta_hoy.id)
        self.assertTrue(ruta_es_operativa_hoy(ruta_hoy))
        self.assertFalse(ruta_es_operativa_hoy(ruta_nocturna))
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=ruta_hoy,
            payload={"latitud": "25.570000", "longitud": "-108.470000"},
        )
        self.assertEqual(ubicacion.ruta, ruta_hoy)

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_recarga_cedis_usa_operacion_y_evento_propios_sin_entrega_ni_geocerca(self, sync_point):
        ruta, _ = self._crear_ruta(estatus=RutaEntrega.ESTATUS_EN_RUTA)
        cedis = PuntoLogistico.objects.create(
            nombre="CEDIS reglas",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.580000",
            longitud="-108.480000",
        )
        parada_cedis = ParadaRuta.objects.create(ruta=ruta, punto=cedis, orden=2)
        checklist = RutaCargaChecklist.objects.create(ruta=ruta, estatus=RutaCargaChecklist.ESTATUS_CONFIRMADA)
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=ruta.paradas.get(orden=1),
            transfer_external_id="T-CEDIS",
            detail_external_id="D-CEDIS",
            source_hash="linea-cedis-reglas",
            item_name="Carga tramo",
            cantidad_solicitada="1",
            cantidad_enviada_esperada="1",
            cantidad_cargada="1",
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
        )
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("api_logistica_ruta_parada_recarga_cedis", kwargs={"ruta_id": ruta.id, "parada_id": parada_cedis.id}),
            {"notas": "Recarga del segundo tramo"},
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200)
        parada_cedis.refresh_from_db()
        self.assertEqual(parada_cedis.estado, ParadaRuta.ESTADO_VISITADA)
        self.assertEqual(parada_cedis.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)
        evento = EventoRuta.objects.get(ruta=ruta, parada=parada_cedis)
        self.assertEqual(evento.tipo, EventoRuta.TIPO_RECARGA_CEDIS)
        self.assertFalse(EventoRuta.objects.filter(ruta=ruta, tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE).exists())
        self.assertFalse(parada_cedis.evidencias_entrega.exists())

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_recarga_cedis_con_objeto_obsoleto_retorna_el_mismo_evento(self, sync_point):
        ruta, _ = self._crear_ruta(estatus=RutaEntrega.ESTATUS_EN_RUTA)
        cedis = PuntoLogistico.objects.create(
            nombre="CEDIS retry",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.580000",
            longitud="-108.480000",
        )
        parada = ParadaRuta.objects.create(ruta=ruta, punto=cedis, orden=2)
        parada_obsoleta = ParadaRuta.objects.get(pk=parada.pk)

        primero = registrar_recarga_cedis(ruta=ruta, user=self.user, parada=parada, notas="Primera")
        segundo = registrar_recarga_cedis(ruta=ruta, user=self.user, parada=parada_obsoleta, notas="Retry")

        self.assertEqual(segundo.id, primero.id)
        self.assertEqual(EventoRuta.objects.filter(ruta=ruta, tipo=EventoRuta.TIPO_RECARGA_CEDIS).count(), 1)

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_recarga_cedis_numera_despues_de_evento_historico_compatible(self, sync_point):
        ruta, _ = self._crear_ruta(estatus=RutaEntrega.ESTATUS_EN_RUTA)
        cedis = PuntoLogistico.objects.create(
            nombre="CEDIS histórico",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.580000",
            longitud="-108.480000",
        )
        historica = ParadaRuta.objects.create(
            ruta=ruta,
            punto=cedis,
            orden=2,
            estado=ParadaRuta.ESTADO_VISITADA,
        )
        EventoRuta.objects.create(
            ruta=ruta,
            parada=historica,
            tipo=EventoRuta.TIPO_INCIDENCIA_MANUAL,
            descripcion="Recarga CEDIS histórica",
            metadata={"tipo": "recarga_cedis"},
        )
        nueva = ParadaRuta.objects.create(ruta=ruta, punto=cedis, orden=3)

        evento = registrar_recarga_cedis(ruta=ruta, user=self.user, parada=nueva, notas="Nueva")

        self.assertEqual(evento.metadata["numero"], 2)
        self.assertEqual(EventoRuta.objects.filter(ruta=ruta).count(), 2)

    def test_migracion_normaliza_recarga_historica_y_es_idempotente(self):
        from importlib import import_module
        from django.apps import apps

        normalizar_recargas_cedis = import_module(
            "logistica.migrations.0033_normalizar_eventos_recarga_cedis"
        ).normalizar_recargas_cedis

        ruta, parada = self._crear_ruta(estatus=RutaEntrega.ESTATUS_EN_RUTA)
        evento = EventoRuta.objects.create(
            ruta=ruta,
            parada=parada,
            tipo=EventoRuta.TIPO_INCIDENCIA_MANUAL,
            descripcion="Recarga histórica PWA",
            metadata={"tipo": "recarga_cedis_pwa"},
        )

        normalizar_recargas_cedis(apps, None)
        normalizar_recargas_cedis(apps, None)

        evento.refresh_from_db()
        self.assertEqual(evento.tipo, EventoRuta.TIPO_RECARGA_CEDIS)
        self.assertEqual(EventoRuta.objects.filter(pk=evento.pk).count(), 1)


class LogisticaEmailTemplateTests(SimpleTestCase):
    def test_email_sources_do_not_use_manual_inline_styles(self):
        email_dir = Path(settings.BASE_DIR) / "logistica" / "templates" / "logistica" / "emails"

        for template_path in email_dir.glob("*.html"):
            with self.subTest(template=template_path.name):
                source = template_path.read_text(encoding="utf-8")
                self.assertNotIn("style=", source)
                self.assertNotIn("<style", source.lower())

    def test_email_renderer_compiles_source_classes_to_inline_styles(self):
        unidad = SimpleNamespace(codigo="QA-LOG-1", descripcion="Unidad QA", placa="QA-123")

        html = render_email_to_string(
            "logistica/emails/alerta_lavado.html",
            {
                "unidad": unidad,
                "dias_sin_lavar": None,
                "ultimo_lavado": None,
            },
        )

        self.assertIn("Alerta de lavado pendiente", html)
        self.assertIn("style=", html)
        self.assertIn("background: #8b2252", html)
        self.assertNotIn("<style", html.lower())


class LogisticaControlRutasTemplateTests(SimpleTestCase):
    def test_bitacoras_muestra_preview_y_semaforo_de_tickets_combustible(self):
        template_path = Path(settings.BASE_DIR) / "logistica" / "templates" / "logistica" / "bitacoras_lista.html"
        source = template_path.read_text(encoding="utf-8")
        css = (Path(settings.BASE_DIR) / "static" / "css" / "styles.css").read_text(encoding="utf-8")

        self.assertIn("<th>Tickets</th>", source)
        self.assertIn("<th>Auditoría</th>", source)
        self.assertIn("logi-ticket-cell", source)
        self.assertIn("logi-audit-cell", source)
        self.assertIn('alt="Ticket combustible cierre"', source)
        self.assertIn('alt="Ticket combustible ruta"', source)
        self.assertIn("log-ticket-thumb", source)
        self.assertIn("object-fit: contain", css)

    def test_programmed_google_polyline_uses_route_source_not_pipe_character(self):
        template_path = Path(settings.BASE_DIR) / "logistica" / "templates" / "logistica" / "control_rutas.html"
        source = template_path.read_text(encoding="utf-8")

        self.assertIn("function decodeRoutePolyline(value, source)", source)
        self.assertIn("decodeRoutePolyline(route.programada_polyline, route.programada_fuente)", source)
        self.assertIn("const hasSegmentPayload = Array.isArray(route.ubicaciones_segmentos);", source)
        self.assertIn("function drawActualRoute(coords, color, tooltip)", source)
        self.assertIn('map.createPane("actualRoutePane");', source)
        self.assertIn("segment.estado === \"fuera_geocerca\" ? \"#d82424\" : \"#e0007a\"", source)
        self.assertIn('dashArray: "1 12"', source)
        self.assertIn("background: repeating-linear-gradient(90deg, #e0007a", source)
        self.assertIn("L.circleMarker(point", source)
        self.assertNotIn('if (value.includes("|")) return parseFallbackPolyline(value);', source)

    def test_ruta_detail_muestra_overlay_en_acciones_lentas(self):
        template_path = Path(settings.BASE_DIR) / "logistica" / "templates" / "logistica" / "ruta_detail.html"
        source = template_path.read_text(encoding="utf-8")

        self.assertIn('id="route-loading-overlay"', source)
        self.assertIn("data-route-loading-form", source)
        self.assertIn("Actualizando carga esperada", source)
        self.assertIn("Actualizando recepción Point", source)
        self.assertIn("Cargando datos de la unidad", source)
        self.assertIn("ti-truck-delivery", source)
        self.assertIn("route-unit-drive", source)
        self.assertIn("route-road", source)
        self.assertIn("function showRouteLoading(form, submit)", source)
        self.assertIn("if (event.defaultPrevented) return;", source)
        self.assertIn('submit.setAttribute("aria-disabled", "true")', source)
        self.assertNotIn("submit.disabled = true", source)
        self.assertIn("form._routeSubmitter = button", source)
        self.assertIn('value="sync_recepcion_point"', source)

    def test_ruta_detail_separa_totales_y_detalle_de_carga(self):
        template_path = Path(settings.BASE_DIR) / "logistica" / "templates" / "logistica" / "ruta_detail.html"
        source = template_path.read_text(encoding="utf-8")

        self.assertIn('class="route-load-subsection"', source)
        self.assertIn("Totales por producto", source)
        self.assertIn("Detalle por parada", source)
        self.assertLess(source.index("Totales por producto"), source.index("Detalle por parada"))
        self.assertIn(".route-load-subsection + .route-load-subsection", source)

    def test_ruta_detail_filtra_detalle_por_parada_en_cliente(self):
        template_path = Path(settings.BASE_DIR) / "logistica" / "templates" / "logistica" / "ruta_detail.html"
        source = template_path.read_text(encoding="utf-8")

        self.assertIn('id="route-load-detail-search"', source)
        self.assertIn("data-route-load-detail-row", source)
        self.assertIn("data-route-load-detail-empty", source)
        self.assertIn("function applyFilter()", source)
        self.assertIn('input.addEventListener("input", applyFilter)', source)

    def test_ruta_detail_liga_hoja_imprimible_de_paradas(self):
        template_path = Path(settings.BASE_DIR) / "logistica" / "templates" / "logistica" / "ruta_detail.html"
        source = template_path.read_text(encoding="utf-8")

        self.assertIn("logistica:ruta_print", source)
        self.assertIn("Imprimir orden de ruta", source)

    def test_ruta_print_es_hoja_de_orden_sin_carga_esperada(self):
        template_path = Path(settings.BASE_DIR) / "logistica" / "templates" / "logistica" / "ruta_print.html"
        source = template_path.read_text(encoding="utf-8")

        self.assertIn("ORDEN DE PARADAS", source)
        self.assertIn("Orden de paradas para surtido", source)
        self.assertIn("[&nbsp;&nbsp;] Surtido", source)
        self.assertIn("Esta hoja NO valida cantidades", source)
        self.assertNotIn("Carga esperada", source)

    def test_base_carga_librerias_de_iconos(self):
        template_path = Path(settings.BASE_DIR) / "templates" / "base.html"
        source = template_path.read_text(encoding="utf-8")

        self.assertIn("@tabler/icons-webfont", source)
        self.assertIn("Material+Symbols+Sharp", source)
        self.assertIn("lucide", source)


VALID_GIF = (
    b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff!"
    b"\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00"
    b"\x00\x02\x02D\x01\x00;"
)


class LogisticaCombustibleAuditoriaTests(TestCase):
    def test_auditoria_marca_ticket_duplicado_como_alto_riesgo(self):
        user = User.objects.create_user(username="auditor.ticket", password="pass123")
        sucursal = Sucursal.objects.create(codigo="QA-AUD", nombre="QA Auditoría", activa=True)
        unidad = Unidad.objects.create(codigo="QA-AUD-1", descripcion="Unidad auditoría", sucursal=sucursal)
        repartidor = Repartidor.objects.create(user=user, sucursal=sucursal, unidad_asignada=unidad)
        bitacora = BitacoraSalidaLlegada.objects.create(
            repartidor=repartidor,
            unidad=unidad,
            km_salida=1000,
            nivel_gas_salida="1/2",
            foto_tablero_salida=SimpleUploadedFile("tablero.gif", VALID_GIF, content_type="image/gif"),
        )
        primera = CargaCombustibleUnidad.objects.create(
            bitacora=bitacora,
            unidad=unidad,
            repartidor=repartidor,
            litros=Decimal("44.00"),
            importe_total=Decimal("1200.00"),
            foto_ticket=SimpleUploadedFile("ticket1.gif", VALID_GIF, content_type="image/gif"),
        )
        segunda = CargaCombustibleUnidad.objects.create(
            bitacora=bitacora,
            unidad=unidad,
            repartidor=repartidor,
            litros=Decimal("44.00"),
            importe_total=Decimal("1200.00"),
            foto_ticket=SimpleUploadedFile("ticket2.gif", VALID_GIF, content_type="image/gif"),
        )

        auditar_carga_combustible(primera.id)
        resultado = auditar_carga_combustible(segunda.id)
        segunda.refresh_from_db()

        self.assertEqual(resultado["estado"], CargaCombustibleUnidad.AUDITORIA_ALTO_RIESGO)
        self.assertEqual(segunda.auditoria_estado, CargaCombustibleUnidad.AUDITORIA_ALTO_RIESGO)
        self.assertIn("ticket_duplicado", segunda.auditoria_motivos)
        self.assertEqual(segunda.auditoria_detalle["modo"], "reglas_locales")


class LogisticaGroupAliasCompatibilityTests(TestCase):
    def test_emails_de_grupo_legacy_dg_uses_canonical_group(self):
        user = User.objects.create_user(
            username="dg.logistica.alias",
            email="dg.logistica@example.com",
        )
        user.groups.add(Group.objects.get_or_create(name="DG")[0])

        self.assertEqual(_emails_de_grupo("dg"), ["dg.logistica@example.com"])


class LogisticaSeedPuntosPollyanasTests(TestCase):
    def test_seed_puntos_logisticos_pollyanas_es_idempotente(self):
        output = StringIO()
        call_command("seed_puntos_logisticos_pollyanas", stdout=output)
        call_command("seed_puntos_logisticos_pollyanas", stdout=output)

        self.assertEqual(Sucursal.objects.filter(codigo__in=["MATRIZ", "GUAMUCHIL"]).count(), 2)
        self.assertEqual(PuntoLogistico.objects.filter(tipo=PuntoLogistico.TIPO_SUCURSAL, activo=True).count(), 9)
        self.assertTrue(Sucursal.objects.filter(codigo="LAS_GLORIAS").exists())
        self.assertTrue(Sucursal.objects.filter(codigo="PLAZA_NIO").exists())
        self.assertTrue(Sucursal.objects.filter(codigo="EL_TUNEL").exists())
        self.assertFalse(Sucursal.objects.filter(codigo__in=["GLORIAS", "NIO", "TUNEL"]).exists())
        self.assertTrue(PuntoLogistico.objects.filter(nombre="Sucursal Matriz", radio_geocerca_metros=120).exists())
        self.assertTrue(PuntoLogistico.objects.filter(nombre="Sucursal Guamuchil", notas__contains="Blvd. Rosales 627").exists())
        self.assertTrue(
            PuntoLogistico.objects.filter(
                sucursal__codigo="CRUCERO",
                nombre="Sucursal Bamoa",
                latitud=Decimal("25.702448"),
                longitud=Decimal("-108.313204"),
                radio_geocerca_metros=120,
                notas__contains="https://maps.app.goo.gl/QY5wRXx5rc1j4Xq39",
            ).exists()
        )

    def test_normalize_branch_aliases_migrates_logistica_and_deletes_alias(self):
        canonical = Sucursal.objects.create(codigo="LAS_GLORIAS", nombre="Las Glorias", activa=True)
        alias = Sucursal.objects.create(codigo="GLORIAS", nombre="Sucursal Plaza Las Glorias", activa=True)
        punto = PuntoLogistico.objects.create(
            sucursal=alias,
            nombre="Sucursal Plaza Las Glorias",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.558665",
            longitud="-108.470713",
            radio_geocerca_metros=120,
            activo=True,
        )

        call_command("normalize_branch_aliases", "--execute", stdout=StringIO())

        punto.refresh_from_db()
        self.assertEqual(punto.sucursal_id, canonical.id)
        self.assertFalse(Sucursal.objects.filter(codigo="GLORIAS").exists())

    def test_normalize_branch_aliases_merges_fact_produccion_and_drops_zero_rentabilidad_alias(self):
        canonical = Sucursal.objects.create(codigo="EL_TUNEL", nombre="El Túnel", activa=True)
        alias = Sucursal.objects.create(codigo="TUNEL", nombre="Sucursal El Tunel", activa=True)
        receta = Receta.objects.create(
            nombre="Pastel QA",
            codigo_point="PASTEL-QA-TUNEL",
            hash_contenido="hash-normalize-branch-aliases-tunel",
        )
        FactProduccionDiaria.objects.create(
            fecha=timezone.localdate(),
            sucursal=canonical,
            receta=receta,
            vendido=Decimal("2"),
            transferido=Decimal("0"),
        )
        FactProduccionDiaria.objects.create(
            fecha=timezone.localdate(),
            sucursal=alias,
            receta=receta,
            vendido=Decimal("0"),
            transferido=Decimal("3"),
        )
        SucursalRentabilidad.objects.create(sucursal=canonical, periodo=timezone.localdate().replace(day=1), ventas_brutas=Decimal("100"))
        SucursalRentabilidad.objects.create(sucursal=alias, periodo=timezone.localdate().replace(day=1), ventas_brutas=Decimal("0"))

        call_command("normalize_branch_aliases", "--execute", stdout=StringIO())

        fact = FactProduccionDiaria.objects.get(sucursal=canonical, fecha=timezone.localdate(), receta=receta)
        self.assertEqual(fact.vendido, Decimal("2"))
        self.assertEqual(fact.transferido, Decimal("3"))
        self.assertEqual(SucursalRentabilidad.objects.filter(sucursal=canonical, periodo=timezone.localdate().replace(day=1)).count(), 1)
        self.assertFalse(Sucursal.objects.filter(codigo="TUNEL").exists())


class LogisticaSeedRepartidoresTests(TestCase):
    def test_seed_repartidores_pwa_asigna_jorge_a_matriz_por_default(self):
        matriz = Sucursal.objects.create(codigo="MATRIZ", nombre="Matriz", activa=True)
        Sucursal.objects.create(codigo="COLOSIO", nombre="Colosio", activa=True)

        output = StringIO()
        call_command("seed_repartidores_pwa", stdout=output)

        repartidor = Repartidor.objects.select_related("sucursal", "user").get(user__username="compras.jorge@pollyanasdolce.com")
        self.assertEqual(repartidor.sucursal, matriz)
        self.assertEqual(repartidor.sucursal.codigo, "MATRIZ")


class LogisticaViewsTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="logistica", password="pass123")
        group, _ = Group.objects.get_or_create(name="LOGISTICA")
        self.user.groups.add(group)
        self.client.login(username="logistica", password="pass123")

    def test_dashboard_view_renders_executive_surface(self):
        cliente = Cliente.objects.create(nombre="Cliente Logística")
        pedido = PedidoCliente.objects.create(
            cliente=cliente,
            descripcion="Pedido de reparto",
            estatus=PedidoCliente.ESTATUS_CONFIRMADO,
            monto_estimado=950,
        )
        ruta = RutaEntrega.objects.create(
            nombre="Ruta Centro",
            fecha_ruta=timezone.localdate(),
            chofer="Mario",
            unidad="Van 1",
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
        )
        EntregaRuta.objects.create(
            ruta=ruta,
            pedido=pedido,
            secuencia=1,
            direccion="Sucursal Centro",
            estatus=EntregaRuta.ESTATUS_EN_CAMINO,
            monto_estimado=950,
        )
        ruta.recompute_totals()
        ruta.save(update_fields=["total_entregas", "entregas_completadas", "entregas_incidencia", "monto_estimado_total"])

        resp = self.client.get(reverse("logistica:home"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Logística en control")
        self.assertContains(resp, "Distribución de estatus de ruta")
        self.assertContains(resp, "Distribución de estatus de entrega")
        self.assertContains(resp, "Últimos 7 días")

    def test_dashboard_view_supports_real_focus_filter(self):
        cliente = Cliente.objects.create(nombre="Cliente Incidencia")
        pedido = PedidoCliente.objects.create(cliente=cliente, descripcion="Pedido con incidencia", monto_estimado=500)
        ruta = RutaEntrega.objects.create(
            nombre="Ruta Incidencia",
            fecha_ruta="2026-03-25",
            chofer="Mario",
            unidad="Van 1",
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
        )
        EntregaRuta.objects.create(
            ruta=ruta,
            pedido=pedido,
            secuencia=1,
            direccion="Sucursal Centro",
            estatus=EntregaRuta.ESTATUS_INCIDENCIA,
            monto_estimado=500,
        )
        ruta.recompute_totals()
        ruta.save(update_fields=["total_entregas", "entregas_completadas", "entregas_incidencia", "monto_estimado_total"])

        resp = self.client.get(reverse("logistica:home"), {"enterprise_focus": "INCIDENCIAS"})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.context["enterprise_focus"], "INCIDENCIAS")
        self.assertIsNotNone(resp.context["focus_summary"])
        self.assertContains(resp, "Foco")

    def test_dashboard_view_supports_real_search_filter(self):
        cliente = Cliente.objects.create(nombre="Cliente Busqueda Logística")
        pedido = PedidoCliente.objects.create(cliente=cliente, descripcion="Pedido filtro", monto_estimado=500)
        ruta = RutaEntrega.objects.create(
            nombre="Ruta Busqueda",
            fecha_ruta="2026-03-25",
            chofer="Mario",
            unidad="Van 99",
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
        )
        EntregaRuta.objects.create(
            ruta=ruta,
            pedido=pedido,
            secuencia=1,
            direccion="Sucursal Centro",
            estatus=EntregaRuta.ESTATUS_PENDIENTE,
            monto_estimado=500,
        )
        ruta.recompute_totals()
        ruta.save(update_fields=["total_entregas", "entregas_completadas", "entregas_incidencia", "monto_estimado_total"])

        resp = self.client.get(reverse("logistica:home"), {"q": "Van 99"})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.context["selected_q"], "Van 99")
        self.assertContains(resp, "Ruta o pedido")

    def test_rutas_view_and_create(self):
        sucursal = Sucursal.objects.create(nombre="Sucursal Centro", codigo="SC01")
        punto = PuntoLogistico.objects.create(
            nombre="Sucursal Centro",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            sucursal=sucursal,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )
        resp = self.client.get(reverse("logistica:rutas"))
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Logística · Rutas")
        self.assertContains(resp, "Planear ruta del día")
        self.assertContains(resp, "Sucursales o puntos a visitar")
        self.assertContains(resp, "Filtro operativo")
        self.assertContains(resp, "Rutas de entrega")
        self.assertNotContains(resp, "Centro de mando ERP")
        self.assertNotContains(resp, "Cadena documental ERP")
        self.assertNotContains(resp, "Ruta crítica ERP")
        self.assertNotContains(resp, "Mesa de gobierno ERP")
        self.assertTrue(resp.context["focus_cards"])
        self.assertTrue(resp.context["enterprise_chain"])
        self.assertTrue(resp.context["operational_health_cards"])

        resp_post = self.client.post(
            reverse("logistica:rutas"),
            {
                "nombre": "Ruta Centro",
                "fecha_ruta": "2026-02-24",
                "chofer": "Mario",
                "unidad": "Van 1",
                "km_estimado": "18.5",
                "puntos_ruta": [str(punto.id)],
            },
            follow=True,
        )
        self.assertEqual(resp_post.status_code, 200)
        self.assertContains(resp_post, "Detalle de ruta")
        self.assertContains(resp_post, "Agregar entrega")
        self.assertContains(resp_post, "Entregas de ruta")
        self.assertContains(resp_post, "Sucursal Centro")
        ruta = RutaEntrega.objects.get(nombre="Ruta Centro")
        self.assertTrue(ruta.paradas.filter(punto=punto, orden=1).exists())
        self.assertNotContains(resp_post, "Centro de mando ERP")
        self.assertNotContains(resp_post, "Cadena documental ERP")
        self.assertNotContains(resp_post, "Mesa de gobierno ERP")

    def test_rutas_view_usa_paradas_para_resumen_operativo(self):
        ruta = RutaEntrega.objects.create(
            nombre="Ruta Operativa",
            fecha_ruta=timezone.localdate(),
            total_entregas=0,
            entregas_completadas=0,
            entregas_incidencia=0,
            monto_estimado_total=Decimal("0"),
        )
        sucursal_ok = Sucursal.objects.create(nombre="Sucursal OK", codigo="SOK")
        sucursal_pendiente = Sucursal.objects.create(nombre="Sucursal Pendiente", codigo="SPE")
        sucursal_diff = Sucursal.objects.create(nombre="Sucursal Diferencia", codigo="SDI")
        origen = PointBranch.objects.create(external_id="cedis", name="CEDIS")
        destino = PointBranch.objects.create(external_id="sok", name="Sucursal OK")
        puntos = [
            PuntoLogistico.objects.create(nombre="Sucursal OK", tipo=PuntoLogistico.TIPO_SUCURSAL, sucursal=sucursal_ok, latitud="25.570000", longitud="-108.470000"),
            PuntoLogistico.objects.create(nombre="Sucursal Pendiente", tipo=PuntoLogistico.TIPO_SUCURSAL, sucursal=sucursal_pendiente, latitud="25.571000", longitud="-108.471000"),
            PuntoLogistico.objects.create(nombre="Sucursal Diferencia", tipo=PuntoLogistico.TIPO_SUCURSAL, sucursal=sucursal_diff, latitud="25.572000", longitud="-108.472000"),
            PuntoLogistico.objects.create(nombre="CEDIS", tipo=PuntoLogistico.TIPO_CEDIS, latitud="25.573000", longitud="-108.473000"),
        ]
        estados = [
            ParadaRuta.ENTREGA_ENTREGADA,
            ParadaRuta.ENTREGA_PENDIENTE,
            ParadaRuta.ENTREGA_CON_DIFERENCIA,
            ParadaRuta.ENTREGA_PENDIENTE,
        ]
        paradas = [
            ParadaRuta.objects.create(ruta=ruta, punto=punto, orden=orden, entrega_estado=entrega_estado)
            for orden, (punto, entrega_estado) in enumerate(zip(puntos, estados), start=1)
        ]
        transfer_line = PointTransferLine.objects.create(
            origin_branch=origen,
            destination_branch=destino,
            erp_destination_branch=sucursal_ok,
            transfer_external_id="transfer-ruta-operativa",
            detail_external_id="detail-ruta-operativa",
            source_hash="transfer-ruta-operativa",
            registered_at=timezone.now(),
            item_name="Pastel",
            item_code="PST",
            unit_cost=Decimal("12.50"),
            requested_quantity=Decimal("2.000"),
            sent_quantity=Decimal("2.000"),
        )
        checklist = RutaCargaChecklist.objects.create(ruta=ruta)
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=paradas[0],
            point_transfer_line=transfer_line,
            transfer_external_id=transfer_line.transfer_external_id,
            detail_external_id=transfer_line.detail_external_id,
            source_hash=transfer_line.source_hash,
            item_code=transfer_line.item_code,
            item_name=transfer_line.item_name,
            unit=transfer_line.unit,
            cantidad_solicitada=transfer_line.requested_quantity,
            cantidad_enviada_esperada=transfer_line.sent_quantity,
        )

        resp = self.client.get(reverse("logistica:rutas"))

        ruta_row = list(resp.context["rutas"])[0]
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(ruta_row.paradas_entrega_total, 3)
        self.assertEqual(ruta_row.paradas_entregadas, 1)
        self.assertEqual(ruta_row.paradas_incidencia, 1)
        self.assertEqual(ruta_row.monto_transferido_point, Decimal("25"))
        self.assertContains(resp, "$25.00")

    def test_rutas_view_muestra_y_filtra_bloqueo_point_sin_enviado(self):
        ruta_ok = RutaEntrega.objects.create(nombre="Ruta sin bloqueo Point", fecha_ruta=timezone.localdate())
        ruta_bloqueada = RutaEntrega.objects.create(nombre="Ruta Point Pendiente", fecha_ruta=timezone.localdate())
        sucursal = Sucursal.objects.create(nombre="Sucursal Point", codigo="SPT")
        punto = PuntoLogistico.objects.create(
            nombre="Sucursal Point",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            sucursal=sucursal,
            latitud="25.570000",
            longitud="-108.470000",
        )
        parada = ParadaRuta.objects.create(ruta=ruta_bloqueada, punto=punto, orden=1)
        checklist = RutaCargaChecklist.objects.create(ruta=ruta_bloqueada, estatus=RutaCargaChecklist.ESTATUS_EN_REVISION)
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="point-sin-enviado-lista",
            item_code="PZA1",
            item_name="Producto sin enviado",
            unit="pz",
            cantidad_solicitada="3.000",
            cantidad_enviada_esperada="0.000",
        )

        resp = self.client.get(reverse("logistica:rutas"), {"enterprise_focus": "POINT_BLOQUEO"})

        rutas = list(resp.context["rutas"])
        self.assertEqual(resp.status_code, 200)
        self.assertEqual([ruta.id for ruta in rutas], [ruta_bloqueada.id])
        self.assertEqual(rutas[0].point_bloqueo_lineas, 1)
        self.assertContains(resp, "Point sin enviado")
        self.assertContains(resp, "1 sin enviado")
        self.assertNotContains(resp, ruta_ok.nombre)

    def test_rutas_selector_omite_repartidores_con_usuario_inactivo(self):
        sucursal = Sucursal.objects.create(nombre="Sucursal Centro", codigo="SC01")
        activo_user = User.objects.create_user(username="rep.activo", password="pass123")
        inactivo_user = User.objects.create_user(username="rep.inactivo", password="pass123", is_active=False)
        activo = Repartidor.objects.create(user=activo_user, sucursal=sucursal)
        inactivo = Repartidor.objects.create(user=inactivo_user, sucursal=sucursal)

        resp = self.client.get(reverse("logistica:rutas"))

        ids = {repartidor.id for repartidor in resp.context["repartidores"]}
        self.assertIn(activo.id, ids)
        self.assertNotIn(inactivo.id, ids)

    def test_rutas_create_requires_route_points(self):
        resp_post = self.client.post(
            reverse("logistica:rutas"),
            {
                "nombre": "Ruta sin paradas",
                "fecha_ruta": "2026-02-24",
            },
            follow=True,
        )
        self.assertEqual(resp_post.status_code, 200)
        self.assertContains(resp_post, "Selecciona al menos una sucursal o punto")
        self.assertFalse(RutaEntrega.objects.filter(nombre="Ruta sin paradas").exists())

    def test_rutas_create_respects_visit_order(self):
        punto_sur = PuntoLogistico.objects.create(
            nombre="Sucursal Sur",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.560000",
            longitud="-108.460000",
            radio_geocerca_metros=120,
        )
        punto_norte = PuntoLogistico.objects.create(
            nombre="Sucursal Norte",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.580000",
            longitud="-108.480000",
            radio_geocerca_metros=120,
        )

        resp_post = self.client.post(
            reverse("logistica:rutas"),
            {
                "nombre": "Ruta con orden",
                "fecha_ruta": "2026-02-24",
                "puntos_ruta": [str(punto_sur.id), str(punto_norte.id)],
                f"punto_orden_{punto_sur.id}": "2",
                f"punto_orden_{punto_norte.id}": "1",
            },
            follow=True,
        )

        self.assertEqual(resp_post.status_code, 200)
        ruta = RutaEntrega.objects.get(nombre="Ruta con orden")
        paradas = list(ruta.paradas.order_by("orden").values_list("punto_id", "orden"))
        self.assertEqual(paradas, [(punto_norte.id, 1), (punto_sur.id, 2)])
        self.assertEqual(ruta.ruta_programada_fuente, "FALLBACK")
        self.assertGreater(ruta.ruta_programada_distancia_metros, 0)
        self.assertGreater(ruta.ruta_programada_duracion_segundos, 0)
        self.assertGreater(ruta.km_estimado, 0)

    def test_rutas_create_deduplicates_repeated_route_points(self):
        punto = PuntoLogistico.objects.create(
            nombre="Sucursal Repetida",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )

        resp_post = self.client.post(
            reverse("logistica:rutas"),
            {
                "nombre": "Ruta sin duplicados",
                "fecha_ruta": "2026-02-24",
                "puntos_ruta": [str(punto.id), str(punto.id)],
                f"punto_orden_{punto.id}": "1",
            },
            follow=True,
        )

        self.assertEqual(resp_post.status_code, 200)
        self.assertContains(resp_post, "Se ignoraron puntos repetidos")
        ruta = RutaEntrega.objects.get(nombre="Ruta sin duplicados")
        self.assertEqual(ruta.paradas.filter(punto=punto).count(), 1)

    def test_rutas_create_bloquea_segunda_vuelta_sin_transferencia_point_nueva(self):
        sucursal = Sucursal.objects.create(nombre="Sucursal Colosio", codigo="COL")
        punto = PuntoLogistico.objects.create(
            nombre="Sucursal Colosio",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            sucursal=sucursal,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )
        ruta_previa = RutaEntrega.objects.create(nombre="CEDIS-Colosio", fecha_ruta="2026-06-21", estatus=RutaEntrega.ESTATUS_COMPLETADA)
        ParadaRuta.objects.create(ruta=ruta_previa, punto=punto, orden=1)

        resp_post = self.client.post(
            reverse("logistica:rutas"),
            {"nombre": "CEDIS-COLOSIO", "fecha_ruta": "2026-06-21", "puntos_ruta": [str(punto.id)]},
            follow=True,
        )

        self.assertEqual(resp_post.status_code, 200)
        self.assertContains(resp_post, "no hay transferencia Point nueva")
        self.assertFalse(RutaEntrega.objects.filter(nombre="CEDIS-COLOSIO").exists())

    def test_rutas_create_permite_segunda_vuelta_con_transferencia_point_nueva(self):
        sucursal = Sucursal.objects.create(nombre="Sucursal Colosio", codigo="COL")
        punto = PuntoLogistico.objects.create(
            nombre="Sucursal Colosio",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            sucursal=sucursal,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )
        ruta_previa = RutaEntrega.objects.create(nombre="CEDIS-Colosio", fecha_ruta="2026-06-21", estatus=RutaEntrega.ESTATUS_COMPLETADA)
        ParadaRuta.objects.create(ruta=ruta_previa, punto=punto, orden=1)
        origin = PointBranch.objects.create(external_id="CEDIS-2DA", name="CEDIS")
        destination = PointBranch.objects.create(external_id="COL-2DA", name="Sucursal Colosio", erp_branch=sucursal)
        PointTransferLine.objects.create(
            origin_branch=origin,
            destination_branch=destination,
            erp_destination_branch=sucursal,
            transfer_external_id="35967",
            detail_external_id="1",
            source_hash="point-colosio-segunda-vuelta",
            registered_at=timezone.make_aware(datetime(2026, 6, 21, 20, 2)),
            sent_at=timezone.make_aware(datetime(2026, 6, 21, 20, 3)),
            item_name="Pastel",
            requested_quantity="1.000",
            sent_quantity="1.000",
            is_open=True,
            is_cancelled=False,
            raw_payload={"transfer": {"isEnviado": True}},
        )

        resp_post = self.client.post(
            reverse("logistica:rutas"),
            {"nombre": "CEDIS-COLOSIO", "fecha_ruta": "2026-06-21", "puntos_ruta": [str(punto.id)]},
            follow=True,
        )

        self.assertEqual(resp_post.status_code, 200)
        self.assertTrue(RutaEntrega.objects.filter(nombre="CEDIS-COLOSIO").exists())

    def test_segunda_vuelta_no_infiere_enviado_por_cantidad_positiva(self):
        sucursal = Sucursal.objects.create(nombre="Sucursal transición", codigo="TRANS")
        punto = PuntoLogistico.objects.create(
            nombre=sucursal.nombre,
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            sucursal=sucursal,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )
        origin = PointBranch.objects.create(external_id="CEDIS-TRANS", name="CEDIS")
        destination = PointBranch.objects.create(
            external_id="TRANS-DEST",
            name=sucursal.nombre,
            erp_branch=sucursal,
        )
        PointTransferLine.objects.create(
            origin_branch=origin,
            destination_branch=destination,
            erp_destination_branch=sucursal,
            transfer_external_id="TRANS-SIN-ENVIAR",
            detail_external_id="TRANS-D-1",
            source_hash="point-positivo-sin-transicion",
            registered_at=timezone.now(),
            sent_at=None,
            item_name="Pastel",
            requested_quantity="1.000",
            sent_quantity="1.000",
            is_open=True,
            is_cancelled=False,
            raw_payload={"transfer": {"isEnviado": False}},
        )

        self.assertFalse(
            ruta_tiene_movimiento_point_nuevo(fecha=timezone.localdate(), puntos=[punto])
        )

    def test_ruta_detail_add_entrega(self):
        cliente = Cliente.objects.create(nombre="Cliente Logística")
        pedido = PedidoCliente.objects.create(cliente=cliente, descripcion="Pastel para entrega")
        ruta = RutaEntrega.objects.create(nombre="Ruta Norte", fecha_ruta="2026-02-24")
        resp = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {
                "action": "add_entrega",
                "pedido_id": str(pedido.id),
                "secuencia": "1",
                "cliente_nombre": "",
                "direccion": "Sucursal Centro",
                "estatus": "PENDIENTE",
                "monto_estimado": "950",
            },
            follow=True,
        )
        self.assertEqual(resp.status_code, 200)
        ruta.refresh_from_db()
        self.assertEqual(ruta.total_entregas, 1)
        self.assertContains(resp, "Agregar entrega")
        self.assertContains(resp, "Entregas de ruta")
        self.assertNotContains(resp, "Cadena documental ERP")
        self.assertNotContains(resp, "Centro de mando ERP")
        self.assertNotContains(resp, "Madurez ERP de logística")
        self.assertTrue(resp.context["enterprise_chain"])
        self.assertIn("dependency_status", resp.context["enterprise_chain"][0])
        self.assertTrue(resp.context["operational_health_cards"])

    def test_ruta_print_renderiza_orden_de_paradas(self):
        sucursal = Sucursal.objects.create(nombre="Sucursal Leyva", codigo="LEY")
        punto = PuntoLogistico.objects.create(
            nombre="Sucursal Leyva",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            sucursal=sucursal,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )
        ruta = RutaEntrega.objects.create(nombre="CEDIS - Sucursales", fecha_ruta="2026-06-22")
        ParadaRuta.objects.create(ruta=ruta, punto=punto, orden=1)

        resp = self.client.get(reverse("logistica:ruta_print", kwargs={"pk": ruta.id}))

        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "ORDEN DE PARADAS")
        self.assertContains(resp, "Sucursal Leyva")
        self.assertContains(resp, "Esta hoja NO valida cantidades")
        self.assertNotContains(resp, "Carga esperada")

    def test_rutas_view_can_focus_enterprise_subset(self):
        cliente = Cliente.objects.create(nombre="Cliente Focus")
        pedido = PedidoCliente.objects.create(cliente=cliente, descripcion="Entrega foco")
        ruta = RutaEntrega.objects.create(nombre="Ruta Focus", fecha_ruta="2026-02-24")
        self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {
                "action": "add_entrega",
                "pedido_id": str(pedido.id),
                "secuencia": "1",
                "direccion": "Centro",
                "estatus": "INCIDENCIA",
                "monto_estimado": "100",
            },
            follow=True,
        )

        resp = self.client.get(reverse("logistica:rutas"), {"enterprise_focus": "INCIDENCIAS"})
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "Focos operativos")
        self.assertContains(resp, "Quitar foco")
        self.assertEqual(resp.context["enterprise_focus"], "INCIDENCIAS")
        self.assertIsNotNone(resp.context["focus_summary"])

    def test_redirect_when_anonymous(self):
        self.client.logout()
        resp = self.client.get(reverse("logistica:rutas"))
        self.assertEqual(resp.status_code, 302)
        self.assertIn("/login/", resp.url)


class LogisticaPwaApiTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="repartidor.api", password="pass123")
        group, _ = Group.objects.get_or_create(name="repartidor")
        self.user.groups.add(group)
        self.sucursal = Sucursal.objects.create(codigo="QA-LOG", nombre="QA Logística", activa=True)
        self.unidad = Unidad.objects.create(codigo="QA-LOG-1", descripcion="Unidad QA", sucursal=self.sucursal)
        self.repartidor = Repartidor.objects.create(user=self.user, sucursal=self.sucursal, unidad_asignada=self.unidad)
        self.repartidor.licencia_expiracion = timezone.localdate() + timezone.timedelta(days=30)
        self.repartidor.save(update_fields=["licencia_expiracion"])
        self.client.force_login(self.user)

    def test_bitacora_activa_without_open_shift_returns_null_not_404(self):
        response = self.client.get(reverse("api_logistica_bitacora_salida_activa"))

        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.content, b"")

    def test_session_token_bridges_unified_app_login_to_logistica_pwa(self):
        response = self.client.get(reverse("api_logistica_auth_session_token"))

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("access", payload)
        self.assertIn("refresh", payload)

        perfil = self.client.get(reverse("api_logistica_mi_perfil"), HTTP_AUTHORIZATION=f"Bearer {payload['access']}")
        self.assertEqual(perfil.status_code, 200)
        self.assertEqual(perfil.json()["user"]["username"], self.user.username)

        self.client.logout()

    def test_bitacora_salida_bloquea_unidad_distinta_si_hay_ruta_activa(self):
        unidad_ruta = Unidad.objects.create(codigo="QA-RUTA", descripcion="Unidad ruta", sucursal=self.sucursal)
        RutaEntrega.objects.create(
            nombre="Ruta Unidad Correcta",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
            repartidor=self.repartidor,
            unidad_operativa=unidad_ruta,
        )

        response = self.client.post(
            reverse("api_logistica_bitacora_salida"),
            {
                "unidad": self.unidad.id,
                "km_salida": "1000",
                "nivel_gas_salida": "lleno",
                "latitud_salida": "25.570000",
                "longitud_salida": "-108.470000",
                "foto_tablero_salida": SimpleUploadedFile("tablero.gif", VALID_GIF, content_type="image/gif"),
            },
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "unidad_ruta_distinta")
        self.assertEqual(response.json()["unidad_requerida"]["codigo"], "QA-RUTA")
        self.assertFalse(BitacoraSalidaLlegada.objects.filter(repartidor=self.repartidor).exists())

    def test_bitacora_salida_permite_unidad_asignada_a_ruta_activa(self):
        RutaEntrega.objects.create(
            nombre="Ruta Unidad Correcta",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )

        response = self.client.post(
            reverse("api_logistica_bitacora_salida"),
            {
                "unidad": self.unidad.id,
                "km_salida": "1000",
                "nivel_gas_salida": "lleno",
                "latitud_salida": "25.570000",
                "longitud_salida": "-108.470000",
                "foto_tablero_salida": SimpleUploadedFile("tablero.gif", VALID_GIF, content_type="image/gif"),
            },
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json()["unidad"], self.unidad.id)

    def test_bitacora_salida_abre_turno_sin_liberar_ruta_planeada(self):
        ruta = RutaEntrega.objects.create(
            nombre="Ruta PWA",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_PLANEADA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )
        punto = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Sucursal PWA",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )
        parada = ParadaRuta.objects.create(ruta=ruta, punto=punto, orden=1)
        checklist = RutaCargaChecklist.objects.create(ruta=ruta, estatus=RutaCargaChecklist.ESTATUS_EN_REVISION)
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="pwa-cargada",
            item_code="PZA1",
            item_name="Producto cargado",
            unit="PZA",
            cantidad_solicitada="1.000",
            cantidad_enviada_esperada="1.000",
            cantidad_cargada="1.000",
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
            validado_por=self.user,
            validado_en=timezone.now(),
        )
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="pwa-pendiente",
            item_code="PZA2",
            item_name="Producto pendiente",
            unit="PZA",
            cantidad_solicitada="1.000",
            cantidad_enviada_esperada="1.000",
        )

        response = self.client.post(
            reverse("api_logistica_bitacora_salida"),
            {
                "unidad": self.unidad.id,
                "km_salida": "1000",
                "nivel_gas_salida": "lleno",
                "latitud_salida": "25.570000",
                "longitud_salida": "-108.470000",
                "foto_tablero_salida": SimpleUploadedFile("tablero.gif", VALID_GIF, content_type="image/gif"),
            },
        )

        self.assertEqual(response.status_code, 201)
        ruta.refresh_from_db()
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertIsNone(ruta.bitacora_salida_id)
        self.assertFalse(EventoRuta.objects.filter(ruta=ruta, tipo=EventoRuta.TIPO_SALIDA).exists())

        liberar = self.client.post(reverse("api_logistica_bitacora_salida_liberar_ruta"))

        self.assertEqual(liberar.status_code, 400)
        self.assertIn("confirma todas las líneas de carga", liberar.json()["mensaje"])
        ruta.refresh_from_db()
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertFalse(EventoRuta.objects.filter(ruta=ruta, tipo=EventoRuta.TIPO_SALIDA).exists())

    def test_bitacora_salida_no_libera_ruta_sin_carga_confirmada(self):
        ruta = RutaEntrega.objects.create(
            nombre="Ruta Sin Carga",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_PLANEADA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )
        punto = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Sucursal Bloqueo",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )
        parada = ParadaRuta.objects.create(ruta=ruta, punto=punto, orden=1)
        checklist = RutaCargaChecklist.objects.create(ruta=ruta, estatus=RutaCargaChecklist.ESTATUS_EN_REVISION)
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="pwa-pendiente-unico",
            item_code="PZA1",
            item_name="Producto pendiente",
            unit="PZA",
            cantidad_solicitada="1.000",
            cantidad_enviada_esperada="1.000",
        )

        response = self.client.post(
            reverse("api_logistica_bitacora_salida"),
            {
                "unidad": self.unidad.id,
                "km_salida": "1000",
                "nivel_gas_salida": "lleno",
                "latitud_salida": "25.570000",
                "longitud_salida": "-108.470000",
                "foto_tablero_salida": SimpleUploadedFile("tablero.gif", VALID_GIF, content_type="image/gif"),
            },
        )

        self.assertEqual(response.status_code, 201)
        ruta.refresh_from_db()
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertIsNone(ruta.bitacora_salida_id)
        self.assertTrue(BitacoraSalidaLlegada.objects.filter(repartidor=self.repartidor, cerrada=False).exists())

        liberar = self.client.post(reverse("api_logistica_bitacora_salida_liberar_ruta"))

        self.assertEqual(liberar.status_code, 400)
        self.assertEqual(liberar.json()["error"], "ruta_no_liberada")
        self.assertIn("confirma todas las líneas de carga", liberar.json()["mensaje"])

    def test_bitacora_salida_no_libera_ruta_si_point_no_ha_enviado(self):
        ruta = RutaEntrega.objects.create(
            nombre="Ruta Point Sin Enviado",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_PLANEADA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )
        punto = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Sucursal Point Sin Enviado",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )
        parada = ParadaRuta.objects.create(ruta=ruta, punto=punto, orden=1)
        checklist = RutaCargaChecklist.objects.create(ruta=ruta, estatus=RutaCargaChecklist.ESTATUS_EN_REVISION)
        linea_checklist = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="pwa-point-sin-enviado",
            item_code="PZA1",
            item_name="Producto pendiente Point",
            unit="PZA",
            cantidad_solicitada="1.000",
            cantidad_enviada_esperada="0.000",
        )
        origin = PointBranch.objects.create(
            external_id="CEDIS-pwa-point-sin-enviado",
            name="CEDIS",
        )
        destination = PointBranch.objects.create(
            external_id="SUC-pwa-point-sin-enviado",
            name=self.sucursal.nombre,
            erp_branch=self.sucursal,
        )
        point_line = PointTransferLine.objects.create(
            origin_branch=origin,
            destination_branch=destination,
            erp_destination_branch=self.sucursal,
            transfer_external_id="T-pwa-point-sin-enviado",
            detail_external_id="D-pwa-point-sin-enviado",
            source_hash="pwa-point-sin-enviado",
            registered_at=timezone.now(),
            sent_at=None,
            item_name="Producto pendiente Point",
            item_code="PZA1",
            unit="PZA",
            requested_quantity="1.000",
            sent_quantity="0.000",
            received_quantity="0.000",
            is_open=True,
            raw_payload={"transfer": {"isEnviado": False}},
        )
        linea_checklist.point_transfer_line = point_line
        linea_checklist.save(update_fields=["point_transfer_line", "actualizado_en"])
        BitacoraSalidaLlegada.objects.create(
            repartidor=self.repartidor,
            unidad=self.unidad,
            km_salida=1000,
            nivel_gas_salida="lleno",
            foto_tablero_salida=SimpleUploadedFile("tablero.gif", VALID_GIF, content_type="image/gif"),
        )

        liberar = self.client.post(reverse("api_logistica_bitacora_salida_liberar_ruta"))

        self.assertEqual(liberar.status_code, 400)
        self.assertEqual(liberar.json()["error"], "ruta_no_liberada")
        self.assertIn("Point", liberar.json()["mensaje"])
        self.assertIn("enviada", liberar.json()["mensaje"])

    def test_bitacora_salida_permite_turno_mandado_si_ruta_planeada_usa_otra_unidad(self):
        unidad_ruta = Unidad.objects.create(codigo="QA-RUTA", descripcion="Unidad ruta", sucursal=self.sucursal)
        ruta = RutaEntrega.objects.create(
            nombre="Ruta Despues",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_PLANEADA,
            repartidor=self.repartidor,
            unidad_operativa=unidad_ruta,
        )

        response = self.client.post(
            reverse("api_logistica_bitacora_salida"),
            {
                "unidad": self.unidad.id,
                "km_salida": "1000",
                "nivel_gas_salida": "lleno",
                "latitud_salida": "25.570000",
                "longitud_salida": "-108.470000",
                "foto_tablero_salida": SimpleUploadedFile("tablero.gif", VALID_GIF, content_type="image/gif"),
            },
        )

        self.assertEqual(response.status_code, 201)
        ruta.refresh_from_db()
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertIsNone(ruta.bitacora_salida_id)

        liberar = self.client.post(reverse("api_logistica_bitacora_salida_liberar_ruta"))

        self.assertEqual(liberar.status_code, 400)
        self.assertEqual(liberar.json()["error"], "unidad_ruta_distinta")

    def test_bitacora_salida_libera_ruta_con_turno_abierto_y_carga(self):
        ruta = RutaEntrega.objects.create(
            nombre="Ruta con turno",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_PLANEADA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )
        punto = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Sucursal con carga",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )
        parada = ParadaRuta.objects.create(ruta=ruta, punto=punto, orden=1)
        checklist = RutaCargaChecklist.objects.create(
            ruta=ruta,
            estatus=RutaCargaChecklist.ESTATUS_CONFIRMADA,
            confirmado_por=self.user,
            confirmado_en=timezone.now(),
        )
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="pwa-turno-abierto",
            item_code="PZA1",
            item_name="Producto cargado",
            unit="PZA",
            cantidad_solicitada="1.000",
            cantidad_enviada_esperada="1.000",
            cantidad_cargada="1.000",
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
            validado_por=self.user,
            validado_en=timezone.now(),
        )
        bitacora = BitacoraSalidaLlegada.objects.create(
            repartidor=self.repartidor,
            unidad=self.unidad,
            km_salida=1000,
            nivel_gas_salida="lleno",
            foto_tablero_salida=SimpleUploadedFile("tablero.gif", VALID_GIF, content_type="image/gif"),
        )

        response = self.client.post(reverse("api_logistica_bitacora_salida_liberar_ruta"))

        self.assertEqual(response.status_code, 200)
        ruta.refresh_from_db()
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)
        self.assertEqual(ruta.bitacora_salida_id, bitacora.id)

    def test_liberar_ruta_en_ruta_rechaza_turno_de_otra_unidad(self):
        unidad_ruta = Unidad.objects.create(codigo="QA-RUTA", descripcion="Unidad ruta", sucursal=self.sucursal)
        RutaEntrega.objects.create(
            nombre="Ruta activa",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
            repartidor=self.repartidor,
            unidad_operativa=unidad_ruta,
        )
        BitacoraSalidaLlegada.objects.create(
            repartidor=self.repartidor,
            unidad=self.unidad,
            km_salida=1000,
            nivel_gas_salida="lleno",
            foto_tablero_salida=SimpleUploadedFile("tablero.gif", VALID_GIF, content_type="image/gif"),
        )

        response = self.client.post(reverse("api_logistica_bitacora_salida_liberar_ruta"))

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "unidad_ruta_distinta")

    def test_bitacora_activa_alerta_si_ruta_sigue_planeada(self):
        bitacora = BitacoraSalidaLlegada.objects.create(
            repartidor=self.repartidor,
            unidad=self.unidad,
            km_salida=1000,
            nivel_gas_salida="lleno",
            foto_tablero_salida=SimpleUploadedFile("tablero.gif", VALID_GIF, content_type="image/gif"),
        )
        ruta = RutaEntrega.objects.create(
            nombre="Ruta Planeada Con Turno",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_PLANEADA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
            bitacora_salida=bitacora,
        )

        response = self.client.get(reverse("api_logistica_bitacora_salida_activa"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["ruta_folio"], ruta.folio)
        self.assertIn("sigue planeada", response.json()["alerta_operativa"])

    def test_occasional_driver_with_repartidor_profile_can_operate_without_repartidor_group(self):
        user = User.objects.create_user(username="conductora.ocasional", password="pass123")
        user.groups.add(Group.objects.get_or_create(name="PRODUCCION")[0])
        Repartidor.objects.create(
            user=user,
            sucursal=self.sucursal,
            tipo_identidad=Repartidor.TIPO_EMPLEADO_CONDUCTOR_OCASIONAL,
            motivo_autorizacion="Uso ocasional de unidades",
            autorizado_por="Direccion",
        )

        self.assertTrue(_can_operate_pwa(user))

    def test_session_token_rejects_users_without_logistica_access(self):
        self.client.logout()
        other = User.objects.create_user(username="sin.logistica", password="pass123")
        self.client.force_login(other)

        response = self.client.get(reverse("api_logistica_auth_session_token"))

        self.assertEqual(response.status_code, 403)

    def test_mantenimiento_user_can_open_logistica_pwa_without_logistica_module(self):
        self.client.logout()
        user = User.objects.create_user(username="mant.pwa", password="pass123")
        UserModuleAccess.objects.create(user=user, module="mantenimiento", access=ACCESS_MANAGE)
        self.client.force_login(user)

        response = self.client.get(reverse("logistica:pwa_app"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Logística")


class LogisticaControlRutasTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="ruta.control", password="pass123")
        self.user.groups.add(Group.objects.get_or_create(name="repartidor")[0])
        self.user.groups.add(Group.objects.get_or_create(name="LOGISTICA")[0])
        self.sucursal = Sucursal.objects.create(codigo="CTRL-LOG", nombre="Control Logística", activa=True)
        self.unidad = Unidad.objects.create(codigo="CTRL-01", descripcion="Unidad control", sucursal=self.sucursal)
        self.repartidor = Repartidor.objects.create(user=self.user, sucursal=self.sucursal, unidad_asignada=self.unidad)
        self.user_acompanante = User.objects.create_user(username="ruta.acompanante", password="pass123")
        self.user_acompanante.groups.add(Group.objects.get_or_create(name="repartidor")[0])
        self.acompanante = Repartidor.objects.create(user=self.user_acompanante, sucursal=self.sucursal)
        self.bitacora = BitacoraSalidaLlegada.objects.create(
            repartidor=self.repartidor,
            unidad=self.unidad,
            km_salida=1000,
            nivel_gas_salida="lleno",
            foto_tablero_salida=SimpleUploadedFile("tablero.gif", b"gif", content_type="image/gif"),
        )
        self.ruta = RutaEntrega.objects.create(
            nombre="Ruta Control",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
            bitacora_salida=self.bitacora,
        )
        self.punto = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Sucursal Control",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )
        self.parada = ParadaRuta.objects.create(ruta=self.ruta, punto=self.punto, orden=1)

    def _crear_transferencia_point_abierta(self, *, sucursal=None, item_name="Pastel Snicker chico", source_hash="transfer-snicker-1", registered_at=None):
        sucursal = sucursal or self.sucursal
        origin = PointBranch.objects.create(external_id=f"CEDIS-{source_hash}", name="CEDIS", erp_branch=None)
        destination = PointBranch.objects.create(external_id=f"SUC-{source_hash}", name=sucursal.nombre, erp_branch=sucursal)
        return PointTransferLine.objects.create(
            origin_branch=origin,
            destination_branch=destination,
            erp_origin_branch=None,
            erp_destination_branch=sucursal,
            transfer_external_id=f"T-{source_hash}",
            detail_external_id=f"D-{source_hash}",
            source_hash=source_hash,
            registered_at=registered_at or timezone.now(),
            sent_at=timezone.now(),
            item_name=item_name,
            item_code="SNICK-CH",
            unit="pz",
            requested_quantity="5.000",
            sent_quantity="5.000",
            received_quantity="0.000",
            is_open=True,
            is_received=False,
            is_cancelled=False,
            is_finalized=False,
        )

    def _crear_solicitud_cedis(self, *, ruta=None, sucursal=None, estado=SolicitudReabastoCedis.ESTADO_ENVIADA, cantidad="5.000"):
        ruta = ruta or self.ruta
        sucursal = sucursal or self.sucursal
        receta = Receta.objects.create(
            nombre=f"Pastel Snicker chico {sucursal.codigo}",
            codigo_point="SNICK-CH",
            hash_contenido=f"hash-logistica-carga-{ruta.id}-{sucursal.id}-{estado}",
        )
        solicitud = SolicitudReabastoCedis.objects.create(
            fecha_operacion=ruta.fecha_ruta,
            sucursal=sucursal,
            estado=estado,
            creado_por=self.user,
        )
        linea = SolicitudReabastoCedisLinea.objects.create(
            solicitud=solicitud,
            receta=receta,
            sugerido=cantidad,
            solicitado=cantidad,
            justificacion="Cierre sucursal",
        )
        return solicitud, linea, receta

    def _crear_linea_carga_con_transferencia_recibida(
        self,
        *,
        source_hash="transfer-recibida-1",
        sent_quantity="5.000",
        loaded_quantity="5.000",
        received_quantity="5.000",
        is_received=True,
    ):
        origin = PointBranch.objects.create(external_id=f"CEDIS-R-{source_hash}", name="CEDIS", erp_branch=None)
        destination = PointBranch.objects.create(external_id=f"SUC-R-{source_hash}", name=self.sucursal.nombre, erp_branch=self.sucursal)
        sync_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_TRANSFERS,
            status=PointSyncJob.STATUS_SUCCESS,
            result_summary={},
        )
        transfer_line = PointTransferLine.objects.create(
            origin_branch=origin,
            destination_branch=destination,
            erp_origin_branch=None,
            erp_destination_branch=self.sucursal,
            sync_job=sync_job,
            transfer_external_id=f"T-R-{source_hash}",
            detail_external_id=f"D-R-{source_hash}",
            source_hash=source_hash,
            registered_at=timezone.now(),
            sent_at=timezone.now(),
            received_at=timezone.now() if is_received else None,
            item_name="Pastel Snicker chico",
            item_code="SNICK-CH",
            unit="pz",
            requested_quantity=sent_quantity,
            sent_quantity=sent_quantity,
            received_quantity=received_quantity,
            is_open=not is_received,
            is_received=is_received,
            is_cancelled=False,
            is_finalized=is_received,
        )
        checklist = RutaCargaChecklist.objects.create(ruta=self.ruta, estatus=RutaCargaChecklist.ESTATUS_CONFIRMADA)
        linea = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=self.parada,
            point_transfer_line=transfer_line,
            transfer_external_id=transfer_line.transfer_external_id,
            detail_external_id=transfer_line.detail_external_id,
            source_hash=transfer_line.source_hash,
            item_code=transfer_line.item_code,
            item_name=transfer_line.item_name,
            unit=transfer_line.unit,
            cantidad_solicitada=transfer_line.requested_quantity,
            cantidad_enviada_esperada=transfer_line.sent_quantity,
            cantidad_cargada=loaded_quantity,
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
            validado_por=self.user,
            validado_en=timezone.now(),
        )
        return checklist, linea, transfer_line

    def _crear_ruta_planeada_para_carga(self):
        self.ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
        self.ruta.save(update_fields=["estatus", "updated_at"])
        ruta = RutaEntrega.objects.create(
            nombre="Ruta Carga Point",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_PLANEADA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )
        parada = ParadaRuta.objects.create(ruta=ruta, punto=self.punto, orden=1)
        return ruta, parada

    def _registrar_llegada_geocerca(self, parada=None):
        parada = parada or self.parada
        parada.estado = ParadaRuta.ESTADO_VISITADA
        parada.hora_llegada_real = timezone.now()
        parada.distancia_llegada_metros = 0
        parada.save(update_fields=["estado", "hora_llegada_real", "distancia_llegada_metros", "actualizado_en"])
        return EventoRuta.objects.create(
            ruta=parada.ruta,
            parada=parada,
            tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            descripcion=f"Llegada detectada en {parada.punto_nombre_snapshot}.",
            creado_por=self.user,
        )

    def test_distancia_metros_detecta_punto_cercano(self):
        self.assertLess(distancia_metros("25.570010", "-108.470010", self.punto.latitud, self.punto.longitud), 5)

    @override_settings(LOGISTICA_FALLBACK_SPEED_KMH=36)
    def test_ruta_programada_fallback_calcula_tiempo_por_velocidad_configurada(self):
        punto_2 = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Sucursal Control Dos",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.580000",
            longitud="-108.480000",
            radio_geocerca_metros=120,
        )
        ruta = RutaEntrega.objects.create(nombre="Ruta Fallback Tiempo", fecha_ruta=timezone.localdate())
        ParadaRuta.objects.create(ruta=ruta, punto=self.punto, orden=1)
        ParadaRuta.objects.create(ruta=ruta, punto=punto_2, orden=2)

        from logistica.services_google_routes import recalcular_ruta_programada

        recalcular_ruta_programada(ruta)
        ruta.refresh_from_db()

        self.assertEqual(ruta.ruta_programada_fuente, "FALLBACK")
        self.assertGreater(ruta.ruta_programada_distancia_metros, 0)
        self.assertEqual(ruta.ruta_programada_duracion_segundos, int(round((ruta.ruta_programada_distancia_metros / 1000) / 36 * 3600)))

    def test_resumen_tiempos_ruta_usa_promedio_historico_en_punto(self):
        llegada = timezone.now() - timezone.timedelta(hours=2)
        ruta_historica = RutaEntrega.objects.create(nombre="Ruta Histórica", fecha_ruta=timezone.localdate() - timezone.timedelta(days=1))
        ParadaRuta.objects.create(
            ruta=ruta_historica,
            punto=self.punto,
            orden=1,
            hora_llegada_real=llegada,
            hora_salida_real=llegada + timezone.timedelta(minutes=24),
            estado=ParadaRuta.ESTADO_VISITADA,
        )
        self.ruta.ruta_programada_duracion_segundos = 1800
        self.ruta.save(update_fields=["ruta_programada_duracion_segundos", "updated_at"])

        resumen = resumen_tiempos_ruta(self.ruta)

        self.assertEqual(resumen.transito_programado_minutos, 30)
        self.assertEqual(resumen.surtido_estimado_minutos, 24)
        self.assertEqual(resumen.total_operativo_estimado_minutos, 54)
        self.assertEqual(resumen.paradas[0].promedio_surtido_minutos, 24)

    def test_resumen_tiempos_no_contamina_promedio_con_parada_actual(self):
        llegada = timezone.now() - timezone.timedelta(hours=2)
        ruta_historica = RutaEntrega.objects.create(nombre="Ruta Histórica Promedio", fecha_ruta=timezone.localdate() - timezone.timedelta(days=1))
        ParadaRuta.objects.create(
            ruta=ruta_historica,
            punto=self.punto,
            orden=1,
            hora_llegada_real=llegada,
            hora_salida_real=llegada + timezone.timedelta(minutes=24),
            estado=ParadaRuta.ESTADO_VISITADA,
        )
        self.parada.hora_llegada_real = llegada + timezone.timedelta(hours=1)
        self.parada.hora_salida_real = self.parada.hora_llegada_real + timezone.timedelta(minutes=60)
        self.parada.save(update_fields=["hora_llegada_real", "hora_salida_real", "actualizado_en"])

        resumen = resumen_tiempos_ruta(self.ruta)

        self.assertEqual(resumen.paradas[0].permanencia_real_minutos, 60)
        self.assertEqual(resumen.paradas[0].promedio_surtido_minutos, 24)

    def test_ruta_detail_muestra_tiempos_de_transito_y_surtido(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        llegada = timezone.now() - timezone.timedelta(hours=2)
        ruta_historica = RutaEntrega.objects.create(nombre="Ruta Histórica Detail", fecha_ruta=timezone.localdate() - timezone.timedelta(days=1))
        ParadaRuta.objects.create(
            ruta=ruta_historica,
            punto=self.punto,
            orden=1,
            hora_llegada_real=llegada,
            hora_salida_real=llegada + timezone.timedelta(minutes=24),
            estado=ParadaRuta.ESTADO_VISITADA,
        )
        self.ruta.ruta_programada_duracion_segundos = 1800
        self.ruta.save(update_fields=["ruta_programada_duracion_segundos", "updated_at"])

        response = self.client.get(reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Tránsito programado")
        self.assertContains(response, "Promedio surtido")
        self.assertContains(response, "24 min")
        self.assertContains(response, "54")

    def test_registrar_ubicacion_en_geocerca_solo_crea_evento(self):
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.570010",
                "longitud": "-108.470010",
                "precision_metros": 0,
                "velocidad_kmh": 0,
                "bateria_porcentaje": 0,
            },
            ip_registro="127.0.0.1",
        )

        self.parada.refresh_from_db()
        self.ruta.refresh_from_db()
        self.assertFalse(ubicacion.fuera_de_geocerca)
        self.assertEqual(ubicacion.precision_metros, 0)
        self.assertEqual(ubicacion.velocidad_kmh, 0)
        self.assertEqual(ubicacion.bateria_porcentaje, 0)
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertEqual(self.ruta.cumplimiento_porcentaje, 0)
        self.assertTrue(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE).exists())

    def test_registrar_ubicacion_marca_visitada_con_permanencia(self):
        registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.570010",
                "longitud": "-108.470010",
                "precision_metros": 0,
            },
            ip_registro="127.0.0.1",
        )
        EventoRuta.objects.filter(ruta=self.ruta, parada=self.parada, tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE).update(
            creado_en=timezone.now() - timezone.timedelta(minutes=6)
        )

        registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.570010",
                "longitud": "-108.470010",
                "precision_metros": 0,
            },
            ip_registro="127.0.0.1",
        )

        self.parada.refresh_from_db()
        self.ruta.refresh_from_db()
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_VISITADA)
        self.assertEqual(self.ruta.cumplimiento_porcentaje, 100)

    def test_geocerca_no_marca_parada_posterior_si_hay_pendiente_previa(self):
        cedis = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="CEDIS Control",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.580000",
            longitud="-108.480000",
            radio_geocerca_metros=120,
        )
        parada_cedis = ParadaRuta.objects.create(ruta=self.ruta, punto=cedis, orden=2)
        EventoRuta.objects.create(
            ruta=self.ruta,
            parada=parada_cedis,
            tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            descripcion="Llegada detectada en CEDIS Control.",
            creado_en=timezone.now() - timezone.timedelta(minutes=6),
        )

        registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.580010",
                "longitud": "-108.480010",
                "precision_metros": 0,
            },
            ip_registro="127.0.0.1",
        )

        self.parada.refresh_from_db()
        parada_cedis.refresh_from_db()
        self.ruta.refresh_from_db()
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertEqual(parada_cedis.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertEqual(self.ruta.cumplimiento_porcentaje, 0)

    def test_parada_conserva_geocerca_planeada_si_punto_maestro_cambia(self):
        self.assertEqual(self.parada.punto_nombre_snapshot, "Sucursal Control")
        self.assertEqual(self.parada.radio_geocerca_metros, 120)

        self.punto.nombre = "Sucursal Control Reubicada"
        self.punto.latitud = "25.900000"
        self.punto.longitud = "-108.900000"
        self.punto.radio_geocerca_metros = 20
        self.punto.save(update_fields=["nombre", "latitud", "longitud", "radio_geocerca_metros", "actualizado_en"])

        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={"latitud": "25.570010", "longitud": "-108.470010"},
        )

        EventoRuta.objects.filter(ruta=self.ruta, parada=self.parada, tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE).update(
            creado_en=timezone.now() - timezone.timedelta(minutes=6)
        )
        registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={"latitud": "25.570010", "longitud": "-108.470010"},
        )

        self.parada.refresh_from_db()
        self.assertFalse(ubicacion.fuera_de_geocerca)
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_VISITADA)
        self.assertEqual(self.parada.punto_nombre_snapshot, "Sucursal Control")
        evento = EventoRuta.objects.get(ruta=self.ruta, tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE)
        self.assertIn("Sucursal Control", evento.descripcion)
        self.assertNotIn("Reubicada", evento.descripcion)

    def test_registrar_ubicacion_fuera_de_geocerca_crea_desvio(self):
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.590000",
                "longitud": "-108.490000",
                "fuera_de_ruta_confirmado": True,
                "desvio_motivo": "Entrega urgente",
            },
        )

        self.assertTrue(ubicacion.fuera_de_geocerca)
        evento = EventoRuta.objects.get(ruta=self.ruta, tipo=EventoRuta.TIPO_DESVIO, severidad=EventoRuta.SEVERIDAD_CRITICA)
        self.assertEqual(evento.metadata["motivo"], "Entrega urgente")

    def test_registrar_ubicacion_fuera_de_geocerca_crea_desvio_automatico_sin_confirmacion(self):
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.590000",
                "longitud": "-108.490000",
            },
        )

        self.assertTrue(ubicacion.fuera_de_geocerca)
        evento = EventoRuta.objects.get(ruta=self.ruta, tipo=EventoRuta.TIPO_DESVIO, severidad=EventoRuta.SEVERIDAD_CRITICA)
        self.assertEqual(evento.metadata["origen"], "automatico_geocerca")
        self.assertEqual(evento.metadata["motivo"], "Desvío detectado automáticamente por GPS fuera de geocerca.")

    def test_tracking_automatico_fuera_de_geocerca_crea_desvio_y_notifica(self):
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.590000",
                "longitud": "-108.490000",
                "timestamp_dispositivo": timezone.now(),
                "tracking_origen": "automatico_pwa",
            },
        )

        self.assertTrue(ubicacion.fuera_de_geocerca)
        evento = EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_DESVIO).first()
        self.assertIsNotNone(evento)
        self.assertEqual(evento.metadata.get("origen"), "automatico_pwa")

    @patch("logistica.tasks.notificar_desvio_ruta_automatico.delay")
    def test_tracking_desvio_confirmado_no_dispara_notificacion_automatica(self, mock_delay):
        registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.590000",
                "longitud": "-108.490000",
                "timestamp_dispositivo": timezone.now(),
                "tracking_origen": "manual_pwa",
                "fuera_de_ruta_confirmado": True,
                "desvio_motivo": "Cierre de calle",
            },
        )

        self.assertTrue(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_DESVIO).exists())
        mock_delay.assert_not_called()

    @patch("logistica.tasks.notificar_desvio_ruta_automatico.delay")
    def test_tracking_automatico_fuera_de_geocerca_dispara_notificacion(self, mock_delay):
        registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.590000",
                "longitud": "-108.490000",
                "timestamp_dispositivo": timezone.now(),
                "tracking_origen": "automatico_pwa",
            },
        )

        mock_delay.assert_called_once()

    def test_tracking_precision_baja_no_marca_parada_visitada(self):
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.570010",
                "longitud": "-108.470010",
                "precision_metros": "180",
                "timestamp_dispositivo": timezone.now(),
                "tracking_origen": "automatico_pwa",
            },
        )

        self.parada.refresh_from_db()
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertIn("precision_baja", ubicacion._alertas_tracking)
        self.assertTrue(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_GPS_PRECISION_BAJA).exists())

    def test_tracking_ubicacion_tardia_crea_alerta_y_no_marca_parada(self):
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.570010",
                "longitud": "-108.470010",
                "timestamp_dispositivo": timezone.now() - timezone.timedelta(minutes=12),
                "tracking_origen": "automatico_pwa",
            },
        )

        self.parada.refresh_from_db()
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertIn("ubicacion_tardia", ubicacion._alertas_tracking)
        self.assertTrue(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_UBICACION_TARDIA).exists())

    def test_tracking_salto_imposible_crea_alerta(self):
        registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.570010",
                "longitud": "-108.470010",
                "timestamp_dispositivo": timezone.now(),
            },
        )
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={
                "latitud": "25.900000",
                "longitud": "-108.900000",
                "timestamp_dispositivo": timezone.now() + timezone.timedelta(seconds=10),
                "tracking_origen": "automatico_pwa",
            },
        )

        self.assertIn("salto_imposible", ubicacion._alertas_tracking)
        self.assertTrue(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_SALTO_IMPOSIBLE).exists())

    def test_tracking_payload_duplicado_por_timestamp_no_duplica_ubicacion(self):
        timestamp = timezone.now()
        payload = {
            "latitud": "25.570010",
            "longitud": "-108.470010",
            "timestamp_dispositivo": timestamp,
            "tracking_origen": "automatico_pwa",
        }

        primera = registrar_ubicacion_ruta(user=self.user, ruta=self.ruta, payload=payload)
        segunda = registrar_ubicacion_ruta(user=self.user, ruta=self.ruta, payload=payload)

        self.assertEqual(primera.id, segunda.id)
        self.assertEqual(self.ruta.ubicaciones.count(), 1)

    def test_tracking_api_rechaza_desvio_confirmado_sin_motivo(self):
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("api_logistica_ruta_tracking", kwargs={"ruta_id": self.ruta.id}),
            json.dumps(
                {
                    "latitud": "25.590000",
                    "longitud": "-108.490000",
                    "fuera_de_ruta_confirmado": True,
                    "desvio_motivo": "   ",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertFalse(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_DESVIO).exists())

    def test_tracking_api_rechaza_coordenadas_invalidas(self):
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("api_logistica_ruta_tracking", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"latitud": "120.000000", "longitud": "-108.470010"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)

    def test_tracking_api_rechaza_precision_negativa(self):
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("api_logistica_ruta_tracking", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"latitud": "25.570010", "longitud": "-108.470010", "precision_metros": "-1"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)

    def test_tracking_api_rechaza_ruta_de_otro_repartidor(self):
        other_user = User.objects.create_user(username="otro.repartidor", password="pass123")
        other_user.groups.add(Group.objects.get_or_create(name="repartidor")[0])
        Repartidor.objects.create(user=other_user, sucursal=self.sucursal, unidad_asignada=self.unidad)
        self.client.force_login(other_user)

        response = self.client.post(
            reverse("api_logistica_ruta_tracking", kwargs={"ruta_id": self.ruta.id}),
            '{"latitud": "25.570010", "longitud": "-108.470010"}',
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 403)

    def test_api_ruta_activa_devuelve_paradas_del_repartidor(self):
        self.client.force_login(self.user)

        response = self.client.get(reverse("api_logistica_ruta_activa"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["ruta"]["id"], self.ruta.id)
        self.assertEqual(response.json()["paradas"][0]["id"], self.parada.id)
        self.assertEqual(response.json()["paradas"][0]["punto_nombre_snapshot"], "Sucursal Control")

    def test_api_ruta_activa_refresca_checklist_point_pendiente_viejo(self):
        checklist = RutaCargaChecklist.objects.create(
            ruta=self.ruta,
            estatus=RutaCargaChecklist.ESTATUS_EN_REVISION,
            sincronizado_en=timezone.now() - timezone.timedelta(minutes=5),
        )
        linea = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=self.parada,
            source_hash="point-viejo",
            item_code="PZA1",
            item_name="Producto Point viejo",
            unit="PZA",
            cantidad_solicitada="2.000",
            cantidad_enviada_esperada="0.000",
        )

        def refrescar(**_kwargs):
            linea.cantidad_enviada_esperada = "2.000"
            linea.notas = ""
            linea.save(update_fields=["cantidad_enviada_esperada", "notas", "actualizado_en"])

        self.client.force_login(self.user)
        with patch("api.logistica_views.sincronizar_checklist_carga_desde_point", side_effect=refrescar) as sync:
            response = self.client.get(reverse("api_logistica_ruta_activa"))

        self.assertEqual(response.status_code, 200)
        sync.assert_called_once_with(ruta=self.ruta, user=self.user, ejecutar_sync=False)
        lineas = response.json()["checklist_carga"]["lineas"]
        self.assertEqual(lineas[0]["cantidad_enviada_esperada"], "2")

    def test_api_ruta_activa_reconoce_ruta_planeada_anoche_como_hoy(self):
        self.ruta.delete()
        previous_day = timezone.localdate() - timezone.timedelta(days=1)
        ruta = RutaEntrega.objects.create(
            nombre="Ruta Noche Operativa",
            fecha_ruta=previous_day,
            estatus=RutaEntrega.ESTATUS_PLANEADA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )
        ParadaRuta.objects.create(ruta=ruta, punto=self.punto, orden=1)
        late_created_at = timezone.make_aware(datetime.combine(previous_day, time(hour=23, minute=30)))
        RutaEntrega.objects.filter(pk=ruta.pk).update(created_at=late_created_at)

        self.client.force_login(self.user)
        response = self.client.get(reverse("api_logistica_ruta_activa"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["ruta"]["id"], ruta.id)

    def test_pwa_finaliza_ruta_asignada_completa(self):
        self.client.force_login(self.user)
        self.parada.estado = ParadaRuta.ESTADO_VISITADA
        self.parada.entrega_estado = ParadaRuta.ENTREGA_ENTREGADA
        self.parada.hora_llegada_real = timezone.now()
        self.parada.entrega_confirmada_en = timezone.now()
        self.parada.entrega_confirmada_por = self.user
        self.parada.save(
            update_fields=[
                "estado",
                "entrega_estado",
                "hora_llegada_real",
                "entrega_confirmada_en",
                "entrega_confirmada_por",
                "actualizado_en",
            ]
        )

        response = self.client.post(reverse("api_logistica_ruta_finalizar_pwa", kwargs={"ruta_id": self.ruta.id}))

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_COMPLETADA)
        self.assertIsNotNone(self.ruta.hora_cierre_real)
        self.assertTrue(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_CIERRE).exists())

    def test_pwa_finalizar_ruta_bloquea_entrega_pendiente(self):
        self.client.force_login(self.user)
        self.parada.estado = ParadaRuta.ESTADO_VISITADA
        self.parada.hora_llegada_real = timezone.now()
        self.parada.save(update_fields=["estado", "hora_llegada_real", "actualizado_en"])

        response = self.client.post(reverse("api_logistica_ruta_finalizar_pwa", kwargs={"ruta_id": self.ruta.id}))

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)
        self.assertIn("entrega confirmada", response.json()["detail"])

    def test_superadmin_puede_ver_pwa_como_repartidor(self):
        admin = User.objects.create_superuser(username="admin.preview", password="pass123")
        self.client.force_login(admin)

        response = self.client.get(reverse("api_logistica_ruta_activa"), {"preview_repartidor": self.repartidor.id})
        perfil = self.client.get(reverse("api_logistica_mi_perfil"), {"preview_repartidor": self.repartidor.id})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["ruta"]["id"], self.ruta.id)
        self.assertEqual(response.json()["paradas"][0]["id"], self.parada.id)
        self.assertEqual(perfil.status_code, 200)
        self.assertTrue(perfil.json()["preview"]["solo_lectura"])
        self.assertEqual(perfil.json()["preview"]["repartidor_id"], self.repartidor.id)

    def test_superadmin_preview_bloquea_capturas(self):
        admin = User.objects.create_superuser(username="admin.preview.block", password="pass123")
        self.client.force_login(admin)

        response = self.client.post(
            f"{reverse('api_logistica_ruta_tracking', kwargs={'ruta_id': self.ruta.id})}?preview_repartidor={self.repartidor.id}",
            '{"latitud": "25.570010", "longitud": "-108.470010"}',
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(UbicacionRuta.objects.filter(ruta=self.ruta).count(), 0)

    def test_tracking_api_registra_ubicacion_de_ruta_activa(self):
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("api_logistica_ruta_tracking", kwargs={"ruta_id": self.ruta.id}),
            '{"latitud": "25.570010", "longitud": "-108.470010", "velocidad_kmh": 0}',
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        self.parada.refresh_from_db()
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertFalse(response.json()["fuera_de_geocerca"])

        EventoRuta.objects.filter(ruta=self.ruta, parada=self.parada, tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE).update(
            creado_en=timezone.now() - timezone.timedelta(minutes=6)
        )
        response = self.client.post(
            reverse("api_logistica_ruta_tracking", kwargs={"ruta_id": self.ruta.id}),
            '{"latitud": "25.570010", "longitud": "-108.470010", "velocidad_kmh": 0}',
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        self.parada.refresh_from_db()
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_VISITADA)
        self.assertFalse(response.json()["fuera_de_geocerca"])

    def test_tracking_api_no_expone_gps_historico_a_consulta_general(self):
        viewer = User.objects.create_user(username="visor.rutas", password="pass123")
        UserModuleAccess.objects.create(user=viewer, module="logistica", access=ACCESS_VIEW)
        self.client.force_login(viewer)

        response = self.client.get(reverse("api_logistica_ruta_tracking", kwargs={"ruta_id": self.ruta.id}))

        self.assertEqual(response.status_code, 403)

    def test_eventos_api_rechaza_eventos_automaticos_manuales(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("api_logistica_ruta_eventos", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"tipo": EventoRuta.TIPO_SALIDA, "severidad": EventoRuta.SEVERIDAD_INFO, "descripcion": "Salida manual"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)

    def test_eventos_api_rechaza_evento_manual_en_ruta_cerrada(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
        self.ruta.save(update_fields=["estatus", "updated_at"])

        response = self.client.post(
            reverse("api_logistica_ruta_eventos", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"tipo": EventoRuta.TIPO_INCIDENCIA_MANUAL, "severidad": EventoRuta.SEVERIDAD_ALERTA, "descripcion": "Evento tardío"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)

    def test_eventos_api_rechaza_coordenadas_invalidas(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("api_logistica_ruta_eventos", kwargs={"ruta_id": self.ruta.id}),
            json.dumps(
                {
                    "tipo": EventoRuta.TIPO_INCIDENCIA_MANUAL,
                    "severidad": EventoRuta.SEVERIDAD_ALERTA,
                    "descripcion": "Incidencia manual",
                    "latitud": "0.000000",
                    "longitud": "0.000000",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)

    def test_resumen_control_no_materializa_gps_perdido(self):
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={"latitud": "25.570010", "longitud": "-108.470010"},
        )
        UbicacionRuta = ubicacion.__class__
        UbicacionRuta.objects.filter(pk=ubicacion.pk).update(timestamp_servidor=timezone.now() - timezone.timedelta(minutes=20))

        resumen_control_rutas(fecha=self.ruta.fecha_ruta)

        self.assertFalse(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_GPS_PERDIDO).exists())

    def test_task_detecta_gps_perdido_por_ultima_senal_y_es_idempotente(self):
        ubicacion = registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={"latitud": "25.570010", "longitud": "-108.470010"},
        )
        UbicacionRuta = ubicacion.__class__
        UbicacionRuta.objects.filter(pk=ubicacion.pk).update(timestamp_servidor=timezone.now() - timezone.timedelta(minutes=20))

        resultado = detectar_gps_perdido_rutas(umbral_minutos=10)
        detectar_gps_perdido_rutas(umbral_minutos=10)

        evento = EventoRuta.objects.get(ruta=self.ruta, tipo=EventoRuta.TIPO_GPS_PERDIDO)
        self.assertEqual(resultado["eventos_gps_perdido"], 1)
        self.assertEqual(evento.ubicacion_id, ubicacion.id)
        self.assertEqual(evento.metadata["detectado_por"], "celery")
        self.assertEqual(evento.metadata["ultima_ubicacion_id"], ubicacion.id)

    def test_task_detecta_gps_perdido_sin_primera_senal_y_es_idempotente(self):
        self.ruta.hora_inicio_real = timezone.now() - timezone.timedelta(minutes=20)
        self.ruta.save(update_fields=["hora_inicio_real", "updated_at"])

        resultado = detectar_gps_perdido_rutas(umbral_minutos=10)
        detectar_gps_perdido_rutas(umbral_minutos=10)

        evento = EventoRuta.objects.get(ruta=self.ruta, tipo=EventoRuta.TIPO_GPS_PERDIDO)
        self.assertEqual(resultado["eventos_gps_perdido"], 1)
        self.assertTrue(evento.metadata["sin_primera_senal"])
        self.assertEqual(evento.metadata["detectado_por"], "celery")

    def test_task_gps_perdido_ignora_ruta_completada(self):
        self.ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
        self.ruta.hora_inicio_real = timezone.now() - timezone.timedelta(minutes=20)
        self.ruta.save(update_fields=["estatus", "hora_inicio_real", "updated_at"])

        resultado = detectar_gps_perdido_rutas(umbral_minutos=10)

        self.assertEqual(resultado["rutas_revisadas"], 0)
        self.assertFalse(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_GPS_PERDIDO).exists())

    @patch("logistica.views.snap_gps_path_to_roads")
    def test_control_rutas_view_renderiza_panel_interno(self, snap_mock):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.ruta.ruta_programada_polyline = "25.570000,-108.470000|25.571000,-108.471000"
        self.ruta.ruta_programada_fuente = "FALLBACK"
        self.ruta.save(update_fields=["ruta_programada_polyline", "ruta_programada_fuente", "updated_at"])
        registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={"latitud": "25.570010", "longitud": "-108.470010", "precision_metros": "12"},
        )
        registrar_ubicacion_ruta(
            user=self.user,
            ruta=self.ruta,
            payload={"latitud": "25.570050", "longitud": "-108.470050", "precision_metros": "12"},
        )
        snap_mock.return_value.coordinates = [(25.57001, -108.47001), (25.57050, -108.47050)]
        snap_mock.return_value.source = "GOOGLE_ROADS"
        snap_mock.return_value.warning = ""

        response = self.client.get(reverse("logistica:control_rutas"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Control interno de rutas")
        self.assertContains(response, "Rutas del día")
        self.assertContains(response, "Mapa real · OpenStreetMap")
        self.assertContains(response, "route-control-map-data")
        self.assertNotContains(response, "Vista esquemática, no evidencia GPS")
        self.assertContains(response, "Filtrar")
        mapa = response.context["mapa_rutas"]
        self.assertEqual(mapa["routes"][0]["paradas"][0]["nombre"], "Sucursal Control")
        self.assertEqual(mapa["routes"][0]["ubicaciones"][0]["lat"], 25.57001)
        self.assertEqual(mapa["routes"][0]["ubicaciones_snapped_fuente"], "GOOGLE_ROADS")
        self.assertEqual(mapa["routes"][0]["ubicaciones_segmentos"][0]["fuente"], "GOOGLE_ROADS")
        self.assertEqual(mapa["routes"][0]["ubicaciones_snapped"][1]["lng"], -108.4705)
        self.assertEqual(mapa["routes"][0]["programada_polyline"], "25.570000,-108.470000|25.571000,-108.471000")

    @patch("logistica.views.snap_gps_path_to_roads")
    def test_control_rutas_mapa_no_conecta_puntos_gps_descartados(self, snap_mock):
        def snap_side_effect(*, ruta_id, coords):
            return SimpleNamespace(coordinates=coords, source="RAW", warning="")

        snap_mock.side_effect = snap_side_effect
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        base = timezone.now()
        ubicaciones = [
            ("25.570000", "-108.470000", base),
            ("25.570100", "-108.470100", base + timezone.timedelta(seconds=45)),
            ("25.590000", "-108.490000", base + timezone.timedelta(seconds=90)),
            ("25.571000", "-108.471000", base + timezone.timedelta(seconds=135)),
            ("25.571100", "-108.471100", base + timezone.timedelta(seconds=180)),
        ]
        creadas = [
            UbicacionRuta.objects.create(
                ruta=self.ruta,
                repartidor=self.repartidor,
                unidad=self.unidad,
                latitud=lat,
                longitud=lng,
                timestamp_servidor=timestamp,
            )
            for lat, lng, timestamp in ubicaciones
        ]
        EventoRuta.objects.create(
            ruta=self.ruta,
            ubicacion=creadas[2],
            tipo=EventoRuta.TIPO_GPS_PRECISION_BAJA,
            severidad=EventoRuta.SEVERIDAD_ALERTA,
            descripcion="Precisión GPS baja: 180 m.",
            latitud=creadas[2].latitud,
            longitud=creadas[2].longitud,
        )

        response = self.client.get(reverse("logistica:control_rutas"))

        self.assertEqual(response.status_code, 200)
        route = response.context["mapa_rutas"]["routes"][0]
        self.assertEqual(len(route["ubicaciones_segmentos"]), 2)
        self.assertEqual([len(segment["coords"]) for segment in route["ubicaciones_segmentos"]], [2, 2])
        descartadas = [point for point in route["ubicaciones"] if point["trazo_descartado"]]
        self.assertEqual(len(descartadas), 1)
        self.assertEqual(descartadas[0]["alertas_tracking"], ["precision_baja"])
        self.assertEqual(snap_mock.call_count, 2)

    @override_settings(GOOGLE_SERVER_API_KEY="server-key", GOOGLE_ROADS_SNAP_ENABLED=True)
    @patch("logistica.services_google_roads.cache")
    @patch("logistica.services_google_roads.requests.get")
    def test_snap_gps_path_to_roads_usa_google_roads(self, get_mock, cache_mock):
        cache_mock.get.return_value = None
        get_mock.return_value.status_code = 200
        get_mock.return_value.json.return_value = {
            "snappedPoints": [
                {"location": {"latitude": 25.57001, "longitude": -108.47001}},
                {"location": {"latitude": 25.57020, "longitude": -108.47020}},
                {"location": {"latitude": 25.57050, "longitude": -108.47050}},
            ]
        }

        result = snap_gps_path_to_roads(
            ruta_id=self.ruta.id,
            coords=[(25.57001, -108.47001), (25.57050, -108.47050)],
        )

        self.assertEqual(result.source, "GOOGLE_ROADS")
        self.assertEqual(len(result.coordinates), 3)
        self.assertIn("snapToRoads", get_mock.call_args.args[0])
        self.assertEqual(get_mock.call_args.kwargs["params"]["interpolate"], "true")
        cache_mock.set.assert_called_once()

    @override_settings(GOOGLE_SERVER_API_KEY="server-key", GOOGLE_ROADS_SNAP_ENABLED=True)
    @patch("logistica.services_google_roads.cache")
    @patch("logistica.services_google_roads.requests.get")
    def test_snap_gps_path_to_roads_fallback_si_google_bloquea(self, get_mock, cache_mock):
        cache_mock.get.return_value = None
        get_mock.return_value.status_code = 403
        get_mock.return_value.json.return_value = {"error": {"status": "PERMISSION_DENIED"}}
        coords = [(25.57001, -108.47001), (25.57050, -108.47050)]

        result = snap_gps_path_to_roads(ruta_id=self.ruta.id, coords=coords)

        self.assertEqual(result.source, "RAW")
        self.assertEqual(result.warning, "google_roads_http_403")
        self.assertEqual(result.coordinates, coords)

    def test_control_rutas_filtra_por_ruta_repartidor_o_unidad(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.get(reverse("logistica:control_rutas"), {"q": "Control"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Ruta Control")
        self.assertContains(response, "Filtro activo: Control")

        response = self.client.get(reverse("logistica:control_rutas"), {"q": "No existe"})

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Ruta Control")
        self.assertContains(response, "No hay rutas programadas para esta fecha.")

    def test_control_rutas_filtra_evidencia_por_tipo(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        EventoRuta.objects.create(
            ruta=self.ruta,
            tipo=EventoRuta.TIPO_SALIDA,
            severidad=EventoRuta.SEVERIDAD_INFO,
            descripcion="Salida visible",
        )
        EventoRuta.objects.create(
            ruta=self.ruta,
            tipo=EventoRuta.TIPO_DESVIO,
            severidad=EventoRuta.SEVERIDAD_CRITICA,
            descripcion="Desvio filtrado",
        )

        response = self.client.get(reverse("logistica:control_rutas"), {"evento": EventoRuta.TIPO_DESVIO})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Desvio filtrado")
        self.assertNotContains(response, "Salida visible")
        self.assertContains(response, f'value="{EventoRuta.TIPO_DESVIO}" selected')

    def test_pwa_tracking_declara_cola_offline_reintento_y_cache_versionado(self):
        from pathlib import Path

        base_dir = Path(__file__).resolve().parent
        pwa_html = (base_dir / "templates" / "logistica" / "pwa.html").read_text(encoding="utf-8")
        sw_js = (base_dir / "static" / "logistica" / "pwa" / "sw.js").read_text(encoding="utf-8")

        self.assertIn("pd_logistica_tracking_queue", pwa_html)
        self.assertIn("enqueueRutaTracking", pwa_html)
        self.assertIn("flushRutaTrackingQueue", pwa_html)
        self.assertIn("Sin conexión: seguimiento guardado para reintento.", pwa_html)
        self.assertIn("pd_logistica_offline_mutations", pwa_html)
        self.assertIn("enqueueOfflineMutation", pwa_html)
        self.assertIn("flushOfflineMutationQueue", pwa_html)
        self.assertIn("responseQueuedOffline", pwa_html)
        self.assertIn("queuedSuccessMessage", pwa_html)
        self.assertIn("X-Offline-Queued", pwa_html)
        self.assertIn("FormData", pwa_html)
        self.assertIn("readFileAsDataUrl", pwa_html)
        self.assertIn("fileFromDataUrl", pwa_html)
        self.assertIn("flushAllOfflineQueues", pwa_html)
        self.assertIn("pendiente${count === 1 ? \"\" : \"s\"} por sincronizar", pwa_html)
        self.assertIn("route-control-v57", pwa_html)
        self.assertIn("logistica:pwa_sw", pwa_html)
        self.assertIn("?v=route-control-v65-recarga-point", pwa_html)
        self.assertIn('scope: "/logistica/"', pwa_html)
        self.assertIn("pollyanas-logistica-pwa-v65-recarga-point", sw_js)
        self.assertIn("operationalModalHtml", pwa_html)
        self.assertIn("function operationalErrorTitle(error, fallback = \"No se puede continuar\")", pwa_html)
        self.assertIn("Falta obligatorio", pwa_html)
        self.assertIn('case "turno_abierto":', pwa_html)
        self.assertIn('return "Turno abierto";', pwa_html)
        self.assertIn('case "ruta_no_liberada":', pwa_html)
        self.assertIn('return "Carga no enviada en Point";', pwa_html)
        self.assertIn('return "Ruta no liberada";', pwa_html)
        self.assertIn("const pendientePoint = pendiente && linea.point_enviada !== true;", pwa_html)
        self.assertIn("La carga aún no aparece enviada en Point.", pwa_html)
        self.assertIn('const enviadoCero = linea.estatus === "ZERO_EXPECTED";', pwa_html)
        self.assertIn("Point confirmó enviado final en cero; no requiere captura.", pwa_html)
        self.assertIn("Logística debe asignar la unidad a la ruta.", pwa_html)
        self.assertIn("Tu turno activo no corresponde a la unidad asignada a esta ruta.", pwa_html)
        api_block = sw_js[sw_js.index('url.pathname.startsWith("/api/")'):sw_js.index('event.request.mode === "navigate"')]
        self.assertIn("event.respondWith(fetch(event.request));", api_block)
        self.assertNotIn("caches.match(event.request)", api_block)
        self.assertIn("if (!state.perfil) await loadPerfil();", pwa_html)
        self.assertIn("segmentoCargaOperativo", pwa_html)
        self.assertIn("Carga del tramo", pwa_html)
        self.assertIn("resuelta: paradaOperativamenteResuelta(parada)", pwa_html)
        self.assertIn("lineas: segmento,", pwa_html)
        self.assertNotIn("anterioresResueltas", pwa_html)
        self.assertIn("Mostrando solo el tramo operativo actual.", pwa_html)
        self.assertNotIn("lineasPostCedis", pwa_html)
        self.assertIn("· cargar ${totalProducto.esperado.toFixed(3)}", pwa_html)
        self.assertIn("Total cargado", pwa_html)
        self.assertIn("const ROUTE_AUTO_TRACKING_INTERVAL_MS = 45 * 1000;", pwa_html)
        self.assertIn('activo: "Activo cada 45 s"', pwa_html)
        self.assertIn('gps_sin_senal: "GPS sin señal"', pwa_html)
        self.assertIn('pendiente_offline: "Sin internet, guardado"', pwa_html)
        self.assertIn('gps_denegado: "Ubicación bloqueada"', pwa_html)
        self.assertNotIn('Activo cada 90 s', pwa_html)
        self.assertNotIn('Pendiente local', pwa_html)
        self.assertIn("TRACKING_QUEUE_TTL_MS", pwa_html)
        self.assertIn("normalizeRutaTrackingQueue", pwa_html)
        self.assertIn("purgeRutaTrackingQueue", pwa_html)
        self.assertIn("velocidad_kmh: Number.isFinite(position.coords.speed)", pwa_html)
        self.assertIn("document.addEventListener(\"visibilitychange\"", pwa_html)
        self.assertIn("window.addEventListener(\"pagehide\"", pwa_html)
        self.assertIn("automatico_pwa", pwa_html)
        self.assertIn("Auto-tracking", pwa_html)
        self.assertIn("let payload = null;", pwa_html)
        self.assertIn("enqueueRutaTracking({ ruta_id: rutaId, payload });", pwa_html)
        self.assertIn("skipOfflineQueue: true", pwa_html)
        self.assertIn("sessionStorage.getItem(ACCESS_TOKEN_KEY)", pwa_html)
        self.assertIn("sessionStorage.setItem(ACCESS_TOKEN_KEY, data.access)", pwa_html)
        self.assertIn("sessionStorage.removeItem(ACCESS_TOKEN_KEY);", pwa_html)
        self.assertIn("const response = await fetch(`${API}/auth/session-token/`", pwa_html)
        self.assertIn('error.error === "unidad_ruta_distinta"', pwa_html)
        self.assertIn('await showScreen("ruta_activa")', pwa_html)
        self.assertIn("Iniciar ruta con este turno", pwa_html)
        self.assertIn("Ir a Mi ruta para iniciar", pwa_html)
        self.assertNotIn("Liberar ruta con este turno", pwa_html)
        self.assertIn("/bitacora-salida/liberar-ruta/", pwa_html)
        self.assertIn("const lineasTramoCarga = segmentoCargaOperativo(checklistCarga?.lineas || [], rutaData?.paradas || []).lineas;", pwa_html)
        self.assertIn('lineasTramoCarga.filter((linea) => linea.estatus === "PENDIENTE").length', pwa_html)
        self.assertIn("const cargaListaParaLiberar = lineasTramoCarga.length > 0 && lineasPendientesCarga === 0;", pwa_html)
        self.assertIn("const puedeLiberarTramo = !rutaEnSeguimiento && turnoOk && unidadOk && lineasTramoCarga.length > 0", pwa_html)
        self.assertIn("Capturar carga pendiente", pwa_html)
        self.assertIn("Logística debe autorizar la salida", pwa_html)
        self.assertIn("confirmarEntregaParada", pwa_html)
        self.assertIn("function evidenciasEntregaParada(paradaId)", pwa_html)
        self.assertIn("linea_carga_id: linea.id", pwa_html)
        self.assertIn("cantidad_entregada: String(linea.cantidad_cargada ?? linea.cantidad_enviada_esperada ?? \"0\")", pwa_html)
        self.assertIn("evidenciasEntregaParada(paradaId)", pwa_html)
        self.assertIn("entregables = (paradas || []).filter(paradaRequiereEntrega)", pwa_html)
        self.assertIn("entregadas = entregables.filter(paradaOperativamenteResuelta)", pwa_html)
        self.assertIn("return parada?.operativamente_resuelta === true;", pwa_html)
        self.assertIn("confirmarEntregaParada(${Number(rutaId)}, ${Number(parada.id)}, this)", pwa_html)
        self.assertIn("paradaDisponibleParaEntrega", pwa_html)
        self.assertIn("const puedeConfirmarEntrega = requiereEntrega && rutaEnSeguimiento && entrega === \"PENDIENTE\" && paradaDisponibleParaEntrega(parada, paradas);", pwa_html)
        self.assertNotIn("const puedeConfirmarEntrega = requiereEntrega && rutaEnSeguimiento && !resolved", pwa_html)
        self.assertIn("const puedeRegistrarRecarga = !requiereEntrega && rutaEnSeguimiento && parada.recarga_cedis_resuelta !== true;", pwa_html)
        self.assertIn("registrarRecargaCedis(${Number(rutaId)}, ${Number(parada.id)}, this)", pwa_html)
        self.assertIn("Reintentar sincronización Point", pwa_html)
        self.assertIn("Sincronización de recarga pendiente", pwa_html)
        self.assertIn("/recarga-cedis/", pwa_html)
        self.assertIn('return renderRutaCarga("✅ Point sincronizado. Revisa la carga del siguiente tramo.");', pwa_html)
        self.assertIn('return renderRutaCarga("✅ Continuación autorizada. Revisa la carga actualizada del tramo.");', pwa_html)
        self.assertIn('return renderRutaActiva(queuedSuccessMessage("Entrega de parada"));', pwa_html)
        self.assertIn('return renderRutaScreen(queuedSuccessMessage("Revisión de carga"));', pwa_html)
        self.assertNotIn('return renderRutaActiva(queuedSuccessMessage("Recarga CEDIS"));', pwa_html)
        self.assertIn("La recarga CEDIS requiere conexión", pwa_html)
        self.assertIn("isRecargaCedisPath(path)", pwa_html)
        self.assertIn("offlineMutationReplayErrors", pwa_html)
        self.assertIn("La operación se conservó y requiere acción explícita", pwa_html)
        self.assertIn("paradaRequiereEntrega", pwa_html)
        self.assertIn('toUpperCase() !== "CEDIS"', pwa_html)
        self.assertIn('button.textContent = "Enviando...";', pwa_html)
        self.assertNotIn('window.confirm("¿Confirmar entrega completa de esta parada?")', pwa_html)
        self.assertIn("Confirmar entrega", pwa_html)
        self.assertNotIn('localStorage.setItem("pd_logistica_refresh"', pwa_html)
        self.assertNotIn("localStorage.setItem(REFRESH_TOKEN_KEY", pwa_html)

    def test_pwa_sw_se_sirve_sin_cache_de_borde(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.get(reverse("logistica:pwa_sw"))

        self.assertEqual(response.status_code, 200)
        self.assertIn("no-cache", response["Cache-Control"])
        self.assertIn("no-store", response["Cache-Control"])
        self.assertIn("pollyanas-logistica-pwa-v65-recarga-point", response.content.decode("utf-8"))

    def test_pwa_mi_ruta_declara_prototipo_operativo(self):
        from pathlib import Path

        base_dir = Path(__file__).resolve().parent
        pwa_html = (base_dir / "templates" / "logistica" / "pwa.html").read_text(encoding="utf-8")

        self.assertIn("route-hero", pwa_html)
        self.assertIn("route-dashboard-card", pwa_html)
        self.assertNotIn("grid-column: 1 / -1", pwa_html)
        self.assertIn("route-signal-grid", pwa_html)
        self.assertIn("route-progress-card", pwa_html)
        self.assertNotIn("Capturar ubicación GPS", pwa_html)
        self.assertIn("Reportar desvío", pwa_html)
        self.assertIn("Confirmar desvío", pwa_html)
        self.assertIn("Tu ubicación se envía automáticamente cada 45 segundos mientras estás en ruta.", pwa_html)
        self.assertIn("Paradas de reparto", pwa_html)
        self.assertLess(pwa_html.index("${renderParadasRuta(paradas, ruta.id, rutaEnSeguimiento)}"), pwa_html.index("showScreen('ruta_carga')"))
        self.assertIn("Pendiente de entrega", pwa_html)
        self.assertIn("Recibido", pwa_html)
        self.assertIn("La ruta puede continuar; cierre final espera recepción Point.", pwa_html)
        self.assertIn("Finalizar ruta del día", pwa_html)
        self.assertIn("finalizarRutaDia", pwa_html)
        self.assertIn('draft.geoStatus === "idle" ? "" : geoOverlay(draft, "confirmarDesvioRuta")', pwa_html)

    def test_puntos_logisticos_crea_punto_manual(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("logistica:puntos_logisticos"),
            {
                "nombre": "Sucursal Nueva",
                "tipo": PuntoLogistico.TIPO_SUCURSAL,
                "sucursal": self.sucursal.id,
                "latitud": "25.571000",
                "longitud": "-108.471000",
                "radio_geocerca_metros": "90",
                "activo": "on",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(PuntoLogistico.objects.filter(nombre="Sucursal Nueva", radio_geocerca_metros=90).exists())
        self.assertContains(response, "Puntos existentes")

    def test_punto_logistico_edit_y_toggle(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        punto_libre = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Punto Libre",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.573000",
            longitud="-108.473000",
            radio_geocerca_metros=80,
        )

        response = self.client.post(
            reverse("logistica:punto_logistico_edit", kwargs={"pk": punto_libre.id}),
            {
                "nombre": "Sucursal Control Editada",
                "tipo": PuntoLogistico.TIPO_SUCURSAL,
                "sucursal": self.sucursal.id,
                "latitud": "25.575000",
                "longitud": "-108.475000",
                "radio_geocerca_metros": "150",
                "activo": "on",
                "notas": "Entrada por estacionamiento",
            },
            follow=True,
        )

        punto_libre.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(punto_libre.nombre, "Sucursal Control Editada")
        self.assertEqual(punto_libre.radio_geocerca_metros, 150)
        self.assertTrue(punto_libre.activo)

        toggle = self.client.post(reverse("logistica:punto_logistico_toggle", kwargs={"pk": punto_libre.id}), follow=True)
        punto_libre.refresh_from_db()
        self.assertEqual(toggle.status_code, 200)
        self.assertFalse(punto_libre.activo)

    def test_punto_logistico_no_desactiva_si_esta_en_ruta_abierta(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(reverse("logistica:punto_logistico_toggle", kwargs={"pk": self.punto.id}), follow=True)

        self.punto.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(self.punto.activo)
        self.assertContains(response, "No se puede desactivar")

    def test_punto_logistico_edit_no_desactiva_si_esta_en_ruta_abierta(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("logistica:punto_logistico_edit", kwargs={"pk": self.punto.id}),
            {
                "nombre": self.punto.nombre,
                "tipo": self.punto.tipo,
                "sucursal": self.sucursal.id,
                "latitud": self.punto.latitud,
                "longitud": self.punto.longitud,
                "radio_geocerca_metros": self.punto.radio_geocerca_metros,
            },
        )

        self.punto.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(self.punto.activo)
        self.assertContains(response, "No se puede desactivar")

    def test_punto_logistico_rechaza_radio_invalido(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("logistica:puntos_logisticos"),
            {
                "nombre": "Punto Inválido",
                "tipo": PuntoLogistico.TIPO_SUCURSAL,
                "latitud": "25.571000",
                "longitud": "-108.471000",
                "radio_geocerca_metros": "abc",
                "activo": "on",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "El radio debe ser un número entero.")
        self.assertFalse(PuntoLogistico.objects.filter(nombre="Punto Inválido").exists())

    def test_punto_logistico_rechaza_coordenadas_fuera_de_rango(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("logistica:puntos_logisticos"),
            {
                "nombre": "Punto Fuera",
                "tipo": PuntoLogistico.TIPO_SUCURSAL,
                "latitud": "120.000000",
                "longitud": "-108.471000",
                "radio_geocerca_metros": "80",
                "activo": "on",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "La latitud debe estar entre -90 y 90.")
        self.assertFalse(PuntoLogistico.objects.filter(nombre="Punto Fuera").exists())

    def test_punto_logistico_rechaza_punto_activo_demasiado_cercano(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("logistica:puntos_logisticos"),
            {
                "nombre": "Sucursal Duplicada",
                "tipo": PuntoLogistico.TIPO_SUCURSAL,
                "sucursal": self.sucursal.id,
                "latitud": "25.570010",
                "longitud": "-108.470010",
                "radio_geocerca_metros": "80",
                "activo": "on",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Ya existe un punto activo")
        self.assertFalse(PuntoLogistico.objects.filter(nombre="Sucursal Duplicada").exists())

    def test_ruta_detail_planea_paradas_y_libera_ruta(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
        self.ruta.save(update_fields=["estatus", "updated_at"])
        ruta = RutaEntrega.objects.create(nombre="Ruta Manual", fecha_ruta=timezone.localdate())

        blocked = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_EN_RUTA},
            follow=True,
        )
        ruta.refresh_from_db()
        self.assertEqual(blocked.status_code, 200)
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertContains(blocked, "No se puede liberar la ruta")

        self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {
                "action": "update_plan",
                "nombre": "Ruta Manual",
                "fecha_ruta": timezone.localdate().isoformat(),
                "repartidor": self.repartidor.id,
                "acompanante": self.acompanante.id,
                "acompanante_manual": "Auxiliar externo",
                "unidad_operativa": self.unidad.id,
                "km_estimado": "12.5",
            },
            follow=True,
        )
        self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "add_parada", "punto": self.punto.id, "orden": "1"},
            follow=True,
        )
        released = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_EN_RUTA},
            follow=True,
        )

        ruta.refresh_from_db()
        self.assertEqual(released.status_code, 200)
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)
        self.assertEqual(ruta.repartidor, self.repartidor)
        self.assertEqual(ruta.acompanante, self.acompanante)
        self.assertEqual(ruta.acompanante_manual, "Auxiliar externo")
        self.assertEqual(ruta.unidad_operativa, self.unidad)
        self.assertTrue(ruta.paradas.filter(punto=self.punto).exists())
        self.assertTrue(EventoRuta.objects.filter(ruta=ruta, tipo=EventoRuta.TIPO_SALIDA).exists())

    def test_ruta_en_ruta_permite_quitar_parada_pendiente_sin_carga(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        punto_extra = PuntoLogistico.objects.create(
            nombre="Sucursal Extra",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.571000",
            longitud="-108.471000",
            radio_geocerca_metros=120,
        )
        ParadaRuta.objects.create(ruta=self.ruta, punto=punto_extra, orden=2)
        checklist = RutaCargaChecklist.objects.create(ruta=self.ruta, estatus=RutaCargaChecklist.ESTATUS_EN_REVISION)
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=self.parada,
            transfer_external_id="T-PEND",
            detail_external_id="D-PEND",
            source_hash="pendiente-quitar",
            item_code="PZA",
            item_name="Producto pendiente",
            unit="PZA",
            cantidad_solicitada="1.000",
            cantidad_enviada_esperada="1.000",
        )

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "delete_parada", "parada_id": self.parada.id},
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertFalse(ParadaRuta.objects.filter(pk=self.parada.id).exists())
        self.assertFalse(RutaCargaChecklistLinea.objects.filter(source_hash="pendiente-quitar").exists())

    def test_ruta_en_ruta_bloquea_quitar_parada_con_carga(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        punto_extra = PuntoLogistico.objects.create(
            nombre="Sucursal Extra",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.571000",
            longitud="-108.471000",
            radio_geocerca_metros=120,
        )
        ParadaRuta.objects.create(ruta=self.ruta, punto=punto_extra, orden=2)
        checklist = RutaCargaChecklist.objects.create(ruta=self.ruta, estatus=RutaCargaChecklist.ESTATUS_EN_REVISION)
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=self.parada,
            transfer_external_id="T-CARGA",
            detail_external_id="D-CARGA",
            source_hash="cargada-no-quitar",
            item_code="PZA",
            item_name="Producto cargado",
            unit="PZA",
            cantidad_solicitada="1.000",
            cantidad_enviada_esperada="1.000",
            cantidad_cargada="1.000",
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
        )

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "delete_parada", "parada_id": self.parada.id},
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(ParadaRuta.objects.filter(pk=self.parada.id).exists())
        self.assertContains(response, "ya tiene carga validada")

    def test_ruta_en_ruta_solo_permite_agregar_cedis(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        cedis = PuntoLogistico.objects.create(
            nombre="CEDIS",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.567916",
            longitud="-108.459969",
            radio_geocerca_metros=120,
        )

        bloqueada = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "add_parada", "punto": self.punto.id, "orden": "2"},
            follow=True,
        )
        permitida = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "add_parada", "punto": cedis.id, "orden": "2"},
            follow=True,
        )

        self.assertEqual(bloqueada.status_code, 200)
        self.assertContains(bloqueada, "solo puedes agregar una parada CEDIS")
        self.assertEqual(permitida.status_code, 200)
        self.assertTrue(ParadaRuta.objects.filter(ruta=self.ruta, punto=cedis).exists())

    def test_ruta_en_ruta_no_permite_actualizar_entrega_oculta(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        entrega = EntregaRuta.objects.create(ruta=self.ruta, secuencia=1, cliente_nombre="Cliente", estatus=EntregaRuta.ESTATUS_PENDIENTE)

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "entrega_status", "entrega_id": entrega.id, "estatus": EntregaRuta.ESTATUS_ENTREGADA},
            follow=True,
        )

        entrega.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(entrega.estatus, EntregaRuta.ESTATUS_PENDIENTE)
        self.assertContains(response, "planeación queda congelada")

    def test_ruta_model_clean_rechaza_en_ruta_sin_estructura(self):
        ruta = RutaEntrega(nombre="Ruta Inválida", fecha_ruta=timezone.localdate(), estatus=RutaEntrega.ESTATUS_EN_RUTA)

        with self.assertRaises(ValidationError) as ctx:
            ruta.full_clean()

        self.assertIn("repartidor", ctx.exception.message_dict)
        self.assertIn("unidad_operativa", ctx.exception.message_dict)
        self.assertIn("estatus", ctx.exception.message_dict)

    def test_ruta_status_bloquea_segunda_ruta_activa_mismo_repartidor(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        ruta = RutaEntrega.objects.create(
            nombre="Ruta Duplicada",
            fecha_ruta=timezone.localdate(),
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )
        ParadaRuta.objects.create(ruta=ruta, punto=self.punto, orden=1)

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_EN_RUTA},
            follow=True,
        )

        ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertContains(response, "el repartidor ya tiene otra ruta en curso")

    def test_ruta_status_bloquea_completar_con_paradas_pendientes(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_COMPLETADA},
            follow=True,
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)
        self.assertContains(response, "hay paradas pendientes")

    def test_ruta_status_bloquea_completar_con_entrega_pendiente(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.parada.estado = ParadaRuta.ESTADO_VISITADA
        self.parada.hora_llegada_real = timezone.now()
        self.parada.save(update_fields=["estado", "hora_llegada_real", "actualizado_en"])

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_COMPLETADA},
            follow=True,
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)
        self.assertContains(response, "hay paradas sin entrega confirmada")

    def test_ruta_status_no_bloquea_completar_por_recepcion_point_pendiente(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self._crear_linea_carga_con_transferencia_recibida(is_received=False, received_quantity="0.000")
        self.parada.estado = ParadaRuta.ESTADO_VISITADA
        self.parada.entrega_estado = ParadaRuta.ENTREGA_ENTREGADA
        self.parada.hora_llegada_real = timezone.now()
        self.parada.entrega_confirmada_en = timezone.now()
        self.parada.entrega_confirmada_por = self.user
        self.parada.save(
            update_fields=[
                "estado",
                "entrega_estado",
                "hora_llegada_real",
                "entrega_confirmada_en",
                "entrega_confirmada_por",
                "actualizado_en",
            ]
        )

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_COMPLETADA},
            follow=True,
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_COMPLETADA)

    def test_ruta_status_no_reabre_entrega_por_recepcion_point_diferente(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self._crear_linea_carga_con_transferencia_recibida(received_quantity="0.000")
        self.parada.estado = ParadaRuta.ESTADO_VISITADA
        self.parada.entrega_estado = ParadaRuta.ENTREGA_ENTREGADA
        self.parada.hora_llegada_real = timezone.now()
        self.parada.entrega_confirmada_en = timezone.now()
        self.parada.entrega_confirmada_por = self.user
        self.parada.save(
            update_fields=[
                "estado",
                "entrega_estado",
                "hora_llegada_real",
                "entrega_confirmada_en",
                "entrega_confirmada_por",
                "actualizado_en",
            ]
        )

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_COMPLETADA},
            follow=True,
        )

        self.ruta.refresh_from_db()
        self.parada.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_COMPLETADA)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_ENTREGADA)

    def test_ruta_status_no_bloquea_completar_por_cedis_visitada_sin_entrega(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        cedis = PuntoLogistico.objects.create(
            nombre="CEDIS",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.567916",
            longitud="-108.459969",
            radio_geocerca_metros=120,
        )
        self.parada.estado = ParadaRuta.ESTADO_VISITADA
        self.parada.entrega_estado = ParadaRuta.ENTREGA_ENTREGADA
        self.parada.hora_llegada_real = timezone.now()
        self.parada.entrega_confirmada_en = timezone.now()
        self.parada.entrega_confirmada_por = self.user
        self.parada.save(
            update_fields=[
                "estado",
                "entrega_estado",
                "hora_llegada_real",
                "entrega_confirmada_en",
                "entrega_confirmada_por",
                "actualizado_en",
            ]
        )
        ParadaRuta.objects.create(
            ruta=self.ruta,
            punto=cedis,
            orden=2,
            estado=ParadaRuta.ESTADO_VISITADA,
            hora_llegada_real=timezone.now(),
        )

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_COMPLETADA},
            follow=True,
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_COMPLETADA)

    def test_serializer_muestra_cedis_como_no_aplica_en_entrega(self):
        cedis = PuntoLogistico.objects.create(
            nombre="CEDIS",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.567916",
            longitud="-108.459969",
            radio_geocerca_metros=120,
        )
        parada = ParadaRuta.objects.create(ruta=self.ruta, punto=cedis, orden=2)

        data = ParadaRutaSerializer(parada).data

        self.assertEqual(data["entrega_estado"], "NO_APLICA")
        self.assertEqual(data["entrega_estado_display"], "No aplica")

    def test_ruta_status_no_reabre_ruta_cerrada(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
        self.ruta.save(update_fields=["estatus", "updated_at"])

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_PLANEADA},
            follow=True,
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_COMPLETADA)
        self.assertContains(response, "no puede reabrirse")

    def test_ruta_status_no_regresa_en_ruta_a_planeada(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_PLANEADA},
            follow=True,
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)
        self.assertContains(response, "no puede regresar a planeada")

    def test_ruta_status_no_completa_ruta_planeada_vacia(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        ruta = RutaEntrega.objects.create(nombre="Ruta Vacía", fecha_ruta=timezone.localdate())

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_COMPLETADA},
            follow=True,
        )

        ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertContains(response, "Solo puedes completar una ruta que ya está en seguimiento")

    def test_api_ruta_status_no_reabre_ruta_cerrada(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
        self.ruta.save(update_fields=["estatus", "updated_at"])

        response = self.client.post(
            reverse("api_logistica_ruta_estatus", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"estatus": RutaEntrega.ESTATUS_PLANEADA}),
            content_type="application/json",
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_COMPLETADA)

    def test_api_ruta_status_no_regresa_en_ruta_a_planeada(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("api_logistica_ruta_estatus", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"estatus": RutaEntrega.ESTATUS_PLANEADA}),
            content_type="application/json",
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)

    def test_api_ruta_status_bloquea_completar_con_entrega_pendiente(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.parada.estado = ParadaRuta.ESTADO_VISITADA
        self.parada.hora_llegada_real = timezone.now()
        self.parada.save(update_fields=["estado", "hora_llegada_real", "actualizado_en"])

        response = self.client.post(
            reverse("api_logistica_ruta_estatus", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"estatus": RutaEntrega.ESTATUS_COMPLETADA}),
            content_type="application/json",
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)
        self.assertIn("entrega confirmada", response.json()["detail"])

    def test_api_ruta_status_no_bloquea_completar_por_cedis_visitada_sin_entrega(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        cedis = PuntoLogistico.objects.create(
            nombre="CEDIS",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.567916",
            longitud="-108.459969",
            radio_geocerca_metros=120,
        )
        self.parada.estado = ParadaRuta.ESTADO_VISITADA
        self.parada.entrega_estado = ParadaRuta.ENTREGA_ENTREGADA
        self.parada.hora_llegada_real = timezone.now()
        self.parada.entrega_confirmada_en = timezone.now()
        self.parada.entrega_confirmada_por = self.user
        self.parada.save(
            update_fields=[
                "estado",
                "entrega_estado",
                "hora_llegada_real",
                "entrega_confirmada_en",
                "entrega_confirmada_por",
                "actualizado_en",
            ]
        )
        ParadaRuta.objects.create(
            ruta=self.ruta,
            punto=cedis,
            orden=2,
            estado=ParadaRuta.ESTADO_VISITADA,
            hora_llegada_real=timezone.now(),
        )

        response = self.client.post(
            reverse("api_logistica_ruta_estatus", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"estatus": RutaEntrega.ESTATUS_COMPLETADA}),
            content_type="application/json",
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_COMPLETADA)

    def test_api_ruta_status_bloquea_completar_con_diferencia_point(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.parada.estado = ParadaRuta.ESTADO_VISITADA
        self.parada.entrega_estado = ParadaRuta.ENTREGA_CON_DIFERENCIA
        self.parada.hora_llegada_real = timezone.now()
        self.parada.save(update_fields=["estado", "entrega_estado", "hora_llegada_real", "actualizado_en"])

        response = self.client.post(
            reverse("api_logistica_ruta_estatus", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"estatus": RutaEntrega.ESTATUS_COMPLETADA}),
            content_type="application/json",
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)
        self.assertIn("diferencias", response.json()["detail"])

    def test_api_confirma_entrega_de_parada_con_evidencia_idempotente(self):
        self.client.force_login(self.user)
        self._registrar_llegada_geocerca()
        checklist = RutaCargaChecklist.objects.create(ruta=self.ruta)
        linea = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=self.parada,
            transfer_external_id="T-ENTREGA-1",
            detail_external_id="D-ENTREGA-1",
            source_hash="entrega-source-1",
            item_code="SNICK-CH",
            item_name="Pastel Snicker chico",
            unit="pz",
            cantidad_solicitada="5.000",
            cantidad_enviada_esperada="5.000",
        )
        payload = {
            "entrega_estado": ParadaRuta.ENTREGA_ENTREGADA,
            "notas": "Recibido completo",
            "client_context": {
                "causa": "GEOFENCE_LEGACY_NO_CONFIABLE",
                "client_timestamp": timezone.now().isoformat(),
                "client_version": "legacy-test",
            },
            "evidencias": [
                {
                    "linea_carga_id": linea.id,
                    "tipo": ParadaEntregaEvidencia.TIPO_CONFIRMACION,
                    "cantidad_entregada": "5.000",
                    "comentario": "Sucursal confirma piezas completas",
                    "latitud": "25.570010",
                    "longitud": "-108.470010",
                    "precision_metros": "12.00",
                    "client_event_id": "evt-entrega-1",
                }
            ],
        }

        url = reverse("api_logistica_ruta_parada_entrega", kwargs={"ruta_id": self.ruta.id, "parada_id": self.parada.id})
        response = self.client.post(url, json.dumps(payload), content_type="application/json")
        retry = self.client.post(url, json.dumps(payload), content_type="application/json")

        self.parada.refresh_from_db()
        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(retry.status_code, 200)
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_VISITADA)
        self.assertIsNotNone(self.parada.hora_llegada_real)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_ENTREGADA)
        self.assertEqual(self.parada.entrega_confirmada_por, self.user)
        self.assertEqual(ParadaEntregaEvidencia.objects.filter(parada=self.parada, client_event_id="evt-entrega-1").count(), 1)
        self.assertEqual(self.ruta.cumplimiento_porcentaje, Decimal("100.00"))
        self.assertEqual(EventoRuta.objects.filter(ruta=self.ruta, parada=self.parada, tipo=EventoRuta.TIPO_ENTREGA_EXCEPCIONAL).count(), 1)

    def test_api_confirma_entrega_guarda_evidencia_por_producto(self):
        self.client.force_login(self.user)
        self._registrar_llegada_geocerca()
        checklist = RutaCargaChecklist.objects.create(ruta=self.ruta)
        linea_1 = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=self.parada,
            transfer_external_id="T-ENTREGA-MULTI",
            detail_external_id="D-ENTREGA-MULTI-1",
            source_hash="entrega-multi-1",
            item_code="PAY-G",
            item_name="Pay de Queso Grande",
            unit="pz",
            cantidad_solicitada="2.000",
            cantidad_enviada_esperada="2.000",
            cantidad_cargada="2.000",
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
        )
        linea_2 = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=self.parada,
            transfer_external_id="T-ENTREGA-MULTI",
            detail_external_id="D-ENTREGA-MULTI-2",
            source_hash="entrega-multi-2",
            item_code="ZAN-M",
            item_name="Pastel de Zanahoria Mediano",
            unit="pz",
            cantidad_solicitada="3.000",
            cantidad_enviada_esperada="3.000",
            cantidad_cargada="3.000",
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
        )

        response = self.client.post(
            reverse("api_logistica_ruta_parada_entrega", kwargs={"ruta_id": self.ruta.id, "parada_id": self.parada.id}),
            json.dumps(
                {
                    "entrega_estado": ParadaRuta.ENTREGA_ENTREGADA,
                    "notas": "Entrega completa por productos",
                    "client_context": {
                        "causa": "GEOFENCE_LEGACY_NO_CONFIABLE",
                        "client_timestamp": timezone.now().isoformat(),
                        "client_version": "legacy-test",
                    },
                    "evidencias": [
                        {
                            "linea_carga_id": linea_1.id,
                            "tipo": ParadaEntregaEvidencia.TIPO_CONFIRMACION,
                            "cantidad_entregada": "2.000",
                            "comentario": "Pay entregado",
                            "client_event_id": "evt-entrega-prod-1",
                        },
                        {
                            "linea_carga_id": linea_2.id,
                            "tipo": ParadaEntregaEvidencia.TIPO_CONFIRMACION,
                            "cantidad_entregada": "3.000",
                            "comentario": "Zanahoria entregado",
                            "client_event_id": "evt-entrega-prod-2",
                        },
                    ],
                }
            ),
            content_type="application/json",
        )

        self.parada.refresh_from_db()
        evidencias = ParadaEntregaEvidencia.objects.filter(parada=self.parada).order_by("linea_carga_id")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_VISITADA)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_ENTREGADA)
        self.assertEqual(evidencias.count(), 2)
        self.assertEqual([row.linea_carga_id for row in evidencias], [linea_1.id, linea_2.id])
        self.assertEqual([row.cantidad_entregada for row in evidencias], [Decimal("2.000"), Decimal("3.000")])

    def test_api_confirmar_entrega_rechaza_linea_carga_id_superada(self):
        self.client.force_login(self.user)
        self._registrar_llegada_geocerca()
        checklist = RutaCargaChecklist.objects.create(ruta=self.ruta)
        linea_superada = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=self.parada,
            transfer_external_id="T-ENTREGA-SUP",
            detail_external_id="D-ENTREGA-SUP-VIEJO",
            source_hash="entrega-superada-vieja",
            item_code="PAY-SUP",
            item_name="Pay superado",
            unit="pz",
            cantidad_solicitada="2.000",
            cantidad_enviada_esperada="0.000",
            cantidad_cargada="0.000",
            estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA,
        )

        response = self.client.post(
            reverse("api_logistica_ruta_parada_entrega", kwargs={"ruta_id": self.ruta.id, "parada_id": self.parada.id}),
            json.dumps(
                {
                    "entrega_estado": ParadaRuta.ENTREGA_ENTREGADA,
                    "notas": "Entrega con línea superada",
                    "client_context": {
                        "causa": "GEOFENCE_LEGACY_NO_CONFIABLE",
                        "client_timestamp": timezone.now().isoformat(),
                        "client_version": "legacy-test",
                    },
                    "evidencias": [
                        {
                            "linea_carga_id": linea_superada.id,
                            "tipo": ParadaEntregaEvidencia.TIPO_CONFIRMACION,
                            "cantidad_entregada": "2.000",
                            "comentario": "Intento sobre línea superada",
                            "client_event_id": "evt-entrega-superada",
                        },
                    ],
                }
            ),
            content_type="application/json",
        )

        self.parada.refresh_from_db()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)
        self.assertFalse(ParadaEntregaEvidencia.objects.filter(parada=self.parada).exists())

    def test_api_no_confirma_entrega_en_parada_cedis(self):
        self.client.force_login(self.user)
        cedis = PuntoLogistico.objects.create(
            nombre="CEDIS",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.567916",
            longitud="-108.459969",
            radio_geocerca_metros=120,
        )
        parada_cedis = ParadaRuta.objects.create(
            ruta=self.ruta,
            punto=cedis,
            orden=2,
            estado=ParadaRuta.ESTADO_VISITADA,
            hora_llegada_real=timezone.now(),
        )

        response = self.client.post(
            reverse("api_logistica_ruta_parada_entrega", kwargs={"ruta_id": self.ruta.id, "parada_id": parada_cedis.id}),
            json.dumps(
                {
                    "entrega_estado": ParadaRuta.ENTREGA_ENTREGADA,
                    "evidencias": [{"comentario": "Intento de entrega en CEDIS"}],
                }
            ),
            content_type="application/json",
        )

        parada_cedis.refresh_from_db()
        self.assertEqual(response.status_code, 400)
        self.assertIn("recarga", response.json()["detail"])
        self.assertEqual(parada_cedis.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)

    def test_api_no_confirma_entrega_despues_de_cedis_pendiente(self):
        self.client.force_login(self.user)
        cedis = PuntoLogistico.objects.create(
            nombre="CEDIS tramo",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.567916",
            longitud="-108.459969",
            radio_geocerca_metros=120,
        )
        parada_cedis = ParadaRuta.objects.create(ruta=self.ruta, punto=cedis, orden=2, estado=ParadaRuta.ESTADO_PENDIENTE)
        self.parada.orden = 3
        self.parada.save(update_fields=["orden", "actualizado_en"])
        url = reverse("api_logistica_ruta_parada_entrega", kwargs={"ruta_id": self.ruta.id, "parada_id": self.parada.id})

        response = self.client.post(
            url,
            json.dumps(
                {
                    "entrega_estado": ParadaRuta.ENTREGA_ENTREGADA,
                    "evidencias": [{"comentario": "Intento antes de recarga"}],
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("recarga CEDIS", response.json()["detail"])
        parada_cedis.estado = ParadaRuta.ESTADO_VISITADA
        parada_cedis.save(update_fields=["estado", "actualizado_en"])
        self._registrar_llegada_geocerca()
        response_visitada = self.client.post(
            url,
            json.dumps(
                {
                    "entrega_estado": ParadaRuta.ENTREGA_ENTREGADA,
                    "notas": "Entrega confirmada después de recarga CEDIS.",
                    "client_event_id": "entrega-tras-cedis",
                    "client_context": {
                        "causa": "GEOFENCE_LEGACY_NO_CONFIABLE",
                        "client_timestamp": timezone.now().isoformat(),
                        "client_version": "legacy-test",
                    },
                    "evidencias": [{"comentario": "Entrega tras recarga"}],
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(response_visitada.status_code, 400, response_visitada.content)
        self.assertIn("recarga CEDIS", response_visitada.json()["detail"])

        EventoRuta.objects.create(
            ruta=self.ruta,
            parada=parada_cedis,
            tipo=EventoRuta.TIPO_RECARGA_CEDIS,
            descripcion="Recarga canónica confirmada",
        )
        response = self.client.post(
            url,
            json.dumps(
                {
                    "entrega_estado": ParadaRuta.ENTREGA_ENTREGADA,
                    "notas": "Entrega confirmada después de recarga CEDIS.",
                    "client_event_id": "entrega-tras-cedis",
                    "client_context": {
                        "causa": "GEOFENCE_LEGACY_NO_CONFIABLE",
                        "client_timestamp": timezone.now().isoformat(),
                        "client_version": "legacy-test",
                    },
                    "evidencias": [{"comentario": "Entrega tras recarga"}],
                }
            ),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200, response.content)

    def test_api_cedis_inicial_no_bloquea_entrega_siguiente(self):
        self.client.force_login(self.user)
        self.parada.orden = 2
        self.parada.save(update_fields=["orden", "actualizado_en"])
        cedis = PuntoLogistico.objects.create(
            nombre="CEDIS inicial entrega",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.567916",
            longitud="-108.459969",
            radio_geocerca_metros=120,
        )
        ParadaRuta.objects.create(
            ruta=self.ruta,
            punto=cedis,
            orden=1,
            estado=ParadaRuta.ESTADO_VISITADA,
        )
        self._registrar_llegada_geocerca()

        response = self.client.post(
            reverse(
                "api_logistica_ruta_parada_entrega",
                kwargs={"ruta_id": self.ruta.id, "parada_id": self.parada.id},
            ),
            json.dumps(
                {
                    "entrega_estado": ParadaRuta.ENTREGA_ENTREGADA,
                    "notas": "Entrega posterior a salida inicial.",
                    "client_event_id": "entrega-tras-cedis-inicial",
                    "client_context": {
                        "causa": "GEOFENCE_LEGACY_NO_CONFIABLE",
                        "client_timestamp": timezone.now().isoformat(),
                        "client_version": "cedis-inicial-test",
                    },
                    "evidencias": [{"comentario": "CEDIS inicial no requiere recarga"}],
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200, response.content)

    def test_api_recarga_cedis_legacy_habilita_entrega_siguiente(self):
        self.client.force_login(self.user)
        cedis = PuntoLogistico.objects.create(
            nombre="CEDIS legacy entrega",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.567916",
            longitud="-108.459969",
            radio_geocerca_metros=120,
        )
        parada_cedis = ParadaRuta.objects.create(
            ruta=self.ruta,
            punto=cedis,
            orden=2,
            estado=ParadaRuta.ESTADO_VISITADA,
        )
        self.parada.orden = 3
        self.parada.save(update_fields=["orden", "actualizado_en"])
        EventoRuta.objects.create(
            ruta=self.ruta,
            parada=parada_cedis,
            tipo=EventoRuta.TIPO_INCIDENCIA_MANUAL,
            descripcion="Recarga histórica compatible",
            metadata={"tipo": "recarga_cedis_pwa"},
        )
        self._registrar_llegada_geocerca()

        response = self.client.post(
            reverse(
                "api_logistica_ruta_parada_entrega",
                kwargs={"ruta_id": self.ruta.id, "parada_id": self.parada.id},
            ),
            json.dumps(
                {
                    "entrega_estado": ParadaRuta.ENTREGA_ENTREGADA,
                    "notas": "Entrega posterior a recarga legacy.",
                    "client_event_id": "entrega-tras-recarga-legacy",
                    "client_context": {
                        "causa": "GEOFENCE_LEGACY_NO_CONFIABLE",
                        "client_timestamp": timezone.now().isoformat(),
                        "client_version": "recarga-legacy-test",
                    },
                    "evidencias": [{"comentario": "Recarga legacy compatible"}],
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200, response.content)

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_api_registra_recarga_cedis_desde_pwa(self, sync_point):
        self.client.force_login(self.user)
        cedis = PuntoLogistico.objects.create(
            nombre="CEDIS",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.567916",
            longitud="-108.459969",
            radio_geocerca_metros=120,
        )
        parada_cedis = ParadaRuta.objects.create(
            ruta=self.ruta,
            punto=cedis,
            orden=2,
            estado=ParadaRuta.ESTADO_PENDIENTE,
        )
        url = reverse("api_logistica_ruta_parada_recarga_cedis", kwargs={"ruta_id": self.ruta.id, "parada_id": parada_cedis.id})

        response = self.client.post(
            url,
            json.dumps({"notas": "Llegué a CEDIS para recargar."}),
            content_type="application/json",
        )
        retry = self.client.post(url, json.dumps({}), content_type="application/json")

        parada_cedis.refresh_from_db()
        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(retry.status_code, 200)
        self.assertEqual(parada_cedis.estado, ParadaRuta.ESTADO_VISITADA)
        self.assertIsNotNone(parada_cedis.hora_llegada_real)
        self.assertIsNotNone(parada_cedis.hora_salida_real)
        self.assertEqual(parada_cedis.notas, "Llegué a CEDIS para recargar.")
        self.assertEqual(
            EventoRuta.objects.filter(ruta=self.ruta, parada=parada_cedis, tipo=EventoRuta.TIPO_RECARGA_CEDIS).count(),
            1,
        )
        self.assertEqual(self.ruta.cumplimiento_porcentaje, Decimal("50.00"))

    def test_api_retry_no_sobrescribe_entrega_confirmada(self):
        self.client.force_login(self.user)
        self.parada.entrega_estado = ParadaRuta.ENTREGA_ENTREGADA
        self.parada.entrega_confirmada_por = self.user
        self.parada.entrega_confirmada_en = timezone.now()
        self.parada.save(update_fields=["entrega_estado", "entrega_confirmada_por", "entrega_confirmada_en", "actualizado_en"])

        response = self.client.post(
            reverse("api_logistica_ruta_parada_entrega", kwargs={"ruta_id": self.ruta.id, "parada_id": self.parada.id}),
            json.dumps({"entrega_estado": ParadaRuta.ENTREGA_CON_DIFERENCIA, "evidencias": [{"comentario": "retry tarde"}]}),
            content_type="application/json",
        )

        self.parada.refresh_from_db()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_ENTREGADA)
        self.assertEqual(EventoRuta.objects.filter(ruta=self.ruta, parada=self.parada).count(), 0)

    def test_api_entrega_completa_exige_evidencia(self):
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("api_logistica_ruta_parada_entrega", kwargs={"ruta_id": self.ruta.id, "parada_id": self.parada.id}),
            json.dumps({"entrega_estado": ParadaRuta.ENTREGA_ENTREGADA, "evidencias": []}),
            content_type="application/json",
        )

        self.parada.refresh_from_db()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)
        self.assertIn("evidencia", response.json()["detail"])

    def test_api_confirma_entrega_con_diferencia_exige_motivo(self):
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("api_logistica_ruta_parada_entrega", kwargs={"ruta_id": self.ruta.id, "parada_id": self.parada.id}),
            json.dumps({"entrega_estado": ParadaRuta.ENTREGA_CON_DIFERENCIA, "evidencias": []}),
            content_type="application/json",
        )

        self.parada.refresh_from_db()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)

    def test_api_no_confirma_entrega_de_otro_repartidor(self):
        other_user = User.objects.create_user(username="otro.entrega", password="pass123")
        other_user.groups.add(Group.objects.get_or_create(name="repartidor")[0])
        Repartidor.objects.create(user=other_user, sucursal=self.sucursal, unidad_asignada=self.unidad)
        self.client.force_login(other_user)

        response = self.client.post(
            reverse("api_logistica_ruta_parada_entrega", kwargs={"ruta_id": self.ruta.id, "parada_id": self.parada.id}),
            json.dumps({"entrega_estado": ParadaRuta.ENTREGA_ENTREGADA, "notas": "Intento ajeno"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 403)

    def test_api_mantenimiento_sin_repartidor_no_confirma_entrega_de_ruta(self):
        mantenimiento = User.objects.create_user(username="mant.sin.repartidor", password="pass123")
        UserModuleAccess.objects.create(user=mantenimiento, module="mantenimiento", access=ACCESS_VIEW)
        self.client.force_login(mantenimiento)

        response = self.client.post(
            reverse("api_logistica_ruta_parada_entrega", kwargs={"ruta_id": self.ruta.id, "parada_id": self.parada.id}),
            json.dumps({"entrega_estado": ParadaRuta.ENTREGA_ENTREGADA, "evidencias": [{"comentario": "Intento"}]}),
            content_type="application/json",
        )

        self.parada.refresh_from_db()
        self.assertEqual(response.status_code, 403)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)

    def test_sincronizar_recepcion_desde_point_registra_evidencia_sin_confirmar_visita(self):
        self._crear_linea_carga_con_transferencia_recibida()

        resumen = sincronizar_recepcion_desde_point(ruta=self.ruta, user=self.user, ejecutar_sync=False)

        self.parada.refresh_from_db()
        self.ruta.refresh_from_db()
        self.assertEqual(resumen.evidencias_creadas, 1)
        self.assertEqual(resumen.paradas_actualizadas, 0)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)
        self.assertIsNone(self.parada.entrega_confirmada_por)
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertEqual(self.ruta.cumplimiento_porcentaje, Decimal("0.00"))
        evidencia = ParadaEntregaEvidencia.objects.get(parada=self.parada)
        self.assertEqual(evidencia.cantidad_entregada, Decimal("5.000"))
        self.assertEqual(evidencia.metadata["origen"], "point_transfer")

    def test_sincronizar_recepcion_no_importa_transferencia_point_recibida_post_salida(self):
        transferencia = self._crear_transferencia_point_abierta(source_hash="transfer-post-salida")
        transferencia.is_open = False
        transferencia.is_received = True
        transferencia.is_finalized = True
        transferencia.received_quantity = Decimal("5.000")
        transferencia.received_at = timezone.now()
        transferencia.save(update_fields=["is_open", "is_received", "is_finalized", "received_quantity", "received_at", "updated_at"])

        resumen = sincronizar_recepcion_desde_point(ruta=self.ruta, user=self.user, ejecutar_sync=False)

        self.parada.refresh_from_db()
        self.assertEqual(resumen.evidencias_creadas, 0)
        self.assertFalse(RutaCargaChecklistLinea.objects.filter(checklist__ruta=self.ruta, source_hash=transferencia.source_hash).exists())
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)

    def test_sincronizar_recepcion_desde_point_no_inventa_si_point_no_recibio(self):
        self._crear_linea_carga_con_transferencia_recibida(is_received=False, received_quantity="0.000")

        resumen = sincronizar_recepcion_desde_point(ruta=self.ruta, user=self.user, ejecutar_sync=False)

        self.parada.refresh_from_db()
        self.assertEqual(resumen.evidencias_creadas, 0)
        self.assertEqual(resumen.lineas_pendientes_point, 1)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)
        self.assertFalse(ParadaEntregaEvidencia.objects.filter(parada=self.parada).exists())

    def test_sincronizar_recepcion_no_exige_recibir_linea_enviada_en_cero(self):
        self._crear_linea_carga_con_transferencia_recibida(
            sent_quantity="0.000",
            loaded_quantity="0.000",
            received_quantity="0.000",
            is_received=False,
        )

        resumen = sincronizar_recepcion_desde_point(ruta=self.ruta, user=self.user, ejecutar_sync=False)

        self.assertEqual(resumen.evidencias_creadas, 0)
        self.assertEqual(resumen.lineas_pendientes_point, 0)

    def test_sincronizar_recepcion_no_cuenta_solicitud_no_enviada_como_recepcion_pendiente(self):
        _, linea, transfer_line = self._crear_linea_carga_con_transferencia_recibida(
            loaded_quantity="0.000",
            is_received=False,
        )
        transfer_line.sent_at = None
        transfer_line.sent_quantity = Decimal("0")
        transfer_line.raw_payload = {"transfer": {"isEnviado": False}}
        transfer_line.save(update_fields=["sent_at", "sent_quantity", "raw_payload", "updated_at"])
        linea.cantidad_enviada_esperada = Decimal("0")
        linea.save(update_fields=["cantidad_enviada_esperada", "actualizado_en"])

        resumen = sincronizar_recepcion_desde_point(ruta=self.ruta, user=self.user, ejecutar_sync=False)

        self.assertEqual(resumen.evidencias_creadas, 0)
        self.assertEqual(resumen.lineas_pendientes_point, 0)

    def test_sincronizar_recepcion_desde_point_no_marca_diferencia_si_recibido_no_cuadra(self):
        self._crear_linea_carga_con_transferencia_recibida(loaded_quantity="5.000", received_quantity="3.000")

        resumen = sincronizar_recepcion_desde_point(ruta=self.ruta, user=self.user, ejecutar_sync=False)

        self.parada.refresh_from_db()
        self.assertEqual(resumen.evidencias_creadas, 1)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)

    def test_sincronizar_recepcion_parcial_point_deja_entrega_pendiente(self):
        checklist, _, _ = self._crear_linea_carga_con_transferencia_recibida(loaded_quantity="5.000", received_quantity="5.000")
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=self.parada,
            transfer_external_id="",
            detail_external_id="",
            source_hash="sin-point-pendiente",
            item_code="PAY-MED",
            item_name="Pay de Queso Mediano",
            unit="pz",
            cantidad_solicitada="2.000",
            cantidad_enviada_esperada="2.000",
            cantidad_cargada="2.000",
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
            validado_por=self.user,
            validado_en=timezone.now(),
        )

        resumen = sincronizar_recepcion_desde_point(ruta=self.ruta, user=self.user, ejecutar_sync=False)

        self.parada.refresh_from_db()
        self.assertEqual(resumen.evidencias_creadas, 1)
        self.assertEqual(resumen.lineas_pendientes_point, 1)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)
        self.assertEqual(self.parada.entrega_notas, "")

    def test_sincronizar_recepcion_no_infiere_confirmacion_pwa_desde_evidencia_aislada(self):
        checklist, _, _ = self._crear_linea_carga_con_transferencia_recibida(
            source_hash="transfer-mixta-point",
            loaded_quantity="5.000",
            received_quantity="5.000",
        )
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=self.parada,
            transfer_external_id="",
            detail_external_id="",
            source_hash="manual-sin-point-pwa",
            item_code="PAY-MED",
            item_name="Pay de Queso Mediano",
            unit="pz",
            cantidad_solicitada="2.000",
            cantidad_enviada_esperada="2.000",
            cantidad_cargada="2.000",
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
            validado_por=self.user,
            validado_en=timezone.now(),
        )
        ParadaEntregaEvidencia.objects.create(
            ruta=self.ruta,
            parada=self.parada,
            tipo=ParadaEntregaEvidencia.TIPO_CONFIRMACION,
            comentario="Entrega completa confirmada por repartidor en PWA.",
            capturado_por=self.user,
            client_event_id="pwa-entrega-completa-mixta",
        )

        resumen = sincronizar_recepcion_desde_point(ruta=self.ruta, user=self.user, ejecutar_sync=False)

        self.parada.refresh_from_db()
        self.assertEqual(resumen.evidencias_creadas, 1)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)
        self.assertEqual(self.parada.entrega_notas, "")

    def test_sincronizar_recepcion_no_reabre_entrega_pwa_con_point_pendiente(self):
        self._crear_linea_carga_con_transferencia_recibida(is_received=False, received_quantity="0.000")
        ParadaEntregaEvidencia.objects.create(
            ruta=self.ruta,
            parada=self.parada,
            tipo=ParadaEntregaEvidencia.TIPO_CONFIRMACION,
            comentario="Entrega completa confirmada por repartidor en PWA.",
            capturado_por=self.user,
            client_event_id="pwa-entrega-completa-point-pendiente",
        )
        self.parada.estado = ParadaRuta.ESTADO_VISITADA
        self.parada.entrega_estado = ParadaRuta.ENTREGA_ENTREGADA
        self.parada.entrega_confirmada_en = timezone.now()
        self.parada.entrega_confirmada_por = self.user
        self.parada.save(
            update_fields=[
                "estado",
                "entrega_estado",
                "entrega_confirmada_en",
                "entrega_confirmada_por",
                "actualizado_en",
            ]
        )

        resumen = sincronizar_recepcion_desde_point(ruta=self.ruta, user=self.user, ejecutar_sync=False)

        self.parada.refresh_from_db()
        self.assertEqual(resumen.evidencias_creadas, 0)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_ENTREGADA)
        self.assertNotIn("Pendiente de sincronizar recepción completa", self.parada.entrega_notas)

    def test_sincronizar_recepcion_no_reabre_entrega_pwa_por_producto(self):
        _, linea, _ = self._crear_linea_carga_con_transferencia_recibida(is_received=False, received_quantity="0.000")
        ParadaEntregaEvidencia.objects.create(
            ruta=self.ruta,
            parada=self.parada,
            linea_carga=linea,
            tipo=ParadaEntregaEvidencia.TIPO_CONFIRMACION,
            cantidad_entregada=Decimal("5.000"),
            comentario="Entrega confirmada: Pastel Snicker chico",
            capturado_por=self.user,
            client_event_id="pwa-linea-entrega-1",
            metadata={"origen": "pwa_entrega_parada"},
        )
        self.parada.estado = ParadaRuta.ESTADO_VISITADA
        self.parada.entrega_estado = ParadaRuta.ENTREGA_ENTREGADA
        self.parada.entrega_confirmada_en = timezone.now()
        self.parada.entrega_confirmada_por = self.user
        self.parada.save(
            update_fields=[
                "estado",
                "entrega_estado",
                "entrega_confirmada_en",
                "entrega_confirmada_por",
                "actualizado_en",
            ]
        )

        resumen = sincronizar_recepcion_desde_point(ruta=self.ruta, user=self.user, ejecutar_sync=False)

        self.parada.refresh_from_db()
        self.assertEqual(resumen.evidencias_creadas, 0)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_ENTREGADA)

    def test_sincronizar_recepcion_no_reabre_ajuste_manual_con_diferencia(self):
        self._crear_linea_carga_con_transferencia_recibida(received_quantity="3.000")
        ParadaEntregaEvidencia.objects.create(
            ruta=self.ruta,
            parada=self.parada,
            tipo=ParadaEntregaEvidencia.TIPO_INCIDENCIA,
            comentario="Ajuste manual de diferencia confirmado por logística.",
            capturado_por=self.user,
            metadata={"origen": "erp_manual", "entrega_estado": ParadaRuta.ENTREGA_CON_DIFERENCIA},
        )
        self.parada.estado = ParadaRuta.ESTADO_VISITADA
        self.parada.entrega_estado = ParadaRuta.ENTREGA_CON_DIFERENCIA
        self.parada.entrega_confirmada_en = timezone.now()
        self.parada.entrega_confirmada_por = self.user
        self.parada.save(
            update_fields=[
                "estado",
                "entrega_estado",
                "entrega_confirmada_en",
                "entrega_confirmada_por",
                "actualizado_en",
            ]
        )

        resumen = sincronizar_recepcion_desde_point(ruta=self.ruta, user=self.user, ejecutar_sync=False)

        self.parada.refresh_from_db()
        self.assertEqual(resumen.evidencias_creadas, 1)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_CON_DIFERENCIA)

    def test_sincronizar_recepcion_desde_point_actualiza_evidencia_si_point_corrige(self):
        _, _, transfer_line = self._crear_linea_carga_con_transferencia_recibida(loaded_quantity="5.000", received_quantity="3.000")
        sincronizar_recepcion_desde_point(ruta=self.ruta, user=self.user, ejecutar_sync=False)
        transfer_line.received_quantity = Decimal("5.000")
        transfer_line.received_at = timezone.now()
        transfer_line.save(update_fields=["received_quantity", "received_at", "updated_at"])

        resumen = sincronizar_recepcion_desde_point(ruta=self.ruta, user=self.user, ejecutar_sync=False)

        self.parada.refresh_from_db()
        evidencia = ParadaEntregaEvidencia.objects.get(parada=self.parada)
        self.assertEqual(resumen.evidencias_creadas, 0)
        self.assertEqual(resumen.evidencias_existentes, 1)
        self.assertEqual(evidencia.cantidad_entregada, Decimal("5.000"))
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)

    def test_api_sincroniza_recepcion_point_y_devuelve_parada_actualizada(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        _, _, transfer_line = self._crear_linea_carga_con_transferencia_recibida()
        sync_job = transfer_line.sync_job

        with patch("logistica.services_carga_ruta.PointMovementSyncService") as service_cls:
            service_cls.return_value.run_transfer_sync.return_value = sync_job
            response = self.client.post(
                reverse("api_logistica_ruta_recepcion_point_sync", kwargs={"ruta_id": self.ruta.id}),
                "{}",
                content_type="application/json",
            )

        self.parada.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["paradas_actualizadas"], 0)
        self.assertEqual(response.json()["lineas_recibidas"], 1)
        self.assertEqual(response.json()["paradas"][0]["entrega_estado"], ParadaRuta.ENTREGA_PENDIENTE)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)

    def test_api_confirmar_entrega_sin_geocerca_la_registra_para_revision(self):
        self.client.force_login(self.user)
        self.parada.estado = ParadaRuta.ESTADO_PENDIENTE
        self.parada.hora_llegada_real = None
        self.parada.save(update_fields=["estado", "hora_llegada_real", "actualizado_en"])

        response = self.client.post(
            reverse("api_logistica_ruta_parada_entrega", kwargs={"ruta_id": self.ruta.id, "parada_id": self.parada.id}),
            json.dumps({
                "entrega_estado": ParadaRuta.ENTREGA_ENTREGADA,
                "notas": "Entrega confirmada en sucursal.",
                "client_context": {
                    "causa": "GPS_SIN_SENAL",
                    "client_timestamp": timezone.now().isoformat(),
                    "client_version": "legacy-test",
                },
                "evidencias": [{"tipo": "CONFIRMACION", "comentario": "Entrega completa.", "client_event_id": "entrega-sin-gps"}],
            }),
            content_type="application/json",
        )

        self.parada.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["requiere_revision"])
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertIsNone(self.parada.hora_llegada_real)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_ENTREGADA)
        self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_PENDIENTE)

    def test_ruta_detail_muestra_carga_esperada_por_producto(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.ruta.estatus = RutaEntrega.ESTATUS_PLANEADA
        self.ruta.save(update_fields=["estatus", "updated_at"])
        self._crear_transferencia_point_abierta()
        sincronizar_checklist_carga_desde_point(ruta=self.ruta, user=self.user, ejecutar_sync=False)

        response = self.client.get(reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Carga esperada CEDIS por parada")
        self.assertContains(response, "Actualizar carga esperada")
        self.assertContains(response, 'value="sync_carga_point"')
        self.assertNotContains(response, "Actualizar recepción Point")
        self.assertContains(response, "Producto total")
        self.assertContains(response, "Esperado total")
        self.assertContains(response, "Pastel Snicker chico")
        self.assertContains(response, ">5</td>")
        self.assertContains(response, "Esperado")
        self.assertContains(response, "Cargado")
        self.assertContains(response, "Carga sin validar")
        self.assertNotContains(response, "Recibido Point")
        self.assertNotContains(response, "Recibido correcto")

    def test_ruta_detail_muestra_recibido_point_en_carga(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self._crear_linea_carga_con_transferencia_recibida(loaded_quantity=None)

        response = self.client.get(reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Recibido correcto")
        self.assertContains(response, ">5</td>")
        self.assertNotContains(response, "Carga sin validar")

    def test_ruta_detail_no_muestra_snapshot_historico_como_pendiente_point(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        _, _, transfer_line = self._crear_linea_carga_con_transferencia_recibida(
            is_received=False,
        )
        transfer_line.is_current_snapshot = False
        transfer_line.save(update_fields=["is_current_snapshot", "updated_at"])

        response = self.client.get(reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}))

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "Pendiente en Point")
        self.assertNotContains(response, "Pastel Snicker chico")

    def test_ruta_detail_muestra_enviado_cero_sin_recepcion_pendiente(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self._crear_linea_carga_con_transferencia_recibida(
            sent_quantity="0.000",
            loaded_quantity="0.000",
            received_quantity="0.000",
            is_received=False,
        )

        response = self.client.get(reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Enviado cero · sin recepción requerida")
        self.assertNotContains(response, "Pendiente en Point")

    def test_ruta_detail_distingue_solicitado_no_enviado_de_recepcion_pendiente(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        _, linea, transfer_line = self._crear_linea_carga_con_transferencia_recibida(
            loaded_quantity="0.000",
            is_received=False,
        )
        transfer_line.sent_at = None
        transfer_line.sent_quantity = Decimal("0")
        transfer_line.raw_payload = {"transfer": {"isEnviado": False}}
        transfer_line.save(update_fields=["sent_at", "sent_quantity", "raw_payload", "updated_at"])
        linea.cantidad_enviada_esperada = Decimal("0")
        linea.save(update_fields=["cantidad_enviada_esperada", "actualizado_en"])

        response = self.client.get(reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Solicitado · no enviado")
        self.assertNotContains(response, "Pendiente en Point")

    def test_ruta_detail_en_ruta_muestra_boton_recepcion_point(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.get(reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Actualizar recepción Point")
        self.assertContains(response, 'value="sync_recepcion_point"')

    def test_ruta_detail_sincroniza_recepcion_point(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        _, _, transfer_line = self._crear_linea_carga_con_transferencia_recibida()
        sync_job = transfer_line.sync_job

        with patch("logistica.services_carga_ruta.PointMovementSyncService") as service_cls:
            service_cls.return_value.run_transfer_sync.return_value = sync_job
            response = self.client.post(
                reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
                {"action": "sync_recepcion_point"},
                follow=True,
            )

        self.parada.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)
        self.assertContains(response, "Recepción Point sincronizada")
        self.assertContains(response, "Recibido correcto")

    def test_ruta_detail_sincroniza_recepcion_point_en_ruta_completada(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        _, _, transfer_line = self._crear_linea_carga_con_transferencia_recibida()
        self.ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
        self.ruta.save(update_fields=["estatus", "updated_at"])
        sync_job = transfer_line.sync_job

        with patch("logistica.services_carga_ruta.PointMovementSyncService") as service_cls:
            service_cls.return_value.run_transfer_sync.return_value = sync_job
            response = self.client.post(
                reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
                {"action": "sync_recepcion_point"},
                follow=True,
            )

        self.parada.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_PENDIENTE)
        self.assertContains(response, "Recepción Point sincronizada")

    def test_ruta_detail_actualiza_carga_point_con_sync_externo(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.ruta.estatus = RutaEntrega.ESTATUS_PLANEADA
        self.ruta.save(update_fields=["estatus", "updated_at"])
        self._crear_transferencia_point_abierta()
        sync_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_TRANSFERS,
            status=PointSyncJob.STATUS_SUCCESS,
            result_summary={},
        )

        with patch("logistica.services_carga_ruta.OpenTransferSyncService") as service_cls:
            service_cls.return_value.sync_open_transfers.return_value = sync_job
            response = self.client.post(
                reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
                {"action": "sync_carga_point"},
                follow=True,
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(service_cls.return_value.sync_open_transfers.call_count, 2)
        self.assertTrue(RutaCargaChecklistLinea.objects.filter(checklist__ruta=self.ruta, point_transfer_line__is_open=True).exists())
        self.assertContains(response, "Carga esperada actualizada")

    def test_ruta_detail_sync_carga_point_deadlock_redirige_sin_500(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        with patch(
            "logistica.views.sincronizar_checklist_carga_desde_point",
            side_effect=OperationalError("deadlock detected"),
        ):
            response = self.client.post(
                reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
                {"action": "sync_carga_point"},
                follow=True,
            )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "La sincronización se cruzó con otra actualización")

    def test_ruta_detail_bloquea_salida_si_checklist_carga_pendiente(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        ruta, _ = self._crear_ruta_planeada_para_carga()
        self._crear_transferencia_point_abierta()
        sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_EN_RUTA},
            follow=True,
        )

        ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertContains(response, "confirma todas las líneas de carga")

    def test_ruta_detail_bloquea_completar_con_diferencia_point(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.parada.estado = ParadaRuta.ESTADO_VISITADA
        self.parada.entrega_estado = ParadaRuta.ENTREGA_CON_DIFERENCIA
        self.parada.hora_llegada_real = timezone.now()
        self.parada.save(update_fields=["estado", "entrega_estado", "hora_llegada_real", "actualizado_en"])

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_COMPLETADA},
            follow=True,
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)
        self.assertContains(response, "diferencias o entregas no recibidas")

    def test_ruta_detail_completa_carga_cedis_sin_recepcion_point_pendiente(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        ruta, parada = self._crear_ruta_planeada_para_carga()
        self._crear_solicitud_cedis(ruta=ruta)
        checklist = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False).checklist
        checklist.lineas.update(
            cantidad_cargada=Decimal("5.000"),
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
            validado_por=self.user,
            validado_en=timezone.now(),
        )
        checklist.estatus = RutaCargaChecklist.ESTATUS_CONFIRMADA
        checklist.save(update_fields=["estatus", "actualizado_en"])
        ruta.estatus = RutaEntrega.ESTATUS_EN_RUTA
        ruta.save(update_fields=["estatus", "updated_at"])
        parada.estado = ParadaRuta.ESTADO_VISITADA
        parada.entrega_estado = ParadaRuta.ENTREGA_ENTREGADA
        parada.hora_llegada_real = timezone.now()
        parada.hora_salida_real = timezone.now()
        parada.entrega_confirmada_en = timezone.now()
        parada.entrega_confirmada_por = self.user
        parada.save(
            update_fields=[
                "estado",
                "entrega_estado",
                "hora_llegada_real",
                "hora_salida_real",
                "entrega_confirmada_en",
                "entrega_confirmada_por",
                "actualizado_en",
            ]
        )

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_COMPLETADA},
            follow=True,
        )

        ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_COMPLETADA)

    def test_ruta_detail_muestra_cierre_con_diferencia_autorizada(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.parada.estado = ParadaRuta.ESTADO_VISITADA
        self.parada.entrega_estado = ParadaRuta.ENTREGA_CON_DIFERENCIA
        self.parada.hora_llegada_real = timezone.now()
        self.parada.save(update_fields=["estado", "entrega_estado", "hora_llegada_real", "actualizado_en"])

        response = self.client.get(reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'value="cerrar_con_diferencia_autorizada"')
        self.assertContains(response, "Cerrar con diferencia autorizada")

    def test_ruta_detail_muestra_ajuste_manual_entrega(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.get(reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'value="ajustar_entrega_manual"')
        self.assertContains(response, "Editar entrega")
        self.assertContains(response, 'id="delivery-edit-modal"')
        self.assertContains(response, 'data-delivery-edit')
        self.assertContains(response, "Estado de entrega")
        self.assertContains(response, "Motivo / evidencia")

    def test_ruta_detail_ajuste_manual_entrega_no_fabrica_visita(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {
                "action": "ajustar_entrega_manual",
                "parada_id": self.parada.id,
                "entrega_estado": ParadaRuta.ENTREGA_ENTREGADA,
                "nota_entrega_manual": "Sucursal confirmó entrega por llamada.",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.parada.refresh_from_db()
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertIsNone(self.parada.hora_llegada_real)
        self.assertEqual(self.parada.entrega_estado, ParadaRuta.ENTREGA_ENTREGADA)
        self.assertEqual(self.parada.revision_entrega_estado, ParadaRuta.REVISION_PENDIENTE)
        self.assertTrue(
            ParadaEntregaEvidencia.objects.filter(
                ruta=self.ruta,
                parada=self.parada,
                tipo=ParadaEntregaEvidencia.TIPO_CONFIRMACION,
                metadata__origen="servicio_entregas",
            ).exists()
        )
        self.assertTrue(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_ENTREGA_EXCEPCIONAL).exists())

    def test_cerrar_ruta_con_diferencia_autorizada_notifica_logistica(self):
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        supervisor = User.objects.create_user(username="logistica.supervisor", password="pass123")
        UserModuleAccess.objects.create(user=supervisor, module="logistica", access=ACCESS_MANAGE)
        checklist, linea, transfer_line = self._crear_linea_carga_con_transferencia_recibida(
            received_quantity="3.000",
            is_received=True,
        )
        self.parada.estado = ParadaRuta.ESTADO_VISITADA
        self.parada.entrega_estado = ParadaRuta.ENTREGA_CON_DIFERENCIA
        self.parada.hora_llegada_real = timezone.now()
        self.parada.hora_salida_real = timezone.now()
        self.parada.save(
            update_fields=[
                "estado",
                "entrega_estado",
                "hora_llegada_real",
                "hora_salida_real",
                "actualizado_en",
            ]
        )

        evento = cerrar_ruta_con_diferencia_autorizada(
            ruta=self.ruta,
            user=self.user,
            notas="Diferencia revisable al cierre.",
        )

        self.ruta.refresh_from_db()
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_COMPLETADA)
        self.assertEqual(evento.metadata["tipo"], "cierre_con_diferencia_autorizada")
        self.assertEqual(evento.metadata["diferencias"][0]["productos"][0]["recibido"], "3.000")
        self.assertTrue(
            Notificacion.objects.filter(
                usuario=supervisor,
                titulo=f"Ruta con diferencia: {self.ruta.folio}",
                prioridad=Notificacion.PRIORIDAD_ALTA,
                url=f"/logistica/rutas/{self.ruta.id}/",
            ).exists()
        )

    def test_ruta_detail_cierre_con_diferencia_no_resincroniza_point(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.parada.estado = ParadaRuta.ESTADO_VISITADA
        self.parada.entrega_estado = ParadaRuta.ENTREGA_CON_DIFERENCIA
        self.parada.hora_llegada_real = timezone.now()
        self.parada.save(update_fields=["estado", "entrega_estado", "hora_llegada_real", "actualizado_en"])

        with patch("logistica.views.sincronizar_recepcion_desde_point") as sync_mock:
            response = self.client.post(
                reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
                {
                    "action": "cerrar_con_diferencia_autorizada",
                    "notas_cierre_diferencia": "Diferencia revisada.",
                },
            )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 302)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_COMPLETADA)
        sync_mock.assert_not_called()

    def test_api_ruta_activa_expone_recepcion_point_por_producto(self):
        self.client.force_login(self.user)
        self._crear_linea_carga_con_transferencia_recibida()

        response = self.client.get(reverse("api_logistica_ruta_activa"))

        self.assertEqual(response.status_code, 200)
        linea = response.json()["checklist_carga"]["lineas"][0]
        self.assertTrue(linea["point_is_received"])
        self.assertEqual(linea["point_received_quantity"], "5")
        self.assertEqual(linea["point_recepcion_estado"], "RECIBIDO_OK")
        self.assertIn("entrega_estado", response.json()["paradas"][0])

    def test_api_ruta_activa_muestra_toda_la_carga_aunque_haya_cedis_intermedio(self):
        self.client.force_login(self.user)
        cedis = PuntoLogistico.objects.create(
            nombre="CEDIS",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.567916",
            longitud="-108.459969",
            radio_geocerca_metros=120,
        )
        sucursal_dos = Sucursal.objects.create(codigo="TRAMO-2", nombre="Sucursal Tramo 2", activa=True)
        punto_dos = PuntoLogistico.objects.create(
            sucursal=sucursal_dos,
            nombre="Sucursal Tramo 2",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.580000",
            longitud="-108.480000",
            radio_geocerca_metros=120,
        )
        ParadaRuta.objects.create(ruta=self.ruta, punto=cedis, orden=2)
        parada_dos = ParadaRuta.objects.create(ruta=self.ruta, punto=punto_dos, orden=3)
        checklist = RutaCargaChecklist.objects.create(ruta=self.ruta, estatus=RutaCargaChecklist.ESTATUS_EN_REVISION)
        for parada, codigo in [(self.parada, "ANTES-CEDIS"), (parada_dos, "DESPUES-CEDIS")]:
            RutaCargaChecklistLinea.objects.create(
                checklist=checklist,
                parada=parada,
                source_hash=f"tramo-{codigo}",
                item_code=codigo,
                item_name=codigo,
                unit="pz",
                cantidad_solicitada="1.000",
                cantidad_enviada_esperada="1.000",
            )

        response = self.client.get(reverse("api_logistica_ruta_activa"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [linea["item_code"] for linea in response.json()["checklist_carga"]["lineas"]],
            ["ANTES-CEDIS", "DESPUES-CEDIS"],
        )

    def test_api_ruta_activa_excluye_lineas_superadas_del_checklist(self):
        self.client.force_login(self.user)
        checklist = RutaCargaChecklist.objects.create(ruta=self.ruta, estatus=RutaCargaChecklist.ESTATUS_EN_REVISION)
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=self.parada,
            source_hash="api-superada-vieja",
            transfer_external_id="T-API-SUP",
            detail_external_id="D-API-VIEJO",
            item_code="API-SUP",
            item_name="Producto superado API",
            unit="pz",
            cantidad_solicitada="1.000",
            cantidad_enviada_esperada="0.000",
            cantidad_cargada="0.000",
            estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA,
        )
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=self.parada,
            source_hash="api-superada-nueva",
            transfer_external_id="T-API-SUP",
            detail_external_id="D-API-NUEVO",
            item_code="API-SUP",
            item_name="Producto superado API",
            unit="pz",
            cantidad_solicitada="1.000",
            cantidad_enviada_esperada="1.000",
        )

        response = self.client.get(reverse("api_logistica_ruta_activa"))

        self.assertEqual(response.status_code, 200)
        source_hashes = [linea["item_code"] for linea in response.json()["checklist_carga"]["lineas"]]
        self.assertEqual(source_hashes, ["API-SUP"])
        self.assertEqual(len(response.json()["checklist_carga"]["lineas"]), 1)

    def test_api_ruta_activa_hidrata_carga_desde_cache_point_si_checklist_esta_vacia(self):
        self.client.force_login(self.user)
        ruta, parada = self._crear_ruta_planeada_para_carga()
        transfer_line = self._crear_transferencia_point_abierta(source_hash="transfer-pwa-vacia")
        RutaCargaChecklist.objects.create(ruta=ruta, estatus=RutaCargaChecklist.ESTATUS_PENDIENTE)

        response = self.client.get(reverse("api_logistica_ruta_activa"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["ruta"]["id"], ruta.id)
        self.assertEqual(len(response.json()["checklist_carga"]["lineas"]), 1)
        linea = response.json()["checklist_carga"]["lineas"][0]
        self.assertEqual(linea["item_code"], transfer_line.item_code)
        self.assertEqual(linea["parada"], parada.id)
        self.assertTrue(
            RutaCargaChecklistLinea.objects.filter(
                checklist__ruta=ruta,
                source_hash=transfer_line.source_hash,
                parada=parada,
            ).exists()
        )

    def test_checklist_detallado_serializa_sin_consultas_por_linea(self):
        self._crear_linea_carga_con_transferencia_recibida()
        checklist = obtener_checklist_carga_detallado(self.ruta)

        with self.assertNumQueries(0):
            data = RutaCargaChecklistSerializer(checklist).data

        self.assertEqual(data["total_lineas"], 1)
        self.assertEqual(data["lineas"][0]["point_recepcion_estado"], "RECIBIDO_OK")

    def test_db_bloquea_dos_rutas_en_ruta_mismo_repartidor(self):
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                RutaEntrega.objects.create(
                    nombre="Ruta Activa Duplicada",
                    fecha_ruta=timezone.localdate(),
                    estatus=RutaEntrega.ESTATUS_EN_RUTA,
                    repartidor=self.repartidor,
                    unidad_operativa=self.unidad,
                )

    def test_api_no_crea_ruta_directamente_en_ruta(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("api_logistica_rutas"),
            json.dumps({
                "nombre": "Ruta API Directa",
                "fecha_ruta": timezone.localdate().isoformat(),
                "estatus": RutaEntrega.ESTATUS_EN_RUTA,
                "repartidor": self.repartidor.id,
                "unidad_operativa": self.unidad.id,
            }),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertFalse(RutaEntrega.objects.filter(nombre="Ruta API Directa").exists())

    def test_checklist_carga_se_genera_desde_solicitud_cedis_enviada(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        solicitud, linea_solicitud, receta = self._crear_solicitud_cedis(ruta=ruta)

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        self.assertEqual(resumen.creadas, 1)
        linea = RutaCargaChecklistLinea.objects.get(checklist=resumen.checklist)
        self.assertEqual(linea.parada, parada)
        self.assertIsNone(linea.point_transfer_line)
        self.assertEqual(linea.transfer_external_id, solicitud.folio)
        self.assertEqual(linea.item_code, receta.codigo_point)
        self.assertEqual(linea.cantidad_solicitada, Decimal(str(linea_solicitud.solicitado)))
        self.assertEqual(linea.cantidad_enviada_esperada, Decimal("0.000"))
        self.assertIn("Point", linea.notas)
        self.assertEqual(linea.source_hash, f"cedis-reabasto-{ruta.fecha_ruta:%Y%m%d}-{self.sucursal.id}-{receta.id}")
        self.assertEqual(
            checklist_bloquea_salida(ruta),
            "confirma todas las líneas de carga antes de liberar la ruta",
        )

    def test_checklist_carga_mantiene_folios_distintos_independientes(self):
        ruta, _ = self._crear_ruta_planeada_para_carga()
        sin_enviado = self._crear_transferencia_point_abierta(
            item_name="Crema Para Fresas",
            source_hash="crema-sin-enviado",
        )
        sin_enviado.sent_quantity = Decimal("0.000")
        sin_enviado.sent_at = None
        sin_enviado.save(update_fields=["sent_quantity", "sent_at", "updated_at"])
        enviado = self._crear_transferencia_point_abierta(
            item_name="Crema Para Fresas",
            source_hash="crema-enviado",
        )

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        linea_cero = RutaCargaChecklistLinea.objects.get(checklist=resumen.checklist, source_hash=sin_enviado.source_hash)
        self.assertEqual(linea_cero.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
        self.assertEqual(linea_cero.cantidad_enviada_esperada, Decimal("0.000"))
        self.assertIn("aún no registra Enviado", linea_cero.notas)
        linea = RutaCargaChecklistLinea.objects.get(checklist=resumen.checklist, source_hash=enviado.source_hash)
        self.assertIsNone(linea_cero.superada_por)
        self.assertNotEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        self.assertEqual(linea.cantidad_enviada_esperada, Decimal("5.000"))

    def test_checklist_carga_mantiene_linea_point_reducida_a_cero_si_transferencia_ya_fue_enviada(self):
        ruta, _ = self._crear_ruta_planeada_para_carga()
        reducida = self._crear_transferencia_point_abierta(
            item_name="Crema Para Fresas",
            source_hash="crema-reducida-cero",
        )
        reducida.sent_quantity = Decimal("0.000")
        reducida.raw_payload = {"transfer": {"isEnviado": True, "Fecha_envio": timezone.now().isoformat()}}
        reducida.save(update_fields=["sent_quantity", "raw_payload", "updated_at"])

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        linea = RutaCargaChecklistLinea.objects.get(checklist=resumen.checklist, source_hash=reducida.source_hash)
        self.assertEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED)

    def test_checklist_carga_mantiene_linea_cero_si_mismo_folio_ya_tiene_enviados(self):
        ruta, _ = self._crear_ruta_planeada_para_carga()
        reducida = self._crear_transferencia_point_abierta(
            item_name="Galleta Lotus",
            source_hash="lotus-reducida-cero",
        )
        reducida.transfer_external_id = "T-LOTUS"
        reducida.sent_quantity = Decimal("0.000")
        reducida.sent_at = None
        reducida.raw_payload = {"transfer": {"isEnviado": False}}
        reducida.save(update_fields=["transfer_external_id", "sent_quantity", "sent_at", "raw_payload", "updated_at"])
        enviada = self._crear_transferencia_point_abierta(
            item_name="Pastel Fresas con Crema Mini",
            source_hash="lotus-folio-enviado",
        )
        enviada.transfer_external_id = "T-LOTUS"
        enviada.save(update_fields=["transfer_external_id", "updated_at"])

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        linea_reducida = RutaCargaChecklistLinea.objects.get(
            checklist=resumen.checklist,
            source_hash=reducida.source_hash,
        )
        self.assertEqual(
            linea_reducida.estatus,
            RutaCargaChecklistLinea.ESTATUS_PENDIENTE,
        )
        self.assertIn("aún no registra Enviado", linea_reducida.notas)
        self.assertTrue(RutaCargaChecklistLinea.objects.filter(checklist=resumen.checklist, source_hash=enviada.source_hash).exists())

    def test_checklist_carga_no_genera_lineas_para_parada_cedis(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        cedis_sucursal = Sucursal.objects.create(codigo="CEDIS-T", nombre="CEDIS Test", activa=True)
        cedis_punto = PuntoLogistico.objects.create(
            sucursal=cedis_sucursal,
            nombre="CEDIS Test",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.571000",
            longitud="-108.471000",
            radio_geocerca_metros=120,
        )
        ParadaRuta.objects.create(ruta=ruta, punto=cedis_punto, orden=2)
        self._crear_solicitud_cedis(ruta=ruta)
        self._crear_solicitud_cedis(ruta=ruta, sucursal=cedis_sucursal)

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        self.assertEqual(resumen.checklist.lineas.count(), 1)
        linea = resumen.checklist.lineas.get()
        self.assertEqual(linea.parada, parada)
        self.assertNotEqual(linea.parada.punto.tipo, PuntoLogistico.TIPO_CEDIS)

    def test_checklist_carga_cedis_usa_point_abierto_como_enviado(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        solicitud, linea_solicitud, receta = self._crear_solicitud_cedis(ruta=ruta, cantidad="5.000")
        transferencia = self._crear_transferencia_point_abierta(source_hash="transfer-ajuste-enviado")
        transferencia.sent_quantity = Decimal("3.000")
        transferencia.save(update_fields=["sent_quantity", "updated_at"])

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        self.assertEqual(resumen.creadas, 1)
        self.assertEqual(resumen.actualizadas, 1)
        linea = RutaCargaChecklistLinea.objects.get(checklist=resumen.checklist)
        self.assertEqual(linea.parada, parada)
        self.assertEqual(linea.source_hash, f"cedis-reabasto-{ruta.fecha_ruta:%Y%m%d}-{self.sucursal.id}-{receta.id}")
        self.assertEqual(linea.point_transfer_line, transferencia)
        self.assertEqual(linea.transfer_external_id, transferencia.transfer_external_id)
        self.assertEqual(linea.cantidad_solicitada, Decimal(str(linea_solicitud.solicitado)))
        self.assertEqual(linea.cantidad_enviada_esperada, Decimal("3.000"))
        self.assertEqual(linea.notas, "")
        self.assertFalse(RutaCargaChecklistLinea.objects.filter(source_hash=transferencia.source_hash).exists())

    def test_checklist_carga_resync_no_duplica_linea_ya_fusionada_con_cedis(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        solicitud, linea_solicitud, receta = self._crear_solicitud_cedis(ruta=ruta, cantidad="5.000")
        transferencia = self._crear_transferencia_point_abierta(source_hash="transfer-resync-fusionada")
        transferencia.sent_quantity = Decimal("3.000")
        transferencia.save(update_fields=["sent_quantity", "updated_at"])

        primer_resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)
        self.assertEqual(primer_resumen.creadas, 1)
        self.assertEqual(RutaCargaChecklistLinea.objects.filter(point_transfer_line=transferencia).count(), 1)

        segundo_resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        self.assertEqual(segundo_resumen.creadas, 0)
        self.assertEqual(
            RutaCargaChecklistLinea.objects.filter(point_transfer_line=transferencia).count(),
            1,
            "Resincronizar la misma transferencia ya fusionada con CEDIS no debe crear una fila duplicada.",
        )
        linea = RutaCargaChecklistLinea.objects.get(point_transfer_line=transferencia)
        self.assertEqual(linea.parada, parada)
        self.assertEqual(linea.cantidad_enviada_esperada, Decimal("3.000"))

    def test_checklist_carga_marca_superada_linea_vieja_mismo_folio_nuevo_detalle(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        original = self._crear_transferencia_point_abierta(source_hash="folio-correccion-original")
        original.transfer_external_id = "T-CORRECCION"
        original.detail_external_id = "D-ORIGINAL"
        original.sent_quantity = Decimal("0.000")
        original.sent_at = None
        original.raw_payload = {"transfer": {"isEnviado": False}}
        original.save(
            update_fields=["transfer_external_id", "detail_external_id", "sent_quantity", "sent_at", "raw_payload", "updated_at"]
        )

        primer_resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)
        self.assertEqual(primer_resumen.creadas, 1)
        linea_vieja = RutaCargaChecklistLinea.objects.get(source_hash=original.source_hash)
        self.assertEqual(linea_vieja.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
        self.assertIn("aún no registra Enviado", linea_vieja.notas)

        corregida = self._crear_transferencia_point_abierta(source_hash="folio-correccion-nueva")
        corregida.transfer_external_id = "T-CORRECCION"
        corregida.detail_external_id = "D-CORREGIDO"
        corregida.sent_quantity = Decimal("4.000")
        corregida.save(update_fields=["transfer_external_id", "detail_external_id", "sent_quantity", "updated_at"])
        original.is_current_snapshot = False
        original.is_open = False
        original.save(update_fields=["is_current_snapshot", "is_open", "updated_at"])

        segundo_resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        linea_vieja.refresh_from_db()
        linea_nueva = RutaCargaChecklistLinea.objects.get(source_hash=corregida.source_hash)
        self.assertEqual(segundo_resumen.creadas, 1)
        self.assertEqual(linea_vieja.estatus, RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        self.assertEqual(linea_vieja.superada_por, linea_nueva)
        self.assertEqual(linea_nueva.parada, parada)
        self.assertEqual(linea_nueva.cantidad_enviada_esperada, Decimal("4.000"))
        self.assertEqual(linea_nueva.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)

    def test_marcar_lineas_superadas_historicas_dry_run_no_escribe(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        checklist = RutaCargaChecklist.objects.create(ruta=ruta)
        duplicada_1 = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="historico-dup-1",
            transfer_external_id="T-HIST",
            detail_external_id="D-HIST-1",
            item_code="HIST-01",
            item_name="Producto histórico",
            unit="pz",
            cantidad_solicitada="2.000",
            cantidad_enviada_esperada="0.000",
            cantidad_cargada="0.000",
            estatus=RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED,
        )
        duplicada_2 = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="historico-dup-2",
            transfer_external_id="T-HIST",
            detail_external_id="D-HIST-2",
            item_code="HIST-01",
            item_name="Producto histórico",
            unit="pz",
            cantidad_solicitada="2.000",
            cantidad_enviada_esperada="0.000",
            cantidad_cargada="0.000",
            estatus=RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED,
        )
        resuelta = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="historico-resuelta",
            transfer_external_id="T-HIST",
            detail_external_id="D-HIST-3",
            item_code="HIST-01",
            item_name="Producto histórico",
            unit="pz",
            cantidad_solicitada="2.000",
            cantidad_enviada_esperada="2.000",
            cantidad_cargada="2.000",
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
            validado_por=self.user,
            validado_en=timezone.now(),
        )

        resumen_dry = marcar_lineas_checklist_superadas_historicas(dry_run=True)
        self.assertEqual(resumen_dry.grupos_afectados, 1)
        self.assertEqual(resumen_dry.lineas_superadas, 2)
        self.assertEqual(resumen_dry.grupos_ambiguos, 0)
        duplicada_1.refresh_from_db()
        duplicada_2.refresh_from_db()
        self.assertEqual(duplicada_1.estatus, RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED)
        self.assertEqual(duplicada_2.estatus, RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED)

        resumen_real = marcar_lineas_checklist_superadas_historicas(dry_run=False)
        self.assertEqual(resumen_real.grupos_afectados, 1)
        self.assertEqual(resumen_real.lineas_superadas, 2)
        duplicada_1.refresh_from_db()
        duplicada_2.refresh_from_db()
        resuelta.refresh_from_db()
        self.assertEqual(duplicada_1.estatus, RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        self.assertEqual(duplicada_1.superada_por, resuelta)
        self.assertEqual(duplicada_2.estatus, RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        self.assertEqual(duplicada_2.superada_por, resuelta)
        self.assertEqual(resuelta.estatus, RutaCargaChecklistLinea.ESTATUS_CARGADA)

    def test_marcar_lineas_superadas_historicas_omite_grupos_con_mas_de_una_resuelta(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        checklist = RutaCargaChecklist.objects.create(ruta=ruta)
        resuelta_1 = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="ambiguo-1",
            transfer_external_id="T-AMBIGUO",
            detail_external_id="D-AMBIGUO-1",
            item_code="AMB-01",
            item_name="Producto ambiguo",
            unit="pz",
            cantidad_solicitada="1.000",
            cantidad_enviada_esperada="1.000",
            cantidad_cargada="1.000",
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
            validado_por=self.user,
        )
        resuelta_2 = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="ambiguo-2",
            transfer_external_id="T-AMBIGUO",
            detail_external_id="D-AMBIGUO-2",
            item_code="AMB-01",
            item_name="Producto ambiguo",
            unit="pz",
            cantidad_solicitada="1.000",
            cantidad_enviada_esperada="1.000",
            cantidad_cargada="1.000",
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
            validado_por=self.user,
        )

        resumen = marcar_lineas_checklist_superadas_historicas(dry_run=False)

        self.assertEqual(resumen.grupos_afectados, 0)
        self.assertEqual(resumen.grupos_ambiguos, 1)
        self.assertEqual(set(resumen.detalle_ambiguos[0]["lineas_resueltas"]), {resuelta_1.id, resuelta_2.id})
        resuelta_1.refresh_from_db()
        resuelta_2.refresh_from_db()
        self.assertEqual(resuelta_1.estatus, RutaCargaChecklistLinea.ESTATUS_CARGADA)
        self.assertEqual(resuelta_2.estatus, RutaCargaChecklistLinea.ESTATUS_CARGADA)

    def test_marcar_lineas_superadas_historicas_conserva_dos_detalles_positivos_reales(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        checklist = RutaCargaChecklist.objects.create(ruta=ruta)
        creadas = []
        for detail, cantidad in (("D-POS-1", "2.000"), ("D-POS-2", "3.000")):
            transferencia = self._crear_transferencia_point_abierta(source_hash=f"positive-{detail}")
            transferencia.transfer_external_id = "T-POSITIVOS"
            transferencia.detail_external_id = detail
            transferencia.sent_quantity = Decimal(cantidad)
            transferencia.sent_at = timezone.now()
            transferencia.raw_payload = {"transfer": {"isEnviado": True}}
            transferencia.save(
                update_fields=[
                    "transfer_external_id",
                    "detail_external_id",
                    "sent_quantity",
                    "sent_at",
                    "raw_payload",
                    "updated_at",
                ]
            )
            creadas.append(
                RutaCargaChecklistLinea.objects.create(
                    checklist=checklist,
                    parada=parada,
                    source_hash=f"checklist-{detail}",
                    point_transfer_line=transferencia,
                    transfer_external_id="T-POSITIVOS",
                    detail_external_id=detail,
                    item_code="POSITIVO-01",
                    item_name="Producto positivo real",
                    unit="pz",
                    cantidad_solicitada=cantidad,
                    cantidad_enviada_esperada=cantidad,
                    estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE,
                )
            )

        resumen = marcar_lineas_checklist_superadas_historicas(dry_run=False)

        self.assertEqual(resumen.lineas_superadas, 0)
        for linea in creadas:
            linea.refresh_from_db()
            self.assertEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)

    def test_historica_enviado_cero_puede_ser_superada_por_detalle_positivo_posterior(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        checklist = RutaCargaChecklist.objects.create(ruta=ruta)
        cero = self._crear_transferencia_point_abierta(source_hash="historica-cero-enviado")
        cero.transfer_external_id = "T-CERO-A-POSITIVO"
        cero.detail_external_id = "D-CERO"
        cero.sent_quantity = Decimal("0")
        cero.sent_at = timezone.now() - timezone.timedelta(minutes=10)
        cero.raw_payload = {"transfer": {"isEnviado": True}}
        cero.save(
            update_fields=[
                "transfer_external_id",
                "detail_external_id",
                "sent_quantity",
                "sent_at",
                "raw_payload",
                "updated_at",
            ]
        )
        linea_cero = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="checklist-historica-cero",
            point_transfer_line=cero,
            transfer_external_id=cero.transfer_external_id,
            detail_external_id=cero.detail_external_id,
            item_code="CERO-POS-01",
            item_name="Producto corregido después de cero",
            unit="pz",
            cantidad_solicitada="2",
            cantidad_enviada_esperada="0",
            cantidad_cargada="0",
            estatus=RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED,
        )
        positiva = self._crear_transferencia_point_abierta(source_hash="historica-positiva-posterior")
        positiva.transfer_external_id = cero.transfer_external_id
        positiva.detail_external_id = "D-POSITIVO"
        positiva.sent_quantity = Decimal("2")
        positiva.sent_at = timezone.now()
        positiva.raw_payload = {"transfer": {"isEnviado": True}}
        positiva.save(
            update_fields=[
                "transfer_external_id",
                "detail_external_id",
                "sent_quantity",
                "sent_at",
                "raw_payload",
                "updated_at",
            ]
        )
        linea_positiva = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="checklist-historica-positiva",
            point_transfer_line=positiva,
            transfer_external_id=positiva.transfer_external_id,
            detail_external_id=positiva.detail_external_id,
            item_code="CERO-POS-01",
            item_name="Producto corregido después de cero",
            unit="pz",
            cantidad_solicitada="2",
            cantidad_enviada_esperada="2",
            estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE,
        )

        resumen = marcar_lineas_checklist_superadas_historicas(dry_run=False)

        self.assertEqual(resumen.lineas_superadas, 1)
        linea_cero.refresh_from_db()
        linea_positiva.refresh_from_db()
        self.assertEqual(linea_cero.estatus, RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        self.assertEqual(linea_cero.superada_por, linea_positiva)
        self.assertEqual(linea_positiva.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)

    def test_marcar_lineas_superadas_historicas_resuelve_validaciones_duplicadas_equivalentes(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        checklist = RutaCargaChecklist.objects.create(ruta=ruta)
        transferencia = self._crear_transferencia_point_abierta(source_hash="equiv-transfer")
        transferencia.transfer_external_id = "T-EQUIV"
        transferencia.detail_external_id = "D-EQUIV"
        transferencia.save(update_fields=["transfer_external_id", "detail_external_id", "updated_at"])

        primera_validacion = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="equiv-1",
            point_transfer_line=transferencia,
            transfer_external_id="T-EQUIV",
            detail_external_id="D-EQUIV",
            item_code="EQUIV-01",
            item_name="Producto validado dos veces",
            unit="pz",
            cantidad_solicitada="4.000",
            cantidad_enviada_esperada="4.000",
            cantidad_cargada="4.000",
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
            validado_por=self.user,
        )
        segunda_validacion = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="equiv-2",
            point_transfer_line=transferencia,
            transfer_external_id="T-EQUIV",
            detail_external_id="D-EQUIV",
            item_code="EQUIV-01",
            item_name="Producto validado dos veces",
            unit="pz",
            cantidad_solicitada="4.000",
            cantidad_enviada_esperada="4.000",
            cantidad_cargada="4.000",
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
            validado_por=self.user,
        )

        resumen = marcar_lineas_checklist_superadas_historicas(dry_run=False)

        self.assertEqual(resumen.grupos_ambiguos, 0)
        self.assertEqual(resumen.grupos_afectados, 1)
        primera_validacion.refresh_from_db()
        segunda_validacion.refresh_from_db()
        self.assertEqual(primera_validacion.estatus, RutaCargaChecklistLinea.ESTATUS_CARGADA)
        self.assertEqual(segunda_validacion.estatus, RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        self.assertEqual(segunda_validacion.superada_por, primera_validacion)

    def test_checklist_carga_conserva_transferencias_point_distintas_mismo_producto(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        _, _, receta = self._crear_solicitud_cedis(ruta=ruta, cantidad="10.000")
        primera = self._crear_transferencia_point_abierta(source_hash="transfer-folio-uno")
        primera.transfer_external_id = "T-FOLIO-1"
        primera.detail_external_id = "D-FOLIO-1"
        primera.sent_quantity = Decimal("4.000")
        primera.save(update_fields=["transfer_external_id", "detail_external_id", "sent_quantity", "updated_at"])
        segunda = self._crear_transferencia_point_abierta(source_hash="transfer-folio-dos")
        segunda.transfer_external_id = "T-FOLIO-2"
        segunda.detail_external_id = "D-FOLIO-2"
        segunda.sent_quantity = Decimal("6.000")
        segunda.save(update_fields=["transfer_external_id", "detail_external_id", "sent_quantity", "updated_at"])

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        self.assertEqual(resumen.checklist.lineas.count(), 2)
        cedis_line = RutaCargaChecklistLinea.objects.get(
            source_hash=f"cedis-reabasto-{ruta.fecha_ruta:%Y%m%d}-{self.sucursal.id}-{receta.id}"
        )
        point_line = RutaCargaChecklistLinea.objects.get(source_hash=segunda.source_hash)
        self.assertEqual(cedis_line.parada, parada)
        self.assertEqual(cedis_line.point_transfer_line, primera)
        self.assertEqual(cedis_line.cantidad_enviada_esperada, Decimal("4.000"))
        self.assertEqual(point_line.point_transfer_line, segunda)
        self.assertEqual(point_line.cantidad_enviada_esperada, Decimal("6.000"))

    def test_checklist_carga_point_en_cero_genera_linea_visible_resuelta(self):
        ruta, _ = self._crear_ruta_planeada_para_carga()
        self._crear_solicitud_cedis(ruta=ruta, cantidad="5.000")
        transferencia = self._crear_transferencia_point_abierta(source_hash="transfer-enviado-cero")
        transferencia.requested_quantity = Decimal("4.000")
        transferencia.sent_quantity = Decimal("0.000")
        transferencia.save(update_fields=["requested_quantity", "sent_quantity", "updated_at"])

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        self.assertEqual(resumen.checklist.lineas.count(), 1)
        self.assertIsNone(checklist_bloquea_salida(ruta))
        linea = resumen.checklist.lineas.get()
        self.assertEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED)
        self.assertEqual(linea.cantidad_cargada, Decimal("0.000"))

    def test_sync_carga_regresa_a_revision_si_quedan_lineas_pendientes(self):
        ruta, _ = self._crear_ruta_planeada_para_carga()
        self._crear_solicitud_cedis(ruta=ruta, cantidad="5.000")
        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)
        resumen.checklist.estatus = RutaCargaChecklist.ESTATUS_CONFIRMADA
        resumen.checklist.save(update_fields=["estatus", "actualizado_en"])

        transferencia = self._crear_transferencia_point_abierta(source_hash="transfer-enviado-cero-revision")
        transferencia.sent_quantity = Decimal("0.000")
        transferencia.save(update_fields=["sent_quantity", "updated_at"])

        segundo = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        self.assertTrue(segundo.checklist.lineas.exists())
        self.assertEqual(segundo.checklist.lineas.get().estatus, RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED)
        self.assertIsNone(checklist_bloquea_salida(ruta))

    def test_checklist_carga_cedis_no_pisa_linea_validada_con_point(self):
        ruta, _ = self._crear_ruta_planeada_para_carga()
        self._crear_solicitud_cedis(ruta=ruta, cantidad="5.000")
        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)
        linea = resumen.checklist.lineas.get()
        linea.cantidad_cargada = Decimal("5.000")
        linea.estatus = RutaCargaChecklistLinea.ESTATUS_CARGADA
        linea.save(update_fields=["cantidad_cargada", "estatus", "actualizado_en"])
        transferencia = self._crear_transferencia_point_abierta(source_hash="transfer-validada-no-pisa")
        transferencia.sent_quantity = Decimal("3.000")
        transferencia.save(update_fields=["sent_quantity", "updated_at"])

        segundo = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        linea.refresh_from_db()
        self.assertEqual(segundo.creadas, 0)
        self.assertEqual(segundo.actualizadas, 1)
        self.assertEqual(linea.point_transfer_line, transferencia)
        self.assertEqual(linea.cantidad_enviada_esperada, Decimal("3.000"))
        self.assertEqual(linea.cantidad_cargada, Decimal("5.000"))

    def test_checklist_carga_sincroniza_point_con_ruta_en_ruta(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        ruta.estatus = RutaEntrega.ESTATUS_EN_RUTA
        ruta.save(update_fields=["estatus", "updated_at"])
        transferencia = self._crear_transferencia_point_abierta(source_hash="transfer-en-ruta")
        transferencia.sent_quantity = Decimal("4.000")
        transferencia.save(update_fields=["sent_quantity", "updated_at"])

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        self.assertEqual(resumen.creadas, 1)
        linea = RutaCargaChecklistLinea.objects.get(checklist=resumen.checklist)
        self.assertEqual(linea.parada, parada)
        self.assertEqual(linea.point_transfer_line, transferencia)
        self.assertEqual(linea.cantidad_enviada_esperada, Decimal("4.000"))

    def test_totales_carga_muestran_parcial_con_lineas_pendientes(self):
        from logistica.views import _recepcion_point_rows, _totales_recepcion_point

        ruta, parada = self._crear_ruta_planeada_para_carga()
        checklist = RutaCargaChecklist.objects.create(ruta=ruta)
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="parcial-cargada",
            transfer_external_id="SRC-1",
            detail_external_id="1",
            item_code="0065",
            item_name="Pastel de Zanahoria Mediano",
            unit="pz",
            cantidad_solicitada=Decimal("3.000"),
            cantidad_enviada_esperada=Decimal("3.000"),
            cantidad_cargada=Decimal("3.000"),
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
        )
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="parcial-pendiente",
            transfer_external_id="SRC-2",
            detail_external_id="2",
            item_code="0065",
            item_name="Pastel de Zanahoria Mediano",
            unit="pz",
            cantidad_solicitada=Decimal("1.000"),
            cantidad_enviada_esperada=Decimal("1.000"),
        )

        total = _totales_recepcion_point(_recepcion_point_rows(checklist))[0]

        self.assertFalse(total["cargado_validado"])
        self.assertTrue(total["cargado_parcial"])
        self.assertEqual(total["cargado"], Decimal("3.000"))

    def test_totales_carga_separan_solicitado_enviado_y_cargado_pwa(self):
        from logistica.views import _recepcion_point_rows, _totales_recepcion_point

        ruta, _ = self._crear_ruta_planeada_para_carga()
        transferencia = self._crear_transferencia_point_abierta(source_hash="transfer-solicitado-enviado")
        transferencia.requested_quantity = Decimal("10.000")
        transferencia.sent_quantity = Decimal("6.000")
        transferencia.save(update_fields=["requested_quantity", "sent_quantity", "updated_at"])

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)
        linea = resumen.checklist.lineas.get()
        linea.cantidad_cargada = Decimal("5.000")
        linea.estatus = RutaCargaChecklistLinea.ESTATUS_PARCIAL
        linea.save(update_fields=["cantidad_cargada", "estatus", "actualizado_en"])

        rows = _recepcion_point_rows(resumen.checklist)
        total = _totales_recepcion_point(rows)[0]

        self.assertEqual(rows[0]["solicitado"], Decimal("10.000"))
        self.assertEqual(rows[0]["enviado"], Decimal("6.000"))
        self.assertEqual(rows[0]["cargado"], Decimal("5.000"))
        self.assertEqual(total["solicitado"], Decimal("10.000"))
        self.assertEqual(total["enviado"], Decimal("6.000"))
        self.assertEqual(total["cargado"], Decimal("5.000"))

    def test_totales_recepcion_point_excluye_lineas_superadas(self):
        from logistica.views import _recepcion_point_rows, _totales_recepcion_point

        ruta, parada = self._crear_ruta_planeada_para_carga()
        checklist = RutaCargaChecklist.objects.create(ruta=ruta)
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="superada-totales-vieja",
            transfer_external_id="T-TOT",
            detail_external_id="D-TOT-VIEJO",
            item_code="0065",
            item_name="Pastel de Zanahoria Mediano",
            unit="pz",
            cantidad_solicitada=Decimal("3.000"),
            cantidad_enviada_esperada=Decimal("0.000"),
            cantidad_cargada=Decimal("0.000"),
            estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA,
        )
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="superada-totales-nueva",
            transfer_external_id="T-TOT",
            detail_external_id="D-TOT-NUEVO",
            item_code="0065",
            item_name="Pastel de Zanahoria Mediano",
            unit="pz",
            cantidad_solicitada=Decimal("3.000"),
            cantidad_enviada_esperada=Decimal("3.000"),
            estatus=RutaCargaChecklistLinea.ESTATUS_PENDIENTE,
        )

        rows = _recepcion_point_rows(checklist)
        self.assertEqual(len(rows), 2, "La fila superada debe seguir visible individualmente para auditoría.")
        fila_superada = next(row for row in rows if row["linea"].estatus == RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        self.assertEqual(fila_superada["estado_label"], "Superada (Point corrigió el envío)")
        self.assertEqual(fila_superada["estado_tone"], "muted")
        total = _totales_recepcion_point(rows)[0]

        self.assertEqual(total["solicitado"], Decimal("3.000"))
        self.assertEqual(total["enviado"], Decimal("3.000"))

    def test_detalle_ruta_muestra_columnas_point_y_pwa(self):
        self.client.force_login(self.user)
        ruta, _ = self._crear_ruta_planeada_para_carga()
        transferencia = self._crear_transferencia_point_abierta(source_hash="transfer-columnas-detail")
        transferencia.requested_quantity = Decimal("10.000")
        transferencia.sent_quantity = Decimal("6.000")
        transferencia.save(update_fields=["requested_quantity", "sent_quantity", "updated_at"])
        sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        response = self.client.get(reverse("logistica:ruta_detail", args=[ruta.id]))

        self.assertContains(response, "Esperado total (solicitado Point)")
        self.assertContains(response, "Ajustado total (enviado Point)")
        self.assertContains(response, "Cargado total (PWA)")
        self.assertContains(response, "Esperado (solicitado Point)")
        self.assertContains(response, "Ajustado (enviado Point)")
        self.assertNotContains(response, "10.000")
        self.assertNotContains(response, "6.000")
        self.assertContains(response, "value=\"6\"")

    def test_checklist_carga_cedis_omite_borrador_y_cancelada(self):
        ruta, _ = self._crear_ruta_planeada_para_carga()
        solicitud, _, _ = self._crear_solicitud_cedis(ruta=ruta, estado=SolicitudReabastoCedis.ESTADO_BORRADOR)

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        self.assertEqual(resumen.creadas, 0)
        self.assertEqual(resumen.checklist.lineas.count(), 0)
        solicitud.estado = SolicitudReabastoCedis.ESTADO_CANCELADA
        solicitud.save(update_fields=["estado", "actualizado_en"])
        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)
        self.assertEqual(resumen.creadas, 0)
        self.assertEqual(resumen.checklist.lineas.count(), 0)

    def test_recepcion_point_empata_checklist_cedis_por_sucursal_y_producto(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        self._crear_solicitud_cedis(ruta=ruta)
        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)
        linea = resumen.checklist.lineas.get()
        linea.cantidad_cargada = linea.cantidad_enviada_esperada
        linea.estatus = RutaCargaChecklistLinea.ESTATUS_CARGADA
        linea.save(update_fields=["cantidad_cargada", "estatus", "actualizado_en"])
        ruta.estatus = RutaEntrega.ESTATUS_EN_RUTA
        ruta.save(update_fields=["estatus", "updated_at"])
        transferencia = self._crear_transferencia_point_abierta(source_hash="transfer-recibe-cedis")
        transferencia.is_open = False
        transferencia.is_received = True
        transferencia.is_finalized = True
        transferencia.received_quantity = linea.cantidad_enviada_esperada
        transferencia.received_at = timezone.now()
        transferencia.save(update_fields=["is_open", "is_received", "is_finalized", "received_quantity", "received_at", "updated_at"])

        recepcion = sincronizar_recepcion_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        linea.refresh_from_db()
        ruta.refresh_from_db()
        self.assertEqual(recepcion.lineas_recibidas, 1)
        self.assertEqual(recepcion.lineas_pendientes_point, 0)
        self.assertEqual(linea.point_transfer_line, transferencia)
        self.assertEqual(parada.evidencias_entrega.get().cantidad_entregada, linea.cantidad_enviada_esperada)
        self.assertEqual(parada.evidencias_entrega.get().metadata["source_hashes"], [transferencia.source_hash])
        self.assertEqual(ruta.cumplimiento_porcentaje, Decimal("0.00"))

    def test_recepcion_point_resuelve_ambiguo_por_cantidad_recibida(self):
        ruta, _ = self._crear_ruta_planeada_para_carga()
        self._crear_solicitud_cedis(ruta=ruta)
        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)
        linea = resumen.checklist.lineas.get()
        linea.cantidad_cargada = Decimal("5.000")
        linea.estatus = RutaCargaChecklistLinea.ESTATUS_CARGADA
        linea.save(update_fields=["cantidad_cargada", "estatus", "actualizado_en"])
        ruta.estatus = RutaEntrega.ESTATUS_EN_RUTA
        ruta.save(update_fields=["estatus", "updated_at"])
        vieja = self._crear_transferencia_point_abierta(source_hash="transfer-recibe-ambiguo-vieja")
        vieja.is_open = False
        vieja.is_received = True
        vieja.is_finalized = True
        vieja.received_quantity = Decimal("1.000")
        vieja.received_at = timezone.now() - timezone.timedelta(hours=2)
        vieja.save(update_fields=["is_open", "is_received", "is_finalized", "received_quantity", "received_at", "updated_at"])
        correcta = self._crear_transferencia_point_abierta(source_hash="transfer-recibe-ambiguo-correcta")
        correcta.is_open = False
        correcta.is_received = True
        correcta.is_finalized = True
        correcta.received_quantity = Decimal("5.000")
        correcta.received_at = timezone.now()
        correcta.save(update_fields=["is_open", "is_received", "is_finalized", "received_quantity", "received_at", "updated_at"])

        recepcion = sincronizar_recepcion_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        linea.refresh_from_db()
        self.assertEqual(recepcion.lineas_recibidas, 1)
        self.assertEqual(recepcion.lineas_pendientes_point, 0)
        self.assertEqual(linea.point_transfer_line, correcta)

    def test_recepcion_point_no_empata_producto_ambiguo(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        self._crear_solicitud_cedis(ruta=ruta)
        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)
        linea = resumen.checklist.lineas.get()
        linea.cantidad_cargada = linea.cantidad_enviada_esperada
        linea.estatus = RutaCargaChecklistLinea.ESTATUS_CARGADA
        linea.save(update_fields=["cantidad_cargada", "estatus", "actualizado_en"])
        ruta.estatus = RutaEntrega.ESTATUS_EN_RUTA
        ruta.save(update_fields=["estatus", "updated_at"])
        for idx in range(2):
            transferencia = self._crear_transferencia_point_abierta(source_hash=f"transfer-recibe-ambiguo-{idx}")
            transferencia.is_open = False
            transferencia.is_received = True
            transferencia.is_finalized = True
            transferencia.received_quantity = Decimal("1.000")
            transferencia.received_at = timezone.now()
            transferencia.save(update_fields=["is_open", "is_received", "is_finalized", "received_quantity", "received_at", "updated_at"])

        recepcion = sincronizar_recepcion_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        linea.refresh_from_db()
        self.assertEqual(recepcion.lineas_recibidas, 0)
        self.assertEqual(recepcion.lineas_pendientes_point, 1)
        self.assertIsNone(linea.point_transfer_line_id)

    def test_sync_carga_no_pisa_lineas_ya_validadas(self):
        ruta, _ = self._crear_ruta_planeada_para_carga()
        transferencia = self._crear_transferencia_point_abierta()
        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)
        linea = resumen.checklist.lineas.get()
        linea.cantidad_cargada = Decimal("5.000")
        linea.estatus = RutaCargaChecklistLinea.ESTATUS_CARGADA
        linea.save(update_fields=["cantidad_cargada", "estatus", "actualizado_en"])
        transferencia.sent_quantity = Decimal("3.000")
        transferencia.save(update_fields=["sent_quantity", "updated_at"])

        segundo = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        linea.refresh_from_db()
        self.assertEqual(segundo.creadas, 0)
        self.assertEqual(segundo.actualizadas, 1)
        self.assertEqual(linea.cantidad_enviada_esperada, Decimal("3.000"))

    def test_checklist_carga_se_genera_desde_transferencia_point_abierta(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        transferencia = self._crear_transferencia_point_abierta()

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        self.assertEqual(resumen.creadas, 1)
        linea = RutaCargaChecklistLinea.objects.get(checklist=resumen.checklist)
        self.assertEqual(linea.parada, parada)
        self.assertEqual(linea.point_transfer_line, transferencia)
        self.assertEqual(linea.item_name, "Pastel Snicker chico")
        self.assertEqual(linea.cantidad_enviada_esperada, Decimal(str(transferencia.sent_quantity)))
        self.assertEqual(linea.source_hash, transferencia.source_hash)

    def test_checklist_carga_mantiene_transferencia_abierta_sin_enviado_visible(self):
        ruta, _ = self._crear_ruta_planeada_para_carga()
        transferencia = self._crear_transferencia_point_abierta(source_hash="transfer-sin-enviado")
        transferencia.sent_quantity = Decimal("0.000")
        transferencia.requested_quantity = Decimal("2.000")
        transferencia.save(update_fields=["sent_quantity", "requested_quantity", "updated_at"])

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        self.assertEqual(resumen.creadas, 1)
        linea = RutaCargaChecklistLinea.objects.get(checklist=resumen.checklist, source_hash=transferencia.source_hash)
        self.assertEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED)
        self.assertIsNone(checklist_bloquea_salida(ruta))

    def test_checklist_carga_point_cero_resuelve_solicitud_cedis_visible(self):
        ruta, _ = self._crear_ruta_planeada_para_carga()
        self._crear_solicitud_cedis(ruta=ruta, cantidad="5.000")
        transferencia = self._crear_transferencia_point_abierta(source_hash="transfer-cero-cancela-carga")
        transferencia.sent_quantity = Decimal("0.000")
        transferencia.requested_quantity = Decimal("5.000")
        transferencia.save(update_fields=["sent_quantity", "requested_quantity", "updated_at"])

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        self.assertEqual(resumen.checklist.lineas.count(), 1)
        linea = resumen.checklist.lineas.get()
        self.assertEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED)
        self.assertEqual(linea.cantidad_solicitada, Decimal("5.000"))

    def test_checklist_carga_incluye_transferencia_del_dia_anterior(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        transferencia = self._crear_transferencia_point_abierta(
            source_hash="transfer-vespertina-previa",
            registered_at=timezone.now() - timezone.timedelta(days=1),
        )

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        self.assertEqual(resumen.creadas, 1)
        linea = RutaCargaChecklistLinea.objects.get(checklist=resumen.checklist)
        self.assertEqual(linea.parada, parada)
        self.assertEqual(linea.point_transfer_line, transferencia)

    def test_checklist_carga_recupera_enviado_aunque_point_ya_lo_marco_recibido(self):
        ruta, _ = self._crear_ruta_planeada_para_carga()
        transferencia = self._crear_transferencia_point_abierta(source_hash="transfer-recibida-carga")
        transferencia.is_open = False
        transferencia.is_received = True
        transferencia.is_finalized = True
        transferencia.received_quantity = transferencia.sent_quantity
        transferencia.received_at = timezone.now()
        transferencia.save(update_fields=["is_open", "is_received", "is_finalized", "received_quantity", "received_at", "updated_at"])

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        self.assertEqual(resumen.creadas, 1)
        self.assertEqual(resumen.checklist.lineas.count(), 1)
        self.assertEqual(
            resumen.checklist.lineas.get().point_transfer_line,
            transferencia,
        )

    def test_checklist_carga_incluye_folios_vigentes_abiertos_y_recibidos(self):
        ruta, _ = self._crear_ruta_planeada_para_carga()
        abierta = self._crear_transferencia_point_abierta(source_hash="transfer-abierta-mixta")
        recibida = self._crear_transferencia_point_abierta(source_hash="transfer-recibida-mixta")
        recibida.is_open = False
        recibida.is_received = True
        recibida.is_finalized = True
        recibida.received_quantity = recibida.sent_quantity
        recibida.received_at = timezone.now()
        recibida.save(update_fields=["is_open", "is_received", "is_finalized", "received_quantity", "received_at", "updated_at"])

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        self.assertEqual(resumen.checklist.lineas.count(), 2)
        self.assertEqual(
            set(resumen.checklist.lineas.values_list("point_transfer_line_id", flat=True)),
            {abierta.id, recibida.id},
        )

    def test_checklist_carga_usa_alias_unico_de_sucursal(self):
        sucursal_point = Sucursal.objects.create(codigo="PLAZA_NIO", nombre="Plaza Nío", activa=True)
        sucursal_ruta = Sucursal.objects.create(codigo="NIO", nombre="Sucursal Plaza Nio", activa=True)
        punto = PuntoLogistico.objects.create(
            sucursal=sucursal_ruta,
            nombre="Sucursal Plaza Nio",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )
        ruta = RutaEntrega.objects.create(
            nombre="Ruta Alias Point",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_PLANEADA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )
        parada = ParadaRuta.objects.create(ruta=ruta, punto=punto, orden=1)
        transferencia = self._crear_transferencia_point_abierta(sucursal=sucursal_point, source_hash="transfer-alias-nio")

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        self.assertEqual(resumen.creadas, 1)
        linea = RutaCargaChecklistLinea.objects.get(checklist=resumen.checklist)
        self.assertEqual(linea.parada, parada)
        self.assertEqual(linea.point_transfer_line, transferencia)

    def test_checklist_carga_desbloquea_si_luego_encuentra_transferencias(self):
        ruta, _ = self._crear_ruta_planeada_para_carga()
        checklist = RutaCargaChecklist.objects.create(
            ruta=ruta,
            estatus=RutaCargaChecklist.ESTATUS_BLOQUEADA,
            notas="No se encontraron transferencias abiertas de Point para las sucursales de esta ruta.",
        )
        self._crear_transferencia_point_abierta(source_hash="transfer-desbloquea")

        resumen = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        checklist.refresh_from_db()
        self.assertEqual(resumen.creadas, 1)
        self.assertEqual(checklist.estatus, RutaCargaChecklist.ESTATUS_EN_REVISION)
        self.assertEqual(checklist.notas, "")

    def test_checklist_carga_no_duplica_transferencia_point_en_otra_ruta(self):
        ruta_uno, _ = self._crear_ruta_planeada_para_carga()
        ruta_dos, _ = self._crear_ruta_planeada_para_carga()
        transferencia = self._crear_transferencia_point_abierta()

        primero = sincronizar_checklist_carga_desde_point(ruta=ruta_uno, user=self.user, ejecutar_sync=False)
        segundo = sincronizar_checklist_carga_desde_point(ruta=ruta_dos, user=self.user, ejecutar_sync=False)

        self.assertEqual(primero.creadas, 1)
        self.assertEqual(segundo.creadas, 0)
        self.assertEqual(segundo.omitidas, 1)
        self.assertEqual(RutaCargaChecklistLinea.objects.filter(source_hash=transferencia.source_hash).count(), 1)

    def test_db_bloquea_source_hash_point_duplicado_entre_checklists(self):
        ruta_uno, parada_uno = self._crear_ruta_planeada_para_carga()
        ruta_dos, parada_dos = self._crear_ruta_planeada_para_carga()
        checklist_uno = RutaCargaChecklist.objects.create(ruta=ruta_uno)
        checklist_dos = RutaCargaChecklist.objects.create(ruta=ruta_dos)
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist_uno,
            parada=parada_uno,
            transfer_external_id="T-DUP",
            detail_external_id="D-DUP-1",
            source_hash="source-duplicado-global",
            item_name="Pastel Snicker chico",
            cantidad_solicitada="5.000",
            cantidad_enviada_esperada="5.000",
        )

        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                RutaCargaChecklistLinea.objects.create(
                    checklist=checklist_dos,
                    parada=parada_dos,
                    transfer_external_id="T-DUP",
                    detail_external_id="D-DUP-2",
                    source_hash="source-duplicado-global",
                    item_name="Pastel Snicker chico",
                    cantidad_solicitada="5.000",
                    cantidad_enviada_esperada="5.000",
                )

    def test_api_ruta_status_bloquea_salida_si_checklist_carga_pendiente(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        ruta, _ = self._crear_ruta_planeada_para_carga()
        self._crear_transferencia_point_abierta()
        sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        response = self.client.post(
            reverse("api_logistica_ruta_estatus", kwargs={"ruta_id": ruta.id}),
            json.dumps({"estatus": RutaEntrega.ESTATUS_EN_RUTA}),
            content_type="application/json",
        )

        ruta.refresh_from_db()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertIn("confirma todas las líneas de carga", response.json()["detail"])

    def test_ruta_detail_confirma_carga_manual_y_permita_liberar(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        ruta, _ = self._crear_ruta_planeada_para_carga()
        self._crear_transferencia_point_abierta()
        checklist = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False).checklist

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "confirmar_carga_manual", "notas_carga_manual": "Carga confirmada en patio."},
            follow=True,
        )

        checklist.refresh_from_db()
        linea = checklist.lineas.get()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_CARGADA)
        self.assertEqual(linea.cantidad_cargada, linea.cantidad_enviada_esperada)
        self.assertEqual(checklist.estatus, RutaCargaChecklist.ESTATUS_CONFIRMADA)
        self.assertEqual(checklist.confirmado_por, self.user)

        released = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_EN_RUTA},
            follow=True,
        )
        ruta.refresh_from_db()
        self.assertEqual(released.status_code, 200)
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)

    def test_ruta_detail_permite_capturar_linea_carga_manual_en_erp(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        ruta, _ = self._crear_ruta_planeada_para_carga()
        self._crear_transferencia_point_abierta()
        checklist = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False).checklist
        linea = checklist.lineas.get()

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {
                "action": "confirmar_linea_carga_manual",
                "linea_carga_id": str(linea.id),
                "cantidad_cargada_manual": "3",
                "motivo_diferencia_manual": RutaCargaChecklistLinea.MOTIVO_STOCK_LIMITADO,
                "notas_carga_manual": "Captura ERP en patio.",
            },
            follow=True,
        )

        checklist.refresh_from_db()
        linea.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_PARCIAL)
        self.assertEqual(linea.cantidad_cargada, Decimal("3"))
        self.assertEqual(linea.validado_por, self.user)
        self.assertEqual(checklist.estatus, RutaCargaChecklist.ESTATUS_CON_INCIDENCIA)

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {
                "action": "confirmar_linea_carga_manual",
                "linea_carga_id": str(linea.id),
                "cantidad_cargada_manual": str(linea.cantidad_enviada_esperada),
                "motivo_diferencia_manual": "",
                "notas_carga_manual": "Corrección ERP.",
            },
            follow=True,
        )

        checklist.refresh_from_db()
        linea.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_CARGADA)
        self.assertEqual(linea.cantidad_cargada, linea.cantidad_enviada_esperada)
        self.assertEqual(checklist.estatus, RutaCargaChecklist.ESTATUS_CONFIRMADA)

    def test_ruta_detail_no_requiere_captura_manual_si_point_envio_cero(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        ruta, _ = self._crear_ruta_planeada_para_carga()
        transferencia = self._crear_transferencia_point_abierta(source_hash="manual-sin-enviado")
        transferencia.sent_quantity = Decimal("0.000")
        transferencia.save(update_fields=["sent_quantity", "updated_at"])
        checklist = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False).checklist
        linea = checklist.lineas.get()

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {
                "action": "confirmar_linea_carga_manual",
                "linea_carga_id": str(linea.id),
                "cantidad_cargada_manual": "0",
                "motivo_diferencia_manual": "",
                "notas_carga_manual": "Intento manual.",
            },
            follow=True,
        )

        linea.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED)
        self.assertEqual(linea.cantidad_cargada, Decimal("0.000"))
        self.assertIsNone(checklist_bloquea_salida(ruta))

    def test_ruta_detail_muestra_captura_erp_en_ruta(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        ruta, _ = self._crear_ruta_planeada_para_carga()
        self._crear_transferencia_point_abierta()
        sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)
        ruta.estatus = RutaEntrega.ESTATUS_EN_RUTA
        ruta.save(update_fields=["estatus"])

        response = self.client.get(reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Captura ERP")
        self.assertContains(response, 'name="action" value="confirmar_linea_carga_manual"')

    def test_validar_linea_carga_rechaza_linea_superada(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        checklist = RutaCargaChecklist.objects.create(ruta=ruta, estatus=RutaCargaChecklist.ESTATUS_EN_REVISION)
        linea_superada = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="linea-superada-validar",
            transfer_external_id="T-SUP",
            detail_external_id="D-SUP-VIEJO",
            item_code="PZA-SUP",
            item_name="Producto superado",
            unit="PZA",
            cantidad_solicitada="2.000",
            cantidad_enviada_esperada="0.000",
            cantidad_cargada="0.000",
            estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA,
        )

        with self.assertRaises(ValidationError):
            validar_linea_carga(
                user=self.user,
                ruta=ruta,
                repartidor=self.repartidor,
                linea_id=linea_superada.id,
                cantidad_cargada="2.000",
            )

        linea_superada.refresh_from_db()
        self.assertEqual(linea_superada.estatus, RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        self.assertIsNone(linea_superada.validado_por)

    def test_validar_linea_carga_sigue_bloqueando_salida_si_quedan_lineas_pendientes(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        checklist = RutaCargaChecklist.objects.create(ruta=ruta, estatus=RutaCargaChecklist.ESTATUS_EN_REVISION)
        linea_uno = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="linea-uno",
            item_code="PZA1",
            item_name="Producto uno",
            unit="PZA",
            cantidad_solicitada="2.000",
            cantidad_enviada_esperada="2.000",
        )
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="linea-dos",
            item_code="PZA2",
            item_name="Producto dos",
            unit="PZA",
            cantidad_solicitada="3.000",
            cantidad_enviada_esperada="3.000",
        )

        validar_linea_carga(
            user=self.user,
            ruta=ruta,
            repartidor=self.repartidor,
            linea_id=linea_uno.id,
            cantidad_cargada="2.000",
        )

        checklist.refresh_from_db()
        self.assertEqual(checklist.estatus, RutaCargaChecklist.ESTATUS_EN_REVISION)
        self.assertEqual(checklist_bloquea_salida(ruta), "confirma todas las líneas de carga antes de liberar la ruta")

    def test_checklist_carga_libera_solo_tramo_antes_de_cedis(self):
        ruta, parada_guamuchil = self._crear_ruta_planeada_para_carga()
        cedis = PuntoLogistico.objects.create(
            nombre="CEDIS tramo",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.567916",
            longitud="-108.459969",
            radio_geocerca_metros=120,
        )
        parada_cedis = ParadaRuta.objects.create(ruta=ruta, punto=cedis, orden=2, punto_nombre_snapshot="CEDIS")
        parada_payan = ParadaRuta.objects.create(ruta=ruta, punto=parada_guamuchil.punto, orden=3, punto_nombre_snapshot="Sucursal Payan")
        checklist = RutaCargaChecklist.objects.create(ruta=ruta, estatus=RutaCargaChecklist.ESTATUS_EN_REVISION)
        linea_guamuchil = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada_guamuchil,
            source_hash="guamuchil-cargada",
            item_code="PZA1",
            item_name="Producto Guamuchil",
            unit="PZA",
            cantidad_solicitada="2.000",
            cantidad_enviada_esperada="2.000",
            cantidad_cargada="2.000",
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
            validado_por=self.user,
            validado_en=timezone.now(),
        )
        linea_sobrante = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada_guamuchil,
            source_hash="guamuchil-sobrante",
            item_code="PZA2",
            item_name="Producto extra Guamuchil",
            unit="PZA",
            cantidad_solicitada="0.000",
            cantidad_enviada_esperada="0.000",
            cantidad_cargada="1.000",
            estatus=RutaCargaChecklistLinea.ESTATUS_SOBRANTE,
            motivo_diferencia=RutaCargaChecklistLinea.MOTIVO_CAMBIO_AUTORIZADO,
            validado_por=self.user,
            validado_en=timezone.now(),
        )
        linea_payan = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada_payan,
            source_hash="payan-pendiente",
            item_code="PZA3",
            item_name="Producto Payan",
            unit="PZA",
            cantidad_solicitada="3.000",
            cantidad_enviada_esperada="3.000",
        )

        self.assertEqual(list(lineas_tramo_operativo_actual(ruta, checklist=checklist).order_by("id")), [linea_guamuchil, linea_sobrante])
        self.assertIsNone(checklist_bloquea_salida(ruta))
        parada_cedis.estado = ParadaRuta.ESTADO_VISITADA
        parada_cedis.save(update_fields=["estado", "actualizado_en"])

        self.assertEqual(
            list(lineas_tramo_operativo_actual(ruta, checklist=checklist).order_by("id")),
            [linea_guamuchil, linea_sobrante],
        )

        EventoRuta.objects.create(
            ruta=ruta,
            parada=parada_cedis,
            tipo=EventoRuta.TIPO_RECARGA_CEDIS,
            severidad=EventoRuta.SEVERIDAD_INFO,
            descripcion="Recarga CEDIS reconciliada.",
            creado_por=self.user,
        )

        self.assertEqual(list(lineas_tramo_operativo_actual(ruta, checklist=checklist)), [linea_payan])
        parada_cedis.delete()
        self.assertEqual(checklist_bloquea_salida(ruta), "confirma todas las líneas de carga antes de liberar la ruta")

    def test_validar_producto_tramo_confirma_sumatoria_antes_de_cedis(self):
        ruta, parada_nio = self._crear_ruta_planeada_para_carga()
        parada_nio.punto_nombre_snapshot = "Sucursal Plaza Nio"
        parada_nio.save(update_fields=["punto_nombre_snapshot", "actualizado_en"])
        parada_payan = ParadaRuta.objects.create(
            ruta=ruta,
            punto=self.punto,
            orden=2,
            punto_nombre_snapshot="Sucursal Payan",
        )
        cedis = PuntoLogistico.objects.create(
            nombre="CEDIS corte de tramo",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.567916",
            longitud="-108.459969",
            radio_geocerca_metros=120,
        )
        ParadaRuta.objects.create(ruta=ruta, punto=cedis, orden=3, punto_nombre_snapshot="CEDIS")
        parada_glorias = ParadaRuta.objects.create(
            ruta=ruta,
            punto=self.punto,
            orden=4,
            punto_nombre_snapshot="Sucursal Las Glorias",
        )
        checklist = RutaCargaChecklist.objects.create(ruta=ruta, estatus=RutaCargaChecklist.ESTATUS_EN_REVISION)
        lineas = []
        for parada, cantidad, source_hash in [
            (parada_nio, "3.000", "vainilla-nio"),
            (parada_payan, "4.000", "vainilla-payan"),
            (parada_glorias, "2.000", "vainilla-glorias"),
        ]:
            lineas.append(RutaCargaChecklistLinea.objects.create(
                checklist=checklist,
                parada=parada,
                source_hash=source_hash,
                item_code="0117",
                item_name="Bollo Vainilla",
                unit="PZA",
                cantidad_solicitada=cantidad,
                cantidad_enviada_esperada=cantidad,
            ))
        lineas[1].item_name = "  bollo   vainilla  "
        lineas[1].unit = " pza "
        lineas[1].save(update_fields=["item_name", "unit", "actualizado_en"])

        self.client.force_login(self.user)
        response = self.client.post(
            f"/api/logistica/rutas/{ruta.id}/carga-checklist/productos/validar/",
            data={
                "item_code": "0117",
                "item_name": "Bollo Vainilla",
                "unit": "PZA",
                "cantidad_cargada": "7.000",
                "client_event_id": "tramo-vainilla-1",
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 200, response.content)
        for linea in lineas[:2]:
            linea.refresh_from_db()
            self.assertEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_CARGADA)
            self.assertEqual(linea.cantidad_cargada, linea.cantidad_enviada_esperada)
        lineas[2].refresh_from_db()
        self.assertEqual(lineas[2].estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
        self.assertIsNone(lineas[2].cantidad_cargada)

    def test_validar_producto_tramo_rechaza_total_distinto_sin_repartirlo(self):
        ruta, parada_nio = self._crear_ruta_planeada_para_carga()
        parada_payan = ParadaRuta.objects.create(ruta=ruta, punto=self.punto, orden=2, punto_nombre_snapshot="Sucursal Payan")
        checklist = RutaCargaChecklist.objects.create(ruta=ruta, estatus=RutaCargaChecklist.ESTATUS_EN_REVISION)
        lineas = [
            RutaCargaChecklistLinea.objects.create(
                checklist=checklist,
                parada=parada,
                source_hash=source_hash,
                item_code="0117",
                item_name="Bollo Vainilla",
                unit="PZA",
                cantidad_solicitada=cantidad,
                cantidad_enviada_esperada=cantidad,
            )
            for parada, cantidad, source_hash in [
                (parada_nio, "3.000", "vainilla-nio-distinta"),
                (parada_payan, "4.000", "vainilla-payan-distinta"),
            ]
        ]

        self.client.force_login(self.user)
        response = self.client.post(
            f"/api/logistica/rutas/{ruta.id}/carga-checklist/productos/validar/",
            data={
                "item_code": "0117",
                "item_name": "Bollo Vainilla",
                "unit": "PZA",
                "cantidad_cargada": "6.000",
                "client_event_id": "tramo-vainilla-diferencia",
            },
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400, response.content)
        self.assertIn("desglose", str(response.json()).lower())
        for linea in lineas:
            linea.refresh_from_db()
            self.assertEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
            self.assertIsNone(linea.cantidad_cargada)

    def test_pwa_captura_sumatoria_y_contrae_desglose_por_sucursal(self):
        from pathlib import Path

        pwa_html = (Path(__file__).resolve().parent / "templates" / "logistica" / "pwa.html").read_text(encoding="utf-8")

        self.assertIn("validarCargaProductoTramo", pwa_html)
        self.assertIn("Ver desglose por sucursal", pwa_html)
        self.assertIn("<details class=\"route-load-breakdown\">", pwa_html)
        self.assertIn("cantidad_total_", pwa_html)
        self.assertIn("carga-checklist/productos/validar/", pwa_html)
        self.assertIn("const total = totalesConCarga.length", pwa_html)
        self.assertIn('${confirmadas} de ${total} producto${total === 1 ? "" : "s"}', pwa_html)
        self.assertIn("resumenCargaRuta(rutaData.checklist_carga, paradas)", pwa_html)
        self.assertIn("route-control-v65-recarga-point", pwa_html)

    def test_checklist_no_entra_en_incidencia_solo_por_linea_superada(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        checklist = RutaCargaChecklist.objects.create(ruta=ruta, estatus=RutaCargaChecklist.ESTATUS_EN_REVISION)
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="incidencia-superada",
            item_code="PZA-SUP",
            item_name="Producto superado",
            unit="PZA",
            cantidad_solicitada="2.000",
            cantidad_enviada_esperada="0.000",
            cantidad_cargada="0.000",
            estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA,
        )
        linea_normal = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="incidencia-normal",
            item_code="PZA-OK",
            item_name="Producto normal",
            unit="PZA",
            cantidad_solicitada="4.000",
            cantidad_enviada_esperada="4.000",
        )

        validar_linea_carga(
            user=self.user,
            ruta=ruta,
            repartidor=self.repartidor,
            linea_id=linea_normal.id,
            cantidad_cargada="4.000",
        )

        checklist.refresh_from_db()
        self.assertEqual(checklist.estatus, RutaCargaChecklist.ESTATUS_CONFIRMADA)

    def test_validar_linea_carga_con_diferencia_notifica_logistica_una_sola_vez(self):
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        supervisor = User.objects.create_user(username="logistica.supervisor.carga", password="pass123")
        UserModuleAccess.objects.create(user=supervisor, module="logistica", access=ACCESS_MANAGE)
        superadmin = User.objects.create_superuser(username="superadmin.carga", password="pass123")
        jefe_user = User.objects.create_user(username="johana.logistica", password="pass123")
        jefe = Empleado.objects.create(nombre="Johana Logistica", usuario_erp=jefe_user, activo=True)
        Empleado.objects.create(nombre="Repartidor con jefe", usuario_erp=self.user, jefe_directo=jefe, activo=True)
        ruta, parada = self._crear_ruta_planeada_para_carga()
        checklist = RutaCargaChecklist.objects.create(ruta=ruta, estatus=RutaCargaChecklist.ESTATUS_EN_REVISION)
        linea = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="linea-diferencia",
            item_code="PZA1",
            item_name="Producto con diferencia",
            unit="PZA",
            cantidad_solicitada="5.000",
            cantidad_enviada_esperada="5.000",
        )

        validar_linea_carga(
            user=self.user,
            ruta=ruta,
            repartidor=self.repartidor,
            linea_id=linea.id,
            cantidad_cargada="3.000",
            motivo_diferencia=RutaCargaChecklistLinea.MOTIVO_STOCK_LIMITADO,
        )

        checklist.refresh_from_db()
        self.assertEqual(checklist.estatus, RutaCargaChecklist.ESTATUS_CON_INCIDENCIA)
        self.assertEqual(checklist_bloquea_salida(ruta), "logística debe autorizar la ruta con la diferencia")
        self.assertEqual(
            Notificacion.objects.filter(usuario=supervisor, titulo=f"Diferencia de carga: {ruta.folio}").count(),
            1,
        )
        self.assertEqual(
            Notificacion.objects.filter(usuario=superadmin, titulo=f"Diferencia de carga: {ruta.folio}").count(),
            1,
        )
        self.assertEqual(
            Notificacion.objects.filter(usuario=jefe_user, titulo=f"Diferencia de carga: {ruta.folio}").count(),
            1,
        )

        validar_linea_carga(
            user=self.user,
            ruta=ruta,
            repartidor=self.repartidor,
            linea_id=linea.id,
            cantidad_cargada="3.000",
            motivo_diferencia=RutaCargaChecklistLinea.MOTIVO_STOCK_LIMITADO,
            notas="Segunda revisión.",
        )
        self.assertEqual(
            Notificacion.objects.filter(usuario=supervisor, titulo=f"Diferencia de carga: {ruta.folio}").count(),
            1,
        )

    def test_ruta_detail_autoriza_diferencia_carga_permite_liberar(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        ruta, parada = self._crear_ruta_planeada_para_carga()
        checklist = RutaCargaChecklist.objects.create(ruta=ruta, estatus=RutaCargaChecklist.ESTATUS_EN_REVISION)
        linea = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="linea-autorizar",
            item_code="PZA1",
            item_name="Producto con diferencia",
            unit="PZA",
            cantidad_solicitada="5.000",
            cantidad_enviada_esperada="5.000",
        )
        validar_linea_carga(
            user=self.user,
            ruta=ruta,
            repartidor=self.repartidor,
            linea_id=linea.id,
            cantidad_cargada="3.000",
            motivo_diferencia=RutaCargaChecklistLinea.MOTIVO_STOCK_LIMITADO,
        )

        bloqueada = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_EN_RUTA},
            follow=True,
        )
        ruta.refresh_from_db()
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertContains(bloqueada, "logística debe autorizar la ruta con la diferencia")

        autorizar = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "autorizar_diferencia_carga", "autorizado": "1", "notas_autorizacion_carga": "Autorizado por jefa de logística."},
            follow=True,
        )
        checklist.refresh_from_db()
        self.assertEqual(autorizar.status_code, 200)
        self.assertIn("Autorizado por jefa de logística.", checklist.motivo_override)

        liberada = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_EN_RUTA},
            follow=True,
        )
        ruta.refresh_from_db()
        self.assertEqual(liberada.status_code, 200)
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)

    def test_ruta_detail_rechaza_diferencia_carga_mantiene_bloqueo(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        ruta, parada = self._crear_ruta_planeada_para_carga()
        checklist = RutaCargaChecklist.objects.create(ruta=ruta, estatus=RutaCargaChecklist.ESTATUS_EN_REVISION)
        linea = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="linea-rechazar",
            item_code="PZA1",
            item_name="Producto con diferencia",
            unit="PZA",
            cantidad_solicitada="5.000",
            cantidad_enviada_esperada="5.000",
        )
        validar_linea_carga(
            user=self.user,
            ruta=ruta,
            repartidor=self.repartidor,
            linea_id=linea.id,
            cantidad_cargada="3.000",
            motivo_diferencia=RutaCargaChecklistLinea.MOTIVO_STOCK_LIMITADO,
        )

        rechazar = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "autorizar_diferencia_carga", "autorizado": "0", "notas_autorizacion_carga": "Falta confirmar con CEDIS."},
            follow=True,
        )
        checklist.refresh_from_db()
        self.assertEqual(rechazar.status_code, 200)
        self.assertFalse(checklist.motivo_override)
        self.assertTrue(
            ruta.eventos.filter(metadata__tipo="autorizacion_diferencia_carga", metadata__autorizado=False).exists()
        )

        bloqueada = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_EN_RUTA},
            follow=True,
        )
        ruta.refresh_from_db()
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)

    def test_ruta_detail_autoriza_salida_parcial_con_recarga_cedis(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        PuntoLogistico.objects.create(
            nombre="CEDIS salida parcial",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.560000",
            longitud="-108.460000",
        )
        ruta, _ = self._crear_ruta_planeada_para_carga()
        self._crear_transferencia_point_abierta()
        checklist = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False).checklist
        linea = checklist.lineas.get()
        linea.cantidad_cargada = Decimal("0")
        linea.estatus = RutaCargaChecklistLinea.ESTATUS_FALTANTE
        linea.motivo_diferencia = RutaCargaChecklistLinea.MOTIVO_PRODUCCION_NO_LISTA
        linea.save(update_fields=["cantidad_cargada", "estatus", "motivo_diferencia", "actualizado_en"])
        checklist.estatus = RutaCargaChecklist.ESTATUS_CON_INCIDENCIA
        checklist.save(update_fields=["estatus", "actualizado_en"])

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "registrar_recarga_cedis", "notas_recarga_cedis": "Regresa a CEDIS por producto pendiente."},
            follow=True,
            secure=True,
        )
        salida = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_EN_RUTA},
            follow=True,
            secure=True,
        )

        ruta.refresh_from_db()
        checklist.refresh_from_db()
        self.assertTrue(
            ruta.eventos.filter(metadata__tipo="recarga_cedis").exists(),
            [str(message) for message in response.context["messages"]],
        )
        evento = ruta.eventos.get(metadata__tipo="recarga_cedis")
        parada_cedis = evento.parada
        self.assertEqual(response.status_code, 200)
        self.assertIn("Regresa a CEDIS", checklist.motivo_override)
        self.assertEqual(evento.metadata["numero"], 1)
        self.assertEqual(evento.metadata["diferencias"], 1)
        self.assertEqual(parada_cedis.punto.tipo, PuntoLogistico.TIPO_CEDIS)
        self.assertEqual(parada_cedis.estado, ParadaRuta.ESTADO_VISITADA)
        self.assertEqual(salida.status_code, 200)
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_registrar_recarga_cedis_en_ruta_no_cierra_ni_duplica_ruta(self, sync_point):
        ruta, _ = self._crear_ruta_planeada_para_carga()
        cedis_punto = PuntoLogistico.objects.create(
            nombre="CEDIS Recarga En Ruta",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.571000",
            longitud="-108.471000",
            radio_geocerca_metros=120,
        )
        self._crear_transferencia_point_abierta()
        checklist = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False).checklist
        checklist.lineas.update(
            cantidad_cargada=Decimal("5.000"),
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
            validado_por=self.user,
            validado_en=timezone.now(),
        )
        checklist.estatus = RutaCargaChecklist.ESTATUS_CONFIRMADA
        checklist.save(update_fields=["estatus", "actualizado_en"])
        ruta.estatus = RutaEntrega.ESTATUS_EN_RUTA
        ruta.save(update_fields=["estatus", "updated_at"])
        parada_cedis_esperada = ParadaRuta.objects.create(
            ruta=ruta,
            punto=cedis_punto,
            orden=(ruta.paradas.order_by("-orden").values_list("orden", flat=True).first() or 0) + 1,
        )

        evento = registrar_recarga_cedis(
            ruta=ruta,
            user=self.user,
            parada=parada_cedis_esperada,
            notas="Segunda carga en CEDIS.",
        )

        ruta.refresh_from_db()
        parada_cedis = evento.parada
        self.assertEqual(ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)
        self.assertEqual(RutaEntrega.objects.filter(pk=ruta.pk).count(), 1)
        self.assertEqual(evento.metadata["tipo"], "recarga_cedis")
        self.assertEqual(evento.metadata["numero"], 1)
        self.assertEqual(parada_cedis.punto.tipo, PuntoLogistico.TIPO_CEDIS)
        self.assertEqual(parada_cedis.estado, ParadaRuta.ESTADO_VISITADA)

    def test_tramo_carga_avanza_con_recarga_cedis(self):
        ruta, primera = self._crear_ruta_planeada_para_carga()
        ruta.estatus = RutaEntrega.ESTATUS_EN_RUTA
        ruta.save(update_fields=["estatus", "updated_at"])
        cedis_punto = PuntoLogistico.objects.create(
            nombre="CEDIS Test",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.571000",
            longitud="-108.471000",
            radio_geocerca_metros=120,
        )
        siguiente_sucursal = Sucursal.objects.create(codigo="CTRL-LOG-2", nombre="Control Logística 2", activa=True)
        siguiente_punto = PuntoLogistico.objects.create(
            sucursal=siguiente_sucursal,
            nombre="Sucursal Control 2",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.572000",
            longitud="-108.472000",
            radio_geocerca_metros=120,
        )
        cedis = ParadaRuta.objects.create(ruta=ruta, punto=cedis_punto, orden=2)
        segunda = ParadaRuta.objects.create(ruta=ruta, punto=siguiente_punto, orden=3)
        primera.estado = ParadaRuta.ESTADO_VISITADA
        primera.entrega_estado = ParadaRuta.ENTREGA_ENTREGADA
        primera.save(update_fields=["estado", "entrega_estado", "actualizado_en"])
        EventoRuta.objects.create(
            ruta=ruta,
            parada=cedis,
            tipo=EventoRuta.TIPO_RECARGA_CEDIS,
            severidad=EventoRuta.SEVERIDAD_OK,
            descripcion="Recarga CEDIS reconciliada.",
            creado_por=self.user,
        )
        checklist = RutaCargaChecklist.objects.create(ruta=ruta)
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=primera,
            source_hash="tramo-anterior",
            item_code="A",
            item_name="Anterior",
            cantidad_solicitada=Decimal("1.000"),
            cantidad_enviada_esperada=Decimal("1.000"),
        )
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=segunda,
            source_hash="tramo-siguiente",
            item_code="B",
            item_name="Siguiente",
            cantidad_solicitada=Decimal("2.000"),
            cantidad_enviada_esperada=Decimal("2.000"),
        )

        detallado = obtener_checklist_carga_detallado(ruta, solo_tramo_actual=True)

        self.assertQuerySetEqual(detallado.lineas.all(), ["Siguiente"], transform=lambda linea: linea.item_name)

    def test_sync_carga_limpia_pendientes_antes_del_tramo_actual(self):
        ruta, primera = self._crear_ruta_planeada_para_carga()
        ruta.estatus = RutaEntrega.ESTATUS_EN_RUTA
        ruta.save(update_fields=["estatus", "updated_at"])
        cedis_punto = PuntoLogistico.objects.create(
            nombre="CEDIS Test",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.571000",
            longitud="-108.471000",
            radio_geocerca_metros=120,
        )
        siguiente_sucursal = Sucursal.objects.create(codigo="CTRL-LOG-3", nombre="Control Logística 3", activa=True)
        siguiente_punto = PuntoLogistico.objects.create(
            sucursal=siguiente_sucursal,
            nombre="Sucursal Control 3",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.573000",
            longitud="-108.473000",
            radio_geocerca_metros=120,
        )
        cedis = ParadaRuta.objects.create(ruta=ruta, punto=cedis_punto, orden=2)
        segunda = ParadaRuta.objects.create(ruta=ruta, punto=siguiente_punto, orden=3)
        primera.estado = ParadaRuta.ESTADO_VISITADA
        primera.entrega_estado = ParadaRuta.ENTREGA_ENTREGADA
        primera.save(update_fields=["estado", "entrega_estado", "actualizado_en"])
        EventoRuta.objects.create(
            ruta=ruta,
            parada=cedis,
            tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            severidad=EventoRuta.SEVERIDAD_OK,
            descripcion="Llegada detectada en CEDIS.",
            creado_por=self.user,
        )
        checklist = RutaCargaChecklist.objects.create(ruta=ruta)
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=primera,
            source_hash="pendiente-viejo",
            item_code="A",
            item_name="Anterior",
            cantidad_solicitada=Decimal("1.000"),
            cantidad_enviada_esperada=Decimal("1.000"),
        )
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=segunda,
            source_hash="pendiente-actual",
            item_code="B",
            item_name="Siguiente",
            cantidad_solicitada=Decimal("2.000"),
            cantidad_enviada_esperada=Decimal("2.000"),
        )

        sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        self.assertTrue(RutaCargaChecklistLinea.objects.filter(source_hash="pendiente-viejo").exists())
        self.assertTrue(RutaCargaChecklistLinea.objects.filter(source_hash="pendiente-actual").exists())

        EventoRuta.objects.create(
            ruta=ruta,
            parada=cedis,
            tipo=EventoRuta.TIPO_RECARGA_CEDIS,
            severidad=EventoRuta.SEVERIDAD_INFO,
            descripcion="Recarga CEDIS reconciliada.",
            creado_por=self.user,
        )
        sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False)

        self.assertFalse(RutaCargaChecklistLinea.objects.filter(source_hash="pendiente-viejo").exists())
        self.assertTrue(RutaCargaChecklistLinea.objects.filter(source_hash="pendiente-actual").exists())

    def test_api_checklist_repartidor_usa_tramo_actual(self):
        self.client.force_login(self.user)
        ruta, primera = self._crear_ruta_planeada_para_carga()
        ruta.estatus = RutaEntrega.ESTATUS_EN_RUTA
        ruta.save(update_fields=["estatus", "updated_at"])
        cedis_punto = PuntoLogistico.objects.create(
            nombre="CEDIS Test",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.571000",
            longitud="-108.471000",
            radio_geocerca_metros=120,
        )
        siguiente_sucursal = Sucursal.objects.create(codigo="CTRL-LOG-4", nombre="Control Logística 4", activa=True)
        siguiente_punto = PuntoLogistico.objects.create(
            sucursal=siguiente_sucursal,
            nombre="Sucursal Control 4",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.574000",
            longitud="-108.474000",
            radio_geocerca_metros=120,
        )
        cedis = ParadaRuta.objects.create(ruta=ruta, punto=cedis_punto, orden=2)
        segunda = ParadaRuta.objects.create(ruta=ruta, punto=siguiente_punto, orden=3)
        primera.estado = ParadaRuta.ESTADO_VISITADA
        primera.entrega_estado = ParadaRuta.ENTREGA_ENTREGADA
        primera.save(update_fields=["estado", "entrega_estado", "actualizado_en"])
        EventoRuta.objects.create(
            ruta=ruta,
            parada=cedis,
            tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            severidad=EventoRuta.SEVERIDAD_OK,
            descripcion="Llegada detectada en CEDIS.",
            creado_por=self.user,
        )
        checklist = RutaCargaChecklist.objects.create(ruta=ruta)
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=primera,
            source_hash="api-tramo-anterior",
            item_code="A",
            item_name="Anterior",
            cantidad_solicitada=Decimal("1.000"),
            cantidad_enviada_esperada=Decimal("1.000"),
        )
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=segunda,
            source_hash="api-tramo-siguiente",
            item_code="B",
            item_name="Siguiente",
            cantidad_solicitada=Decimal("2.000"),
            cantidad_enviada_esperada=Decimal("2.000"),
        )

        response = self.client.get(reverse("api_logistica_ruta_carga_checklist", kwargs={"ruta_id": ruta.id}))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["total_lineas"], 1)
        self.assertEqual(response.json()["lineas"][0]["item_name"], "Anterior")

        EventoRuta.objects.create(
            ruta=ruta,
            parada=cedis,
            tipo=EventoRuta.TIPO_RECARGA_CEDIS,
            severidad=EventoRuta.SEVERIDAD_INFO,
            descripcion="Recarga CEDIS reconciliada.",
            creado_por=self.user,
        )
        response = self.client.get(reverse("api_logistica_ruta_carga_checklist", kwargs={"ruta_id": ruta.id}))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["total_lineas"], 1)
        self.assertEqual(response.json()["lineas"][0]["item_name"], "Siguiente")

    def test_api_checklist_carga_formatea_cantidades_maximo_dos_decimales(self):
        ruta, parada = self._crear_ruta_planeada_para_carga()
        checklist = RutaCargaChecklist.objects.create(ruta=ruta)
        RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=parada,
            source_hash="api-decimales",
            item_code="DEC",
            item_name="Decimal",
            cantidad_solicitada=Decimal("2.555"),
            cantidad_enviada_esperada=Decimal("4.000"),
            cantidad_cargada=Decimal("0.600"),
        )

        data = RutaCargaChecklistSerializer(checklist).data["lineas"][0]

        self.assertEqual(data["cantidad_solicitada"], "2.56")
        self.assertEqual(data["cantidad_enviada_esperada"], "4")
        self.assertEqual(data["cantidad_cargada"], "0.6")

    def test_api_repartidor_valida_linea_carga_e_idempotencia(self):
        self.client.force_login(self.user)
        ruta, _ = self._crear_ruta_planeada_para_carga()
        self._crear_transferencia_point_abierta()
        checklist = sincronizar_checklist_carga_desde_point(ruta=ruta, user=self.user, ejecutar_sync=False).checklist
        linea = checklist.lineas.get()

        url = reverse("api_logistica_ruta_carga_linea_validar", kwargs={"ruta_id": ruta.id, "linea_id": linea.id})
        payload = {
            "cantidad_cargada": "5.000",
            "client_event_id": "evt-carga-1",
        }
        response = self.client.post(url, json.dumps(payload), content_type="application/json")
        second = self.client.post(url, json.dumps(payload), content_type="application/json")

        linea.refresh_from_db()
        checklist.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_CARGADA)
        self.assertEqual(linea.client_event_id, "evt-carga-1")
        self.assertEqual(checklist.estatus, RutaCargaChecklist.ESTATUS_CONFIRMADA)

    def test_api_no_crea_ruta_sin_paradas(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("api_logistica_rutas"),
            json.dumps({
                "nombre": "Ruta API Vacía",
                "fecha_ruta": timezone.localdate().isoformat(),
                "repartidor": self.repartidor.id,
                "acompanante": self.acompanante.id,
                "acompanante_manual": "Auxiliar externo",
                "unidad_operativa": self.unidad.id,
                "paradas": [],
            }),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertFalse(RutaEntrega.objects.filter(nombre="Ruta API Vacía").exists())

    def test_api_crea_ruta_con_paradas_ordenadas_y_deduplicadas(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.ruta.estatus = RutaEntrega.ESTATUS_COMPLETADA
        self.ruta.save(update_fields=["estatus", "updated_at"])
        punto_sur = PuntoLogistico.objects.create(
            nombre="Sucursal API Sur",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.560000",
            longitud="-108.460000",
            radio_geocerca_metros=120,
        )

        response = self.client.post(
            reverse("api_logistica_rutas"),
            json.dumps({
                "nombre": "Ruta API Ordenada",
                "fecha_ruta": timezone.localdate().isoformat(),
                "repartidor": self.repartidor.id,
                "acompanante": self.acompanante.id,
                "acompanante_manual": "Auxiliar externo",
                "unidad_operativa": self.unidad.id,
                "paradas": [
                    {"punto_id": punto_sur.id, "orden": 2},
                    {"punto_id": self.punto.id, "orden": 1},
                    {"punto_id": punto_sur.id, "orden": 3},
                ],
            }),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 201)
        data = response.json()
        self.assertEqual(data["ruta_programada_fuente"], "FALLBACK")
        self.assertGreater(data["ruta_programada_distancia_metros"], 0)
        self.assertGreater(data["ruta_programada_duracion_segundos"], 0)
        ruta = RutaEntrega.objects.get(nombre="Ruta API Ordenada")
        self.assertEqual(ruta.acompanante, self.acompanante)
        self.assertEqual(ruta.acompanante_manual, "Auxiliar externo")
        paradas = list(ruta.paradas.order_by("orden").values_list("punto_id", "orden"))
        self.assertEqual(paradas, [(self.punto.id, 1), (punto_sur.id, 2)])
        self.assertEqual(ruta.ruta_programada_fuente, "FALLBACK")
        self.assertGreater(ruta.ruta_programada_distancia_metros, 0)

    def test_api_no_crea_ruta_directamente_cerrada(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("api_logistica_rutas"),
            json.dumps(
                {
                    "nombre": "Ruta API Cerrada",
                    "fecha_ruta": timezone.localdate().isoformat(),
                    "estatus": RutaEntrega.ESTATUS_COMPLETADA,
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertFalse(RutaEntrega.objects.filter(nombre="Ruta API Cerrada").exists())

    def test_api_no_agrega_entrega_a_ruta_en_ruta(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)

        response = self.client.post(
            reverse("api_logistica_ruta_entregas", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"secuencia": 1, "cliente_nombre": "Cliente", "direccion": "Sucursal"}),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 400)

    def test_ruta_detail_reordena_paradas_sin_integrity_error(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        self.ruta.estatus = RutaEntrega.ESTATUS_PLANEADA
        self.ruta.save(update_fields=["estatus", "updated_at"])
        punto_2 = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Sucursal Segunda",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.572000",
            longitud="-108.472000",
            radio_geocerca_metros=100,
        )
        parada_2 = ParadaRuta.objects.create(ruta=self.ruta, punto=punto_2, orden=2)

        response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "move_parada", "parada_id": parada_2.id, "direction": "up"},
            follow=True,
        )

        self.parada.refresh_from_db()
        parada_2.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(parada_2.orden, 1)
        self.assertEqual(self.parada.orden, 2)

    def test_punto_logistico_toggle_no_activa_duplicado_cercano(self):
        self.client.force_login(self.user)
        UserModuleAccess.objects.create(user=self.user, module="logistica", access=ACCESS_MANAGE)
        punto_inactivo = PuntoLogistico.objects.create(
            nombre="Punto Inactivo Cercano",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.570010",
            longitud="-108.470010",
            radio_geocerca_metros=80,
            activo=False,
        )

        response = self.client.post(reverse("logistica:punto_logistico_toggle", kwargs={"pk": punto_inactivo.id}), follow=True)

        punto_inactivo.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertFalse(punto_inactivo.activo)
        self.assertContains(response, "No se puede activar")
