from datetime import datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
import re
import subprocess
from threading import Barrier, BrokenBarrierError, Event, Thread
from types import SimpleNamespace
from unittest.mock import patch
from uuid import UUID

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.exceptions import ValidationError
from django.db import DatabaseError, IntegrityError, close_old_connections, connection, transaction
from django.test import TestCase, TransactionTestCase
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from django.utils import timezone

from core.access import ACCESS_MANAGE
from core.models import Notificacion, Sucursal, UserModuleAccess
from pos_bridge.models import PointBranch, PointSyncJob, PointTransferLine
from pos_bridge.services.open_transfer_sync_service import (
    OpenTransferSyncService,
    resolve_requesting_erp_branch,
)
from pos_bridge.services.movement_sync_service import PointMovementSyncService
from recetas.models import Receta, SolicitudReabastoCedis, SolicitudReabastoCedisLinea
from recetas.services.consolidado_service import ConsolidadoNocturnoCedisService

from .domain_ruta import parada_resuelta_operativamente, point_transfer_enviada
from .models import (
    EventoRuta,
    BitacoraSalidaLlegada,
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
from .services_carga_ruta import (
    PointSyncUnavailableError,
    RecargaCedisPendienteEnviado,
    RecargaCedisPointError,
    RecargaCedisSnapshotObsoleto,
    RecargaCedisSinLineasPoint,
    _ordenes_tramo_carga_actual,
    _point_recibidas_por_ruta,
    _registrar_alerta_recarga_sync,
    _sincronizar_lineas_point_para_ruta,
    checklist_bloquea_salida,
    cerrar_ruta_con_diferencia_autorizada,
    lineas_tramo_operativo_actual,
    registrar_recarga_cedis,
    ruta_tiene_paradas_entregables_pendientes,
    sincronizar_checklist_carga_desde_point,
    ultima_alerta_recarga_cedis_revisable,
)
from .services_entregas import confirmar_entrega_parada, revisar_entrega_excepcional
from .services_rutas_control import (
    LiberacionRutaConflicto,
    LiberacionRutaError,
    _marcar_visitada_por_permanencia,
    liberar_ruta_con_turno,
    registrar_ubicacion_ruta,
)
from api.logistica_views import _liberar_ruta_desde_bitacora_salida
from api.logistica_serializers import ParadaRutaSerializer, RutaCargaChecklistLineaSerializer
from . import services_carga_ruta


User = get_user_model()


class RecargaCedisAlertConcurrencyTests(TransactionTestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(
            codigo="INV-CONC",
            nombre="Sucursal concurrencia recarga",
            activa=True,
        )
        self.actor = User.objects.create_user(username="actor.recarga.concurrente")
        self.manager = User.objects.create_superuser(
            username="jefe.recarga.concurrente",
            email="jefe-concurrente@example.com",
            password="test",
        )
        unidad = Unidad.objects.create(
            codigo="INV-CONC",
            descripcion="Unidad concurrencia recarga",
            sucursal=self.sucursal,
        )
        repartidor = Repartidor.objects.create(
            user=self.actor,
            sucursal=self.sucursal,
            unidad_asignada=unidad,
        )
        self.ruta = RutaEntrega.objects.create(
            nombre="Ruta concurrencia recarga",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
            repartidor=repartidor,
            unidad_operativa=unidad,
        )
        cedis = PuntoLogistico.objects.create(
            nombre="CEDIS concurrencia recarga",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.560000",
            longitud="-108.460000",
        )
        self.parada = ParadaRuta.objects.create(ruta=self.ruta, punto=cedis, orden=1)
        self.snapshot = {
            "snapshot_hash": "snapshot-concurrente",
            "capturado_en": timezone.now().isoformat(),
            "sucursales": [],
        }

    def test_reintentos_concurrentes_serializan_reparacion_de_notificacion(self):
        if connection.vendor != "postgresql":
            self.skipTest("La prueba de bloqueo requiere PostgreSQL real.")

        alerta = _registrar_alerta_recarga_sync(
            ruta=self.ruta,
            parada=self.parada,
            estado_sync="ERROR_POINT",
            snapshot=self.snapshot,
            detalle="Point no respondió",
            actor=self.actor,
        )
        creado_en_original = alerta.creado_en
        observacion_original = alerta.metadata["ultima_observacion_en"]
        Notificacion.objects.filter(
            usuario=self.manager,
            objeto_tipo="logistica.EventoRuta",
            objeto_id=str(alerta.id),
        ).delete()

        inicio = Barrier(2)
        intercalado_notificacion = Barrier(2)
        errores = []

        def get_or_create_intercalado(*, defaults=None, **lookup):
            existente = Notificacion.objects.filter(**lookup).first()
            if existente is not None:
                return existente, False
            try:
                intercalado_notificacion.wait(timeout=1)
            except BrokenBarrierError:
                pass
            return Notificacion.objects.create(**lookup, **(defaults or {})), True

        def reparar():
            close_old_connections()
            try:
                ruta = RutaEntrega.objects.get(pk=self.ruta.pk)
                parada = ParadaRuta.objects.get(pk=self.parada.pk)
                actor = User.objects.get(pk=self.actor.pk)
                inicio.wait(timeout=3)
                _registrar_alerta_recarga_sync(
                    ruta=ruta,
                    parada=parada,
                    estado_sync="ERROR_POINT",
                    snapshot=self.snapshot,
                    detalle="Point no respondió",
                    actor=actor,
                )
            except Exception as exc:
                errores.append(exc)
            finally:
                close_old_connections()

        with patch.object(
            Notificacion.objects,
            "get_or_create",
            side_effect=get_or_create_intercalado,
        ):
            hilos = [Thread(target=reparar), Thread(target=reparar)]
            for hilo in hilos:
                hilo.start()
            for hilo in hilos:
                hilo.join(timeout=8)

        self.assertFalse(any(hilo.is_alive() for hilo in hilos), "Los reintentos quedaron bloqueados.")
        self.assertEqual(errores, [])
        self.assertEqual(
            EventoRuta.objects.filter(clave_auditoria=alerta.clave_auditoria).count(),
            1,
        )
        alerta.refresh_from_db()
        self.assertEqual(alerta.creado_en, creado_en_original)
        self.assertGreater(
            datetime.fromisoformat(alerta.metadata["ultima_observacion_en"]),
            datetime.fromisoformat(observacion_original),
        )
        self.assertEqual(
            Notificacion.objects.filter(
                usuario=self.manager,
                objeto_tipo="logistica.EventoRuta",
                objeto_id=str(alerta.id),
            ).count(),
            1,
        )


class RecargaCedisAutomationConcurrencyTests(TransactionTestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(
            codigo="INV-AUTO-CONC",
            nombre="Sucursal automatización concurrente",
            activa=True,
        )
        self.user = User.objects.create_user(username="actor.auto.recarga.concurrente")
        self.unidad = Unidad.objects.create(
            codigo="INV-AUTO-CONC",
            descripcion="Unidad automatización concurrente",
            sucursal=self.sucursal,
        )
        self.repartidor = Repartidor.objects.create(
            user=self.user,
            sucursal=self.sucursal,
            unidad_asignada=self.unidad,
        )
        self.ruta = RutaEntrega.objects.create(
            nombre="Ruta automatización concurrente",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )
        punto_previo = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Sucursal previa automatización",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.550000",
            longitud="-108.450000",
        )
        self.previa = ParadaRuta.objects.create(
            ruta=self.ruta,
            punto=punto_previo,
            orden=1,
            estado=ParadaRuta.ESTADO_VISITADA,
            entrega_estado=ParadaRuta.ENTREGA_ENTREGADA,
        )
        cedis = PuntoLogistico.objects.create(
            nombre="CEDIS automatización concurrente",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.560000",
            longitud="-108.460000",
            radio_geocerca_metros=120,
        )
        self.cedis = ParadaRuta.objects.create(
            ruta=self.ruta,
            punto=cedis,
            orden=2,
            estado=ParadaRuta.ESTADO_VISITADA,
        )
        self.sucursal_siguiente = Sucursal.objects.create(
            codigo="INV-AUTO-SIG",
            nombre="Sucursal siguiente automatización",
            activa=True,
        )
        destino = PuntoLogistico.objects.create(
            sucursal=self.sucursal_siguiente,
            nombre=self.sucursal_siguiente.nombre,
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.570000",
            longitud="-108.470000",
        )
        self.siguiente = ParadaRuta.objects.create(
            ruta=self.ruta,
            punto=destino,
            orden=3,
        )

    def _preparar_checklist_enviado(self):
        origen = PointBranch.objects.create(external_id="INV-AUTO-CEDIS", name="CEDIS")
        destino = PointBranch.objects.create(
            external_id="INV-AUTO-SUC",
            name=self.sucursal_siguiente.nombre,
            erp_branch=self.sucursal_siguiente,
        )
        PointTransferLine.objects.create(
            origin_branch=origen,
            destination_branch=destino,
            erp_destination_branch=self.sucursal_siguiente,
            transfer_external_id="INV-AUTO-T",
            detail_external_id="INV-AUTO-D",
            source_hash="inv-auto-concurrente",
            registered_at=timezone.now(),
            sent_at=timezone.now(),
            item_name="Pastel automatización concurrente",
            item_code="INV-AUTO-PASTEL",
            unit="pz",
            requested_quantity="2",
            sent_quantity="2",
            received_quantity="0",
            is_open=True,
            raw_payload={"transfer": {"isEnviado": True}},
        )
        return sincronizar_checklist_carga_desde_point(
            ruta=self.ruta,
            user=self.user,
            ejecutar_sync=False,
        )

    def test_transicion_visitada_concurrente_tiene_un_solo_ganador(self):
        if connection.vendor != "postgresql":
            self.skipTest("La prueba de transición condicional requiere PostgreSQL real.")

        self.cedis.estado = ParadaRuta.ESTADO_PENDIENTE
        self.cedis.save(update_fields=["estado", "actualizado_en"])
        llegada = UbicacionRuta.objects.create(
            ruta=self.ruta,
            repartidor=self.repartidor,
            unidad=self.unidad,
            latitud=self.cedis.latitud_geocerca,
            longitud=self.cedis.longitud_geocerca,
        )
        llegada_evento = EventoRuta.objects.create(
            ruta=self.ruta,
            parada=self.cedis,
            ubicacion=llegada,
            tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            latitud=llegada.latitud,
            longitud=llegada.longitud,
            metadata={
                "origen_servicio": "registrar_ubicacion_ruta",
                "ubicacion_confiable": True,
                "ruta_id": self.ruta.id,
                "repartidor_id": self.repartidor.id,
                "unidad_id": self.unidad.id,
            },
        )
        EventoRuta.objects.filter(pk=llegada_evento.pk).update(
            creado_en=timezone.now() - timedelta(minutes=6)
        )
        ubicaciones = [
            UbicacionRuta.objects.create(
                ruta=self.ruta,
                repartidor=self.repartidor,
                unidad=self.unidad,
                latitud=self.cedis.latitud_geocerca,
                longitud=self.cedis.longitud_geocerca,
            )
            for _ in range(2)
        ]
        inicio = Barrier(2)
        resultados = []
        errores = []

        def marcar(ubicacion_id):
            close_old_connections()
            try:
                ruta = RutaEntrega.objects.get(pk=self.ruta.id)
                parada = ParadaRuta.objects.get(pk=self.cedis.id)
                ubicacion = UbicacionRuta.objects.get(pk=ubicacion_id)
                inicio.wait(timeout=5)
                resultados.append(
                    _marcar_visitada_por_permanencia(
                        ruta=ruta,
                        parada=parada,
                        ubicacion_actual=ubicacion,
                        distancia_metros_value=0,
                    )
                )
            except Exception as exc:
                errores.append(exc)
            finally:
                close_old_connections()

        hilos = [Thread(target=marcar, args=(ubicacion.id,)) for ubicacion in ubicaciones]
        for hilo in hilos:
            hilo.start()
        for hilo in hilos:
            hilo.join(timeout=10)

        self.assertFalse(any(hilo.is_alive() for hilo in hilos))
        self.assertEqual(errores, [])
        self.assertCountEqual(resultados, [True, False])
        self.cedis.refresh_from_db()
        self.assertEqual(self.cedis.estado, ParadaRuta.ESTADO_VISITADA)

    def test_tareas_concurrentes_hacen_una_sola_sincronizacion_completa(self):
        if connection.vendor != "postgresql":
            self.skipTest("La prueba de bloqueo de tarea requiere PostgreSQL real.")

        from .services_rutas_control import _reclamar_lease_recarga_para_encolar

        resumen = self._preparar_checklist_enviado()
        lease_token = _reclamar_lease_recarga_para_encolar(
            ruta=self.ruta,
            parada=self.cedis,
            user=self.user,
        )
        self.assertTrue(lease_token)
        sync_iniciado = Event()
        liberar_sync = Event()
        errores = []
        resultados = []
        errores_lock = []

        def sync_bloqueado(**kwargs):
            sync_iniciado.set()
            if not liberar_sync.wait(timeout=5):
                raise TimeoutError("La primera sincronización no fue liberada.")
            return resumen

        def procesar():
            close_old_connections()
            try:
                from .tasks import procesar_recarga_cedis_automatica

                resultados.append(
                    procesar_recarga_cedis_automatica(
                        ruta_id=self.ruta.id,
                        parada_id=self.cedis.id,
                        user_id=self.user.id,
                        lease_token=lease_token,
                    )
                )
            except Exception as exc:
                errores.append(exc)
            finally:
                close_old_connections()

        with patch(
            "logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point",
            side_effect=sync_bloqueado,
        ) as sync:
            primero = Thread(target=procesar)
            segundo = Thread(target=procesar)
            primero.start()
            self.assertTrue(sync_iniciado.wait(timeout=5))
            segundo.start()
            try:
                with transaction.atomic():
                    RutaEntrega.objects.select_for_update(nowait=True).get(pk=self.ruta.id)
                    ParadaRuta.objects.select_for_update(nowait=True).get(pk=self.cedis.id)
            except DatabaseError as exc:
                errores_lock.append(exc)
            finally:
                liberar_sync.set()
            primero.join(timeout=10)
            segundo.join(timeout=10)

        self.assertFalse(primero.is_alive() or segundo.is_alive())
        self.assertEqual(errores, [])
        self.assertEqual(errores_lock, [], "Point se ejecutó mientras Ruta/Parada seguían bloqueadas.")
        self.assertEqual(
            sync.call_count,
            1,
            {
                "resultados": resultados,
                "eventos": list(
                    EventoRuta.objects.filter(ruta=self.ruta).values(
                        "id", "parada_id", "tipo", "metadata"
                    )
                ),
            },
        )
        self.assertEqual(len(resultados), 2)
        self.assertEqual(
            sorted(resultado["estado_sync"] for resultado in resultados),
            ["ACTUALIZADO", "EN_PROCESO"],
        )
        self.assertEqual(
            EventoRuta.objects.filter(
                ruta=self.ruta,
                parada=self.cedis,
                tipo=EventoRuta.TIPO_RECARGA_CEDIS,
            ).count(),
            1,
        )

    def test_worker_obsoleto_no_pisa_token_nuevo_mientras_lock_sigue_activo(self):
        if connection.vendor != "postgresql":
            self.skipTest("El fencing con advisory lock requiere PostgreSQL real.")

        from .services_rutas_control import _reclamar_lease_recarga_para_encolar
        from .tasks import procesar_recarga_cedis_automatica

        resumen = self._preparar_checklist_enviado()
        token_viejo = _reclamar_lease_recarga_para_encolar(
            ruta=self.ruta,
            parada=self.cedis,
            user=self.user,
        )
        sync_iniciado = Event()
        liberar_sync = Event()
        resultados_viejo = []
        errores = []

        def sync_lento(**kwargs):
            sync_iniciado.set()
            if not liberar_sync.wait(timeout=5):
                raise TimeoutError("No se liberó el worker viejo.")
            return resumen

        def worker_viejo():
            close_old_connections()
            try:
                resultados_viejo.append(
                    procesar_recarga_cedis_automatica(
                        ruta_id=self.ruta.id,
                        parada_id=self.cedis.id,
                        user_id=self.user.id,
                        lease_token=token_viejo,
                    )
                )
            except Exception as exc:
                errores.append(exc)
            finally:
                close_old_connections()

        with patch(
            "logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point",
            side_effect=sync_lento,
        ) as sync:
            hilo_viejo = Thread(target=worker_viejo)
            hilo_viejo.start()
            self.assertTrue(sync_iniciado.wait(timeout=5))

            lease = EventoRuta.objects.get(
                clave_auditoria=f"recarga-auto-lease:{self.ruta.id}:{self.cedis.id}"
            )
            metadata = dict(lease.metadata)
            metadata["lease_hasta"] = (timezone.now() - timedelta(seconds=1)).isoformat()
            lease.metadata = metadata
            lease.save(update_fields=["metadata"])
            token_nuevo = _reclamar_lease_recarga_para_encolar(
                ruta=self.ruta,
                parada=self.cedis,
                user=self.user,
            )
            self.assertNotEqual(token_viejo, token_nuevo)

            segundo = procesar_recarga_cedis_automatica(
                ruta_id=self.ruta.id,
                parada_id=self.cedis.id,
                user_id=self.user.id,
                lease_token=token_nuevo,
            )
            self.assertEqual(segundo["estado_sync"], "EN_PROCESO")
            liberar_sync.set()
            hilo_viejo.join(timeout=10)

        self.assertFalse(hilo_viejo.is_alive())
        self.assertEqual(errores, [])
        self.assertEqual(sync.call_count, 1)
        self.assertEqual(resultados_viejo[0]["estado_sync"], "ACTUALIZADO")
        lease.refresh_from_db()
        self.assertEqual(lease.metadata["lease_token"], token_nuevo)
        self.assertEqual(lease.metadata["estado"], "ENCOLADA")


class LiberacionRutaConcurrencyTests(TransactionTestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(
            codigo="INV-LIB-CONC",
            nombre="Sucursal concurrencia liberación",
            activa=True,
        )
        self.actor = User.objects.create_user(username="actor.liberacion.concurrente")
        self.unidad = Unidad.objects.create(
            codigo="INV-LIB-CONC",
            descripcion="Unidad concurrencia liberación",
            sucursal=self.sucursal,
        )
        self.repartidor = Repartidor.objects.create(
            user=self.actor,
            sucursal=self.sucursal,
            unidad_asignada=self.unidad,
        )
        self.turno = BitacoraSalidaLlegada.objects.create(
            repartidor=self.repartidor,
            unidad=self.unidad,
            km_salida=1000,
            nivel_gas_salida="lleno",
            foto_tablero_salida=SimpleUploadedFile(
                "tablero.gif",
                b"gif",
                content_type="image/gif",
            ),
        )
        punto = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre="Sucursal concurrencia liberación",
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.570000",
            longitud="-108.470000",
        )
        self.rutas = []
        for indice in range(2):
            ruta = RutaEntrega.objects.create(
                nombre=f"Ruta concurrente {indice}",
                fecha_ruta=timezone.localdate(),
                repartidor=self.repartidor,
                unidad_operativa=self.unidad,
            )
            ParadaRuta.objects.create(ruta=ruta, punto=punto, orden=1)
            self.rutas.append(ruta)

    def test_dos_liberaciones_concurrentes_dejan_una_ruta_y_un_evento(self):
        if connection.vendor != "postgresql":
            self.skipTest("La prueba de unicidad concurrente requiere PostgreSQL real.")

        inicio = Barrier(2)
        resultados = []
        errores = []

        def liberar(ruta_id):
            close_old_connections()
            try:
                ruta = RutaEntrega.objects.get(pk=ruta_id)
                actor = User.objects.get(pk=self.actor.pk)
                turno = BitacoraSalidaLlegada.objects.get(pk=self.turno.pk)
                inicio.wait(timeout=5)
                liberar_ruta_con_turno(ruta=ruta, actor=actor, bitacora=turno)
                resultados.append("liberada")
            except LiberacionRutaConflicto:
                resultados.append("conflicto")
            except Exception as exc:
                errores.append(exc)
            finally:
                close_old_connections()

        hilos = [Thread(target=liberar, args=(ruta.id,)) for ruta in self.rutas]
        for hilo in hilos:
            hilo.start()
        for hilo in hilos:
            hilo.join(timeout=10)

        self.assertFalse(any(hilo.is_alive() for hilo in hilos))
        self.assertEqual(errores, [])
        self.assertCountEqual(resultados, ["liberada", "conflicto"])
        self.assertEqual(
            RutaEntrega.objects.filter(estatus=RutaEntrega.ESTATUS_EN_RUTA).count(),
            1,
        )
        self.assertEqual(EventoRuta.objects.filter(tipo=EventoRuta.TIPO_SALIDA).count(), 1)


class LogisticaInvariantFixtures(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(
            codigo="INV-RUTA",
            nombre="Sucursal Invariantes Ruta",
            activa=True,
        )
        self.unidad = Unidad.objects.create(
            codigo="INV-01",
            descripcion="Unidad invariantes",
            sucursal=self.sucursal,
        )
        self.user = User.objects.create_user(username="invariantes.ruta")
        self.repartidor = Repartidor.objects.create(
            user=self.user,
            sucursal=self.sucursal,
            unidad_asignada=self.unidad,
        )
        self.ruta = RutaEntrega.objects.create(
            nombre="Ruta invariantes Point",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_PLANEADA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )
        self.punto = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre=self.sucursal.nombre,
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.570000",
            longitud="-108.470000",
            radio_geocerca_metros=120,
        )
        self.parada = ParadaRuta.objects.create(
            ruta=self.ruta,
            punto=self.punto,
            orden=1,
        )
        self._line_sequence = 0

    def point_line(
        self,
        *,
        requested,
        sent,
        sent_at,
        is_enviado=None,
        transfer=None,
        detail=None,
        item_code=None,
    ):
        self._line_sequence += 1
        suffix = str(self._line_sequence)
        origin = PointBranch.objects.create(
            external_id=f"INV-CEDIS-{suffix}",
            name="CEDIS",
        )
        destination = PointBranch.objects.create(
            external_id=f"INV-SUC-{suffix}",
            name=self.sucursal.nombre,
            erp_branch=self.sucursal,
        )
        transfer_payload = {}
        if is_enviado is not None:
            transfer_payload["isEnviado"] = is_enviado
        return PointTransferLine.objects.create(
            origin_branch=origin,
            destination_branch=destination,
            erp_destination_branch=self.sucursal,
            transfer_external_id=transfer or f"INV-T-{suffix}",
            detail_external_id=detail or f"INV-D-{suffix}",
            source_hash=f"invariante-point-{suffix}",
            registered_at=timezone.now(),
            sent_at=sent_at,
            item_name="Pastel prueba invariantes",
            item_code=item_code or f"INV-{suffix}",
            unit="pz",
            requested_quantity=requested,
            sent_quantity=sent,
            received_quantity="0",
            is_open=True,
            raw_payload={"transfer": transfer_payload},
        )

    def sync_line(self, line):
        resumen = sincronizar_checklist_carga_desde_point(
            ruta=self.ruta,
            user=self.user,
            ejecutar_sync=False,
        )
        return RutaCargaChecklistLinea.objects.get(
            checklist=resumen.checklist,
            source_hash=line.source_hash,
        )


class PointFullReloadInvariantTests(LogisticaInvariantFixtures):
    def _sync_job(self, *, status):
        return PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_TRANSFERS,
            status=status,
            triggered_by=self.user,
        )

    def _extracted_line(self, point_line, *, sent, received):
        ahora = timezone.now()
        return SimpleNamespace(
            origin_branch={
                "external_id": point_line.origin_branch.external_id,
                "name": point_line.origin_branch.name,
            },
            destination_branch={
                "external_id": point_line.destination_branch.external_id,
                "name": point_line.destination_branch.name,
            },
            transfer_external_id=point_line.transfer_external_id,
            detail_external_id=point_line.detail_external_id,
            registered_at=point_line.registered_at,
            sent_at=ahora,
            received_at=ahora,
            requested_by="cedis",
            sent_by="cedis",
            received_by="sucursal",
            item_name=point_line.item_name,
            item_code=point_line.item_code,
            unit=point_line.unit,
            unit_cost=point_line.unit_cost,
            requested_quantity=point_line.requested_quantity,
            sent_quantity=Decimal(sent),
            received_quantity=Decimal(received),
            is_insumo=False,
            is_received=True,
            is_cancelled=False,
            is_finalized=True,
            is_open=False,
            raw_payload={"transfer": {"isEnviado": True}},
            source_hash=point_line.source_hash,
        )

    def test_recarga_consume_snapshot_enviado_aunque_ya_este_recibido_y_excluye_version_vieja(self):
        vieja = self.point_line(
            requested="2",
            sent="0",
            sent_at=None,
            is_enviado=False,
            transfer="INV-SNAPSHOT-36654",
            detail="519914",
            item_code="0001",
        )
        actual = self.point_line(
            requested="3",
            sent="3",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-SNAPSHOT-36654",
            detail="520176",
            item_code="0001",
        )
        actual_cero = self.point_line(
            requested="1",
            sent="0",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-SNAPSHOT-36654",
            detail="520177",
            item_code="0065",
        )
        PointTransferLine.objects.filter(pk__in=[actual.pk, actual_cero.pk]).update(
            is_open=False,
            is_received=True,
            is_finalized=True,
        )
        PointTransferLine.objects.filter(pk=vieja.pk).update(
            is_open=False,
            is_current_snapshot=False,
        )
        checklist = RutaCargaChecklist.objects.create(
            ruta=self.ruta,
            estatus=RutaCargaChecklist.ESTATUS_EN_REVISION,
        )

        _sincronizar_lineas_point_para_ruta(
            ruta=self.ruta,
            checklist=checklist,
            solo_abiertas=True,
        )

        activas = checklist.lineas.exclude(
            estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA,
        )
        self.assertEqual(
            set(activas.values_list("point_transfer_line_id", flat=True)),
            {actual.id, actual_cero.id},
        )
        self.assertFalse(activas.filter(point_transfer_line=vieja).exists())
        self.assertEqual(
            activas.get(point_transfer_line=actual_cero).estatus,
            RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED,
        )

    def test_sync_de_snapshots_no_duplica_version_enviada_positiva_y_conserva_cero(self):
        vieja_positiva = self.point_line(
            requested="2",
            sent="2",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-SNAPSHOT-EDITADO",
            detail="OLD-POS",
            item_code="0001",
        )
        vieja_cero = self.point_line(
            requested="1",
            sent="0",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-SNAPSHOT-EDITADO",
            detail="OLD-ZERO",
            item_code="0065",
        )
        service = PointMovementSyncService()
        service.persist_transfer_lines(
            self._sync_job(status=PointSyncJob.STATUS_RUNNING),
            [
                self._extracted_line(vieja_positiva, sent="2", received="2"),
                self._extracted_line(vieja_cero, sent="0", received="0"),
            ],
        )
        checklist = RutaCargaChecklist.objects.create(
            ruta=self.ruta,
            estatus=RutaCargaChecklist.ESTATUS_EN_REVISION,
        )
        _sincronizar_lineas_point_para_ruta(
            ruta=self.ruta,
            checklist=checklist,
            solo_abiertas=False,
        )
        self.assertEqual(
            set(
                checklist.lineas.exclude(
                    estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA,
                ).values_list("point_transfer_line_id", flat=True)
            ),
            {vieja_positiva.id, vieja_cero.id},
        )
        actual_positiva = self.point_line(
            requested="3",
            sent="3",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-SNAPSHOT-EDITADO",
            detail="NEW-POS",
            item_code="0001",
        )
        actual_cero = self.point_line(
            requested="1",
            sent="0",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-SNAPSHOT-EDITADO",
            detail="NEW-ZERO",
            item_code="0065",
        )
        service.persist_transfer_lines(
            self._sync_job(status=PointSyncJob.STATUS_RUNNING),
            [
                self._extracted_line(actual_positiva, sent="3", received="3"),
                self._extracted_line(actual_cero, sent="0", received="0"),
            ],
        )

        _sincronizar_lineas_point_para_ruta(
            ruta=self.ruta,
            checklist=checklist,
            solo_abiertas=False,
        )

        activas = checklist.lineas.exclude(
            estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA,
        )
        self.assertEqual(
            set(activas.values_list("point_transfer_line_id", flat=True)),
            {actual_positiva.id, actual_cero.id},
        )
        self.assertEqual(
            activas.get(point_transfer_line=actual_positiva).cantidad_enviada_esperada,
            Decimal("3"),
        )
        self.assertEqual(
            activas.get(point_transfer_line=actual_cero).estatus,
            RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED,
        )

    def test_folios_distintos_del_mismo_producto_permanecen_independientes(self):
        primero = self.point_line(
            requested="2",
            sent="2",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-FOLIO-CEDIS-1",
            detail="D-1",
            item_code="0117",
        )
        segundo = self.point_line(
            requested="3",
            sent="3",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-FOLIO-CEDIS-2",
            detail="D-2",
            item_code="0117",
        )
        checklist = RutaCargaChecklist.objects.create(
            ruta=self.ruta,
            estatus=RutaCargaChecklist.ESTATUS_EN_REVISION,
        )

        _sincronizar_lineas_point_para_ruta(
            ruta=self.ruta,
            checklist=checklist,
            solo_abiertas=True,
        )

        activas = checklist.lineas.exclude(
            estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA,
        )
        self.assertEqual(
            set(activas.values_list("point_transfer_line_id", flat=True)),
            {primero.id, segundo.id},
        )
        self.assertEqual(
            sum(activas.values_list("cantidad_enviada_esperada", flat=True), Decimal("0")),
            Decimal("5"),
        )

    def test_folio_pendiente_no_se_supera_por_otro_folio_enviado_del_mismo_producto(self):
        pendiente = self.point_line(
            requested="2",
            sent="0",
            sent_at=None,
            is_enviado=False,
            transfer="INV-SOLICITUD-INDEPENDIENTE-1",
            detail="D-PENDIENTE",
            item_code="0117",
        )
        enviada = self.point_line(
            requested="3",
            sent="3",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-SOLICITUD-INDEPENDIENTE-2",
            detail="D-ENVIADA",
            item_code="0117",
        )
        checklist = RutaCargaChecklist.objects.create(
            ruta=self.ruta,
            estatus=RutaCargaChecklist.ESTATUS_EN_REVISION,
        )

        _sincronizar_lineas_point_para_ruta(
            ruta=self.ruta,
            checklist=checklist,
            solo_abiertas=True,
        )

        activas = checklist.lineas.exclude(
            estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA,
        )
        self.assertEqual(
            set(activas.values_list("point_transfer_line_id", flat=True)),
            {pendiente.id, enviada.id},
        )

    def test_recarga_completa_actualiza_snapshot_con_transferencias_cerradas(self):
        positiva = self.point_line(
            requested="41",
            sent="0",
            sent_at=None,
            is_enviado=False,
            transfer="INV-RECARGA-COMPLETA",
            detail="1",
        )
        cero = self.point_line(
            requested="12",
            sent="0",
            sent_at=None,
            is_enviado=False,
            transfer="INV-RECARGA-COMPLETA",
            detail="2",
        )
        snapshot_inicial = sincronizar_checklist_carga_desde_point(
            ruta=self.ruta,
            user=self.user,
            ejecutar_sync=False,
        )
        self.assertEqual(
            sum(
                (linea.cantidad_solicitada for linea in snapshot_inicial.checklist.lineas.all()),
                Decimal("0"),
            ),
            Decimal("53"),
        )

        funcion_recarga = getattr(
            services_carga_ruta,
            "sincronizar_checklist_recarga_desde_point",
            None,
        )
        self.assertTrue(callable(funcion_recarga))
        fechas = []
        extracted = [
            self._extracted_line(positiva, sent="41", received="41"),
            self._extracted_line(cero, sent="0", received="0"),
        ]

        def extraer_transferencias(**kwargs):
            fechas.append((kwargs["start_date"], kwargs["end_date"]))
            return extracted if kwargs["start_date"] == self.ruta.fecha_ruta else []

        with patch(
            "pos_bridge.services.movement_sync_service.PointTransferExtractor.extract",
            side_effect=extraer_transferencias,
        ):
            resumen = funcion_recarga(ruta=self.ruta, user=self.user)

        self.assertEqual(
            fechas,
            [
                (self.ruta.fecha_ruta - timedelta(days=1), self.ruta.fecha_ruta - timedelta(days=1)),
                (self.ruta.fecha_ruta, self.ruta.fecha_ruta),
            ],
        )
        self.assertEqual(resumen.checklist.point_sync_job.status, PointSyncJob.STATUS_SUCCESS)
        activas = resumen.checklist.lineas.exclude(
            estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA,
        )
        self.assertEqual(
            sum((linea.cantidad_enviada_esperada for linea in activas), Decimal("0")),
            Decimal("41"),
        )
        linea_cero = activas.get(point_transfer_line=cero)
        self.assertEqual(linea_cero.estatus, RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED)
        self.assertNotEqual(linea_cero.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
        positiva.refresh_from_db()
        self.assertEqual(positiva.sent_quantity, Decimal("41"))
        self.assertEqual(positiva.received_quantity, Decimal("41"))
        self.assertFalse(positiva.is_open)
        self.assertEqual(PointTransferLine.objects.filter(source_hash=positiva.source_hash).count(), 1)
        self.assertEqual(PointTransferLine.objects.filter(source_hash=cero.source_hash).count(), 1)

    def test_recarga_completa_preserva_auditoria_humana_si_point_cambia_esperado(self):
        point_line = self.point_line(
            requested="5",
            sent="3",
            sent_at=timezone.now(),
            is_enviado=True,
        )
        row = self.sync_line(point_line)
        validado_en = timezone.now() - timedelta(minutes=10)
        row.cantidad_cargada = Decimal("2")
        row.estatus = RutaCargaChecklistLinea.ESTATUS_FALTANTE
        row.motivo_diferencia = RutaCargaChecklistLinea.MOTIVO_STOCK_LIMITADO
        row.notas = "Operador: faltó producto por stock físico."
        row.client_event_id = "evento-operador-auditado"
        row.validado_por = self.user
        row.validado_en = validado_en
        row.save(
            update_fields=[
                "cantidad_cargada",
                "estatus",
                "motivo_diferencia",
                "notas",
                "client_event_id",
                "validado_por",
                "validado_en",
                "actualizado_en",
            ]
        )
        extracted = [self._extracted_line(point_line, sent="5", received="5")]

        with patch(
            "pos_bridge.services.movement_sync_service.PointTransferExtractor.extract",
            side_effect=[[], extracted],
        ):
            services_carga_ruta.sincronizar_checklist_recarga_desde_point(
                ruta=self.ruta,
                user=self.user,
            )

        row.refresh_from_db()
        self.assertEqual(row.cantidad_enviada_esperada, Decimal("5"))
        self.assertEqual(row.cantidad_cargada, Decimal("2"))
        self.assertEqual(row.motivo_diferencia, RutaCargaChecklistLinea.MOTIVO_STOCK_LIMITADO)
        self.assertEqual(row.client_event_id, "evento-operador-auditado")
        self.assertEqual(row.validado_por, self.user)
        self.assertEqual(row.validado_en, validado_en)
        self.assertIn("Operador: faltó producto por stock físico.", row.notas)
        self.assertIn("Point actualizó enviado de 3.000 a 5.000", row.notas)

    def test_recarga_completa_sin_lineas_no_afirma_que_busco_solo_abiertas(self):
        with patch(
            "pos_bridge.services.movement_sync_service.PointTransferExtractor.extract",
            return_value=[],
        ):
            resumen = services_carga_ruta.sincronizar_checklist_recarga_desde_point(
                ruta=self.ruta,
                user=self.user,
            )

        self.assertEqual(
            resumen.checklist.notas,
            "No se encontraron transferencias de Point en el snapshot completo para las sucursales de esta ruta.",
        )

    def test_recarga_completa_falla_conservando_job_no_exitoso(self):
        funcion_recarga = getattr(
            services_carga_ruta,
            "sincronizar_checklist_recarga_desde_point",
            None,
        )
        self.assertTrue(callable(funcion_recarga))
        success = self._sync_job(status=PointSyncJob.STATUS_SUCCESS)
        failed = self._sync_job(status=PointSyncJob.STATUS_FAILED)

        with patch.object(
            services_carga_ruta.PointMovementSyncService,
            "run_transfer_sync",
            side_effect=[success, failed],
        ):
            with self.assertRaises(PointSyncUnavailableError) as ctx:
                funcion_recarga(ruta=self.ruta, user=self.user)

        self.assertEqual(ctx.exception.sync_job, failed)


class PointEnviadoInvariantTests(LogisticaInvariantFixtures):
    def test_serializer_expone_transicion_enviado_sin_inferir_por_cantidad(self):
        point_line = self.point_line(
            requested="7",
            sent="5",
            sent_at=None,
            is_enviado=False,
        )
        row = self.sync_line(point_line)

        payload = RutaCargaChecklistLineaSerializer(row).data

        self.assertEqual(payload["cantidad_enviada_point"], "5")
        self.assertFalse(payload["point_enviada"])

    def test_point_sin_transicion_enviado_permanece_pendiente_y_bloquea(self):
        line = self.point_line(
            requested="7",
            sent="0",
            sent_at=None,
            is_enviado=None,
        )

        row = self.sync_line(line)

        self.assertEqual(row.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
        self.assertIn("aún no registra Enviado", row.notas)
        self.assertIsNotNone(checklist_bloquea_salida(self.ruta))

    def test_point_enviado_confirmado_cero_genera_zero_expected_visible(self):
        line = self.point_line(
            requested="7",
            sent="0",
            sent_at=None,
            is_enviado=True,
        )

        row = self.sync_line(line)

        self.assertEqual(row.estatus, RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED)
        self.assertEqual(row.cantidad_enviada_esperada, Decimal("0"))
        self.assertEqual(row.cantidad_cargada, Decimal("0"))

    def test_point_is_enviado_false_explicito_permanece_pendiente(self):
        line = self.point_line(
            requested="7",
            sent="0",
            sent_at=None,
            is_enviado=False,
        )

        row = self.sync_line(line)

        self.assertEqual(row.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
        self.assertIn("aún no registra Enviado", row.notas)
        self.assertIsNotNone(checklist_bloquea_salida(self.ruta))

    def test_point_enviado_distinto_de_solicitado_usa_enviado(self):
        line = self.point_line(
            requested="7",
            sent="5",
            sent_at=timezone.now(),
            is_enviado=True,
        )

        row = self.sync_line(line)

        self.assertEqual(row.cantidad_solicitada, Decimal("7"))
        self.assertEqual(row.cantidad_enviada_esperada, Decimal("5"))

    def test_point_transfer_enviada_tolera_payloads_malformados(self):
        for raw_payload in ("invalido", [], {"transfer": "invalido"}, {"transfer": []}):
            with self.subTest(raw_payload=raw_payload):
                line = SimpleNamespace(sent_at=None, raw_payload=raw_payload)
                self.assertFalse(point_transfer_enviada(line))

    def test_checklist_bloquea_salida_precarga_transferencias_point(self):
        first_line = None
        for _ in range(3):
            line = self.point_line(
                requested="5",
                sent="5",
                sent_at=timezone.now(),
            )
            first_line = first_line or line
        row = self.sync_line(first_line)
        self.ruta.checklist_carga = row.checklist

        with self.assertNumQueries(3):
            bloqueo = checklist_bloquea_salida(self.ruta)

        self.assertEqual(
            bloqueo,
            "confirma todas las líneas de carga antes de liberar la ruta",
        )


class PointCargaRouteSelectionInvariantTests(LogisticaInvariantFixtures):
    def test_resync_deja_una_sola_fila_activa_por_linea_point_y_preserva_auditoria(self):
        point_line = self.point_line(
            requested="1",
            sent="0",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-FOLIO-DUPLICADO-INTERNO",
            detail="INV-DETALLE-CANONICO",
        )
        checklist = RutaCargaChecklist.objects.create(
            ruta=self.ruta,
            estatus=RutaCargaChecklist.ESTATUS_EN_REVISION,
        )
        placeholder = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=self.parada,
            point_transfer_line=point_line,
            transfer_external_id="INV-FOLIO-ANTERIOR",
            detail_external_id="INV-DETALLE-ANTERIOR",
            source_hash="cedis-reabasto-placeholder-duplicado-interno",
            item_code=point_line.item_code,
            item_name=point_line.item_name,
            unit=point_line.unit,
            cantidad_solicitada="1",
            cantidad_enviada_esperada="0",
            cantidad_cargada="0",
            estatus=RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED,
            notas="Evidencia histórica del placeholder.",
            client_event_id="duplicado-interno-auditado",
            validado_por=self.user,
            validado_en=timezone.now(),
        )
        validado_en_original = placeholder.validado_en
        canonica = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=self.parada,
            point_transfer_line=point_line,
            transfer_external_id=point_line.transfer_external_id,
            detail_external_id=point_line.detail_external_id,
            source_hash=point_line.source_hash,
            item_code=point_line.item_code,
            item_name=point_line.item_name,
            unit=point_line.unit,
            cantidad_solicitada=point_line.requested_quantity,
            cantidad_enviada_esperada="0",
            cantidad_cargada="0",
            estatus=RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED,
        )

        _sincronizar_lineas_point_para_ruta(
            ruta=self.ruta,
            checklist=checklist,
            solo_abiertas=False,
        )

        placeholder.refresh_from_db()
        canonica.refresh_from_db()
        self.assertEqual(placeholder.estatus, RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        self.assertEqual(placeholder.superada_por, canonica)
        self.assertEqual(placeholder.cantidad_cargada, Decimal("0"))
        self.assertEqual(placeholder.client_event_id, "duplicado-interno-auditado")
        self.assertEqual(placeholder.validado_por, self.user)
        self.assertEqual(placeholder.validado_en, validado_en_original)
        self.assertIn("Evidencia histórica del placeholder.", placeholder.notas)
        self.assertIn("misma línea Point", placeholder.notas)
        self.assertNotEqual(canonica.estatus, RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        self.assertEqual(
            checklist.lineas.exclude(
                estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA,
            ).filter(point_transfer_line=point_line).count(),
            1,
        )

    def test_carga_de_ruta_excluye_transferencias_de_sucursal_hacia_cedis(self):
        origin = PointBranch.objects.create(
            external_id="INV-RETORNO-SUC",
            name=self.sucursal.nombre,
            erp_branch=self.sucursal,
        )
        destination = PointBranch.objects.create(
            external_id="INV-RETORNO-CEDIS",
            name="CEDIS",
        )
        retorno = PointTransferLine.objects.create(
            origin_branch=origin,
            destination_branch=destination,
            erp_origin_branch=self.sucursal,
            transfer_external_id="INV-RETORNO-1",
            detail_external_id="INV-RETORNO-D-1",
            source_hash="invariante-retorno-sucursal-cedis",
            registered_at=timezone.now(),
            sent_at=timezone.now(),
            item_name="Producto devuelto a CEDIS",
            item_code="INV-RETORNO",
            unit="pz",
            requested_quantity="3",
            sent_quantity="3",
            received_quantity="3",
            is_open=False,
            is_received=True,
            is_finalized=True,
            raw_payload={"transfer": {"isEnviado": True}},
        )

        checklist = RutaCargaChecklist.objects.create(
            ruta=self.ruta,
            estatus=RutaCargaChecklist.ESTATUS_EN_REVISION,
        )
        _sincronizar_lineas_point_para_ruta(
            ruta=self.ruta,
            checklist=checklist,
            solo_abiertas=False,
        )

        self.assertFalse(
            checklist.lineas.filter(point_transfer_line=retorno).exists()
        )

    def test_resync_supera_retorno_sucursal_cedis_ya_existente_y_preserva_auditoria(self):
        origin = PointBranch.objects.create(
            external_id="INV-RETORNO-PREVIO-SUC",
            name=self.sucursal.nombre,
            erp_branch=self.sucursal,
        )
        destination = PointBranch.objects.create(
            external_id="INV-RETORNO-PREVIO-CEDIS",
            name="CEDIS",
        )
        retorno = PointTransferLine.objects.create(
            origin_branch=origin,
            destination_branch=destination,
            erp_origin_branch=self.sucursal,
            transfer_external_id="INV-RETORNO-PREVIO-1",
            detail_external_id="INV-RETORNO-PREVIO-D-1",
            source_hash="invariante-retorno-previo-sucursal-cedis",
            registered_at=timezone.now(),
            sent_at=timezone.now(),
            item_name="Producto devuelto a CEDIS ya capturado",
            item_code="INV-RETORNO-PREVIO",
            unit="pz",
            requested_quantity="3",
            sent_quantity="3",
            received_quantity="3",
            is_open=False,
            is_received=True,
            is_finalized=True,
            raw_payload={"transfer": {"isEnviado": True}},
        )
        checklist = RutaCargaChecklist.objects.create(
            ruta=self.ruta,
            estatus=RutaCargaChecklist.ESTATUS_EN_REVISION,
        )
        linea = RutaCargaChecklistLinea.objects.create(
            checklist=checklist,
            parada=self.parada,
            point_transfer_line=retorno,
            transfer_external_id=retorno.transfer_external_id,
            detail_external_id=retorno.detail_external_id,
            source_hash=retorno.source_hash,
            item_code=retorno.item_code,
            item_name=retorno.item_name,
            unit=retorno.unit,
            cantidad_solicitada=retorno.requested_quantity,
            cantidad_enviada_esperada=retorno.sent_quantity,
            cantidad_cargada="3",
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
            notas="Captura previa del repartidor.",
            client_event_id="retorno-previo-auditado",
            validado_por=self.user,
            validado_en=timezone.now(),
        )
        validado_en_original = linea.validado_en

        _sincronizar_lineas_point_para_ruta(
            ruta=self.ruta,
            checklist=checklist,
            solo_abiertas=False,
        )

        linea.refresh_from_db()
        self.assertEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        self.assertEqual(linea.cantidad_cargada, Decimal("3"))
        self.assertEqual(linea.client_event_id, "retorno-previo-auditado")
        self.assertEqual(linea.validado_por, self.user)
        self.assertEqual(linea.validado_en, validado_en_original)
        self.assertIn("Captura previa del repartidor.", linea.notas)
        self.assertIn("regreso a CEDIS", linea.notas)

    def test_resync_conserva_destino_mapeado_a_sucursal_aunque_nombre_parezca_cedis(self):
        segunda_sucursal = Sucursal.objects.create(
            codigo="INV-ALMACEN-GLORIAS",
            nombre="Almacén Las Glorias",
            activa=True,
        )
        segundo_punto = PuntoLogistico.objects.create(
            sucursal=segunda_sucursal,
            nombre=segunda_sucursal.nombre,
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.580000",
            longitud="-108.480000",
            radio_geocerca_metros=120,
        )
        segunda_parada = ParadaRuta.objects.create(
            ruta=self.ruta,
            punto=segundo_punto,
            orden=2,
        )
        origin = PointBranch.objects.create(
            external_id="INV-ORIGEN-INTERSUCURSAL",
            name=self.sucursal.nombre,
            erp_branch=self.sucursal,
        )
        destination = PointBranch.objects.create(
            external_id="INV-DESTINO-ALMACEN-GLORIAS",
            name=segunda_sucursal.nombre,
            erp_branch=segunda_sucursal,
        )
        transferencia = PointTransferLine.objects.create(
            origin_branch=origin,
            destination_branch=destination,
            erp_origin_branch=self.sucursal,
            erp_destination_branch=segunda_sucursal,
            transfer_external_id="INV-INTERSUCURSAL-1",
            detail_external_id="INV-INTERSUCURSAL-D-1",
            source_hash="invariante-intersucursal-destino-almacen",
            registered_at=timezone.now(),
            sent_at=timezone.now(),
            item_name="Producto legítimo para Almacén Las Glorias",
            item_code="INV-INTERSUCURSAL",
            unit="pz",
            requested_quantity="4",
            sent_quantity="4",
            received_quantity="4",
            is_open=True,
            is_received=True,
            raw_payload={"transfer": {"isEnviado": True}},
        )
        receta = Receta.objects.create(
            nombre="Producto legítimo para Almacén Las Glorias",
            codigo_point="INV-INTERSUCURSAL",
            hash_contenido="hash-invariante-destino-estructurado",
        )
        transferencia.receta = receta
        transferencia.save(update_fields=["receta", "updated_at"])
        checklist = RutaCargaChecklist.objects.create(
            ruta=self.ruta,
            estatus=RutaCargaChecklist.ESTATUS_EN_REVISION,
        )

        self.assertEqual(resolve_requesting_erp_branch(transferencia), segunda_sucursal)
        self.assertEqual(
            OpenTransferSyncService()._requesting_branch_ids(
                fecha=self.ruta.fecha_ruta,
                transfer_ids={transferencia.transfer_external_id},
            ),
            {segunda_sucursal.id},
        )
        recibidas = _point_recibidas_por_ruta(self.ruta)
        self.assertIn((segunda_sucursal.id, "inv-intersucursal"), recibidas)
        self.assertNotIn((self.sucursal.id, "inv-intersucursal"), recibidas)
        ConsolidadoNocturnoCedisService()._crear_solicitudes_desde_point(
            fecha_operacion=self.ruta.fecha_ruta,
            usuario=self.user,
        )
        self.assertTrue(
            SolicitudReabastoCedis.objects.filter(
                fecha_operacion=self.ruta.fecha_ruta,
                sucursal=segunda_sucursal,
                lineas__receta=receta,
            ).exists()
        )
        self.assertFalse(
            SolicitudReabastoCedis.objects.filter(
                fecha_operacion=self.ruta.fecha_ruta,
                sucursal=self.sucursal,
                lineas__receta=receta,
            ).exists()
        )

        _sincronizar_lineas_point_para_ruta(
            ruta=self.ruta,
            checklist=checklist,
            solo_abiertas=False,
        )

        linea = checklist.lineas.get(point_transfer_line=transferencia)
        self.assertNotEqual(linea.estatus, RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        self.assertEqual(linea.parada, segunda_parada)

    def test_linea_point_ya_asignada_a_otra_ruta_conserva_captura_como_auditoria(self):
        point_line = self.point_line(
            requested="1",
            sent="0",
            sent_at=None,
            is_enviado=False,
            transfer="INV-FOLIO-RUTA-PREVIA",
            detail="INV-DETALLE-RUTA-PREVIA",
        )
        ruta_previa = RutaEntrega.objects.create(
            nombre="Ruta previa que ya reclamó Point",
            fecha_ruta=self.ruta.fecha_ruta - timedelta(days=1),
            estatus=RutaEntrega.ESTATUS_COMPLETADA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )
        parada_previa = ParadaRuta.objects.create(
            ruta=ruta_previa,
            punto=self.punto,
            orden=1,
        )
        checklist_previo = RutaCargaChecklist.objects.create(
            ruta=ruta_previa,
            estatus=RutaCargaChecklist.ESTATUS_CONFIRMADA,
        )
        linea_previa = RutaCargaChecklistLinea.objects.create(
            checklist=checklist_previo,
            parada=parada_previa,
            point_transfer_line=point_line,
            transfer_external_id=point_line.transfer_external_id,
            detail_external_id=point_line.detail_external_id,
            source_hash=point_line.source_hash,
            item_code=point_line.item_code,
            item_name=point_line.item_name,
            unit=point_line.unit,
            cantidad_solicitada=point_line.requested_quantity,
            cantidad_enviada_esperada="0",
            cantidad_cargada="0",
            estatus=RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED,
        )
        checklist_actual = RutaCargaChecklist.objects.create(
            ruta=self.ruta,
            estatus=RutaCargaChecklist.ESTATUS_EN_REVISION,
        )
        linea_duplicada = RutaCargaChecklistLinea.objects.create(
            checklist=checklist_actual,
            parada=self.parada,
            point_transfer_line=point_line,
            transfer_external_id=point_line.transfer_external_id,
            detail_external_id=point_line.detail_external_id,
            source_hash="cedis-reabasto-placeholder-duplicado",
            item_code=point_line.item_code,
            item_name=point_line.item_name,
            unit=point_line.unit,
            cantidad_solicitada=point_line.requested_quantity,
            cantidad_enviada_esperada="0",
            cantidad_cargada="1",
            estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
            notas="Captura física del repartidor conservada.",
            client_event_id="captura-duplicada-auditada",
            validado_por=self.user,
            validado_en=timezone.now(),
        )
        validado_en_original = linea_duplicada.validado_en

        sincronizar_checklist_carga_desde_point(
            ruta=self.ruta,
            user=self.user,
            ejecutar_sync=False,
        )

        linea_previa.refresh_from_db()
        linea_duplicada.refresh_from_db()
        self.assertEqual(
            linea_previa.estatus,
            RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED,
        )
        self.assertEqual(
            linea_duplicada.estatus,
            RutaCargaChecklistLinea.ESTATUS_SUPERADA,
        )
        self.assertEqual(linea_duplicada.cantidad_cargada, Decimal("1"))
        self.assertEqual(linea_duplicada.validado_por, self.user)
        self.assertEqual(linea_duplicada.validado_en, validado_en_original)
        self.assertEqual(
            linea_duplicada.client_event_id,
            "captura-duplicada-auditada",
        )
        self.assertIn("Captura física del repartidor conservada.", linea_duplicada.notas)
        self.assertFalse(
            checklist_actual.lineas.exclude(
                estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA,
            ).filter(point_transfer_line=point_line).exists()
        )

    def test_reserva_superada_de_otra_ruta_no_reclama_point_en_la_actual(self):
        point_line = self.point_line(
            requested="5",
            sent="5",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-FOLIO-SUPERADO",
            detail="INV-DETALLE-SUPERADO",
        )
        ruta_previa = RutaEntrega.objects.create(
            nombre="Ruta previa con auditoría superada",
            fecha_ruta=self.ruta.fecha_ruta - timedelta(days=1),
            estatus=RutaEntrega.ESTATUS_COMPLETADA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )
        parada_previa = ParadaRuta.objects.create(
            ruta=ruta_previa,
            punto=self.punto,
            orden=1,
        )
        checklist_previo = RutaCargaChecklist.objects.create(
            ruta=ruta_previa,
            estatus=RutaCargaChecklist.ESTATUS_CONFIRMADA,
        )
        linea_previa = RutaCargaChecklistLinea.objects.create(
            checklist=checklist_previo,
            parada=parada_previa,
            point_transfer_line=point_line,
            transfer_external_id=point_line.transfer_external_id,
            detail_external_id=point_line.detail_external_id,
            source_hash=point_line.source_hash,
            item_code=point_line.item_code,
            item_name=point_line.item_name,
            unit=point_line.unit,
            cantidad_solicitada=point_line.requested_quantity,
            cantidad_enviada_esperada=point_line.sent_quantity,
            cantidad_cargada=point_line.sent_quantity,
            estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA,
        )
        checklist_actual = RutaCargaChecklist.objects.create(
            ruta=self.ruta,
            estatus=RutaCargaChecklist.ESTATUS_EN_REVISION,
        )

        _sincronizar_lineas_point_para_ruta(
            ruta=self.ruta,
            checklist=checklist_actual,
            solo_abiertas=False,
        )

        linea_previa.refresh_from_db()
        self.assertEqual(
            linea_previa.estatus,
            RutaCargaChecklistLinea.ESTATUS_SUPERADA,
        )
        linea_actual = checklist_actual.lineas.get(point_transfer_line=point_line)
        self.assertEqual(linea_actual.cantidad_enviada_esperada, Decimal("5"))
        self.assertNotEqual(
            linea_actual.estatus,
            RutaCargaChecklistLinea.ESTATUS_SUPERADA,
        )


class EntregaOperativaInvariantTests(LogisticaInvariantFixtures):
    def setUp(self):
        super().setUp()
        self.user.groups.add(Group.objects.get_or_create(name="repartidor")[0])
        self.manager = User.objects.create_superuser(
            username="jefe.entrega.operativa",
            email="jefe-entrega-operativa@example.com",
            password="test",
        )
        UserModuleAccess.objects.create(
            user=self.manager,
            module="logistica.rutas",
            access=ACCESS_MANAGE,
            updated_by=self.manager,
        )
        self.ruta.estatus = RutaEntrega.ESTATUS_EN_RUTA
        self.ruta.save(update_fields=["estatus", "updated_at"])
        self.second_branch = Sucursal.objects.create(
            codigo="INV-RUTA-2",
            nombre="Segunda sucursal invariantes",
            activa=True,
        )
        self.second_point = PuntoLogistico.objects.create(
            sucursal=self.second_branch,
            nombre=self.second_branch.nombre,
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.580000",
            longitud="-108.480000",
            radio_geocerca_metros=120,
        )
        self.second_stop = ParadaRuta.objects.create(
            ruta=self.ruta,
            punto=self.second_point,
            orden=2,
        )

    def confirmar_sin_geocerca(self, parada=None, *, entrega_estado=ParadaRuta.ENTREGA_ENTREGADA):
        parada = parada or self.parada
        return confirmar_entrega_parada(
            ruta=self.ruta,
            parada=parada,
            actor=self.user,
            entrega_estado=entrega_estado,
            motivo="GPS sin señal durante la entrega",
            client_event_id=f"excepcional-{parada.id}-{entrega_estado}",
            ubicacion={
                "causa": "GPS_SIN_SENAL",
                "client_timestamp": timezone.now().isoformat(),
                "client_version": "invariantes-task4",
            },
            origen="PWA",
        )

    def resolver_segunda_parada(self):
        self.second_stop.estado = ParadaRuta.ESTADO_VISITADA
        self.second_stop.hora_llegada_real = timezone.now()
        self.second_stop.entrega_estado = ParadaRuta.ENTREGA_ENTREGADA
        self.second_stop.entrega_confirmada_en = timezone.now()
        self.second_stop.entrega_confirmada_por = self.user
        self.second_stop.save(
            update_fields=[
                "estado",
                "hora_llegada_real",
                "entrega_estado",
                "entrega_confirmada_en",
                "entrega_confirmada_por",
                "actualizado_en",
            ]
        )

    def registrar_posicion_confiable(self, parada):
        if not BitacoraSalidaLlegada.objects.filter(
            repartidor=self.repartidor,
            unidad=self.unidad,
            cerrada=False,
        ).exists():
            BitacoraSalidaLlegada.objects.create(
                repartidor=self.repartidor,
                unidad=self.unidad,
                km_salida=1000,
                nivel_gas_salida="lleno",
                foto_tablero_salida=SimpleUploadedFile("tablero.gif", b"gif", content_type="image/gif"),
            )
        payload = {
            "latitud": str(parada.latitud_geocerca),
            "longitud": str(parada.longitud_geocerca),
            "precision_metros": "8.00",
            "tracking_origen": "automatico_pwa",
        }
        return registrar_ubicacion_ruta(user=self.user, ruta=self.ruta, payload=payload)

    def registrar_dos_posiciones_confiables(self, parada):
        self.registrar_posicion_confiable(parada)
        EventoRuta.objects.filter(
            ruta=self.ruta,
            parada=parada,
            tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            metadata__origen_servicio="registrar_ubicacion_ruta",
        ).update(creado_en=timezone.now() - timedelta(minutes=6))
        self.registrar_posicion_confiable(parada)

    def test_entrega_excepcional_resuelve_sin_fabricar_visita_y_serializer_la_expone(self):
        result = self.confirmar_sin_geocerca()

        self.parada.refresh_from_db()
        data = ParadaRutaSerializer(self.parada).data
        self.assertTrue(result.requiere_revision)
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertIsNone(self.parada.hora_llegada_real)
        self.assertIsNone(self.parada.distancia_llegada_metros)
        self.assertTrue(parada_resuelta_operativamente(self.parada))
        self.assertTrue(data["operativamente_resuelta"])

    def test_geocerca_siguiente_no_queda_atrapada_en_entrega_resuelta(self):
        self.confirmar_sin_geocerca()

        self.registrar_dos_posiciones_confiables(self.second_stop)

        self.parada.refresh_from_db()
        self.second_stop.refresh_from_db()
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertIsNone(self.parada.hora_llegada_real)
        self.assertEqual(self.second_stop.estado, ParadaRuta.ESTADO_VISITADA)

    def test_geocerca_solapada_ignora_entrega_resuelta_y_marca_la_siguiente(self):
        self.confirmar_sin_geocerca()
        self.second_stop.latitud_geocerca = self.parada.latitud_geocerca
        self.second_stop.longitud_geocerca = self.parada.longitud_geocerca
        self.second_stop.radio_geocerca_metros = self.parada.radio_geocerca_metros
        self.second_stop.save(
            update_fields=[
                "latitud_geocerca",
                "longitud_geocerca",
                "radio_geocerca_metros",
                "actualizado_en",
            ]
        )

        self.registrar_dos_posiciones_confiables(self.second_stop)

        self.parada.refresh_from_db()
        self.second_stop.refresh_from_db()
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertIsNone(self.parada.hora_llegada_real)
        self.assertEqual(self.second_stop.estado, ParadaRuta.ESTADO_VISITADA)

    def test_ping_en_geocerca_resuelta_no_reabre_visita_ni_crea_desvio(self):
        self.confirmar_sin_geocerca()

        ubicacion = self.registrar_posicion_confiable(self.parada)

        self.parada.refresh_from_db()
        self.assertFalse(ubicacion.fuera_de_geocerca)
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertIsNone(self.parada.hora_llegada_real)
        self.assertFalse(
            EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_DESVIO).exists()
        )
        self.assertFalse(
            EventoRuta.objects.filter(
                ruta=self.ruta,
                parada=self.parada,
                tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            ).exists()
        )

    def test_ping_en_geocerca_planeada_con_todas_resueltas_no_crea_desvio(self):
        self.confirmar_sin_geocerca()
        self.resolver_segunda_parada()

        ubicacion = self.registrar_posicion_confiable(self.parada)

        self.assertFalse(ubicacion.fuera_de_geocerca)
        self.assertFalse(
            EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_DESVIO).exists()
        )
        self.assertFalse(
            EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE).exists()
        )

    def test_api_web_y_pwa_comparten_cierre_con_revision_pendiente(self):
        self.confirmar_sin_geocerca()
        self.resolver_segunda_parada()
        self.assertFalse(ruta_tiene_paradas_entregables_pendientes(self.ruta))

        self.client.force_login(self.manager)
        api_response = self.client.post(
            reverse("api_logistica_ruta_estatus", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"estatus": RutaEntrega.ESTATUS_COMPLETADA}),
            content_type="application/json",
        )
        self.assertEqual(api_response.status_code, 200)

        for surface in ("web", "pwa"):
            with self.subTest(surface=surface):
                self.ruta.estatus = RutaEntrega.ESTATUS_EN_RUTA
                self.ruta.hora_cierre_real = None
                self.ruta.save(update_fields=["estatus", "hora_cierre_real", "updated_at"])
                EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_CIERRE).delete()
                if surface == "web":
                    response = self.client.post(
                        reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
                        {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_COMPLETADA},
                    )
                    self.assertEqual(response.status_code, 302)
                else:
                    self.client.force_login(self.user)
                    response = self.client.post(
                        reverse("api_logistica_ruta_finalizar_pwa", kwargs={"ruta_id": self.ruta.id})
                    )
                    self.assertEqual(response.status_code, 200)
                self.ruta.refresh_from_db()
                self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_COMPLETADA)

    def test_cierre_con_diferencia_usa_misma_resolucion_operativa(self):
        self.confirmar_sin_geocerca(entrega_estado=ParadaRuta.ENTREGA_CON_DIFERENCIA)
        self.resolver_segunda_parada()

        evento = cerrar_ruta_con_diferencia_autorizada(
            ruta=self.ruta,
            user=self.manager,
            notas="Diferencia documentada",
        )

        self.ruta.refresh_from_db()
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_COMPLETADA)
        self.assertEqual(evento.metadata["tipo"], "cierre_con_diferencia_autorizada")

    def test_ui_ofrece_cierre_con_diferencia_para_entrega_excepcional_resuelta(self):
        self.confirmar_sin_geocerca(entrega_estado=ParadaRuta.ENTREGA_CON_DIFERENCIA)
        self.resolver_segunda_parada()
        self.client.force_login(self.manager)

        response = self.client.get(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id})
        )

        self.parada.refresh_from_db()
        self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertTrue(response.context["cierre_diferencia_disponible"])
        self.assertContains(response, "Cerrar con diferencia autorizada")

    def test_cedis_pendiente_siempre_bloquea_cierre(self):
        self.confirmar_sin_geocerca()
        self.resolver_segunda_parada()
        cedis = PuntoLogistico.objects.create(
            nombre="CEDIS pendiente Task 4",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.590000",
            longitud="-108.490000",
        )
        ParadaRuta.objects.create(ruta=self.ruta, punto=cedis, orden=3)

        self.assertTrue(ruta_tiene_paradas_entregables_pendientes(self.ruta))
        self.client.force_login(self.manager)
        response = self.client.post(
            reverse("api_logistica_ruta_estatus", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"estatus": RutaEntrega.ESTATUS_COMPLETADA}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 400)
        self.ruta.refresh_from_db()
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)

    def test_cedis_omitida_sigue_bloqueando_hasta_registrar_recarga(self):
        self.confirmar_sin_geocerca()
        self.resolver_segunda_parada()
        cedis = PuntoLogistico.objects.create(
            nombre="CEDIS omitida Task 4",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.590000",
            longitud="-108.490000",
        )
        parada_cedis = ParadaRuta.objects.create(
            ruta=self.ruta,
            punto=cedis,
            orden=3,
            estado=ParadaRuta.ESTADO_OMITIDA,
        )

        self.assertFalse(parada_resuelta_operativamente(parada_cedis))
        self.assertTrue(ruta_tiene_paradas_entregables_pendientes(self.ruta))

    def test_revision_administrativa_no_modifica_visita_ni_gps(self):
        self.confirmar_sin_geocerca()

        for decision in (ParadaRuta.REVISION_AUTORIZADA, ParadaRuta.REVISION_RECHAZADA):
            with self.subTest(decision=decision):
                if decision == ParadaRuta.REVISION_RECHAZADA:
                    self.parada.revision_entrega_estado = ParadaRuta.REVISION_PENDIENTE
                    self.parada.save(update_fields=["revision_entrega_estado", "actualizado_en"])
                revisar_entrega_excepcional(
                    parada=self.parada,
                    actor=self.manager,
                    decision=decision,
                    motivo=f"Decisión {decision} sin fabricar ubicación",
                )
                self.parada.refresh_from_db()
                self.assertEqual(self.parada.estado, ParadaRuta.ESTADO_PENDIENTE)
                self.assertIsNone(self.parada.hora_llegada_real)
                self.assertIsNone(self.parada.hora_salida_real)
                self.assertIsNone(self.parada.distancia_llegada_metros)


class LiberacionRutaInvariantTests(LogisticaInvariantFixtures):
    def setUp(self):
        super().setUp()
        self.user.groups.add(Group.objects.get_or_create(name="repartidor")[0])
        self.manager = User.objects.create_superuser(
            username="jefe.liberacion.ruta",
            email="jefe-liberacion-ruta@example.com",
            password="test",
        )
        UserModuleAccess.objects.create(
            user=self.manager,
            module="logistica.rutas",
            access=ACCESS_MANAGE,
            updated_by=self.manager,
        )

    def abrir_turno(self, *, repartidor=None, unidad=None):
        return BitacoraSalidaLlegada.objects.create(
            repartidor=repartidor or self.repartidor,
            unidad=unidad or self.unidad,
            km_salida=1000,
            nivel_gas_salida="lleno",
            foto_tablero_salida=SimpleUploadedFile(
                "tablero.gif",
                b"gif",
                content_type="image/gif",
            ),
        )

    def liberar_api(self):
        self.client.force_login(self.manager)
        return self.client.post(
            reverse("api_logistica_ruta_estatus", kwargs={"ruta_id": self.ruta.id}),
            json.dumps({"estatus": RutaEntrega.ESTATUS_EN_RUTA}),
            content_type="application/json",
        )

    def liberar_web(self):
        self.client.force_login(self.manager)
        return self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_EN_RUTA},
        )

    def test_liberacion_administrativa_sin_turno_no_modifica_ruta_ni_evento(self):
        response = self.liberar_api()

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 400)
        self.assertIn("turno", response.json()["detail"].lower())
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertIsNone(self.ruta.bitacora_salida_id)
        self.assertFalse(
            EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_SALIDA).exists()
        )

    def test_liberacion_web_rechaza_turno_de_unidad_distinta_sin_mutar(self):
        otra_unidad = Unidad.objects.create(
            codigo="INV-OTRA",
            descripcion="Otra unidad",
            sucursal=self.sucursal,
        )
        self.abrir_turno(unidad=otra_unidad)

        response = self.liberar_web()

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 302)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertIsNone(self.ruta.bitacora_salida_id)
        self.assertFalse(
            EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_SALIDA).exists()
        )

    def test_helper_pwa_rechaza_turno_de_otro_repartidor(self):
        otro_user = User.objects.create_user(username="otro.repartidor.liberacion")
        otro_repartidor = Repartidor.objects.create(
            user=otro_user,
            sucursal=self.sucursal,
            unidad_asignada=self.unidad,
        )
        turno_ajeno = self.abrir_turno(repartidor=otro_repartidor)

        with self.assertRaisesRegex(ValidationError, "otro repartidor"):
            _liberar_ruta_desde_bitacora_salida(
                ruta=self.ruta,
                bitacora=turno_ajeno,
                user=self.manager,
            )

        self.ruta.refresh_from_db()
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertFalse(
            EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_SALIDA).exists()
        )

    def test_liberacion_compatible_liga_bitacora_preserva_hora_y_una_salida(self):
        turno = self.abrir_turno()
        hora_previa = timezone.now() - timedelta(minutes=30)
        self.ruta.hora_inicio_real = hora_previa
        self.ruta.save(update_fields=["hora_inicio_real", "updated_at"])

        first = self.liberar_api()
        retry = self.liberar_api()

        self.ruta.refresh_from_db()
        self.assertEqual((first.status_code, retry.status_code), (200, 200))
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)
        self.assertEqual(self.ruta.bitacora_salida_id, turno.id)
        self.assertEqual(self.ruta.hora_inicio_real, hora_previa)
        self.assertEqual(
            EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_SALIDA).count(),
            1,
        )

    def test_retry_en_ruta_retiene_bitacora_original_ante_turno_posterior(self):
        turno_original = self.abrir_turno()
        self.assertEqual(self.liberar_api().status_code, 200)
        turno_posterior = self.abrir_turno()
        self.assertNotEqual(turno_original.id, turno_posterior.id)
        self.client.force_login(self.user)

        response = self.client.post(
            reverse("api_logistica_bitacora_salida_liberar_ruta")
        )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.ruta.bitacora_salida_id, turno_original.id)
        self.assertEqual(
            EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_SALIDA).count(),
            1,
        )

    def test_dos_turnos_abiertos_bloquean_api_web_y_pwa_sin_mutar(self):
        turno_compatible = self.abrir_turno()
        otra_unidad = Unidad.objects.create(
            codigo="INV-TURNO-AMB",
            descripcion="Unidad turno ambiguo",
            sucursal=self.sucursal,
        )
        self.abrir_turno(unidad=otra_unidad)

        api_response = self.liberar_api()
        self.assertEqual(api_response.status_code, 400)
        self.assertEqual(api_response.json()["error"], "turno_ambiguo")

        self.client.force_login(self.manager)
        web_response = self.client.post(
            reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id}),
            {"action": "ruta_status", "estatus": RutaEntrega.ESTATUS_EN_RUTA},
            follow=True,
        )
        self.assertEqual(web_response.status_code, 200)
        self.assertContains(web_response, "más de un turno abierto")

        self.client.force_login(self.user)
        pwa_response = self.client.post(
            reverse("api_logistica_bitacora_salida_liberar_ruta")
        )
        self.assertEqual(pwa_response.status_code, 400)
        self.assertEqual(pwa_response.json()["error"], "turno_ambiguo")

        self.ruta.refresh_from_db()
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertIsNone(self.ruta.bitacora_salida_id)
        self.assertIsNone(self.ruta.hora_inicio_real)
        self.assertFalse(
            EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_SALIDA).exists()
        )
        self.assertFalse(turno_compatible.cerrada)

    def test_bitacora_explicita_compatible_no_elude_turno_ambiguo(self):
        turno_compatible = self.abrir_turno()
        otra_unidad = Unidad.objects.create(
            codigo="INV-TURNO-EXP",
            descripcion="Unidad segundo turno explícito",
            sucursal=self.sucursal,
        )
        self.abrir_turno(unidad=otra_unidad)

        with self.assertRaises(LiberacionRutaError) as ctx:
            _liberar_ruta_desde_bitacora_salida(
                ruta=self.ruta,
                bitacora=turno_compatible,
                user=self.manager,
            )

        self.assertEqual(ctx.exception.error_code, "turno_ambiguo")
        self.ruta.refresh_from_db()
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertIsNone(self.ruta.bitacora_salida_id)
        self.assertFalse(
            EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_SALIDA).exists()
        )

    def test_api_otra_ruta_activa_devuelve_conflicto_controlado(self):
        otra_ruta = RutaEntrega.objects.create(
            nombre="Ruta activa incompatible",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_EN_RUTA,
            repartidor=self.repartidor,
            unidad_operativa=self.unidad,
        )
        self.abrir_turno()

        response = self.liberar_api()

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 409)
        self.assertIn("otra ruta", response.json()["detail"].lower())
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertFalse(
            EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_SALIDA).exists()
        )
        self.assertEqual(otra_ruta.estatus, RutaEntrega.ESTATUS_EN_RUTA)

    def test_pwa_traduce_integrity_error_sin_evento_ni_cambio(self):
        self.abrir_turno()
        self.client.force_login(self.user)

        with patch.object(RutaEntrega, "save", side_effect=IntegrityError("conflicto concurrente")):
            response = self.client.post(
                reverse("api_logistica_bitacora_salida_liberar_ruta")
            )

        self.ruta.refresh_from_db()
        self.assertEqual(response.status_code, 409)
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_PLANEADA)
        self.assertFalse(
            EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_SALIDA).exists()
        )


class PointCanonicalLineTests(LogisticaInvariantFixtures):
    def sync_all(self):
        return sincronizar_checklist_carga_desde_point(
            ruta=self.ruta,
            user=self.user,
            ejecutar_sync=False,
        )

    def test_sync_cache_no_actualiza_sincronizado_en_como_sync_externo(self):
        checklist = RutaCargaChecklist.objects.create(ruta=self.ruta)
        anterior = timezone.now() - timedelta(hours=2)
        checklist.sincronizado_en = anterior
        checklist.save(update_fields=["sincronizado_en"])

        self.sync_all()

        checklist.refresh_from_db()
        self.assertEqual(checklist.sincronizado_en, anterior)

    def test_mismo_folio_producto_dos_detalles_enviados_no_se_superan_y_suman(self):
        self.point_line(
            requested="2",
            sent="2",
            sent_at=timezone.now(),
            transfer="INV-FOLIO-COMPARTIDO",
            detail="10",
            item_code="INV-PRODUCTO-COMPARTIDO",
        )
        self.point_line(
            requested="3",
            sent="3",
            sent_at=timezone.now(),
            transfer="INV-FOLIO-COMPARTIDO",
            detail="11",
            item_code="INV-PRODUCTO-COMPARTIDO",
        )

        resumen = self.sync_all()

        activas = resumen.checklist.lineas.exclude(
            estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA,
        )
        self.assertEqual(activas.count(), 2)
        self.assertEqual(
            sum((row.cantidad_enviada_esperada for row in activas), Decimal("0")),
            Decimal("5"),
        )

    def test_detalle_cero_no_enviado_es_superado_por_reemplazo_positivo_confirmado(self):
        old = self.point_line(
            requested="3",
            sent="0",
            sent_at=None,
            is_enviado=False,
            transfer="INV-FOLIO-CORREGIDO",
            detail="10",
            item_code="INV-PRODUCTO-CORREGIDO",
        )
        self.sync_all()
        new = self.point_line(
            requested="3",
            sent="3",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-FOLIO-CORREGIDO",
            detail="11",
            item_code="INV-PRODUCTO-CORREGIDO",
        )
        old.is_current_snapshot = False
        old.is_open = False
        old.save(update_fields=["is_current_snapshot", "is_open", "updated_at"])

        self.sync_all()

        old_row = RutaCargaChecklistLinea.objects.get(point_transfer_line=old)
        new_row = RutaCargaChecklistLinea.objects.get(point_transfer_line=new)
        self.assertEqual(old_row.estatus, RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        self.assertEqual(old_row.superada_por, new_row)

    def test_folio_no_enviado_permanece_independiente_de_otro_folio_enviado(self):
        old = self.point_line(
            requested="7",
            sent="0",
            sent_at=None,
            is_enviado=False,
            transfer="INV-FOLIO-SOLICITADO",
            detail="10",
            item_code="INV-PRODUCTO-RECARGA",
        )
        self.sync_all()
        new = self.point_line(
            requested="5",
            sent="5",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-FOLIO-ENVIADO",
            detail="20",
            item_code="INV-PRODUCTO-RECARGA",
        )

        self.sync_all()

        old_row = RutaCargaChecklistLinea.objects.get(point_transfer_line=old)
        new_row = RutaCargaChecklistLinea.objects.get(point_transfer_line=new)
        self.assertEqual(old_row.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
        self.assertIsNone(old_row.superada_por)
        self.assertNotEqual(new_row.estatus, RutaCargaChecklistLinea.ESTATUS_SUPERADA)

    def test_dos_folios_enviados_del_mismo_producto_permanecen_activos_y_suman(self):
        self.point_line(
            requested="2",
            sent="2",
            sent_at=timezone.now() - timedelta(minutes=5),
            is_enviado=True,
            transfer="INV-FOLIO-ENVIADO-A",
            detail="10",
            item_code="INV-PRODUCTO-DOS-ENVIOS",
        )
        self.point_line(
            requested="3",
            sent="3",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-FOLIO-ENVIADO-B",
            detail="20",
            item_code="INV-PRODUCTO-DOS-ENVIOS",
        )

        resumen = self.sync_all()

        activas = resumen.checklist.lineas.exclude(
            estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA,
        )
        self.assertEqual(activas.count(), 2)
        self.assertEqual(
            sum((row.cantidad_enviada_esperada for row in activas), Decimal("0")),
            Decimal("5"),
        )

    def test_folio_no_enviado_de_otro_producto_no_es_superado(self):
        old = self.point_line(
            requested="4",
            sent="0",
            sent_at=None,
            is_enviado=False,
            transfer="INV-FOLIO-SOLICITADO-OTRO",
            detail="10",
            item_code="INV-PRODUCTO-PENDIENTE-REAL",
        )
        self.sync_all()
        self.point_line(
            requested="4",
            sent="4",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-FOLIO-ENVIADO-OTRO",
            detail="20",
            item_code="INV-PRODUCTO-DISTINTO",
        )

        self.sync_all()

        old_row = RutaCargaChecklistLinea.objects.get(point_transfer_line=old)
        self.assertNotEqual(old_row.estatus, RutaCargaChecklistLinea.ESTATUS_SUPERADA)

    def test_folio_enviado_en_cero_no_oculta_otro_folio_pendiente(self):
        old = self.point_line(
            requested="4",
            sent="0",
            sent_at=None,
            is_enviado=False,
            transfer="INV-FOLIO-PENDIENTE-CERO",
            detail="10",
            item_code="INV-PRODUCTO-ENVIADO-CERO",
        )
        self.sync_all()
        new = self.point_line(
            requested="0",
            sent="0",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-FOLIO-ENVIADO-CERO",
            detail="20",
            item_code="INV-PRODUCTO-ENVIADO-CERO",
        )

        self.sync_all()

        old_row = RutaCargaChecklistLinea.objects.get(point_transfer_line=old)
        new_row = RutaCargaChecklistLinea.objects.get(point_transfer_line=new)
        self.assertEqual(old_row.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
        self.assertIsNone(old_row.superada_por)
        self.assertEqual(new_row.estatus, RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED)

    def test_folio_pendiente_se_actualiza_si_point_lo_confirma_enviado_despues(self):
        old = self.point_line(
            requested="2",
            sent="0",
            sent_at=None,
            is_enviado=False,
            transfer="INV-FOLIO-REACTIVABLE",
            detail="10",
            item_code="INV-PRODUCTO-REACTIVABLE",
        )
        self.sync_all()
        self.point_line(
            requested="3",
            sent="3",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-FOLIO-POSTERIOR",
            detail="20",
            item_code="INV-PRODUCTO-REACTIVABLE",
        )
        self.sync_all()
        old_row = RutaCargaChecklistLinea.objects.get(point_transfer_line=old)
        self.assertEqual(old_row.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
        self.assertIsNone(old_row.superada_por)

        old.sent_quantity = Decimal("2")
        old.sent_at = timezone.now()
        old.raw_payload = {"transfer": {"isEnviado": True}}
        old.save(update_fields=["sent_quantity", "sent_at", "raw_payload", "updated_at"])

        resumen = self.sync_all()

        old_row.refresh_from_db()
        self.assertEqual(old_row.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
        self.assertIsNone(old_row.superada_por)
        self.assertEqual(
            resumen.checklist.lineas.exclude(
                estatus=RutaCargaChecklistLinea.ESTATUS_SUPERADA,
            ).count(),
            2,
        )

    def test_reactivacion_preserva_cantidad_evidencia_y_notas_de_captura_humana(self):
        old = self.point_line(
            requested="2",
            sent="0",
            sent_at=None,
            is_enviado=False,
            transfer="INV-FOLIO-REACTIVA-AUDITADO",
            detail="10",
            item_code="INV-PRODUCTO-REACTIVA-AUDITADO",
        )
        self.sync_all()
        self.point_line(
            requested="3",
            sent="3",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-FOLIO-POSTERIOR-AUDITADO",
            detail="20",
            item_code="INV-PRODUCTO-REACTIVA-AUDITADO",
        )
        self.sync_all()
        old_row = RutaCargaChecklistLinea.objects.get(point_transfer_line=old)
        old_row.cantidad_cargada = Decimal("1")
        old_row.validado_por = self.user
        old_row.validado_en = timezone.now()
        old_row.client_event_id = "captura-humana-reactivada"
        old_row.notas = f"{old_row.notas} Captura física conservada."
        old_row.save(
            update_fields=[
                "cantidad_cargada",
                "validado_por",
                "validado_en",
                "client_event_id",
                "notas",
                "actualizado_en",
            ]
        )
        ParadaEntregaEvidencia.objects.create(
            ruta=self.ruta,
            parada=self.parada,
            linea_carga=old_row,
            cantidad_entregada=Decimal("1"),
            comentario="Evidencia humana previa a confirmación tardía Point.",
            client_event_id="evidencia-reactivada",
            capturado_por=self.user,
        )

        old.sent_quantity = Decimal("2")
        old.sent_at = timezone.now()
        old.raw_payload = {"transfer": {"isEnviado": True}}
        old.save(update_fields=["sent_quantity", "sent_at", "raw_payload", "updated_at"])
        self.sync_all()

        old_row.refresh_from_db()
        self.assertEqual(old_row.cantidad_cargada, Decimal("1"))
        self.assertEqual(old_row.estatus, RutaCargaChecklistLinea.ESTATUS_PARCIAL)
        self.assertIsNone(old_row.superada_por)
        self.assertIn("Captura física conservada", old_row.notas)
        self.assertIn("captura conservada en 1.000", old_row.notas)

    def test_folio_cancelado_no_puede_superar_solicitud_pendiente(self):
        old = self.point_line(
            requested="2",
            sent="0",
            sent_at=None,
            is_enviado=False,
            transfer="INV-FOLIO-PENDIENTE-CANCELADO",
            detail="10",
            item_code="INV-PRODUCTO-CANCELADO",
        )
        self.sync_all()
        new = self.point_line(
            requested="2",
            sent="2",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-FOLIO-POSTERIOR-CANCELADO",
            detail="20",
            item_code="INV-PRODUCTO-CANCELADO",
        )
        self.sync_all()
        old_row = RutaCargaChecklistLinea.objects.get(point_transfer_line=old)
        old_row.estatus = RutaCargaChecklistLinea.ESTATUS_PENDIENTE
        old_row.superada_por = None
        old_row.save(update_fields=["estatus", "superada_por", "actualizado_en"])
        new.is_cancelled = True
        new.save(update_fields=["is_cancelled", "updated_at"])

        self.sync_all()

        old_row.refresh_from_db()
        self.assertEqual(old_row.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
        self.assertIsNone(old_row.superada_por)

    def test_folio_cancelado_despues_de_cargarse_sale_de_operacion_y_conserva_auditoria(self):
        point_line = self.point_line(
            requested="3",
            sent="3",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-FOLIO-CANCELADO-DESPUES",
            detail="10",
            item_code="INV-PRODUCTO-CANCELADO-DESPUES",
        )
        row = self.sync_line(point_line)
        row.client_event_id = "captura-antes-cancelacion"
        row.notas = "Captura preservada."
        row.save(update_fields=["client_event_id", "notas", "actualizado_en"])
        point_line.is_cancelled = True
        point_line.is_open = False
        point_line.save(update_fields=["is_cancelled", "is_open", "updated_at"])

        self.sync_all()

        row.refresh_from_db()
        self.assertEqual(row.estatus, RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        self.assertEqual(row.client_event_id, "captura-antes-cancelacion")
        self.assertIn("Captura preservada", row.notas)
        self.assertIn("canceló este folio", row.notas)

    def test_folio_no_enviado_con_captura_humana_no_se_supera_entre_folios(self):
        old = self.point_line(
            requested="4",
            sent="0",
            sent_at=None,
            is_enviado=False,
            transfer="INV-FOLIO-CAPTURADO",
            detail="10",
            item_code="INV-PRODUCTO-CAPTURADO",
        )
        old_row = self.sync_line(old)
        old_row.client_event_id = "captura-humana-1"
        old_row.save(update_fields=["client_event_id"])
        self.point_line(
            requested="4",
            sent="4",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-FOLIO-POSTERIOR-CAPTURA",
            detail="20",
            item_code="INV-PRODUCTO-CAPTURADO",
        )

        self.sync_all()

        old_row.refresh_from_db()
        self.assertNotEqual(old_row.estatus, RutaCargaChecklistLinea.ESTATUS_SUPERADA)

    def test_linea_fusionada_cedis_no_puede_reutilizarse_en_otra_ruta(self):
        receta = Receta.objects.create(
            nombre="Pastel prueba invariantes",
            codigo_point="INV-PRODUCTO-CEDIS",
            hash_contenido="hash-invariante-linea-fusionada",
        )
        solicitud = SolicitudReabastoCedis.objects.create(
            fecha_operacion=self.ruta.fecha_ruta,
            sucursal=self.sucursal,
            estado=SolicitudReabastoCedis.ESTADO_ENVIADA,
            creado_por=self.user,
        )
        SolicitudReabastoCedisLinea.objects.create(
            solicitud=solicitud,
            receta=receta,
            sugerido="3",
            solicitado="3",
            justificacion="Prueba de identidad global",
        )
        point_line = self.point_line(
            requested="3",
            sent="3",
            sent_at=timezone.now(),
            item_code="INV-PRODUCTO-CEDIS",
        )
        first = self.sync_all()
        fused = first.checklist.lineas.get(point_transfer_line=point_line)
        self.assertTrue(fused.source_hash.startswith("cedis-reabasto-"))

        other_unit = Unidad.objects.create(
            codigo="INV-02",
            descripcion="Segunda unidad invariantes",
            sucursal=self.sucursal,
        )
        other_route = RutaEntrega.objects.create(
            nombre="Otra ruta invariantes Point",
            fecha_ruta=self.ruta.fecha_ruta,
            estatus=RutaEntrega.ESTATUS_PLANEADA,
            repartidor=self.repartidor,
            unidad_operativa=other_unit,
        )
        ParadaRuta.objects.create(ruta=other_route, punto=self.punto, orden=1)

        other = sincronizar_checklist_carga_desde_point(
            ruta=other_route,
            user=self.user,
            ejecutar_sync=False,
        )

        self.assertFalse(other.checklist.lineas.filter(point_transfer_line=point_line).exists())

    def test_resync_misma_linea_es_idempotente(self):
        point_line = self.point_line(
            requested="3",
            sent="3",
            sent_at=timezone.now(),
        )
        first = self.sync_all()
        first_row = first.checklist.lineas.get(point_transfer_line=point_line)

        second = self.sync_all()

        self.assertEqual(second.creadas, 0)
        self.assertEqual(
            second.checklist.lineas.filter(point_transfer_line=point_line).count(),
            1,
        )
        self.assertEqual(
            second.checklist.lineas.get(point_transfer_line=point_line).pk,
            first_row.pk,
        )

    def test_sync_bloquea_candidatas_point_antes_de_reclamarlas(self):
        self.point_line(
            requested="3",
            sent="3",
            sent_at=timezone.now(),
        )

        with CaptureQueriesContext(connection) as queries:
            self.sync_all()

        point_table = connection.ops.quote_name(PointTransferLine._meta.db_table)
        point_locks = [
            query["sql"]
            for query in queries.captured_queries
            if f"FOR UPDATE OF {point_table}" in query["sql"]
        ]
        self.assertTrue(
            point_locks,
            "La selección debe bloquear específicamente PointTransferLine con FOR UPDATE OF.",
        )

    def test_snapshot_anterior_con_multiples_detalles_se_conserva_superado_sin_inventar_incidencia(self):
        old_lines = [
            self.point_line(
                requested="3",
                sent="0",
                sent_at=None,
                is_enviado=False,
                transfer="INV-FOLIO-AMBIGUO",
                detail=detail,
                item_code="INV-PRODUCTO-AMBIGUO",
            )
            for detail in ("10", "11")
        ]
        self.sync_all()
        new_line = self.point_line(
            requested="3",
            sent="3",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-FOLIO-AMBIGUO",
            detail="12",
            item_code="INV-PRODUCTO-AMBIGUO",
        )
        PointTransferLine.objects.filter(pk__in=[line.id for line in old_lines]).update(
            is_current_snapshot=False,
            is_open=False,
        )

        self.sync_all()
        self.sync_all()

        old_rows = RutaCargaChecklistLinea.objects.filter(
            point_transfer_line_id__in=[line.id for line in old_lines],
        )
        self.assertEqual(
            set(old_rows.values_list("estatus", flat=True)),
            {RutaCargaChecklistLinea.ESTATUS_SUPERADA},
        )
        incidencias = EventoRuta.objects.filter(
            ruta=self.ruta,
            metadata__regla="CARGA_POINT_REEMPLAZO_AMBIGUO",
        )
        self.assertFalse(incidencias.exists())
        self.assertTrue(
            old_rows.filter(notas__contains="Snapshot anterior del mismo folio").count(),
        )
        self.assertNotEqual(
            RutaCargaChecklistLinea.objects.get(point_transfer_line=new_line).estatus,
            RutaCargaChecklistLinea.ESTATUS_SUPERADA,
        )

    def test_snapshot_anterior_superado_conserva_validado_en_sin_usuario(self):
        old = self.point_line(
            requested="3",
            sent="0",
            sent_at=None,
            is_enviado=False,
            transfer="INV-FOLIO-VALIDADO-EN",
            detail="10",
            item_code="INV-PRODUCTO-VALIDADO-EN",
        )
        old_row = self.sync_line(old)
        old_row.validado_en = timezone.now()
        old_row.save(update_fields=["validado_en"])
        new = self.point_line(
            requested="3",
            sent="3",
            sent_at=timezone.now(),
            is_enviado=True,
            transfer="INV-FOLIO-VALIDADO-EN",
            detail="11",
            item_code="INV-PRODUCTO-VALIDADO-EN",
        )
        old.is_current_snapshot = False
        old.is_open = False
        old.save(update_fields=["is_current_snapshot", "is_open", "updated_at"])

        self.sync_all()

        old_row.refresh_from_db()
        self.assertEqual(old_row.estatus, RutaCargaChecklistLinea.ESTATUS_SUPERADA)
        self.assertIsNotNone(old_row.validado_en)
        self.assertEqual(
            old_row.superada_por,
            RutaCargaChecklistLinea.objects.get(point_transfer_line=new),
        )

    def test_zero_expected_confirmado_pasa_a_positivo_en_misma_linea(self):
        point_line = self.point_line(
            requested="3",
            sent="0",
            sent_at=timezone.now(),
            is_enviado=True,
        )
        row = self.sync_line(point_line)
        self.assertEqual(row.estatus, RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED)
        point_line.sent_quantity = Decimal("3")
        point_line.save(update_fields=["sent_quantity", "updated_at"])

        self.sync_all()

        row.refresh_from_db()
        self.assertEqual(row.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
        self.assertEqual(row.cantidad_enviada_esperada, Decimal("3"))
        self.assertEqual(
            RutaCargaChecklistLinea.objects.filter(point_transfer_line=point_line).count(),
            1,
        )

    def test_sync_crece_acotado_de_una_a_cinco_lineas_point(self):
        self.point_line(
            requested="3",
            sent="3",
            sent_at=timezone.now(),
        )
        checklist = RutaCargaChecklist.objects.create(ruta=self.ruta)

        with CaptureQueriesContext(connection) as queries_one:
            with transaction.atomic():
                _sincronizar_lineas_point_para_ruta(
                    ruta=self.ruta,
                    checklist=checklist,
                    solo_abiertas=True,
                )

        for _ in range(4):
            self.point_line(
                requested="3",
                sent="3",
                sent_at=timezone.now(),
            )
        with CaptureQueriesContext(connection) as queries_five:
            with transaction.atomic():
                _sincronizar_lineas_point_para_ruta(
                    ruta=self.ruta,
                    checklist=checklist,
                    solo_abiertas=True,
                )

        self.assertLessEqual(
            len(queries_five),
            len(queries_one) + 6,
            "Cuatro candidatas adicionales sólo deben agregar sus escrituras, no consultas de descubrimiento por fila.",
        )


class RecargaCedisInvariantTests(LogisticaInvariantFixtures):
    def setUp(self):
        super().setUp()
        self.ruta.estatus = RutaEntrega.ESTATUS_EN_RUTA
        self.ruta.save(update_fields=["estatus", "updated_at"])
        self.parada.orden = 3
        self.parada.save(update_fields=["orden", "actualizado_en"])
        sucursal_previa = Sucursal.objects.create(
            codigo="INV-RUTA-PREVIA",
            nombre="Sucursal previa a recarga invariantes",
            activa=True,
        )
        punto_previo = PuntoLogistico.objects.create(
            sucursal=sucursal_previa,
            nombre=sucursal_previa.nombre,
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.550000",
            longitud="-108.450000",
            radio_geocerca_metros=120,
        )
        self.parada_previa = ParadaRuta.objects.create(
            ruta=self.ruta,
            punto=punto_previo,
            orden=1,
            estado=ParadaRuta.ESTADO_VISITADA,
        )
        self.cedis_punto = PuntoLogistico.objects.create(
            nombre="CEDIS invariantes",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.560000",
            longitud="-108.460000",
            radio_geocerca_metros=120,
        )
        self.cedis = ParadaRuta.objects.create(ruta=self.ruta, punto=self.cedis_punto, orden=2)
        self.manager = User.objects.create_superuser(
            username="jefe.logistica.invariantes",
            email="jefe-invariantes@example.com",
            password="test",
        )
        self.url = reverse(
            "api_logistica_ruta_parada_recarga_cedis",
            kwargs={"ruta_id": self.ruta.id, "parada_id": self.cedis.id},
        )

    def _post(self, *, user=None, **payload):
        self.client.force_login(user or self.user)
        return self.client.post(
            self.url,
            json.dumps(payload),
            content_type="application/json",
            secure=True,
        )

    def _pending_next_segment(self):
        point_line = self.point_line(requested="3", sent="0", sent_at=None, is_enviado=False)
        row = self.sync_line(point_line)
        self.assertEqual(row.parada_id, self.parada.id)
        return row

    def _sync_summary(self):
        return SimpleNamespace(
            checklist=RutaCargaChecklist.objects.get(ruta=self.ruta),
            creadas=0,
            actualizadas=0,
            omitidas=0,
        )

    def _mark_external_snapshot_valid(self):
        job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_TRANSFERS,
            status=PointSyncJob.STATUS_SUCCESS,
            triggered_by=self.user,
        )
        checklist, _ = RutaCargaChecklist.objects.get_or_create(ruta=self.ruta)
        checklist.point_sync_job = job
        checklist.sincronizado_en = timezone.now()
        checklist.save(update_fields=["point_sync_job", "sincronizado_en", "actualizado_en"])
        return checklist

    def _crear_alerta_revisable(self, estado_sync="PENDIENTE_ENVIADO"):
        from .services_carga_ruta import _snapshot_siguiente_tramo

        snapshot = _snapshot_siguiente_tramo(self.ruta, self.cedis, actor=self.manager)
        _registrar_alerta_recarga_sync(
            ruta=self.ruta,
            parada=self.cedis,
            estado_sync=estado_sync,
            snapshot=snapshot,
            detalle=f"Alerta revisable {estado_sync}",
            actor=self.manager,
        )
        return snapshot["snapshot_hash"]

    def _evento_alerta_manual(self, *, clave, snapshot_hash, ultima_observacion_en=None):
        metadata = {
            "tipo": "alerta_recarga_cedis_sync",
            "estado_sync": "PENDIENTE_ENVIADO",
            "snapshot": {"snapshot_hash": snapshot_hash},
        }
        if ultima_observacion_en is not None:
            metadata["ultima_observacion_en"] = ultima_observacion_en
        return EventoRuta.objects.create(
            ruta=self.ruta,
            parada=self.cedis,
            tipo=EventoRuta.TIPO_INCIDENCIA_MANUAL,
            severidad=EventoRuta.SEVERIDAD_ALERTA,
            descripcion="Alerta manual para selección determinista",
            metadata=metadata,
            clave_auditoria=clave,
            creado_por=self.manager,
        )

    def test_serializer_separa_visita_de_recarga(self):
        self.cedis.estado = ParadaRuta.ESTADO_VISITADA
        self.cedis.save(update_fields=["estado", "actualizado_en"])

        self.assertFalse(ParadaRutaSerializer(self.cedis).data["recarga_cedis_resuelta"])

        canonico = EventoRuta.objects.create(
            ruta=self.ruta,
            parada=self.cedis,
            tipo=EventoRuta.TIPO_RECARGA_CEDIS,
            descripcion="Recarga canónica confirmada",
        )
        self.assertTrue(ParadaRutaSerializer(self.cedis).data["recarga_cedis_resuelta"])

        canonico.delete()
        EventoRuta.objects.create(
            ruta=self.ruta,
            parada=self.cedis,
            tipo=EventoRuta.TIPO_INCIDENCIA_MANUAL,
            descripcion="Recarga histórica compatible",
            metadata={"tipo": "recarga_cedis_pwa"},
        )
        self.assertTrue(ParadaRutaSerializer(self.cedis).data["recarga_cedis_resuelta"])

    def test_serializer_coleccion_precarga_recargas_sin_n_mas_uno(self):
        cedis_adicionales = [
            ParadaRuta.objects.create(
                ruta=self.ruta,
                punto=PuntoLogistico.objects.create(
                    nombre=f"CEDIS colección {indice}",
                    tipo=PuntoLogistico.TIPO_CEDIS,
                    latitud=f"25.56{indice}000",
                    longitud=f"-108.46{indice}000",
                    radio_geocerca_metros=120,
                ),
                orden=10 + indice,
                estado=ParadaRuta.ESTADO_VISITADA,
            )
            for indice in range(1, 4)
        ]
        EventoRuta.objects.create(
            ruta=self.ruta,
            parada=cedis_adicionales[1],
            tipo=EventoRuta.TIPO_RECARGA_CEDIS,
            descripcion="Recarga de colección",
        )
        paradas = self.ruta.paradas.select_related(
            "ruta",
            "punto",
            "punto__sucursal",
            "entrega_confirmada_por",
            "revision_entrega_revisada_por",
        ).order_by("orden", "id")

        with CaptureQueriesContext(connection) as queries:
            data = ParadaRutaSerializer(paradas, many=True).data

        tabla_eventos = EventoRuta._meta.db_table
        consultas_recarga = [
            query["sql"]
            for query in queries
            if tabla_eventos in query["sql"] and EventoRuta.TIPO_RECARGA_CEDIS in query["sql"]
        ]
        self.assertEqual(len(consultas_recarga), 1)
        por_id = {item["id"]: item for item in data}
        self.assertTrue(por_id[cedis_adicionales[1].id]["recarga_cedis_resuelta"])
        self.assertFalse(por_id[cedis_adicionales[0].id]["recarga_cedis_resuelta"])

    def test_serializer_iterator_materializa_todas_y_precarga_recarga_una_vez(self):
        cedis_adicional = ParadaRuta.objects.create(
            ruta=self.ruta,
            punto=PuntoLogistico.objects.create(
                nombre="CEDIS iterator",
                tipo=PuntoLogistico.TIPO_CEDIS,
                latitud="25.565000",
                longitud="-108.465000",
                radio_geocerca_metros=120,
            ),
            orden=4,
            estado=ParadaRuta.ESTADO_VISITADA,
        )
        EventoRuta.objects.create(
            ruta=self.ruta,
            parada=cedis_adicional,
            tipo=EventoRuta.TIPO_RECARGA_CEDIS,
            descripcion="Recarga iterator",
        )
        ubicacion = UbicacionRuta.objects.create(
            ruta=self.ruta,
            repartidor=self.repartidor,
            unidad=self.unidad,
            latitud=cedis_adicional.latitud_geocerca,
            longitud=cedis_adicional.longitud_geocerca,
            precision_metros="8.00",
        )
        EventoRuta.objects.create(
            ruta=self.ruta,
            parada=cedis_adicional,
            ubicacion=ubicacion,
            tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            descripcion="Geocerca confiable iterator",
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
            creado_por=self.user,
        )
        iterable = self.ruta.paradas.select_related(
            "ruta",
            "punto",
            "punto__sucursal",
            "entrega_confirmada_por",
            "revision_entrega_revisada_por",
        ).order_by("orden", "id").iterator()

        with CaptureQueriesContext(connection) as queries:
            data = ParadaRutaSerializer(iterable, many=True).data

        consultas_recarga = [
            query["sql"]
            for query in queries
            if EventoRuta._meta.db_table in query["sql"]
            and EventoRuta.TIPO_RECARGA_CEDIS in query["sql"]
        ]
        self.assertEqual(len(data), self.ruta.paradas.count())
        self.assertEqual(len(consultas_recarga), 1)
        cedis_data = next(item for item in data if item["id"] == cedis_adicional.id)
        self.assertTrue(cedis_data["recarga_cedis_resuelta"])
        self.assertTrue(cedis_data["geocerca_confiable"])

    def test_serializer_cedis_inicial_resuelto_aunque_view_precargue_cache_vacio(self):
        self.parada_previa.delete()
        inicial = ParadaRuta.objects.create(
            ruta=self.ruta,
            punto=PuntoLogistico.objects.create(
                nombre="CEDIS inicial",
                tipo=PuntoLogistico.TIPO_CEDIS,
                latitud="25.555000",
                longitud="-108.455000",
                radio_geocerca_metros=120,
            ),
            orden=1,
            estado=ParadaRuta.ESTADO_VISITADA,
        )
        paradas = list(
            self.ruta.paradas.select_related("ruta", "punto", "punto__sucursal").order_by("orden", "id")
        )

        data = ParadaRutaSerializer(
            paradas,
            many=True,
            context={"_recarga_cedis_parada_ids": set()},
        ).data

        self.assertTrue(next(item for item in data if item["id"] == inicial.id)["recarga_cedis_resuelta"])
        self.assertTrue(ParadaRutaSerializer(inicial).data["recarga_cedis_resuelta"])

    def test_pwa_conserva_reintento_hasta_recarga(self):
        html = Path("logistica/templates/logistica/pwa.html").read_text(encoding="utf-8")
        render_function = re.search(
            r"function renderParadasRuta\(paradas, rutaId, rutaEnSeguimiento\) \{(?P<body>.*?)\n      \}",
            html,
            re.DOTALL,
        )

        self.assertIsNotNone(render_function)
        body = render_function.group("body")
        self.assertIn(
            "const puedeRegistrarRecarga = !requiereEntrega && rutaEnSeguimiento && parada.recarga_cedis_resuelta !== true;",
            body,
        )
        self.assertIn("Sincronización de recarga pendiente", body)
        self.assertIn("Reintentar sincronización Point", body)
        self.assertIn(
            'requiereEntrega ? entregaTone(entrega) : (recargaResuelta ? "ok" : "warn")',
            body,
        )
        self.assertIn(
            'const puedeConfirmarEntrega = requiereEntrega && rutaEnSeguimiento && entrega === "PENDIENTE" && paradaDisponibleParaEntrega(parada, paradas);',
            body,
        )
        recarga_function = re.search(
            r"async function registrarRecargaCedis\(.*?\) \{(?P<body>.*?)\n      \}",
            html,
            re.DOTALL,
        )
        self.assertIsNotNone(recarga_function)
        self.assertIn("skipOfflineQueue: true", recarga_function.group("body"))

    def test_pwa_visitada_con_entrega_pendiente_conserva_confirmar_entrega(self):
        html = Path("logistica/templates/logistica/pwa.html").read_text(encoding="utf-8")
        render_function = re.search(
            r"function renderParadasRuta\(paradas, rutaId, rutaEnSeguimiento\) \{(?P<body>.*?)\n      \}",
            html,
            re.DOTALL,
        )

        self.assertIsNotNone(render_function)
        body = render_function.group("body")
        self.assertIn(
            'const puedeConfirmarEntrega = requiereEntrega && rutaEnSeguimiento && entrega === "PENDIENTE" && paradaDisponibleParaEntrega(parada, paradas);',
            body,
        )
        self.assertNotIn(
            "const puedeConfirmarEntrega = requiereEntrega && rutaEnSeguimiento && !resolved",
            body,
        )
        self.assertIn("Confirmar entrega</button>", body)

    def test_pwa_bloquea_tramo_por_recarga_real_y_exenta_cedis_inicial(self):
        html = Path("logistica/templates/logistica/pwa.html").read_text(encoding="utf-8")
        availability_function = re.search(
            r"function paradaDisponibleParaEntrega\(parada, paradas\) \{(?P<body>.*?)\n      \}",
            html,
            re.DOTALL,
        )

        self.assertIsNotNone(availability_function)
        body = availability_function.group("body")
        self.assertIn("Number(item.orden || 0) > 1", body)
        self.assertIn("item.recarga_cedis_resuelta !== true", body)
        self.assertNotIn('item.estado !== "VISITADA"', body)

    @patch("logistica.services_rutas_control.logger.exception")
    @patch("logistica.tasks.procesar_recarga_cedis_automatica.delay")
    def test_permanencia_cedis_reintenta_broker_y_deja_de_agendar_al_resolverse(
        self,
        delay,
        log_exception,
    ):
        delay.side_effect = [RuntimeError("broker no disponible"), None, None]
        BitacoraSalidaLlegada.objects.create(
            repartidor=self.repartidor,
            unidad=self.unidad,
            km_salida=1000,
            nivel_gas_salida="lleno",
            foto_tablero_salida=SimpleUploadedFile(
                "tablero-recarga.gif",
                b"gif",
                content_type="image/gif",
            ),
        )
        payload = {
            "latitud": str(self.cedis.latitud_geocerca),
            "longitud": str(self.cedis.longitud_geocerca),
            "precision_metros": "8.00",
            "tracking_origen": "automatico_pwa",
        }
        registrar_ubicacion_ruta(user=self.user, ruta=self.ruta, payload=payload)
        EventoRuta.objects.filter(
            ruta=self.ruta,
            parada=self.cedis,
            tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            metadata__origen_servicio="registrar_ubicacion_ruta",
        ).update(creado_en=timezone.now() - timedelta(minutes=6))

        with self.captureOnCommitCallbacks(execute=True):
            segunda_ubicacion = registrar_ubicacion_ruta(user=self.user, ruta=self.ruta, payload=payload)

        delay.assert_called_once_with(
            ruta_id=self.ruta.id,
            parada_id=self.cedis.id,
            user_id=self.user.id,
            lease_token=delay.call_args.kwargs["lease_token"],
        )
        token_fallido = delay.call_args.kwargs["lease_token"]
        UUID(token_fallido)
        self.cedis.refresh_from_db()
        self.assertEqual(self.cedis.estado, ParadaRuta.ESTADO_VISITADA)
        self.assertTrue(UbicacionRuta.objects.filter(pk=segunda_ubicacion.id).exists())
        lease = EventoRuta.objects.get(
            clave_auditoria=f"recarga-auto-lease:{self.ruta.id}:{self.cedis.id}"
        )
        self.assertEqual(lease.metadata["estado"], "FALLA_ENCOLADO")
        self.assertIsNone(lease.metadata["lease_hasta"])

        with self.captureOnCommitCallbacks(execute=True):
            registrar_ubicacion_ruta(user=self.user, ruta=self.ruta, payload=payload)

        self.assertEqual(delay.call_count, 2)
        token_encolado = delay.call_args.kwargs["lease_token"]
        UUID(token_encolado)
        self.assertNotEqual(token_fallido, token_encolado)
        log_exception.assert_called_once()

        lease.refresh_from_db()
        self.assertEqual(lease.metadata["tipo"], "recarga_cedis_lease_interno")
        self.assertEqual(lease.metadata["estado"], "ENCOLADA")
        self.assertEqual(lease.severidad, EventoRuta.SEVERIDAD_INFO)
        self.assertEqual(lease.revision_alerta_estado, EventoRuta.REVISION_ALERTA_RESUELTA)
        self.assertIsNone(
            ultima_alerta_recarga_cedis_revisable(ruta=self.ruta, parada=self.cedis)
        )

        with self.captureOnCommitCallbacks(execute=True):
            registrar_ubicacion_ruta(user=self.user, ruta=self.ruta, payload=payload)
        self.assertEqual(delay.call_count, 2, "Un lease vigente no debe producir otra tarea.")

        metadata = dict(lease.metadata)
        metadata["estado"] = "EN_PROCESO"
        metadata["lease_hasta"] = (timezone.now() - timedelta(seconds=1)).isoformat()
        lease.metadata = metadata
        lease.save(update_fields=["metadata"])
        with self.captureOnCommitCallbacks(execute=True):
            registrar_ubicacion_ruta(user=self.user, ruta=self.ruta, payload=payload)
        self.assertEqual(delay.call_count, 3, "Un worker muerto debe recuperarse al vencer su lease.")

        EventoRuta.objects.create(
            ruta=self.ruta,
            parada=self.cedis,
            tipo=EventoRuta.TIPO_INCIDENCIA_MANUAL,
            descripcion="Recarga compatible registrada por cliente legado",
            metadata={"tipo": "recarga_cedis_pwa"},
        )
        with self.captureOnCommitCallbacks(execute=True):
            registrar_ubicacion_ruta(user=self.user, ruta=self.ruta, payload=payload)

        self.assertEqual(delay.call_count, 3)

    @patch("logistica.tasks.procesar_recarga_cedis_automatica.delay")
    def test_permanencia_cedis_inicial_marca_visita_sin_agendar_recarga(self, delay):
        BitacoraSalidaLlegada.objects.create(
            repartidor=self.repartidor,
            unidad=self.unidad,
            km_salida=1000,
            nivel_gas_salida="lleno",
            foto_tablero_salida=SimpleUploadedFile(
                "tablero-cedis-inicial.gif",
                b"gif",
                content_type="image/gif",
            ),
        )
        self.cedis.orden = 99
        self.cedis.save(update_fields=["orden", "actualizado_en"])
        self.parada_previa.orden = 2
        self.parada_previa.save(update_fields=["orden", "actualizado_en"])
        self.cedis.orden = 1
        self.cedis.save(update_fields=["orden", "actualizado_en"])
        payload = {
            "latitud": str(self.cedis.latitud_geocerca),
            "longitud": str(self.cedis.longitud_geocerca),
            "precision_metros": "8.00",
            "tracking_origen": "automatico_pwa",
        }
        registrar_ubicacion_ruta(user=self.user, ruta=self.ruta, payload=payload)
        EventoRuta.objects.filter(
            ruta=self.ruta,
            parada=self.cedis,
            tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            metadata__origen_servicio="registrar_ubicacion_ruta",
        ).update(creado_en=timezone.now() - timedelta(minutes=6))

        with self.captureOnCommitCallbacks(execute=True):
            registrar_ubicacion_ruta(user=self.user, ruta=self.ruta, payload=payload)

        self.cedis.refresh_from_db()
        self.assertEqual(self.cedis.estado, ParadaRuta.ESTADO_VISITADA)
        delay.assert_not_called()
        self.assertFalse(
            EventoRuta.objects.filter(
                clave_auditoria=f"recarga-auto-lease:{self.ruta.id}:{self.cedis.id}"
            ).exists()
        )

    @patch("logistica.tasks.procesar_recarga_cedis_automatica.delay")
    def test_resultado_point_aplica_backoff_durable_y_luego_reintenta(self, delay):
        from .services_rutas_control import _agendar_recarga_cedis_si_pendiente
        from .tasks import procesar_recarga_cedis_automatica

        self.cedis.estado = ParadaRuta.ESTADO_VISITADA
        self.cedis.save(update_fields=["estado", "actualizado_en"])
        with self.captureOnCommitCallbacks(execute=True):
            agendada = _agendar_recarga_cedis_si_pendiente(
                ruta=self.ruta,
                parada=self.cedis,
                user=self.user,
            )
        self.assertTrue(agendada)
        delay.assert_called_once()
        lease = EventoRuta.objects.get(
            clave_auditoria=f"recarga-auto-lease:{self.ruta.id}:{self.cedis.id}"
        )
        lease_token = lease.metadata["lease_token"]

        with patch(
            "logistica.tasks.registrar_recarga_cedis",
            side_effect=RecargaCedisPendienteEnviado("Point sigue pendiente"),
        ):
            resultado = procesar_recarga_cedis_automatica(
                ruta_id=self.ruta.id,
                parada_id=self.cedis.id,
                user_id=self.user.id,
                lease_token=lease_token,
            )
        self.assertEqual(resultado["estado_sync"], "PENDIENTE_ENVIADO")
        lease = EventoRuta.objects.get(
            clave_auditoria=f"recarga-auto-lease:{self.ruta.id}:{self.cedis.id}"
        )
        self.assertEqual(lease.metadata["estado"], "BACKOFF")
        siguiente_intento = datetime.fromisoformat(lease.metadata["proximo_intento_en"])

        with self.captureOnCommitCallbacks(execute=True):
            self.assertFalse(
                _agendar_recarga_cedis_si_pendiente(
                    ruta=self.ruta,
                    parada=self.cedis,
                    user=self.user,
                )
            )
        delay.assert_called_once()

        with patch("logistica.services_rutas_control.timezone.now", return_value=siguiente_intento + timedelta(seconds=1)):
            with self.captureOnCommitCallbacks(execute=True):
                self.assertTrue(
                    _agendar_recarga_cedis_si_pendiente(
                        ruta=self.ruta,
                        parada=self.cedis,
                        user=self.user,
                    )
                )
        self.assertEqual(delay.call_count, 2)

    def test_task_recarga_no_inventa_exito_si_point_pendiente(self):
        from .services_rutas_control import _reclamar_lease_recarga_para_encolar
        from .tasks import procesar_recarga_cedis_automatica

        errores_revisables = (
            (RecargaCedisPendienteEnviado, "PENDIENTE_ENVIADO"),
            (RecargaCedisSinLineasPoint, "SIN_LINEAS_POINT"),
            (RecargaCedisPointError, "ERROR_POINT"),
        )
        for exception_class, estado_sync in errores_revisables:
            EventoRuta.objects.filter(
                clave_auditoria=f"recarga-auto-lease:{self.ruta.id}:{self.cedis.id}"
            ).delete()
            self.cedis.estado = ParadaRuta.ESTADO_VISITADA
            self.cedis.save(update_fields=["estado", "actualizado_en"])
            lease_token = _reclamar_lease_recarga_para_encolar(
                ruta=self.ruta,
                parada=self.cedis,
                user=self.user,
            )
            with self.subTest(estado_sync=estado_sync), patch(
                "logistica.tasks.registrar_recarga_cedis",
                side_effect=exception_class("Falla revisable de Point"),
            ) as registrar:
                resultado = procesar_recarga_cedis_automatica(
                    ruta_id=self.ruta.id,
                    parada_id=self.cedis.id,
                    user_id=self.user.id,
                    lease_token=lease_token,
                )

                self.assertEqual(resultado["estado_sync"], estado_sync)
                self.assertEqual(resultado["ruta_id"], self.ruta.id)
                self.assertEqual(resultado["parada_id"], self.cedis.id)
                registrar.assert_called_once()
                lease = EventoRuta.objects.get(
                    clave_auditoria=f"recarga-auto-lease:{self.ruta.id}:{self.cedis.id}"
                )
                self.assertEqual(lease.metadata["estado"], "BACKOFF")
                self.assertEqual(lease.metadata["estado_sync"], estado_sync)
                self.assertTrue(lease.metadata["proximo_intento_en"])
                self.assertFalse(
                    EventoRuta.objects.filter(
                        ruta=self.ruta,
                        parada=self.cedis,
                        tipo=EventoRuta.TIPO_RECARGA_CEDIS,
                    ).exists()
                )

    def test_task_error_no_clasificado_sale_de_en_proceso_y_propaga(self):
        from .services_rutas_control import _reclamar_lease_recarga_para_encolar
        from .tasks import procesar_recarga_cedis_automatica

        self.cedis.estado = ParadaRuta.ESTADO_VISITADA
        self.cedis.save(update_fields=["estado", "actualizado_en"])
        lease_token = _reclamar_lease_recarga_para_encolar(
            ruta=self.ruta,
            parada=self.cedis,
            user=self.user,
        )

        with patch(
            "logistica.tasks.registrar_recarga_cedis",
            side_effect=RecargaCedisSnapshotObsoleto("Snapshot cambió"),
        ), self.assertRaises(RecargaCedisSnapshotObsoleto):
            procesar_recarga_cedis_automatica(
                ruta_id=self.ruta.id,
                parada_id=self.cedis.id,
                user_id=self.user.id,
                lease_token=lease_token,
            )

        lease = EventoRuta.objects.get(
            clave_auditoria=f"recarga-auto-lease:{self.ruta.id}:{self.cedis.id}"
        )
        self.assertEqual(lease.metadata["estado"], "BACKOFF")
        self.assertEqual(lease.metadata["estado_sync"], "SNAPSHOT_OBSOLETO")
        self.assertEqual(lease.metadata["ultimo_error"], "RecargaCedisSnapshotObsoleto")
        self.assertIsNotNone(lease.metadata["proximo_intento_en"])

    def test_task_error_token_viejo_no_pisa_nueva_generacion(self):
        from .services_rutas_control import _reclamar_lease_recarga_para_encolar
        from .tasks import procesar_recarga_cedis_automatica

        self.cedis.estado = ParadaRuta.ESTADO_VISITADA
        self.cedis.save(update_fields=["estado", "actualizado_en"])
        token_viejo = _reclamar_lease_recarga_para_encolar(
            ruta=self.ruta,
            parada=self.cedis,
            user=self.user,
        )
        tokens_nuevos = []

        def reemplazar_generacion_y_fallar(**kwargs):
            lease = EventoRuta.objects.get(
                clave_auditoria=f"recarga-auto-lease:{self.ruta.id}:{self.cedis.id}"
            )
            metadata = dict(lease.metadata)
            metadata["lease_hasta"] = (timezone.now() - timedelta(seconds=1)).isoformat()
            lease.metadata = metadata
            lease.save(update_fields=["metadata"])
            tokens_nuevos.append(
                _reclamar_lease_recarga_para_encolar(
                    ruta=self.ruta,
                    parada=self.cedis,
                    user=self.user,
                )
            )
            raise ValidationError("Falla posterior de generación vieja")

        with patch(
            "logistica.tasks.registrar_recarga_cedis",
            side_effect=reemplazar_generacion_y_fallar,
        ), self.assertRaises(ValidationError):
            procesar_recarga_cedis_automatica(
                ruta_id=self.ruta.id,
                parada_id=self.cedis.id,
                user_id=self.user.id,
                lease_token=token_viejo,
            )

        lease = EventoRuta.objects.get(
            clave_auditoria=f"recarga-auto-lease:{self.ruta.id}:{self.cedis.id}"
        )
        self.assertNotEqual(token_viejo, tokens_nuevos[0])
        self.assertEqual(lease.metadata["lease_token"], tokens_nuevos[0])
        self.assertEqual(lease.metadata["estado"], "ENCOLADA")

    @patch("logistica.tasks.registrar_recarga_cedis")
    def test_task_recarga_exitosa_reporta_evento_reconciliado(self, registrar):
        from .services_rutas_control import _reclamar_lease_recarga_para_encolar
        from .tasks import procesar_recarga_cedis_automatica

        registrar.return_value = SimpleNamespace(
            id=812,
            metadata={"estado_sync": "ACTUALIZADO"},
        )
        self.cedis.estado = ParadaRuta.ESTADO_VISITADA
        self.cedis.save(update_fields=["estado", "actualizado_en"])
        lease_token = _reclamar_lease_recarga_para_encolar(
            ruta=self.ruta,
            parada=self.cedis,
            user=self.user,
        )

        resultado = procesar_recarga_cedis_automatica(
            ruta_id=self.ruta.id,
            parada_id=self.cedis.id,
            user_id=self.user.id,
            lease_token=lease_token,
        )

        self.assertEqual(
            resultado,
            {"estado_sync": "ACTUALIZADO", "evento_id": 812},
        )
        registrar.assert_called_once()

    @patch("logistica.tasks.registrar_recarga_cedis")
    def test_task_con_token_viejo_se_descarta_sin_tocar_point(self, registrar):
        from .services_rutas_control import _reclamar_lease_recarga_para_encolar
        from .tasks import procesar_recarga_cedis_automatica

        self.cedis.estado = ParadaRuta.ESTADO_VISITADA
        self.cedis.save(update_fields=["estado", "actualizado_en"])
        token_viejo = _reclamar_lease_recarga_para_encolar(
            ruta=self.ruta,
            parada=self.cedis,
            user=self.user,
        )
        lease = EventoRuta.objects.get(
            clave_auditoria=f"recarga-auto-lease:{self.ruta.id}:{self.cedis.id}"
        )
        metadata = dict(lease.metadata)
        metadata["lease_hasta"] = (timezone.now() - timedelta(seconds=1)).isoformat()
        lease.metadata = metadata
        lease.save(update_fields=["metadata"])
        token_nuevo = _reclamar_lease_recarga_para_encolar(
            ruta=self.ruta,
            parada=self.cedis,
            user=self.user,
        )

        resultado = procesar_recarga_cedis_automatica(
            ruta_id=self.ruta.id,
            parada_id=self.cedis.id,
            user_id=self.user.id,
            lease_token=token_viejo,
        )

        self.assertEqual(resultado["estado_sync"], "OBSOLETA")
        registrar.assert_not_called()
        lease.refresh_from_db()
        self.assertEqual(lease.metadata["lease_token"], token_nuevo)

    @patch("logistica.tasks.registrar_recarga_cedis")
    def test_task_sin_token_no_reclama_lease_ni_toca_point(self, registrar):
        from .tasks import procesar_recarga_cedis_automatica

        resultado = procesar_recarga_cedis_automatica(
            ruta_id=self.ruta.id,
            parada_id=self.cedis.id,
            user_id=self.user.id,
        )

        self.assertEqual(resultado["estado_sync"], "OBSOLETA")
        registrar.assert_not_called()
        self.assertFalse(
            EventoRuta.objects.filter(
                clave_auditoria=f"recarga-auto-lease:{self.ruta.id}:{self.cedis.id}"
            ).exists()
        )

    def test_selector_alerta_interpreta_zona_horaria_en_vez_de_ordenar_json(self):
        cronologicamente_nueva = self._evento_alerta_manual(
            clave="alerta-offset-a",
            snapshot_hash="hash-offset-a",
            ultima_observacion_en="2026-07-14T10:00:00-07:00",
        )
        self._evento_alerta_manual(
            clave="alerta-offset-b",
            snapshot_hash="hash-offset-b",
            ultima_observacion_en="2026-07-14T16:30:00+00:00",
        )

        seleccionada = ultima_alerta_recarga_cedis_revisable(
            ruta=self.ruta,
            parada=self.cedis,
        )

        self.assertEqual(seleccionada.id, cronologicamente_nueva.id)

    def test_selector_alerta_historica_sin_marca_usa_creado_en(self):
        antigua = self._evento_alerta_manual(
            clave="alerta-legada-a",
            snapshot_hash="hash-legado-a",
        )
        nueva = self._evento_alerta_manual(
            clave="alerta-legada-b",
            snapshot_hash="hash-legado-b",
        )
        ahora = timezone.now()
        EventoRuta.objects.filter(pk=antigua.pk).update(creado_en=ahora - timedelta(minutes=2))
        EventoRuta.objects.filter(pk=nueva.pk).update(creado_en=ahora - timedelta(minutes=1))

        seleccionada = ultima_alerta_recarga_cedis_revisable(
            ruta=self.ruta,
            parada=self.cedis,
        )

        self.assertEqual(seleccionada.id, nueva.id)

    @patch("logistica.services_carga_ruta.OpenTransferSyncService.sync_open_transfers")
    def test_sync_job_fallido_lanza_excepcion_point_especifica(self, sync_open):
        failed_job = PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_TRANSFERS,
            status=PointSyncJob.STATUS_FAILED,
            triggered_by=self.user,
        )
        sync_open.return_value = failed_job

        with self.assertRaises(PointSyncUnavailableError) as captured:
            sincronizar_checklist_carga_desde_point(
                ruta=self.ruta,
                user=self.user,
                ejecutar_sync=True,
            )

        self.assertEqual(captured.exception.sync_job, failed_job)

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_error_inesperado_no_es_autorizable_ni_marca_cedis(self, sync):
        reviewed_hash = self._crear_alerta_revisable()
        sync.side_effect = RuntimeError("fallo de programación")

        with self.assertRaises(RuntimeError):
            registrar_recarga_cedis(
                ruta=self.ruta,
                user=self.manager,
                parada=self.cedis,
                notas="No debe aplicar",
                autorizar_sin_sync=True,
                motivo_autorizacion="No autoriza errores internos",
                expected_snapshot_hash=reviewed_hash,
            )

        self.cedis.refresh_from_db()
        self.assertEqual(self.cedis.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertFalse(
            EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_RECARGA_CEDIS).exists()
        )

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_recarga_acepta_cedis_visitada(self, sync):
        enviada = self.point_line(
            requested="2",
            sent="2",
            sent_at=timezone.now(),
            is_enviado=True,
        )
        self.sync_line(enviada)
        sync.side_effect = lambda **kwargs: self._sync_summary()
        self.cedis.estado = ParadaRuta.ESTADO_VISITADA
        self.cedis.save(update_fields=["estado", "actualizado_en"])

        evento = registrar_recarga_cedis(
            ruta=self.ruta,
            user=self.user,
            parada=self.cedis,
            notas="Recarga después de permanencia",
            autorizar_sin_sync=False,
            motivo_autorizacion="",
        )

        self.assertEqual(evento.tipo, EventoRuta.TIPO_RECARGA_CEDIS)
        self.assertEqual(evento.parada_id, self.cedis.id)
        self.assertEqual(
            EventoRuta.objects.filter(
                ruta=self.ruta,
                parada=self.cedis,
                tipo=EventoRuta.TIPO_RECARGA_CEDIS,
            ).count(),
            1,
        )
        sync.assert_called_once_with(ruta=self.ruta, user=self.user)

    def test_visita_cedis_no_abre_tramo_sin_recarga(self):
        self.cedis.estado = ParadaRuta.ESTADO_VISITADA
        self.cedis.save(update_fields=["estado", "actualizado_en"])

        self.assertNotIn(
            self.parada.orden,
            _ordenes_tramo_carga_actual(self.ruta),
        )

        EventoRuta.objects.create(
            ruta=self.ruta,
            parada=self.cedis,
            tipo=EventoRuta.TIPO_RECARGA_CEDIS,
            descripcion="Recarga reconciliada",
        )

        self.assertIn(
            self.parada.orden,
            _ordenes_tramo_carga_actual(self.ruta),
        )

    def test_geocerca_cedis_no_abre_tramo_sin_recarga(self):
        EventoRuta.objects.create(
            ruta=self.ruta,
            parada=self.cedis,
            tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            descripcion="Llegada detectada",
        )

        self.assertNotIn(
            self.parada.orden,
            _ordenes_tramo_carga_actual(self.ruta),
        )

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_parada_cedis_no_siguiente_se_rechaza_antes_de_sincronizar(self, sync):
        otra_cedis = ParadaRuta.objects.create(
            ruta=self.ruta,
            punto=self.cedis_punto,
            orden=4,
        )

        with self.assertRaises(ValidationError):
            registrar_recarga_cedis(
                ruta=self.ruta,
                user=self.user,
                parada=otra_cedis,
                notas="Inválida",
                autorizar_sin_sync=False,
                motivo_autorizacion="",
            )

        sync.assert_not_called()

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_recarga_sincroniza_antes_de_marcar_visita(self, sync):
        enviada = self.point_line(
            requested="2",
            sent="2",
            sent_at=timezone.now(),
            is_enviado=True,
        )
        self.sync_line(enviada)

        def assert_cedis_pending(**kwargs):
            self.assertEqual(ParadaRuta.objects.get(pk=self.cedis.pk).estado, ParadaRuta.ESTADO_PENDIENTE)
            return self._sync_summary()

        sync.side_effect = assert_cedis_pending

        response = self._post()

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(response.json()["estado_sync"], "ACTUALIZADO")
        sync.assert_called_once_with(ruta=self.ruta, user=self.user)

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_tramo_con_sucursal_planeada_sin_lineas_point_bloquea_y_alerta(self, sync):
        RutaCargaChecklist.objects.create(ruta=self.ruta)
        sync.side_effect = lambda **kwargs: self._sync_summary()

        response = self._post()

        self.assertEqual(response.status_code, 409, response.content)
        self.assertEqual(response.json()["estado_sync"], "SIN_LINEAS_POINT")
        self.cedis.refresh_from_db()
        self.assertEqual(self.cedis.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertTrue(
            EventoRuta.objects.filter(
                ruta=self.ruta,
                parada=self.cedis,
                metadata__estado_sync="SIN_LINEAS_POINT",
            ).exists()
        )

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_jefe_puede_autorizar_tramo_planeado_sin_lineas_con_motivo(self, sync):
        RutaCargaChecklist.objects.create(ruta=self.ruta)
        sync.side_effect = lambda **kwargs: self._sync_summary()
        bloqueo = self._post()
        reviewed_hash = bloqueo.json()["snapshot_hash"]

        response = self._post(
            user=self.manager,
            autorizar_sin_sync=True,
            motivo_autorizacion="Se verificó físicamente que el tramo no lleva carga",
            expected_snapshot_hash=reviewed_hash,
        )

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(response.json()["estado_sync"], "AUTORIZADO")

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_tramo_realmente_sin_sucursales_no_exige_lineas_point(self, sync):
        self.parada.delete()
        RutaCargaChecklist.objects.create(ruta=self.ruta)
        sync.side_effect = lambda **kwargs: self._sync_summary()

        response = self._post()

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(response.json()["estado_sync"], "ACTUALIZADO")

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_recarga_fallida_no_desbloquea_y_notifica_una_vez(self, sync):
        sync.side_effect = PointSyncUnavailableError("Point no respondió")

        first = self._post()
        second = self._post()

        self.assertEqual((first.status_code, second.status_code), (503, 503))
        self.assertEqual(first.json()["estado_sync"], "ERROR_POINT")
        self.cedis.refresh_from_db()
        self.assertEqual(self.cedis.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertEqual(EventoRuta.objects.filter(ruta=self.ruta, parada=self.cedis, metadata__estado_sync="ERROR_POINT").count(), 1)
        self.assertEqual(Notificacion.objects.filter(usuario=self.manager, objeto_tipo="logistica.EventoRuta").count(), 1)

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_error_point_snapshot_lista_sucursal_planeada_sin_lineas_cacheadas(self, sync):
        sync.side_effect = PointSyncUnavailableError("Point no respondió")

        response = self._post()

        self.assertEqual(response.status_code, 503, response.content)
        alerta = EventoRuta.objects.get(
            ruta=self.ruta,
            parada=self.cedis,
            metadata__estado_sync="ERROR_POINT",
        )
        self.assertEqual(alerta.metadata["snapshot"]["lineas"], [])
        self.assertEqual(
            alerta.metadata["snapshot"]["sucursales"],
            [{"id": self.sucursal.id, "nombre": self.sucursal.nombre}],
        )

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_error_point_sin_snapshot_externo_previo_no_es_autorizable(self, sync):
        sync.side_effect = PointSyncUnavailableError("Point no respondió")
        bloqueo = self._post()
        reviewed_hash = bloqueo.json()["snapshot_hash"]

        response = self._post(
            user=self.manager,
            autorizar_sin_sync=True,
            motivo_autorizacion="Sin evidencia externa previa",
            expected_snapshot_hash=reviewed_hash,
        )

        self.assertEqual(response.status_code, 503, response.content)
        self.assertEqual(response.json()["estado_sync"], "ERROR_POINT")
        self.cedis.refresh_from_db()
        self.assertEqual(self.cedis.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertFalse(
            EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_RECARGA_CEDIS).exists()
        )

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_retry_alerta_repara_notificacion_faltante_sin_duplicar(self, sync):
        sync.side_effect = PointSyncUnavailableError("Point no respondió")

        first = self._post()
        alerta = EventoRuta.objects.get(
            ruta=self.ruta,
            parada=self.cedis,
            metadata__estado_sync="ERROR_POINT",
        )
        Notificacion.objects.filter(
            usuario=self.manager,
            objeto_tipo="logistica.EventoRuta",
            objeto_id=str(alerta.id),
        ).delete()
        retry = self._post()

        self.assertEqual((first.status_code, retry.status_code), (503, 503))
        self.assertEqual(
            Notificacion.objects.filter(
                usuario=self.manager,
                objeto_tipo="logistica.EventoRuta",
                objeto_id=str(alerta.id),
            ).count(),
            1,
        )

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_solicitud_sin_enviado_solicita_autorizacion_y_alerta_una_vez(self, sync):
        self._pending_next_segment()
        sync.side_effect = lambda **kwargs: self._sync_summary()

        first = self._post()
        alerta = EventoRuta.objects.get(
            ruta=self.ruta,
            parada=self.cedis,
            metadata__estado_sync="PENDIENTE_ENVIADO",
        )
        creado_en_original = alerta.creado_en
        observacion_original = alerta.metadata["ultima_observacion_en"]
        second = self._post()

        self.assertEqual((first.status_code, second.status_code), (409, 409))
        self.assertEqual(first.json()["estado_sync"], "PENDIENTE_ENVIADO")
        alerta.refresh_from_db()
        self.assertEqual(alerta.creado_en, creado_en_original)
        self.assertGreater(
            datetime.fromisoformat(alerta.metadata["ultima_observacion_en"]),
            datetime.fromisoformat(observacion_original),
        )
        self.assertEqual(EventoRuta.objects.filter(ruta=self.ruta, parada=self.cedis, metadata__estado_sync="PENDIENTE_ENVIADO").count(), 1)
        self.assertEqual(Notificacion.objects.filter(usuario=self.manager, objeto_tipo="logistica.EventoRuta").count(), 1)

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_jefe_autoriza_snapshot_con_motivo_sin_convertir_cero(self, sync):
        pending = self._pending_next_segment()
        sync.side_effect = lambda **kwargs: self._sync_summary()
        bloqueo = self._post()
        reviewed_hash = bloqueo.json()["snapshot_hash"]

        response = self._post(
            user=self.manager,
            autorizar_sin_sync=True,
            motivo_autorizacion="Point pendiente; conteo físico revisado",
            expected_snapshot_hash=reviewed_hash,
        )

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(response.json()["estado_sync"], "AUTORIZADO")
        pending.refresh_from_db()
        self.assertEqual(pending.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
        evento = EventoRuta.objects.get(ruta=self.ruta, parada=self.cedis, tipo=EventoRuta.TIPO_RECARGA_CEDIS)
        self.assertEqual(evento.metadata["estado_sync"], "AUTORIZADO")
        self.assertEqual(evento.metadata["autorizacion"]["actor_id"], self.manager.id)
        self.assertEqual(evento.metadata["autorizacion"]["motivo"], "Point pendiente; conteo físico revisado")

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_jefe_autoriza_ultimo_snapshot_cacheado_cuando_sync_falla(self, sync):
        pending = self._pending_next_segment()
        self._mark_external_snapshot_valid()
        sync.side_effect = PointSyncUnavailableError("Point no respondió")
        bloqueo = self._post()
        reviewed_hash = bloqueo.json()["snapshot_hash"]

        response = self._post(
            user=self.manager,
            autorizar_sin_sync=True,
            motivo_autorizacion="Snapshot cacheado y carga física verificados",
            expected_snapshot_hash=reviewed_hash,
        )

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(response.json()["estado_sync"], "AUTORIZADO")
        pending.refresh_from_db()
        self.assertEqual(pending.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
        recarga = EventoRuta.objects.get(ruta=self.ruta, parada=self.cedis, tipo=EventoRuta.TIPO_RECARGA_CEDIS)
        alerta = EventoRuta.objects.get(ruta=self.ruta, parada=self.cedis, metadata__estado_sync="ERROR_POINT")
        self.assertEqual(recarga.metadata["snapshot"]["snapshot_hash"], alerta.metadata["snapshot"]["snapshot_hash"])
        self.assertEqual(recarga.metadata["autorizacion"]["snapshot_hash"], alerta.metadata["snapshot"]["snapshot_hash"])
        self.assertEqual(recarga.metadata["autorizacion"]["parada_id"], self.cedis.id)
        self.assertEqual(recarga.metadata["snapshot"]["sync_error"], "Point no respondió")

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_alerta_snapshot_identifica_tramo_hora_y_actor(self, sync):
        self._pending_next_segment()
        checklist = self._mark_external_snapshot_valid()
        sync.side_effect = lambda **kwargs: self._sync_summary()

        response = self._post()

        self.assertEqual(response.status_code, 409)
        alerta = EventoRuta.objects.get(ruta=self.ruta, parada=self.cedis, metadata__estado_sync="PENDIENTE_ENVIADO")
        snapshot = alerta.metadata["snapshot"]
        self.assertEqual(snapshot["ruta_id"], self.ruta.id)
        self.assertEqual(snapshot["parada_id"], self.cedis.id)
        self.assertEqual(snapshot["actor_id"], self.user.id)
        self.assertTrue(snapshot["capturado_en"])
        self.assertEqual(snapshot["checklist_sincronizado_en"], checklist.sincronizado_en.isoformat())
        self.assertEqual(snapshot["point_sync_job_id"], checklist.point_sync_job_id)
        self.assertEqual(snapshot["point_sync_job_status"], PointSyncJob.STATUS_SUCCESS)
        self.assertEqual(snapshot["sucursales"], [{"id": self.sucursal.id, "nombre": self.sucursal.nombre}])
        self.assertEqual(alerta.metadata["actor_id"], self.user.id)
        self.assertEqual(alerta.metadata["capturado_en"], snapshot["capturado_en"])

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_cambio_entre_snapshot_y_confirmacion_rechaza_como_obsoleto(self, sync):
        pending = self._pending_next_segment()
        sync.side_effect = lambda **kwargs: self._sync_summary()
        reviewed_hash = self._crear_alerta_revisable()
        from . import services_carga_ruta

        snapshot_original = services_carga_ruta._snapshot_siguiente_tramo
        mutado = False

        def snapshot_con_cambio(*args, **kwargs):
            nonlocal mutado
            snapshot = snapshot_original(*args, **kwargs)
            if not mutado:
                mutado = True
                RutaCargaChecklistLinea.objects.filter(pk=pending.pk).update(
                    cantidad_enviada_esperada=Decimal("4")
                )
            return snapshot

        with patch(
            "logistica.services_carga_ruta._snapshot_siguiente_tramo",
            side_effect=snapshot_con_cambio,
        ):
            response = self._post(
                user=self.manager,
                autorizar_sin_sync=True,
                motivo_autorizacion="Snapshot revisado antes del cambio",
                expected_snapshot_hash=reviewed_hash,
            )

        self.assertEqual(response.status_code, 409, response.content)
        self.assertEqual(response.json()["estado_sync"], "SNAPSHOT_OBSOLETO")
        self.cedis.refresh_from_db()
        self.assertEqual(self.cedis.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertFalse(
            EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_RECARGA_CEDIS).exists()
        )

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_repartidor_no_puede_autorizar_snapshot(self, sync):
        self._pending_next_segment()
        sync.side_effect = lambda **kwargs: self._sync_summary()

        response = self._post(autorizar_sin_sync=True, motivo_autorizacion="Yo lo revisé")

        self.assertEqual(response.status_code, 403)
        self.cedis.refresh_from_db()
        self.assertEqual(self.cedis.estado, ParadaRuta.ESTADO_PENDIENTE)

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_retry_autorizado_no_duplica_evento_y_autorizacion_es_por_parada(self, sync):
        self._pending_next_segment()
        sync.side_effect = lambda **kwargs: self._sync_summary()
        bloqueo = self._post()
        payload = {
            "autorizar_sin_sync": True,
            "motivo_autorizacion": "Snapshot verificado por jefatura",
            "expected_snapshot_hash": bloqueo.json()["snapshot_hash"],
        }

        first = self._post(user=self.manager, **payload)
        retry = self._post(user=self.manager, **payload)

        self.assertEqual((first.status_code, retry.status_code), (200, 200))
        self.assertEqual(EventoRuta.objects.filter(ruta=self.ruta, parada=self.cedis, tipo=EventoRuta.TIPO_RECARGA_CEDIS).count(), 1)
        self.assertEqual(sync.call_count, 2)

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_web_en_ruta_envia_parada_cedis_explicita_y_rechaza_omision(self, sync):
        RutaCargaChecklist.objects.create(ruta=self.ruta)
        sync.side_effect = lambda **kwargs: self._sync_summary()
        detail_url = reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id})
        self.client.force_login(self.manager)

        detail = self.client.get(detail_url, secure=True)
        missing = self.client.post(
            detail_url,
            {"action": "registrar_recarga_cedis", "notas_recarga_cedis": "Sin parada"},
        )

        self.assertContains(detail, f'name="parada_cedis_id" value="{self.cedis.id}"')
        self.assertEqual(missing.status_code, 302)
        self.cedis.refresh_from_db()
        self.assertEqual(self.cedis.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertFalse(EventoRuta.objects.filter(ruta=self.ruta, tipo=EventoRuta.TIPO_RECARGA_CEDIS).exists())
        sync.assert_not_called()

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_web_error_point_conserva_contexto_y_mensaje_especifico(self, sync):
        sync.side_effect = PointSyncUnavailableError("Point no respondió")
        detail_url = reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id})
        self.client.force_login(self.manager)

        response = self.client.post(
            detail_url,
            {
                "action": "registrar_recarga_cedis",
                "parada_cedis_id": self.cedis.id,
            },
            follow=True,
        )

        self.assertEqual(response.redirect_chain[-1][0], detail_url)
        self.assertContains(response, "No fue posible consultar Point")

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_web_pendiente_enviado_conserva_contexto_y_mensaje_especifico(self, sync):
        self._pending_next_segment()
        sync.side_effect = lambda **kwargs: self._sync_summary()
        detail_url = reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id})
        self.client.force_login(self.manager)

        response = self.client.post(
            detail_url,
            {
                "action": "registrar_recarga_cedis",
                "parada_cedis_id": self.cedis.id,
            },
            follow=True,
        )

        self.assertEqual(response.redirect_chain[-1][0], detail_url)
        self.assertContains(response, "Point todavía no confirma Enviado")

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_web_muestra_autorizacion_motivada_solo_a_jefatura_y_la_aplica(self, sync):
        self._pending_next_segment()
        sync.side_effect = lambda **kwargs: self._sync_summary()
        detail_url = reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id})

        self.client.force_login(self.manager)
        bloqueo = self.client.post(
            detail_url,
            {"action": "registrar_recarga_cedis", "parada_cedis_id": self.cedis.id},
            secure=True,
        )
        self.assertEqual(bloqueo.status_code, 302)
        alerta = EventoRuta.objects.get(
            ruta=self.ruta,
            parada=self.cedis,
            metadata__estado_sync="PENDIENTE_ENVIADO",
        )
        reviewed_hash = alerta.metadata["snapshot"]["snapshot_hash"]
        detail = self.client.get(detail_url, secure=True)
        self.assertContains(detail, 'name="autorizar_sin_sync" value="1"')
        self.assertContains(detail, 'name="motivo_autorizacion"')
        self.assertContains(
            detail,
            f'name="expected_snapshot_hash" value="{reviewed_hash}"',
        )

        authorized = self.client.post(
            detail_url,
            {
                "action": "registrar_recarga_cedis",
                "parada_cedis_id": self.cedis.id,
                "autorizar_sin_sync": "1",
                "motivo_autorizacion": "Carga física y ausencia de líneas verificadas",
                "expected_snapshot_hash": reviewed_hash,
            },
            secure=True,
        )
        self.assertEqual(authorized.status_code, 302)
        self.cedis.refresh_from_db()
        self.assertEqual(self.cedis.estado, ParadaRuta.ESTADO_VISITADA)

        self.cedis.estado = ParadaRuta.ESTADO_PENDIENTE
        self.cedis.save(update_fields=["estado", "actualizado_en"])
        EventoRuta.objects.filter(ruta=self.ruta, parada=self.cedis).delete()
        self.client.force_login(self.user)
        conductor = self.client.get(detail_url, secure=True)
        self.assertEqual(conductor.status_code, 302)
        self.assertNotIn("autorizar_sin_sync", conductor.content.decode("utf-8"))

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_web_autorizacion_rechaza_si_point_cambia_desde_snapshot_revisado(self, sync):
        pending = self._pending_next_segment()
        sync.side_effect = lambda **kwargs: self._sync_summary()
        detail_url = reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id})
        self.client.force_login(self.manager)

        self.client.post(
            detail_url,
            {"action": "registrar_recarga_cedis", "parada_cedis_id": self.cedis.id},
            secure=True,
        )
        alerta = EventoRuta.objects.get(
            ruta=self.ruta,
            parada=self.cedis,
            metadata__estado_sync="PENDIENTE_ENVIADO",
        )
        reviewed_hash = alerta.metadata["snapshot"]["snapshot_hash"]

        def sync_con_cambio_point(**kwargs):
            RutaCargaChecklistLinea.objects.filter(pk=pending.pk).update(
                cantidad_enviada_esperada=Decimal("4")
            )
            return self._sync_summary()

        sync.side_effect = sync_con_cambio_point
        response = self.client.post(
            detail_url,
            {
                "action": "registrar_recarga_cedis",
                "parada_cedis_id": self.cedis.id,
                "autorizar_sin_sync": "1",
                "motivo_autorizacion": "Snapshot que ya quedó viejo",
                "expected_snapshot_hash": reviewed_hash,
            },
            follow=True,
            secure=True,
        )

        self.assertContains(response, "El snapshot quedó obsoleto")
        self.cedis.refresh_from_db()
        self.assertEqual(self.cedis.estado, ParadaRuta.ESTADO_PENDIENTE)

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_web_autorizacion_acepta_resync_sin_cambio_de_contenido(self, sync):
        self._pending_next_segment()
        sync.side_effect = lambda **kwargs: self._sync_summary()
        detail_url = reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id})
        self.client.force_login(self.manager)

        self.client.post(
            detail_url,
            {"action": "registrar_recarga_cedis", "parada_cedis_id": self.cedis.id},
            secure=True,
        )
        alerta = EventoRuta.objects.get(
            ruta=self.ruta,
            parada=self.cedis,
            metadata__estado_sync="PENDIENTE_ENVIADO",
        )
        reviewed_hash = alerta.metadata["snapshot"]["snapshot_hash"]

        response = self.client.post(
            detail_url,
            {
                "action": "registrar_recarga_cedis",
                "parada_cedis_id": self.cedis.id,
                "autorizar_sin_sync": "1",
                "motivo_autorizacion": "Contenido Point verificado sin cambios",
                "expected_snapshot_hash": reviewed_hash,
            },
            secure=True,
        )

        self.assertEqual(response.status_code, 302)
        self.cedis.refresh_from_db()
        self.assertEqual(self.cedis.estado, ParadaRuta.ESTADO_VISITADA)

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_api_no_reutiliza_alerta_anterior_a_la_ultima_bloqueante(self, sync):
        pending = self._pending_next_segment()
        sync.side_effect = lambda **kwargs: self._sync_summary()
        primera = self._post()
        hash_anterior = primera.json()["snapshot_hash"]

        pending.cantidad_enviada_esperada = Decimal("4")
        pending.save(update_fields=["cantidad_enviada_esperada", "actualizado_en"])
        segunda = self._post()
        self.assertNotEqual(segunda.json()["snapshot_hash"], hash_anterior)

        pending.cantidad_enviada_esperada = Decimal("0")
        pending.save(update_fields=["cantidad_enviada_esperada", "actualizado_en"])
        response = self._post(
            user=self.manager,
            autorizar_sin_sync=True,
            motivo_autorizacion="No debe reutilizar la primera alerta",
            expected_snapshot_hash=hash_anterior,
        )

        self.assertEqual(response.status_code, 400, response.content)
        self.cedis.refresh_from_db()
        self.assertEqual(self.cedis.estado, ParadaRuta.ESTADO_PENDIENTE)

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_api_reobservacion_a_b_a_renueva_a_y_solo_autoriza_a(self, sync):
        pending = self._pending_next_segment()
        sync.side_effect = lambda **kwargs: self._sync_summary()

        primera_a = self._post()
        hash_a = primera_a.json()["snapshot_hash"]
        evento_a = EventoRuta.objects.get(
            ruta=self.ruta,
            parada=self.cedis,
            metadata__estado_sync="PENDIENTE_ENVIADO",
            metadata__snapshot__snapshot_hash=hash_a,
        )
        creado_a = evento_a.creado_en
        observacion_a_inicial = evento_a.metadata["ultima_observacion_en"]

        pending.cantidad_enviada_esperada = Decimal("4")
        pending.save(update_fields=["cantidad_enviada_esperada", "actualizado_en"])
        bloqueo_b = self._post()
        hash_b = bloqueo_b.json()["snapshot_hash"]
        self.assertNotEqual(hash_b, hash_a)

        pending.cantidad_enviada_esperada = Decimal("0")
        pending.save(update_fields=["cantidad_enviada_esperada", "actualizado_en"])
        segunda_a = self._post()
        self.assertEqual(segunda_a.json()["snapshot_hash"], hash_a)

        evento_a.refresh_from_db()
        self.assertEqual(evento_a.creado_en, creado_a)
        self.assertIn("ultima_observacion_en", evento_a.metadata)
        self.assertGreater(
            datetime.fromisoformat(evento_a.metadata["ultima_observacion_en"]),
            datetime.fromisoformat(observacion_a_inicial),
        )
        self.assertEqual(
            EventoRuta.objects.filter(
                ruta=self.ruta,
                parada=self.cedis,
                metadata__estado_sync="PENDIENTE_ENVIADO",
            ).count(),
            2,
        )
        self.assertEqual(
            Notificacion.objects.filter(
                usuario=self.manager,
                objeto_tipo="logistica.EventoRuta",
            ).count(),
            2,
        )

        detail_url = reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id})
        self.client.force_login(self.manager)
        detail = self.client.get(detail_url, secure=True)
        self.assertContains(detail, f'name="expected_snapshot_hash" value="{hash_a}"')
        self.assertNotContains(detail, f'name="expected_snapshot_hash" value="{hash_b}"')

        b_viejo = self._post(
            user=self.manager,
            autorizar_sin_sync=True,
            motivo_autorizacion="No debe autorizar el estado B anterior",
            expected_snapshot_hash=hash_b,
        )
        self.assertEqual(b_viejo.status_code, 400, b_viejo.content)

        a_vigente = self._post(
            user=self.manager,
            autorizar_sin_sync=True,
            motivo_autorizacion="Se revisó nuevamente el estado A vigente",
            expected_snapshot_hash=hash_a,
        )
        self.assertEqual(a_vigente.status_code, 200, a_vigente.content)
        self.cedis.refresh_from_db()
        self.assertEqual(self.cedis.estado, ParadaRuta.ESTADO_VISITADA)

    @patch("logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point")
    def test_web_snapshot_obsoleto_conserva_contexto_y_mensaje_especifico(self, sync):
        pending = self._pending_next_segment()
        sync.side_effect = lambda **kwargs: self._sync_summary()
        detail_url = reverse("logistica:ruta_detail", kwargs={"pk": self.ruta.id})
        self.client.force_login(self.manager)
        from . import services_carga_ruta

        self.client.post(
            detail_url,
            {"action": "registrar_recarga_cedis", "parada_cedis_id": self.cedis.id},
            secure=True,
        )
        alerta = EventoRuta.objects.get(
            ruta=self.ruta,
            parada=self.cedis,
            metadata__estado_sync="PENDIENTE_ENVIADO",
        )
        reviewed_hash = alerta.metadata["snapshot"]["snapshot_hash"]

        detail = self.client.get(detail_url, secure=True)
        self.assertContains(detail, 'name="autorizar_sin_sync" value="1"')
        self.assertContains(detail, 'name="motivo_autorizacion"')

        snapshot_original = services_carga_ruta._snapshot_siguiente_tramo
        mutado = False

        def snapshot_con_cambio(*args, **kwargs):
            nonlocal mutado
            snapshot = snapshot_original(*args, **kwargs)
            if not mutado:
                mutado = True
                RutaCargaChecklistLinea.objects.filter(pk=pending.pk).update(
                    cantidad_enviada_esperada=Decimal("4")
                )
            return snapshot

        with patch(
            "logistica.services_carga_ruta._snapshot_siguiente_tramo",
            side_effect=snapshot_con_cambio,
        ):
            response = self.client.post(
                detail_url,
                {
                    "action": "registrar_recarga_cedis",
                    "parada_cedis_id": self.cedis.id,
                    "autorizar_sin_sync": "1",
                    "motivo_autorizacion": "Snapshot revisado",
                    "expected_snapshot_hash": reviewed_hash,
                },
                follow=True,
            )

        self.assertEqual(response.redirect_chain[-1][0], detail_url)
        self.assertContains(response, "El snapshot quedó obsoleto")


class RutaJourneyInvariantTests(LogisticaInvariantFixtures):
    """Recorrido realista por tramos sin depender de nombres de sucursales de producción."""

    def setUp(self):
        super().setUp()
        self.user.groups.add(Group.objects.get_or_create(name="repartidor")[0])
        self.turno = BitacoraSalidaLlegada.objects.create(
            repartidor=self.repartidor,
            unidad=self.unidad,
            km_salida=1000,
            nivel_gas_salida="lleno",
            foto_tablero_salida=SimpleUploadedFile(
                "tablero-journey.gif",
                b"gif",
                content_type="image/gif",
            ),
        )
        self.parada.delete()

        self.cedis_point = PuntoLogistico.objects.create(
            nombre="Centro de distribución prueba journey",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.560000",
            longitud="-108.460000",
            radio_geocerca_metros=120,
        )
        self.cedis_salida = ParadaRuta.objects.create(
            ruta=self.ruta,
            punto=self.cedis_point,
            orden=1,
            estado=ParadaRuta.ESTADO_VISITADA,
        )
        self.branch_stops = []
        for index in range(3):
            branch = Sucursal.objects.create(
                codigo=f"JOURNEY-{index + 1}",
                nombre=f"Sucursal prueba tramo {index + 1}",
                activa=True,
            )
            point = PuntoLogistico.objects.create(
                sucursal=branch,
                nombre=branch.nombre,
                tipo=PuntoLogistico.TIPO_SUCURSAL,
                latitud=f"25.{570000 + index * 1000:06d}",
                longitud=f"-108.{470000 + index * 1000:06d}",
                radio_geocerca_metros=120,
            )
            order = index + 2 if index < 2 else 5
            self.branch_stops.append(
                ParadaRuta.objects.create(ruta=self.ruta, punto=point, orden=order)
            )
        self.cedis_regreso = ParadaRuta.objects.create(
            ruta=self.ruta,
            punto=self.cedis_point,
            orden=4,
        )
        self.origin = PointBranch.objects.create(
            external_id="JOURNEY-CEDIS",
            name="Centro de distribución journey",
        )
        self.ruta = liberar_ruta_con_turno(
            ruta=self.ruta,
            actor=self.user,
            bitacora=self.turno,
        )
        self._journey_line_sequence = 0

    def transfer_line(self, *, stop, item_code, item_name, requested, sent, sent_at, transfer, is_enviado=True):
        self._journey_line_sequence += 1
        branch = stop.punto.sucursal
        destination, _ = PointBranch.objects.get_or_create(
            external_id=f"JOURNEY-{branch.codigo}",
            defaults={"name": branch.nombre, "erp_branch": branch},
        )
        if destination.erp_branch_id != branch.id:
            destination.erp_branch = branch
            destination.save(update_fields=["erp_branch", "updated_at"])
        return PointTransferLine.objects.create(
            origin_branch=self.origin,
            destination_branch=destination,
            erp_destination_branch=branch,
            transfer_external_id=transfer,
            detail_external_id=f"{transfer}-D-{self._journey_line_sequence}",
            source_hash=f"journey-{self._journey_line_sequence}",
            registered_at=timezone.now(),
            sent_at=sent_at,
            item_name=item_name,
            item_code=item_code,
            unit="PZA",
            requested_quantity=requested,
            sent_quantity=sent,
            received_quantity="0",
            is_open=True,
            raw_payload={"transfer": {"isEnviado": is_enviado}},
        )

    def sync_cache(self):
        return sincronizar_checklist_carga_desde_point(
            ruta=self.ruta,
            user=self.user,
            ejecutar_sync=False,
        )

    def run_pwa_contract(self, operation):
        template = Path("logistica/templates/logistica/pwa.html").resolve()
        harness = r'''
const fs = require("fs");
const vm = require("vm");
const html = fs.readFileSync(process.argv[1], "utf8");
const start = html.indexOf("<script>", html.indexOf("offline_queue_compat")) + "<script>".length;
const end = html.lastIndexOf("</script>");
const source = html.slice(start, end).replace(/\n\s*boot\(\);\s*$/, "\n");
const storageValues = new Map();
const storage = {
  getItem: (key) => storageValues.has(key) ? storageValues.get(key) : null,
  setItem: (key, value) => storageValues.set(key, String(value)),
  removeItem: (key) => storageValues.delete(key)
};
const context = {
  console, URLSearchParams, URL, FormData: global.FormData, Blob: global.Blob, File: global.File, Response: global.Response,
  localStorage: storage, sessionStorage: storage,
  document: {getElementById: () => ({}), addEventListener: () => {}, hidden: false},
  window: {
    location: {search: ""}, addEventListener: () => {},
    PDLogisticaOfflineQueue: {prepareReplay: (item) => item}
  },
  navigator: {onLine: true},
  setTimeout, clearTimeout, setInterval, clearInterval, fetch: async () => ({})
};
vm.createContext(context);
vm.runInContext(source, context);
const operation = process.argv[2];
if (operation === "segment") {
  const stops = [
    {orden: 1, punto: {tipo: "CEDIS"}, estado: "VISITADA", entrega_estado: "NO_APLICA", operativamente_resuelta: true},
    {orden: 2, punto: {tipo: "SUCURSAL"}, estado: "PENDIENTE", entrega_estado: "PENDIENTE", operativamente_resuelta: true},
    {orden: 4, punto: {tipo: "CEDIS"}, estado: "VISITADA", entrega_estado: "NO_APLICA", operativamente_resuelta: true},
    {orden: 5, punto: {tipo: "SUCURSAL"}, estado: "PENDIENTE", entrega_estado: "PENDIENTE", operativamente_resuelta: false}
  ];
  const oldLine = {id: 1, parada_orden: 2, item_name: "Tramo anterior"};
  const newLine = {id: 2, parada_orden: 5, item_name: "Tramo actual"};
  const current = vm.runInContext(`segmentoCargaOperativo(${JSON.stringify([oldLine, newLine])}, ${JSON.stringify(stops)})`, context);
  const empty = vm.runInContext(`segmentoCargaOperativo(${JSON.stringify([oldLine])}, ${JSON.stringify(stops)})`, context);
  process.stdout.write(JSON.stringify({current: current.lineas.map((row) => row.id), empty: empty.lineas.map((row) => row.id)}));
} else if (operation === "zero") {
  const checklist = {
    estatus: "CONFIRMADA", estatus_display: "Confirmada",
    lineas: [{
      id: 7, parada: 2, parada_orden: 2, parada_nombre: "Sucursal cero",
      item_code: "PAY-0", item_name: "Pay enviado en cero", unit: "PZA",
      cantidad_enviada_esperada: "0.000", cantidad_cargada: "0.000",
      point_enviada: true,
      estatus: "ZERO_EXPECTED", estatus_display: "Enviado en cero",
      notas: "Point confirmó enviado final en cero; no requiere captura."
    }]
  };
  const stops = [{orden: 2, punto: {tipo: "SUCURSAL"}, operativamente_resuelta: false}];
  process.stdout.write(vm.runInContext(`renderChecklistCarga(${JSON.stringify(checklist)}, ${JSON.stringify(stops)})`, context));
} else if (operation === "pending-point") {
  const checklist = {
    estatus: "PENDIENTE", estatus_display: "Pendiente",
    lineas: [{
      id: 8, parada: 2, parada_orden: 2, parada_nombre: "Sucursal espera",
      item_code: "PAY-POINT", item_name: "Pay esperando Point", unit: "PZA",
      cantidad_enviada_esperada: "0.000", cantidad_cargada: null,
      point_enviada: false,
      estatus: "PENDIENTE", estatus_display: "Pendiente de Enviado",
      notas: "La solicitud aún no cambia a Enviado en Point."
    }]
  };
  const stops = [{orden: 2, punto: {tipo: "SUCURSAL"}, operativamente_resuelta: false}];
  const rendered = vm.runInContext(`renderChecklistCarga(${JSON.stringify(checklist)}, ${JSON.stringify(stops)})`, context);
  const summary = vm.runInContext(`resumenCargaRuta(${JSON.stringify(checklist)}, ${JSON.stringify(stops)})`, context);
  process.stdout.write(JSON.stringify({rendered, summary}));
} else if (operation === "positive-untransitioned") {
  const checklist = {
    estatus: "PENDIENTE", estatus_display: "Pendiente",
    lineas: [{
      id: 9, parada: 2, parada_orden: 2, parada_nombre: "Sucursal espera",
      item_code: "PAY-POINT-5", item_name: "Pay con cantidad sin transición", unit: "PZA",
      cantidad_enviada_esperada: "5.000", cantidad_cargada: null,
      point_enviada: false, estatus: "PENDIENTE", estatus_display: "Pendiente de Enviado"
    }]
  };
  const stops = [{orden: 2, punto: {tipo: "SUCURSAL"}, operativamente_resuelta: false}];
  process.stdout.write(vm.runInContext(`renderChecklistCarga(${JSON.stringify(checklist)}, ${JSON.stringify(stops)})`, context));
} else if (operation === "canonical-product") {
  const rows = [
    {id: 21, item_code: " 0117 ", item_name: "Bollo Vainilla", unit: "pza", cantidad_enviada_esperada: "2", cantidad_cargada: null, point_enviada: true, estatus: "PENDIENTE"},
    {id: 22, item_code: "0117", item_name: "  bollo vainilla  ", unit: " PZA ", cantidad_enviada_esperada: "5", cantidad_cargada: null, point_enviada: true, estatus: "PENDIENTE"},
    {id: 23, item_code: "", item_name: "Pastel  Chocolate", unit: "PZA", cantidad_enviada_esperada: "1", cantidad_cargada: null, point_enviada: true, estatus: "PENDIENTE"},
    {id: 24, item_code: null, item_name: "  pastel chocolate ", unit: "pza", cantidad_enviada_esperada: "2", cantidad_cargada: null, point_enviada: true, estatus: "PENDIENTE"}
  ];
  const totals = vm.runInContext(`totalesCargaPorProducto(${JSON.stringify(rows)})`, context);
  process.stdout.write(JSON.stringify(totals.map((row) => ({
    item_code: row.item_code, item_name: row.item_name, unit: row.unit,
    esperado: row.esperado, ids: row.lineas.map((linea) => linea.id)
  }))));
} else if (operation === "product-draft-identity") {
  const previous = {item_code: "ANTERIOR", item_name: "Producto anterior", unit: "PZA", esperado: 9};
  const current = {item_code: "0160", item_name: "Bollo Lotus", unit: "PZA", esperado: 2};
  const previousKey = vm.runInContext(`productoCargaKey(${JSON.stringify(previous)})`, context);
  const currentKey = vm.runInContext(`productoCargaKey(${JSON.stringify(current)})`, context);
  vm.runInContext(`cargaProductoDraftFor(${JSON.stringify(previousKey)}, 9).cantidad = "9.000"`, context);
  const currentDraft = vm.runInContext(`cargaProductoDraftFor(${JSON.stringify(currentKey)}, 2)`, context);
  process.stdout.write(JSON.stringify({previousKey, currentKey, currentQuantity: currentDraft.cantidad}));
} else if (operation === "recarga-status") {
  void (async () => {
    async function run(status, payload) {
      vm.runInContext(`state.rutaActiva = {ruta: {id: 9}}`, context);
      vm.runInContext(`state.rutaCargaDraft = {77: {cantidad: "4"}}; state.rutaCargaProductoDraft = {"CODIGO:OLD|UNIDAD:PZA": {cantidad: "8"}};`, context);
      vm.runInContext(`saveOfflineMutationQueue([{id: "old-load", path: "/rutas/9/carga-checklist/productos/validar/", method: "POST", username: "", queued_at: new Date().toISOString(), attempts: 0, body: {kind: "text", value: "{}"}}]);`, context);
      context.responseStatus = status;
      context.responsePayload = payload;
      vm.runInContext(`
        apiFetch = async () => new Response(JSON.stringify(responsePayload), {status: responseStatus, headers: {"Content-Type": "application/json"}});
        renderRutaActiva = (message, title) => ({screen: "activa", message, title, rutaActiva: state.rutaActiva});
        renderRutaCarga = (message) => ({screen: "carga", message, rutaActiva: state.rutaActiva});
      `, context);
      const rendered = await vm.runInContext(`registrarRecargaCedis(9, 4)`, context);
      const drafts = vm.runInContext(`({lineas: Object.keys(state.rutaCargaDraft).length, productos: Object.keys(state.rutaCargaProductoDraft).length, encoladas: loadOfflineMutationQueue().length})`, context);
      return {rendered, drafts};
    }
    const results = {
      conflict: await run(409, {detail: "Point aún no confirma Enviado"}),
      unavailable: await run(503, {detail: "Point no disponible"}),
      authorized: await run(200, {estado_sync: "AUTORIZADO"}),
      updated: await run(200, {estado_sync: "ACTUALIZADO"})
    };
    process.stdout.write(JSON.stringify(results));
  })().catch((error) => { console.error(error); process.exitCode = 1; });
} else if (operation === "recarga-offline") {
  void (async () => {
    vm.runInContext(`
      state.perfil = {username: "ivan"}; state.token = "token"; state.rutaActiva = {ruta: {id: 9}};
      fetch = async () => { throw new TypeError("Failed to fetch"); };
      renderRutaActiva = (message, title) => ({screen: "activa", message, title, rutaActiva: state.rutaActiva});
    `, context);
    const recarga = await vm.runInContext(`registrarRecargaCedis(9, 4)`, context);
    const afterRecarga = vm.runInContext(`loadOfflineMutationQueue().length`, context);
    const other = await vm.runInContext(`apiFetch("/reportes/", {method: "POST", body: "{}"})`, context);
    const afterOther = vm.runInContext(`loadOfflineMutationQueue().length`, context);
    process.stdout.write(JSON.stringify({recarga, afterRecarga, afterOther, otherQueued: vm.runInContext(`responseQueuedOffline(otherResponse)`, vm.createContext({...context, otherResponse: other}))}));
  })().catch((error) => { console.error(error); process.exitCode = 1; });
} else if (operation === "legacy-replay") {
  void (async () => {
    vm.runInContext(`state.perfil = {username: "ivan"}; state.token = "token";`, context);
    async function replay(status) {
      context.replayStatus = status;
      vm.runInContext(`
        saveOfflineMutationQueue([{
          id: "legacy-cedis", path: "/rutas/9/paradas/4/recarga-cedis/", method: "POST",
          headers: {"Content-Type": "application/json"}, body: {kind: "text", value: "{}"},
          label: "Recarga CEDIS", username: "ivan", queued_at: new Date().toISOString(), attempts: 0
        }]);
        apiFetch = async () => new Response(JSON.stringify({detail: replayStatus === 409 ? "Point aún no confirma Enviado" : "Point no disponible"}), {status: replayStatus, headers: {"Content-Type": "application/json"}});
      `, context);
      await vm.runInContext(`flushOfflineMutationQueue()`, context);
      return vm.runInContext(`({remaining: loadOfflineMutationQueue().length, banner: offlineMutationBanner(), failed: state.offlineMutationFailedCount})`, context);
    }
    process.stdout.write(JSON.stringify({conflict: await replay(409), unavailable: await replay(503)}));
  })().catch((error) => { console.error(error); process.exitCode = 1; });
} else {
  throw new Error(`Operación desconocida: ${operation}`);
}
'''
        result = subprocess.run(
            ["node", "-e", harness, str(template), operation],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout

    def test_pwa_segmento_usa_resolucion_backend_y_no_remezcla_lineas_si_esta_vacio(self):
        result = json.loads(self.run_pwa_contract("segment"))

        self.assertEqual(result["current"], [2])
        self.assertEqual(result["empty"], [])

    def test_journey_libera_ruta_mediante_servicio_y_registra_salida(self):
        self.assertEqual(self.ruta.bitacora_salida_id, self.turno.id)
        self.assertEqual(
            EventoRuta.objects.filter(
                ruta=self.ruta,
                tipo=EventoRuta.TIPO_SALIDA,
            ).count(),
            1,
        )

    def test_pwa_tramo_solo_enviado_cero_lo_muestra_sin_solicitar_captura(self):
        rendered = self.run_pwa_contract("zero")

        self.assertIn("Pay enviado en cero", rendered)
        self.assertIn("cargar 0.000 PZA", rendered)
        self.assertIn("Point confirmó enviado final en cero; no requiere captura.", rendered)
        self.assertNotIn('id="cantidad_total_', rendered)
        self.assertNotIn("validarCargaProductoTramo", rendered)
        self.assertNotIn("Guardar revisión", rendered)

    def test_pwa_cantidad_positiva_sin_transicion_enviado_bloquea_captura(self):
        rendered = self.run_pwa_contract("positive-untransitioned")

        self.assertIn("espera confirmación de Enviado en Point", rendered)
        self.assertNotIn("Confirmar</button>", rendered)
        self.assertNotIn('id="cantidad_total_', rendered)
        self.assertNotIn("validarCargaProductoTramo", rendered)
        self.assertNotIn("Guardar revisión", rendered)

    def test_pwa_pendiente_de_point_permanece_bloqueado_y_no_cuenta_como_revisado(self):
        result = json.loads(self.run_pwa_contract("pending-point"))

        self.assertEqual(result["summary"], "0 de 1 producto revisado.")
        self.assertIn("0 de 1 producto", result["rendered"])
        self.assertIn("0%", result["rendered"])
        self.assertIn("espera confirmación de Enviado en Point", result["rendered"])
        self.assertIn("bloqueada", result["rendered"])
        self.assertNotIn("Carga revisada.", result["rendered"])
        self.assertNotIn('id="cantidad_total_', result["rendered"])

    def test_pwa_consolida_por_codigo_y_unidad_normalizados_con_nombre_como_fallback(self):
        totals = json.loads(self.run_pwa_contract("canonical-product"))

        self.assertEqual(len(totals), 2)
        coded = next(row for row in totals if row["item_code"].strip() == "0117")
        fallback = next(row for row in totals if not row["item_code"])
        self.assertEqual(coded["esperado"], 7)
        self.assertEqual(coded["ids"], [21, 22])
        self.assertEqual(fallback["esperado"], 3)
        self.assertEqual(fallback["ids"], [23, 24])

    def test_pwa_borrador_total_se_identifica_por_producto_y_no_por_posicion(self):
        result = json.loads(self.run_pwa_contract("product-draft-identity"))

        self.assertNotEqual(result["previousKey"], result["currentKey"])
        self.assertEqual(result["currentQuantity"], "2.000")

    def test_pwa_recarga_mantiene_contexto_en_409_503_y_recarga_en_estados_exitosos(self):
        result = json.loads(self.run_pwa_contract("recarga-status"))

        self.assertEqual(result["conflict"]["rendered"]["screen"], "activa")
        self.assertIn("Enviado", result["conflict"]["rendered"]["message"])
        self.assertIsNotNone(result["conflict"]["rendered"]["rutaActiva"])
        self.assertEqual(result["conflict"]["drafts"], {"lineas": 1, "productos": 1, "encoladas": 1})
        self.assertEqual(result["unavailable"]["rendered"]["screen"], "activa")
        self.assertIn("no disponible", result["unavailable"]["rendered"]["message"])
        self.assertIsNotNone(result["unavailable"]["rendered"]["rutaActiva"])
        self.assertEqual(result["unavailable"]["drafts"], {"lineas": 1, "productos": 1, "encoladas": 1})
        self.assertEqual(result["authorized"]["rendered"]["screen"], "carga")
        self.assertIn("autorizada", result["authorized"]["rendered"]["message"])
        self.assertIsNone(result["authorized"]["rendered"]["rutaActiva"])
        self.assertEqual(result["authorized"]["drafts"], {"lineas": 0, "productos": 0, "encoladas": 0})
        self.assertEqual(result["updated"]["rendered"]["screen"], "carga")
        self.assertIn("sincronizado", result["updated"]["rendered"]["message"])
        self.assertIsNone(result["updated"]["rendered"]["rutaActiva"])
        self.assertEqual(result["updated"]["drafts"], {"lineas": 0, "productos": 0, "encoladas": 0})

    def test_pwa_recarga_cedis_sin_red_no_se_encola_pero_otras_mutaciones_si(self):
        result = json.loads(self.run_pwa_contract("recarga-offline"))

        self.assertEqual(result["afterRecarga"], 0)
        self.assertEqual(result["recarga"]["screen"], "activa")
        self.assertIn("requiere conexión", result["recarga"]["message"])
        self.assertIsNotNone(result["recarga"]["rutaActiva"])
        self.assertEqual(result["afterOther"], 1)
        self.assertTrue(result["otherQueued"])

    def test_pwa_replay_legacy_recarga_retenido_y_visible_en_409_y_503(self):
        result = json.loads(self.run_pwa_contract("legacy-replay"))

        for replay in (result["conflict"], result["unavailable"]):
            self.assertEqual(replay["remaining"], 1)
            self.assertEqual(replay["failed"], 1)
            self.assertIn("Recarga CEDIS", replay["banner"])
            self.assertIn("acción explícita", replay["banner"])

    def test_recorrido_dos_tramos_consolida_detalles_incluye_pastel_y_cero_sin_duplicar(self):
        first, second, third = self.branch_stops
        now = timezone.now()
        vainilla = [
            self.transfer_line(
                stop=first,
                item_code="BOLLO-V",
                item_name="Bollo Vainilla journey",
                requested="2",
                sent="2",
                sent_at=now,
                transfer="FOLIO-JOURNEY-V",
            ),
            self.transfer_line(
                stop=second,
                item_code="BOLLO-V",
                item_name="Bollo Vainilla journey",
                requested="2",
                sent="2",
                sent_at=now,
                transfer="FOLIO-JOURNEY-V",
            ),
            self.transfer_line(
                stop=second,
                item_code="BOLLO-V",
                item_name="Bollo Vainilla journey",
                requested="3",
                sent="3",
                sent_at=now,
                transfer="FOLIO-JOURNEY-V",
            ),
        ]
        pastel = self.transfer_line(
            stop=first,
            item_code="PASTEL-J",
            item_name="Pastel Chocolate journey",
            requested="1",
            sent="1",
            sent_at=now,
            transfer="FOLIO-JOURNEY-PASTEL",
        )
        pay_zero = self.transfer_line(
            stop=second,
            item_code="PAY-J",
            item_name="Pay Limón journey",
            requested="2",
            sent="0",
            sent_at=now,
            transfer="FOLIO-JOURNEY-PAY",
        )
        second_segment = self.transfer_line(
            stop=third,
            item_code="PASTEL-SEGUNDO",
            item_name="Pastel segundo tramo journey",
            requested="4",
            sent="4",
            sent_at=now,
            transfer="FOLIO-JOURNEY-SEGUNDO",
        )

        checklist = self.sync_cache().checklist
        first_rows = list(lineas_tramo_operativo_actual(self.ruta, checklist=checklist))
        first_ids = {row.point_transfer_line_id for row in first_rows}
        self.assertTrue({line.id for line in vainilla}.issubset(first_ids))
        self.assertIn(pastel.id, first_ids)
        self.assertIn(pay_zero.id, first_ids)
        self.assertNotIn(second_segment.id, first_ids)
        self.assertEqual(
            sum(
                (row.cantidad_enviada_esperada for row in first_rows if row.item_code == "BOLLO-V"),
                Decimal("0"),
            ),
            Decimal("7"),
        )
        self.assertEqual(
            {row.parada_id for row in first_rows if row.item_code == "BOLLO-V"},
            {first.id, second.id},
        )
        zero_row = next(row for row in first_rows if row.point_transfer_line_id == pay_zero.id)
        self.assertEqual(zero_row.estatus, RutaCargaChecklistLinea.ESTATUS_ZERO_EXPECTED)
        self.assertEqual(zero_row.cantidad_cargada, Decimal("0"))
        self.assertEqual(
            RutaCargaChecklistLinea.objects.filter(
                checklist=checklist,
                point_transfer_line_id__in=[line.id for line in vainilla],
            ).count(),
            3,
        )

        confirmar_entrega_parada(
            ruta=self.ruta,
            parada=first,
            actor=self.user,
            entrega_estado=ParadaRuta.ENTREGA_ENTREGADA,
            motivo="Entrega excepcional documentada",
            client_event_id="journey-entrega-excepcional",
            ubicacion={
                "causa": "GPS_SIN_SENAL",
                "client_timestamp": timezone.now().isoformat(),
                "client_version": "journey-task6",
            },
            origen="PWA",
        )
        first.refresh_from_db()
        self.assertTrue(parada_resuelta_operativamente(first))
        self.assertEqual(first.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertIsNone(first.hora_llegada_real)

        ubicacion_payload = {
            "latitud": str(second.latitud_geocerca),
            "longitud": str(second.longitud_geocerca),
            "precision_metros": "8.00",
            "tracking_origen": "automatico_pwa",
        }
        registrar_ubicacion_ruta(user=self.user, ruta=self.ruta, payload=ubicacion_payload)
        EventoRuta.objects.filter(
            ruta=self.ruta,
            parada=second,
            tipo=EventoRuta.TIPO_LLEGADA_GEOFENCE,
            metadata__origen_servicio="registrar_ubicacion_ruta",
        ).update(creado_en=timezone.now() - timedelta(minutes=6))
        registrar_ubicacion_ruta(user=self.user, ruta=self.ruta, payload=ubicacion_payload)
        second.refresh_from_db()
        self.assertEqual(second.estado, ParadaRuta.ESTADO_VISITADA)
        confirmar_entrega_parada(
            ruta=self.ruta,
            parada=second,
            actor=self.user,
            entrega_estado=ParadaRuta.ENTREGA_ENTREGADA,
            motivo="Entrega confirmada en geocerca journey",
            client_event_id="journey-entrega-segunda",
            origen="PWA",
        )

        with patch(
            "logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point",
            side_effect=lambda **kwargs: self.sync_cache(),
        ) as sync:
            evento = registrar_recarga_cedis(
                ruta=self.ruta,
                user=self.user,
                parada=self.cedis_regreso,
                notas="Regreso journey al centro de distribución",
            )
        sync.assert_called_once()
        self.assertEqual(evento.metadata["estado_sync"], "ACTUALIZADO")
        self.cedis_regreso.refresh_from_db()
        self.assertEqual(self.cedis_regreso.estado, ParadaRuta.ESTADO_VISITADA)

        second_rows = list(lineas_tramo_operativo_actual(self.ruta, checklist=checklist))
        self.assertEqual({row.parada_id for row in second_rows}, {third.id})
        self.assertEqual({row.point_transfer_line_id for row in second_rows}, {second_segment.id})
        confirmar_entrega_parada(
            ruta=self.ruta,
            parada=third,
            actor=self.user,
            entrega_estado=ParadaRuta.ENTREGA_ENTREGADA,
            motivo="GPS sin señal en segundo tramo",
            client_event_id="journey-entrega-tercera",
            ubicacion={
                "causa": "GPS_SIN_SENAL",
                "client_timestamp": timezone.now().isoformat(),
                "client_version": "journey-task6",
            },
            origen="PWA",
        )
        self.assertFalse(ruta_tiene_paradas_entregables_pendientes(self.ruta))
        self.client.force_login(self.user)
        cierre = self.client.post(
            reverse("api_logistica_ruta_finalizar_pwa", kwargs={"ruta_id": self.ruta.id})
        )
        self.assertEqual(cierre.status_code, 200, cierre.content)
        self.ruta.refresh_from_db()
        self.assertEqual(self.ruta.estatus, RutaEntrega.ESTATUS_COMPLETADA)

    def test_solicitud_sin_transicion_bloquea_recarga_y_genera_alerta(self):
        first, second, _third = self.branch_stops
        first.estado = ParadaRuta.ESTADO_VISITADA
        first.entrega_estado = ParadaRuta.ENTREGA_ENTREGADA
        first.save(update_fields=["estado", "entrega_estado", "actualizado_en"])
        second.estado = ParadaRuta.ESTADO_VISITADA
        second.entrega_estado = ParadaRuta.ENTREGA_ENTREGADA
        second.save(update_fields=["estado", "entrega_estado", "actualizado_en"])
        pending = self.transfer_line(
            stop=self.branch_stops[2],
            item_code="PAY-PENDIENTE",
            item_name="Pay pendiente journey",
            requested="2",
            sent="0",
            sent_at=None,
            transfer="FOLIO-JOURNEY-PENDIENTE",
            is_enviado=False,
        )
        self.sync_cache()

        with patch(
            "logistica.services_carga_ruta.sincronizar_checklist_recarga_desde_point",
            side_effect=lambda **kwargs: self.sync_cache(),
        ):
            with self.assertRaises(RecargaCedisPendienteEnviado) as captured:
                registrar_recarga_cedis(
                    ruta=self.ruta,
                    user=self.user,
                    parada=self.cedis_regreso,
                    notas="No debe avanzar",
                )

        self.assertEqual(getattr(captured.exception, "estado_sync", None), "PENDIENTE_ENVIADO")
        self.cedis_regreso.refresh_from_db()
        self.assertEqual(self.cedis_regreso.estado, ParadaRuta.ESTADO_PENDIENTE)
        self.assertTrue(
            EventoRuta.objects.filter(
                ruta=self.ruta,
                parada=self.cedis_regreso,
                metadata__estado_sync="PENDIENTE_ENVIADO",
            ).exists()
        )
        pending.refresh_from_db()
        self.assertIsNone(pending.sent_at)
        self.assertEqual(pending.sent_quantity, Decimal("0"))

    def test_pwa_consume_resolucion_operativa_y_estados_de_recarga(self):
        html = Path("logistica/templates/logistica/pwa.html").read_text(encoding="utf-8")
        resolution_function = re.search(
            r"function paradaOperativamenteResuelta\(parada\) \{(?P<body>.*?)\n      \}",
            html,
            re.DOTALL,
        )
        next_function = re.search(
            r"function proximaParadaId\(paradas\) \{(?P<body>.*?)\n      \}",
            html,
            re.DOTALL,
        )
        render_function = re.search(
            r"function renderParadasRuta\(paradas, rutaId, rutaEnSeguimiento\) \{(?P<body>.*?)\n      \}",
            html,
            re.DOTALL,
        )
        recarga_function = re.search(
            r"async function registrarRecargaCedis\(.*?\) \{(?P<body>.*?)\n      \}",
            html,
            re.DOTALL,
        )
        self.assertIsNotNone(resolution_function)
        self.assertIsNotNone(next_function)
        self.assertIsNotNone(render_function)
        self.assertIsNotNone(recarga_function)
        self.assertIn("operativamente_resuelta", resolution_function.group("body"))
        self.assertIn("paradaOperativamenteResuelta", next_function.group("body"))
        self.assertIn("paradaOperativamenteResuelta", render_function.group("body"))
        self.assertIn("response.status === 409", recarga_function.group("body"))
        self.assertIn("response.status === 503", recarga_function.group("body"))
        self.assertIn('estado_sync === "AUTORIZADO"', recarga_function.group("body"))
        self.assertIn("state.rutaActiva = null", recarga_function.group("body"))
        self.assertIn("renderRutaCarga", recarga_function.group("body"))

    def test_service_worker_y_registro_comparten_version_nueva_unica(self):
        sw = Path("logistica/static/logistica/pwa/sw.js").read_text(encoding="utf-8")
        html = Path("logistica/templates/logistica/pwa.html").read_text(encoding="utf-8")
        cache_match = re.search(r'const CACHE_NAME = "([^"]+)";', sw)
        self.assertIsNotNone(cache_match)
        self.assertEqual(cache_match.group(1), "pollyanas-logistica-pwa-v67-carga-tramo-segura")
        self.assertIn("?v=route-control-v67-carga-tramo-segura", html)

    def test_pwa_carga_por_sucursal_usa_un_solo_guardado_atomico(self):
        html = Path("logistica/templates/logistica/pwa.html").read_text(encoding="utf-8")

        self.assertIn("function renderResumenSucursalesCarga", html)
        self.assertIn("function renderCapturaSucursal", html)
        self.assertIn("function abrirModalDiferencias", html)
        self.assertIn("async function guardarCargaSucursal", html)
        self.assertIn("contexto_operativo", html)
        self.assertIn("version_checklist", html)
        self.assertIn("client_event_id", html)
        self.assertIn("/carga-checklist/sucursales/", html)
        self.assertNotIn("/carga-checklist/productos/validar/", html)

    def test_pwa_carga_sucursal_incluye_busqueda_y_orden_alfabetico(self):
        html = Path("logistica/templates/logistica/pwa.html").read_text(encoding="utf-8")

        self.assertIn('localeCompare(b.item_name, "es"', html)
        self.assertIn('type="search"', html)
        self.assertIn("Buscar producto o código", html)
        self.assertIn("Guardar sucursal", html)
        self.assertIn("Explica los cambios", html)
