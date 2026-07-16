from decimal import Decimal

from django.contrib.auth import get_user_model
from django.db import IntegrityError, transaction
from django.test import TestCase
from django.utils import timezone

from core.models import Sucursal
from logistica.models import (
    DiscrepanciaLogistica,
    ParadaRuta,
    PuntoLogistico,
    Repartidor,
    RutaCargaChecklist,
    RutaCargaChecklistLinea,
    RutaCargaSucursalEvento,
    RutaEntrega,
    Unidad,
)
from logistica.services_carga_sucursal import (
    CargaSucursalError,
    ConflictoIdempotencia,
    guardar_carga_sucursal,
)
from logistica.services_contexto_operativo import construir_contexto_operativo


User = get_user_model()


class PersistenciaCargaSucursalTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="AT-SUC", nombre="Sucursal Atómica", activa=True)
        self.unidad = Unidad.objects.create(codigo="AT-01", descripcion="Unidad atómica", sucursal=self.sucursal)
        self.user = User.objects.create_user(username="chofer.atomico")
        self.jefe = User.objects.create_user(username="jefe.atomico")
        self.repartidor = Repartidor.objects.create(user=self.user, sucursal=self.sucursal)
        self.ruta = RutaEntrega.objects.create(
            nombre="Ruta atómica",
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
        )
        self.punto_cedis = PuntoLogistico.objects.create(
            nombre="CEDIS Atómico",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.560000",
            longitud="-108.460000",
        )
        self.cedis = ParadaRuta.objects.create(ruta=self.ruta, punto=self.punto_cedis, orden=1)
        self.parada = ParadaRuta.objects.create(ruta=self.ruta, punto=self.punto, orden=2)
        self.checklist = RutaCargaChecklist.objects.create(ruta=self.ruta)
        self.linea = RutaCargaChecklistLinea.objects.create(
            checklist=self.checklist,
            parada=self.parada,
            transfer_external_id="AT-T-1",
            detail_external_id="AT-D-1",
            source_hash="at-source-1",
            item_name="Pastel atómico",
            unit="PZA",
            cantidad_solicitada=Decimal("10"),
            cantidad_enviada_esperada=Decimal("10"),
        )

    def test_evento_cliente_es_unico_por_ruta(self):
        RutaCargaSucursalEvento.objects.create(
            ruta=self.ruta,
            parada=self.parada,
            client_event_id="evt-1",
            payload_hash="a" * 64,
            contexto_version="b" * 64,
            respuesta={"ok": True},
            creado_por=self.user,
        )

        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                RutaCargaSucursalEvento.objects.create(
                    ruta=self.ruta,
                    parada=self.parada,
                    client_event_id="evt-1",
                    payload_hash="c" * 64,
                    contexto_version="b" * 64,
                    respuesta={},
                    creado_por=self.user,
                )

    def test_discrepancias_de_carga_y_recepcion_son_eventos_separados(self):
        carga = DiscrepanciaLogistica.objects.create(
            ruta=self.ruta,
            parada=self.parada,
            linea_carga=self.linea,
            origen=DiscrepanciaLogistica.ORIGEN_CARGA,
            cantidad_enviada=Decimal("10"),
            cantidad_cargada=Decimal("8"),
            motivo="faltante_fisico",
            asignado_a=self.jefe,
            creado_por=self.user,
        )
        recepcion = DiscrepanciaLogistica.objects.create(
            ruta=self.ruta,
            parada=self.parada,
            linea_carga=self.linea,
            origen=DiscrepanciaLogistica.ORIGEN_RECEPCION,
            cantidad_enviada=Decimal("10"),
            cantidad_cargada=Decimal("8"),
            cantidad_recibida=Decimal("7"),
            motivo="faltante_fisico",
            asignado_a=self.jefe,
            creado_por=self.user,
        )

        self.assertNotEqual(carga.id, recepcion.id)
        self.assertEqual(carga.estado, DiscrepanciaLogistica.ESTADO_PENDIENTE_JEFE)
        self.assertEqual(recepcion.estado, DiscrepanciaLogistica.ESTADO_PENDIENTE_JEFE)

    def test_no_permite_dos_discrepancias_abiertas_del_mismo_origen(self):
        datos = {
            "ruta": self.ruta,
            "parada": self.parada,
            "linea_carga": self.linea,
            "origen": DiscrepanciaLogistica.ORIGEN_CARGA,
            "cantidad_enviada": Decimal("10"),
            "cantidad_cargada": Decimal("8"),
            "motivo": "faltante_fisico",
            "asignado_a": self.jefe,
            "creado_por": self.user,
        }
        DiscrepanciaLogistica.objects.create(**datos)

        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                DiscrepanciaLogistica.objects.create(**datos)


class GuardarCargaSucursalTests(PersistenciaCargaSucursalTests):
    def setUp(self):
        super().setUp()
        self.linea_2 = RutaCargaChecklistLinea.objects.create(
            checklist=self.checklist,
            parada=self.parada,
            transfer_external_id="AT-T-2",
            detail_external_id="AT-D-2",
            source_hash="at-source-2",
            item_name="Pay atómico",
            unit="PZA",
            cantidad_solicitada=Decimal("5"),
            cantidad_enviada_esperada=Decimal("5"),
        )
        self.contexto = construir_contexto_operativo(ruta=self.ruta, actor=self.user)

    def payload(self, *, cantidad_1="10", cantidad_2="5", motivo_1="", linea_2_id=None):
        return [
            {
                "linea_id": self.linea.id,
                "source_hash": self.linea.source_hash,
                "cantidad_cargada": Decimal(cantidad_1),
                "motivo_diferencia": motivo_1,
                "notas": "",
            },
            {
                "linea_id": linea_2_id or self.linea_2.id,
                "source_hash": self.linea_2.source_hash,
                "cantidad_cargada": Decimal(cantidad_2),
                "motivo_diferencia": "",
                "notas": "",
            },
        ]

    def guardar(self, *, event_id="evt-guardar-1", lineas=None):
        return guardar_carga_sucursal(
            actor=self.user,
            ruta=self.ruta,
            contexto_token=self.contexto.token,
            parada_id=self.parada.id,
            client_event_id=event_id,
            lineas=lineas or self.payload(),
        )

    def test_guarda_todas_las_lineas_de_la_sucursal(self):
        respuesta = self.guardar()

        self.linea.refresh_from_db()
        self.linea_2.refresh_from_db()
        self.assertEqual(self.linea.estatus, RutaCargaChecklistLinea.ESTATUS_CARGADA)
        self.assertEqual(self.linea_2.estatus, RutaCargaChecklistLinea.ESTATUS_CARGADA)
        self.assertEqual(respuesta["parada_id"], self.parada.id)
        self.assertEqual(respuesta["lineas_guardadas"], 2)

    def test_diferencia_exige_motivo_y_revierte_toda_la_sucursal(self):
        with self.assertRaises(CargaSucursalError) as error:
            self.guardar(lineas=self.payload(cantidad_1="8"))

        self.linea.refresh_from_db()
        self.linea_2.refresh_from_db()
        self.assertEqual(error.exception.codigo, "motivo_diferencia_requerido")
        self.assertEqual(self.linea.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
        self.assertEqual(self.linea_2.estatus, RutaCargaChecklistLinea.ESTATUS_PENDIENTE)
        self.assertFalse(RutaCargaSucursalEvento.objects.exists())

    def test_linea_ajena_revierte_toda_la_sucursal(self):
        with self.assertRaises(CargaSucursalError) as error:
            self.guardar(lineas=self.payload(linea_2_id=999999))

        self.assertEqual(error.exception.codigo, "producto_no_vigente")
        self.assertFalse(RutaCargaSucursalEvento.objects.exists())
        self.assertFalse(
            RutaCargaChecklistLinea.objects.filter(
                pk__in=[self.linea.id, self.linea_2.id],
                estatus=RutaCargaChecklistLinea.ESTATUS_CARGADA,
            ).exists()
        )

    def test_reintento_identico_devuelve_resultado_sin_duplicar(self):
        primera = self.guardar()
        segunda = self.guardar()

        self.assertEqual(primera, segunda)
        self.assertEqual(RutaCargaSucursalEvento.objects.count(), 1)

    def test_reintento_distinto_rechaza_conflicto(self):
        self.guardar()

        with self.assertRaises(ConflictoIdempotencia):
            self.guardar(lineas=self.payload(cantidad_1="9", motivo_1=RutaCargaChecklistLinea.MOTIVO_STOCK_LIMITADO))

        self.assertEqual(RutaCargaSucursalEvento.objects.count(), 1)
