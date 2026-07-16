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
        self.parada = ParadaRuta.objects.create(ruta=self.ruta, punto=self.punto, orden=1)
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
