from decimal import Decimal

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied
from django.test import TestCase
from django.utils import timezone

from core.models import Sucursal
from logistica.models import (
    ParadaRuta,
    PuntoLogistico,
    Repartidor,
    RutaCargaChecklist,
    RutaCargaChecklistLinea,
    RutaEntrega,
    Unidad,
)
from logistica.services_contexto_operativo import (
    ContextoOperativoObsoleto,
    construir_contexto_operativo,
    validar_contexto_operativo,
)


User = get_user_model()


class ContextoOperativoTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="CTX-SUC", nombre="Sucursal Contexto", activa=True)
        self.unidad = Unidad.objects.create(codigo="CTX-01", descripcion="Unidad contexto", sucursal=self.sucursal)
        self.user_chofer = User.objects.create_user(username="chofer.contexto")
        self.user_acompanante = User.objects.create_user(username="acompanante.contexto")
        self.repartidor = Repartidor.objects.create(
            user=self.user_chofer,
            sucursal=self.sucursal,
            unidad_asignada=self.unidad,
        )
        self.acompanante = Repartidor.objects.create(
            user=self.user_acompanante,
            sucursal=self.sucursal,
        )
        self.ruta = RutaEntrega.objects.create(
            nombre="Ruta contexto",
            fecha_ruta=timezone.localdate(),
            estatus=RutaEntrega.ESTATUS_PLANEADA,
            repartidor=self.repartidor,
            acompanante=self.acompanante,
            unidad_operativa=self.unidad,
        )
        self.punto_cedis = PuntoLogistico.objects.create(
            nombre="CEDIS Contexto",
            tipo=PuntoLogistico.TIPO_CEDIS,
            latitud="25.570000",
            longitud="-108.470000",
        )
        self.punto_sucursal = PuntoLogistico.objects.create(
            sucursal=self.sucursal,
            nombre=self.sucursal.nombre,
            tipo=PuntoLogistico.TIPO_SUCURSAL,
            latitud="25.580000",
            longitud="-108.480000",
        )
        self.cedis_inicial = ParadaRuta.objects.create(ruta=self.ruta, punto=self.punto_cedis, orden=1)
        self.parada = ParadaRuta.objects.create(ruta=self.ruta, punto=self.punto_sucursal, orden=2)
        self.checklist = RutaCargaChecklist.objects.create(
            ruta=self.ruta,
            estatus=RutaCargaChecklist.ESTATUS_EN_REVISION,
        )
        self.linea = RutaCargaChecklistLinea.objects.create(
            checklist=self.checklist,
            parada=self.parada,
            transfer_external_id="CTX-T-1",
            detail_external_id="CTX-D-1",
            source_hash="ctx-source-1",
            item_code="CTX-P-1",
            item_name="Pastel contexto",
            unit="PZA",
            erp_destination_branch=self.sucursal,
            cantidad_solicitada=Decimal("3"),
            cantidad_enviada_esperada=Decimal("3"),
        )

    def test_contexto_usa_chofer_unidad_y_tramo_de_la_ruta(self):
        contexto = construir_contexto_operativo(ruta=self.ruta, actor=self.user_chofer)

        self.assertEqual(contexto.ruta_id, self.ruta.id)
        self.assertEqual(contexto.chofer_autorizado_id, self.repartidor.id)
        self.assertEqual(contexto.unidad_id, self.ruta.unidad_operativa_id)
        self.assertEqual(contexto.parada_cedis_origen_id, self.cedis_inicial.id)
        self.assertEqual(contexto.sucursales_permitidas, (self.sucursal.id,))
        self.assertEqual(contexto.productos_permitidos, (self.linea.id,))
        self.assertTrue(contexto.token)

    def test_acompanante_recibe_contexto_propio_para_la_misma_ruta(self):
        contexto = construir_contexto_operativo(ruta=self.ruta, actor=self.user_acompanante)

        self.assertEqual(contexto.ruta_id, self.ruta.id)
        self.assertEqual(contexto.chofer_autorizado_id, self.acompanante.id)
        self.assertTrue(contexto.token)

    def test_tramo_inicial_antes_del_primer_cedis_no_bloquea_la_carga(self):
        self.cedis_inicial.orden = 3
        self.cedis_inicial.save(update_fields=["orden", "actualizado_en"])
        self.parada.orden = 1
        self.parada.save(update_fields=["orden", "actualizado_en"])

        contexto = construir_contexto_operativo(ruta=self.ruta, actor=self.user_acompanante)

        self.assertIsNone(contexto.parada_cedis_origen_id)
        self.assertEqual(contexto.tramo_id, f"salida-inicial:hasta-{self.cedis_inicial.id}")
        self.assertEqual(contexto.productos_permitidos, (self.linea.id,))

    def test_cambio_de_checklist_invalida_firma_anterior(self):
        firmado = construir_contexto_operativo(ruta=self.ruta, actor=self.user_chofer).token
        self.linea.cantidad_enviada_esperada = Decimal("4")
        self.linea.save(update_fields=["cantidad_enviada_esperada", "actualizado_en"])

        with self.assertRaises(ContextoOperativoObsoleto) as error:
            validar_contexto_operativo(token=firmado, ruta=self.ruta, actor=self.user_chofer)

        self.assertEqual(error.exception.codigo, "checklist_actualizado")
        self.assertEqual(error.exception.productos_afectados, (self.linea.id,))

    def test_cambio_de_unidad_invalida_firma_anterior(self):
        firmado = construir_contexto_operativo(ruta=self.ruta, actor=self.user_chofer).token
        unidad_nueva = Unidad.objects.create(
            codigo="CTX-02",
            descripcion="Unidad contexto nueva",
            sucursal=self.sucursal,
        )
        self.ruta.unidad_operativa = unidad_nueva
        self.ruta.save(update_fields=["unidad_operativa", "updated_at"])

        with self.assertRaises(ContextoOperativoObsoleto) as error:
            validar_contexto_operativo(token=firmado, ruta=self.ruta, actor=self.user_chofer)

        self.assertEqual(error.exception.codigo, "contexto_obsoleto")
