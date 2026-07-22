from decimal import Decimal

from django.apps import apps
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.db import IntegrityError, transaction
from django.test import TestCase
from django.utils import timezone

from core.models import Sucursal
from pos_bridge.models import PointBranch, PointInventorySnapshot, PointProduct, PointSyncJob, PointTransferLine
from rrhh.models import Empleado


User = get_user_model()


class MermaInsumoDomainTests(TestCase):
    def setUp(self):
        try:
            self.MermaInsumo = apps.get_model("mermas", "MermaInsumo")
            self.MermaInsumoEvento = apps.get_model("mermas", "MermaInsumoEvento")
            self.OrdenAjustePoint = apps.get_model("mermas", "OrdenAjustePoint")
        except LookupError:
            self.fail("Falta implementar el dominio independiente de merma de insumos")

        self.sucursal = Sucursal.objects.create(codigo="PAYAN", nombre="Payán")
        self.reportante = User.objects.create_user(username="encargada.payan")
        self.jefe = User.objects.create_user(username="jefe.ventas")

    def nueva_merma(self, **overrides):
        values = {
            "sucursal": self.sucursal,
            "reportado_por": self.reportante,
            "jefe_inmediato": self.jefe,
            "codigo_point": "INS-001",
            "nombre_point": "Fresa fresca",
            "unidad_point": "KG",
            "cantidad_reportada": Decimal("3.000"),
            "motivo": "DESCOMPOSICION",
            "comentario": "Producto demasiado maduro",
            "justificacion_sin_foto": "Se desechó antes de tomar evidencia",
            "estatus": self.MermaInsumo.ESTATUS_ENVIADA,
        }
        values.update(overrides)
        return self.MermaInsumo.objects.create(**values)

    def test_merma_sin_foto_exige_justificacion(self):
        merma = self.nueva_merma(justificacion_sin_foto="")

        with self.assertRaisesMessage(ValidationError, "justificación"):
            merma.full_clean()

    def test_jefe_no_puede_aprobar_cantidad_mayor(self):
        merma = self.nueva_merma()

        with self.assertRaisesMessage(ValidationError, "aumentar"):
            merma.aprobar(jefe=self.jefe, cantidad=Decimal("3.500"), motivo="")

    def test_aprobar_cantidad_menor_conserva_ambas_y_exige_motivo(self):
        merma = self.nueva_merma()

        with self.assertRaisesMessage(ValidationError, "motivo"):
            merma.aprobar(jefe=self.jefe, cantidad=Decimal("2.500"), motivo="")

        merma.aprobar(jefe=self.jefe, cantidad=Decimal("2.500"), motivo="Solo 2.5 kg comprobados")
        merma.refresh_from_db()

        self.assertEqual(merma.cantidad_reportada, Decimal("3.000"))
        self.assertEqual(merma.cantidad_aprobada, Decimal("2.500"))
        self.assertEqual(merma.estatus, self.MermaInsumo.ESTATUS_APROBADA)
        self.assertTrue(
            self.MermaInsumoEvento.objects.filter(
                merma=merma,
                estado_nuevo=self.MermaInsumo.ESTATUS_APROBADA,
                actor=self.jefe,
            ).exists()
        )

    def test_solo_jefe_asignado_puede_aprobar(self):
        otro = User.objects.create_user(username="otro.jefe")
        merma = self.nueva_merma()

        with self.assertRaisesMessage(ValidationError, "jefe inmediato"):
            merma.aprobar(jefe=otro, cantidad=Decimal("3.000"), motivo="")

    def test_orden_point_es_unica_y_payload_inmutable(self):
        merma = self.nueva_merma()
        merma.aprobar(jefe=self.jefe, cantidad=Decimal("3.000"), motivo="")
        orden = self.OrdenAjustePoint.crear_desde_merma(merma)

        self.assertEqual(orden.cantidad, Decimal("-3.000"))
        self.assertEqual(orden.codigo_point, "INS-001")
        self.assertTrue(orden.idempotency_key)

        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                self.OrdenAjustePoint.objects.create(
                    merma=merma,
                    sucursal=self.sucursal,
                    codigo_point="INS-001",
                    unidad_point="KG",
                    cantidad=Decimal("-3.000"),
                    idempotency_key="otra-clave",
                )

        orden.codigo_point = "CAMBIADO"
        with self.assertRaisesMessage(ValidationError, "inmutable"):
            orden.save()


class EnviarMermaInsumoTests(TestCase):
    def setUp(self):
        self.MermaInsumo = apps.get_model("mermas", "MermaInsumo")
        self.MermaInsumoEvento = apps.get_model("mermas", "MermaInsumoEvento")
        self.OrdenAjustePoint = apps.get_model("mermas", "OrdenAjustePoint")
        self.sucursal = Sucursal.objects.create(codigo="PAYAN", nombre="Payán")
        self.usuario = User.objects.create_user(username="encargada.payan")
        self.usuario_jefe = User.objects.create_user(username="jefe.payan")

    def nueva_merma(self, **overrides):
        values = {
            "sucursal": self.sucursal,
            "reportado_por": self.usuario,
            "codigo_point": "INS-001",
            "nombre_point": "Fresa fresca",
            "unidad_point": "KG",
            "cantidad_reportada": Decimal("3.000"),
            "motivo": "DESCOMPOSICION",
            "comentario": "Producto demasiado maduro",
            "justificacion_sin_foto": "Se desechó antes de tomar evidencia",
        }
        values.update(overrides)
        return self.MermaInsumo.objects.create(**values)

    def crear_identidad_valida(self):
        jefe = Empleado.objects.create(
            nombre="Jefa Payán",
            usuario_erp=self.usuario_jefe,
            sucursal_ref=self.sucursal,
        )
        reportante = Empleado.objects.create(
            nombre="Encargada Payán",
            usuario_erp=self.usuario,
            sucursal_ref=self.sucursal,
            jefe_directo=jefe,
        )
        return reportante, jefe

    def test_envia_con_identidad_rrhh_literal_y_congela_responsables(self):
        reportante, jefe = self.crear_identidad_valida()
        merma = self.nueva_merma()
        from mermas.services_insumos import enviar_merma_insumo

        enviada = enviar_merma_insumo(merma_id=merma.pk, usuario=self.usuario)

        self.assertEqual(enviada.estatus, self.MermaInsumo.ESTATUS_ENVIADA)
        self.assertEqual(enviada.reportante_empleado, reportante)
        self.assertEqual(enviada.jefe_empleado, jefe)
        self.assertEqual(enviada.jefe_inmediato, self.usuario_jefe)
        self.assertTrue(
            self.MermaInsumoEvento.objects.filter(
                merma=merma,
                estado_anterior=self.MermaInsumo.ESTATUS_BORRADOR,
                estado_nuevo=self.MermaInsumo.ESTATUS_ENVIADA,
                actor=self.usuario,
            ).exists()
        )
        self.assertFalse(self.OrdenAjustePoint.objects.filter(merma=merma).exists())

    def test_sin_empleado_vinculado_marca_sin_responsable_y_crea_evento(self):
        merma = self.nueva_merma()
        from mermas.services_insumos import enviar_merma_insumo

        enviada = enviar_merma_insumo(merma_id=merma.pk, usuario=self.usuario)

        self.assertEqual(enviada.estatus, self.MermaInsumo.ESTATUS_SIN_RESPONSABLE)
        self.assertTrue(
            self.MermaInsumoEvento.objects.filter(
                merma=merma,
                estado_nuevo=self.MermaInsumo.ESTATUS_SIN_RESPONSABLE,
                actor=self.usuario,
            ).exists()
        )
        self.assertFalse(self.OrdenAjustePoint.objects.filter(merma=merma).exists())

    def test_identidad_invalida_nunca_asigna_responsable(self):
        otra_sucursal = Sucursal.objects.create(codigo="CENTRO", nombre="Centro")
        jefe = Empleado.objects.create(
            nombre="Jefa inactiva",
            usuario_erp=self.usuario_jefe,
            sucursal_ref=self.sucursal,
            activo=False,
        )
        Empleado.objects.create(
            nombre="Encargada otra sucursal",
            usuario_erp=self.usuario,
            sucursal_ref=otra_sucursal,
            jefe_directo=jefe,
        )
        merma = self.nueva_merma()
        from mermas.services_insumos import enviar_merma_insumo

        enviada = enviar_merma_insumo(merma_id=merma.pk, usuario=self.usuario)

        self.assertEqual(enviada.estatus, self.MermaInsumo.ESTATUS_SIN_RESPONSABLE)
        self.assertIsNone(enviada.reportante_empleado_id)
        self.assertIsNone(enviada.jefe_empleado_id)
        self.assertIsNone(enviada.jefe_inmediato_id)
        self.assertFalse(self.OrdenAjustePoint.objects.filter(merma=merma).exists())

    def test_rechaza_reenvio_desde_estado_no_permitido(self):
        self.crear_identidad_valida()
        merma = self.nueva_merma(estatus=self.MermaInsumo.ESTATUS_ENVIADA)
        from mermas.services_insumos import enviar_merma_insumo

        with self.assertRaisesMessage(ValidationError, "borrador"):
            enviar_merma_insumo(merma_id=merma.pk, usuario=self.usuario)

        self.assertEqual(self.MermaInsumoEvento.objects.filter(merma=merma).count(), 0)


class InsumosElegiblesPointTests(TestCase):
    def setUp(self):
        self.sucursal = Sucursal.objects.create(codigo="PAYAN", nombre="Payán")
        self.cedis = PointBranch.objects.create(external_id="1", name="CEDIS")
        self.branch = PointBranch.objects.create(external_id="2", name="Payán", erp_branch=self.sucursal)
        self.job = PointSyncJob.objects.create(status=PointSyncJob.STATUS_SUCCESS)
        self.product = PointProduct.objects.create(external_id="p-1", sku="INS-001", name="Fresa fresca")

    def recepcion(self, *, days_ago=0, unit="KG"):
        moment = timezone.now() - timezone.timedelta(days=days_ago)
        return PointTransferLine.objects.create(
            origin_branch=self.cedis,
            destination_branch=self.branch,
            erp_destination_branch=self.sucursal,
            sync_job=self.job,
            transfer_external_id=f"T-{days_ago}-{unit}",
            detail_external_id=f"D-{days_ago}-{unit}",
            source_hash=f"hash-{days_ago}-{unit}",
            registered_at=moment,
            received_at=moment,
            item_name="Fresa fresca",
            item_code="INS-001",
            unit=unit,
            received_quantity=Decimal("5.000"),
            is_insumo=True,
            is_received=True,
            is_cancelled=False,
            is_current_snapshot=True,
        )

    def snapshot(self, stock):
        return PointInventorySnapshot.objects.create(
            branch=self.branch,
            product=self.product,
            stock=Decimal(stock),
            captured_at=timezone.now(),
            sync_job=self.job,
        )

    def test_stock_positivo_muestra_solo_insumo_recibido_por_sucursal(self):
        self.recepcion(days_ago=30)
        self.snapshot("4.250")
        from mermas.services_insumos import insumos_elegibles_para_sucursal

        rows = insumos_elegibles_para_sucursal(self.sucursal)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].codigo_point, "INS-001")
        self.assertEqual(rows[0].unidad_point, "KG")
        self.assertEqual(rows[0].existencia, Decimal("4.250"))

    def test_stock_cero_permanece_siete_dias_pero_no_ocho(self):
        self.recepcion(days_ago=7)
        self.snapshot("0")
        from mermas.services_insumos import insumos_elegibles_para_sucursal

        self.assertEqual(len(insumos_elegibles_para_sucursal(self.sucursal)), 1)

        PointTransferLine.objects.update(received_at=timezone.now() - timezone.timedelta(days=8))
        self.assertEqual(insumos_elegibles_para_sucursal(self.sucursal), [])

    def test_conflicto_de_unidad_excluye_insumo(self):
        self.recepcion(days_ago=1, unit="KG")
        self.recepcion(days_ago=0, unit="PZA")
        self.snapshot("5")
        from mermas.services_insumos import insumos_elegibles_para_sucursal

        self.assertEqual(insumos_elegibles_para_sucursal(self.sucursal), [])


class SimulacionOrdenPointTests(TestCase):
    def setUp(self):
        from mermas.models import MermaInsumo, OrdenAjustePoint

        self.MermaInsumo = MermaInsumo
        self.OrdenAjustePoint = OrdenAjustePoint
        self.sucursal = Sucursal.objects.create(codigo="PAYAN", nombre="Payán")
        self.user = User.objects.create_user(username="encargada.simulacion")
        self.jefe = User.objects.create_user(username="jefe.simulacion")
        self.branch = PointBranch.objects.create(external_id="sim-2", name="Payán", erp_branch=self.sucursal)
        self.cedis = PointBranch.objects.create(external_id="sim-1", name="CEDIS")
        self.job = PointSyncJob.objects.create(status=PointSyncJob.STATUS_SUCCESS)
        self.product = PointProduct.objects.create(external_id="sim-p1", sku="INS-001", name="Fresa fresca")
        PointTransferLine.objects.create(
            origin_branch=self.cedis, destination_branch=self.branch, erp_destination_branch=self.sucursal,
            sync_job=self.job, transfer_external_id="SIM-T1", detail_external_id="SIM-D1",
            source_hash="sim-hash", registered_at=timezone.now(), received_at=timezone.now(),
            item_name="Fresa fresca", item_code="INS-001", unit="KG", received_quantity=Decimal("8"),
            is_insumo=True, is_received=True, is_current_snapshot=True,
        )
        self.merma = MermaInsumo.objects.create(
            sucursal=self.sucursal, reportado_por=self.user, jefe_inmediato=self.jefe,
            codigo_point="INS-001", nombre_point="Fresa fresca", unidad_point="KG",
            cantidad_reportada=Decimal("3"), cantidad_aprobada=Decimal("3"),
            motivo="CALIDAD", comentario="No apta", justificacion_sin_foto="Sin cámara",
            estatus=MermaInsumo.ESTATUS_APROBADA,
        )
        self.orden = OrdenAjustePoint.crear_desde_merma(self.merma)

    def snapshot(self, stock):
        return PointInventorySnapshot.objects.create(
            branch=self.branch, product=self.product, stock=Decimal(stock), sync_job=self.job
        )

    def test_simula_descuento_completo_sin_escribir_en_point_y_es_idempotente(self):
        self.snapshot("8")
        from mermas.services_insumos import simular_orden_ajuste_point

        first = simular_orden_ajuste_point(self.orden.pk)
        second = simular_orden_ajuste_point(self.orden.pk)

        self.assertEqual(first.estatus, self.OrdenAjustePoint.ESTATUS_SIMULADA)
        self.assertEqual(second.estatus, self.OrdenAjustePoint.ESTATUS_SIMULADA)
        self.assertEqual(second.existencia_antes, Decimal("8"))
        self.assertEqual(second.existencia_despues, Decimal("5"))
        self.assertEqual(second.intentos, 1)
        self.assertEqual(second.evidencia_tecnica["modo"], "SIMULACION")

    def test_stock_insuficiente_no_aplica_parcial_y_manda_a_revision(self):
        self.snapshot("2")
        from mermas.services_insumos import simular_orden_ajuste_point

        result = simular_orden_ajuste_point(self.orden.pk)

        self.assertEqual(result.estatus, self.OrdenAjustePoint.ESTATUS_REQUIERE_REVISION)
        self.assertIsNone(result.existencia_despues)
        self.merma.refresh_from_db()
        self.assertEqual(self.merma.estatus, self.MermaInsumo.ESTATUS_REQUIERE_REVISION)
        self.assertTrue(
            self.merma.eventos.filter(estado_nuevo=self.MermaInsumo.ESTATUS_REQUIERE_REVISION).exists()
        )
