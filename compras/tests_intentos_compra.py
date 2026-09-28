from decimal import Decimal

from django.core.exceptions import ValidationError
from django.core.files.uploadedfile import SimpleUploadedFile
from django.conf import settings
from django.db import IntegrityError, connection, transaction
from django.db.migrations.executor import MigrationExecutor
from django.test import TestCase, TransactionTestCase
from django.utils import timezone

from compras.models import (
    CompraRealizadaDepartamental,
    CompromisoCompraDepartamental,
    CotizacionCompraDepartamental,
    IntentoCompraDepartamental,
    LineaOrdenCompraDepartamental,
    OrdenCompraDepartamental,
    ReembolsoCompraDepartamental,
)
from compras.tests_edicion_compra import _CompraDepartamentalBase


class IntentoCompraModelTests(_CompraDepartamentalBase, TestCase):
    def crear_intento(self, estado=IntentoCompraDepartamental.ESTADO_VIGENTE):
        return IntentoCompraDepartamental.objects.create(
            item=self.item, cotizacion=self.quote, estado=estado,
        )

    def otra_cotizacion(self):
        otro_item = self.solicitud.items.create(descripcion="Artículo ajeno", cantidad=1)
        cotizacion = CotizacionCompraDepartamental.objects.create(
            item=otro_item, proveedor=self.proveedor,
            cantidad_ofertada=Decimal("1"), costo_unitario=Decimal("50"),
        )
        return otro_item, cotizacion

    def test_historial_numera_intentos_y_expone_unico_vigente(self):
        primero = self.crear_intento(IntentoCompraDepartamental.ESTADO_CANCELADO_SIN_PAGO)
        segundo = self.crear_intento()
        self.assertEqual([intento.numero for intento in self.item.intentos_compra.all()], [1, 2])
        self.assertEqual(self.item.intento_vigente, segundo)
        self.assertNotEqual(primero.pk, segundo.pk)
        segundo.estado = IntentoCompraDepartamental.ESTADO_ENTREGADO
        segundo.save(update_fields=["estado"])
        self.assertIsNone(self.item.intento_vigente)

    def test_no_admite_dos_intentos_vigentes_ni_numero_duplicado(self):
        primero = self.crear_intento()
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                self.crear_intento()
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                IntentoCompraDepartamental.objects.create(
                    item=self.item, cotizacion=self.quote,
                    estado=IntentoCompraDepartamental.ESTADO_REEMBOLSADO,
                    numero=primero.numero,
                )

    def test_estados_y_cotizacion_corresponden_al_item(self):
        self.assertEqual(
            {estado for estado, _ in IntentoCompraDepartamental.ESTADO_CHOICES},
            {"VIGENTE", "CANCELADO_SIN_PAGO", "REEMBOLSO_SOLICITADO", "REEMBOLSADO", "ENTREGADO"},
        )
        otro_item = self.solicitud.items.create(descripcion="Otro", cantidad=1)
        with self.assertRaises(ValidationError):
            IntentoCompraDepartamental(item=otro_item, cotizacion=self.quote).full_clean()

    def test_intento_create_y_save_rechazan_cotizacion_de_otro_item(self):
        _, cotizacion_ajena = self.otra_cotizacion()
        with self.assertRaises(ValidationError):
            IntentoCompraDepartamental.objects.create(item=self.item, cotizacion=cotizacion_ajena)
        intento = self.crear_intento()
        intento.cotizacion = cotizacion_ajena
        with self.assertRaises(ValidationError):
            intento.save(update_fields=["cotizacion"])

    def test_linea_create_y_save_rechazan_relaciones_cruzadas(self):
        intento = self.crear_intento()
        otro_item, cotizacion_ajena = self.otra_cotizacion()
        orden = OrdenCompraDepartamental.objects.create(proveedor=self.proveedor, creado_por=self.user)
        datos = dict(
            orden=orden, intento=intento, cantidad=Decimal("2"),
            costo_unitario=Decimal("100"), total=Decimal("200"),
        )
        with self.assertRaises(ValidationError):
            LineaOrdenCompraDepartamental.objects.create(item=otro_item, cotizacion=self.quote, **datos)
        linea = LineaOrdenCompraDepartamental.objects.create(
            item=self.item, cotizacion=self.quote, **datos,
        )
        linea.cotizacion = cotizacion_ajena
        with self.assertRaises(ValidationError):
            linea.save(update_fields=["cotizacion"])
        linea.total = Decimal("180")
        linea.save(update_fields=["total"])
        linea.refresh_from_db()
        self.assertEqual(linea.cotizacion, self.quote)
        self.assertEqual(linea.total, Decimal("180"))

    def test_compra_create_rechaza_cotizacion_de_otro_intento(self):
        intento = self.crear_intento()
        _, cotizacion_ajena = self.otra_cotizacion()
        with self.assertRaises(ValidationError):
            CompraRealizadaDepartamental.objects.create(
                intento=intento, item=self.item, cotizacion=cotizacion_ajena,
                fecha_compra=timezone.localdate(), importe_final=Decimal("200"),
                comprobante="compras/prueba.pdf", registrado_por=self.user,
            )

    def test_compromiso_create_rechaza_item_de_otro_intento(self):
        intento = self.crear_intento()
        otro_item, _ = self.otra_cotizacion()
        with self.assertRaises(ValidationError):
            CompromisoCompraDepartamental.objects.create(
                intento=intento, item=otro_item, cotizacion=self.quote,
                monto=Decimal("200"), activo=False,
            )

    def test_lineas_y_compras_historicas_se_vinculan_a_intentos_distintos(self):
        primero = self.crear_intento(IntentoCompraDepartamental.ESTADO_CANCELADO_SIN_PAGO)
        segundo = self.crear_intento()
        orden = OrdenCompraDepartamental.objects.create(proveedor=self.proveedor, creado_por=self.user)
        for intento in (primero, segundo):
            LineaOrdenCompraDepartamental.objects.create(
                orden=orden, intento=intento, item=self.item, cotizacion=self.quote,
                cantidad=Decimal("2"), costo_unitario=Decimal("100"), total=Decimal("200"),
            )
            CompraRealizadaDepartamental.objects.create(
                intento=intento, item=self.item, cotizacion=self.quote,
                fecha_compra=timezone.localdate(), importe_final=Decimal("200"),
                comprobante=SimpleUploadedFile("compra.pdf", b"%PDF-1.4\n%%EOF"),
                registrado_por=self.user,
            )
        self.assertEqual(self.item.lineas_orden.count(), 2)
        self.assertEqual(self.item.compras_realizadas.count(), 2)
        self.assertEqual(primero.linea_orden.item, segundo.linea_orden.item)
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                LineaOrdenCompraDepartamental.objects.create(
                    orden=orden, intento=segundo, item=self.item, cotizacion=self.quote,
                    cantidad=Decimal("2"), costo_unitario=Decimal("100"), total=Decimal("200"),
                )

    def test_reserva_preorden_es_opcional_y_solo_una_activa(self):
        reserva = CompromisoCompraDepartamental.objects.get(item=self.item)
        self.assertIsNone(reserva.intento)
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                CompromisoCompraDepartamental.objects.create(
                    item=self.item, cotizacion=self.quote, monto=Decimal("200"), activo=True,
                )
        reserva.activo = False
        reserva.save(update_fields=["activo"])
        self.assertIsNotNone(CompromisoCompraDepartamental.objects.create(
            item=self.item, cotizacion=self.quote, monto=Decimal("200"), activo=True,
        ).pk)

    def test_reembolsos_son_positivos_acumulables_e_inmutables(self):
        intento = self.crear_intento(IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO)
        intento.reembolso_solicitado = Decimal("1000")
        intento.reembolso_solicitado_en = timezone.localdate()
        intento.save(update_fields=["reembolso_solicitado", "reembolso_solicitado_en"])
        primero = ReembolsoCompraDepartamental.objects.create(
            intento=intento, importe=Decimal("400"), fecha=timezone.localdate(),
            registrado_por=self.user,
        )
        ReembolsoCompraDepartamental.objects.create(
            intento=intento, importe=Decimal("150"), fecha=timezone.localdate(),
            registrado_por=self.user,
        )
        self.assertEqual(intento.total_reembolsado, Decimal("550"))
        self.assertEqual(intento.saldo_reembolso, Decimal("450"))
        with self.assertRaises(ValidationError):
            ReembolsoCompraDepartamental.objects.create(
                intento=intento, importe=Decimal("0"), fecha=timezone.localdate(),
                registrado_por=self.user,
            )
        primero.referencia = "CAMBIO"
        with self.assertRaises(ValidationError):
            primero.save(update_fields=["referencia"])
        with self.assertRaises(ValidationError):
            primero.delete()
        with self.assertRaises(ValidationError):
            ReembolsoCompraDepartamental.objects.filter(pk=primero.pk).update(referencia="CAMBIO")
        with self.assertRaises(ValidationError):
            ReembolsoCompraDepartamental.objects.filter(pk=primero.pk).delete()

    def test_reembolso_no_excede_solicitud_y_permite_completarla(self):
        intento = self.crear_intento(IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO)
        intento.reembolso_solicitado = Decimal("1000")
        intento.save(update_fields=["reembolso_solicitado"])
        datos = dict(intento=intento, fecha=timezone.localdate(), registrado_por=self.user)
        ReembolsoCompraDepartamental.objects.create(importe=Decimal("400"), **datos)
        with self.assertRaises(ValidationError):
            ReembolsoCompraDepartamental.objects.create(importe=Decimal("700"), **datos)
        self.assertEqual(intento.total_reembolsado, Decimal("400"))
        ReembolsoCompraDepartamental.objects.create(importe=Decimal("600"), **datos)
        self.assertEqual(intento.total_reembolsado, Decimal("1000"))
        self.assertEqual(intento.saldo_reembolso, Decimal("0"))

    def test_reembolso_rechaza_solicitud_ausente(self):
        intento = self.crear_intento(IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO)
        with self.assertRaises(ValidationError):
            ReembolsoCompraDepartamental.objects.create(
                intento=intento, importe=Decimal("100"), fecha=timezone.localdate(),
                registrado_por=self.user,
            )


class MigracionIntentosCompraTests(TransactionTestCase):
    migrate_from = ("compras", "0015_comprarealizadadepartamental_version_and_more")
    migrate_to = ("compras", "0016_intentos_compra_reembolsos")

    def setUp(self):
        super().setUp()
        self.executor = MigrationExecutor(connection)
        self.executor.migrate([self.migrate_from])
        self.apps_0015 = self.executor.loader.project_state([self.migrate_from]).apps

    def tearDown(self):
        MigrationExecutor(connection).migrate([self.migrate_to])
        super().tearDown()

    def _datos_base(self):
        Usuario = self.apps_0015.get_model(*settings.AUTH_USER_MODEL.split("."))
        Area = self.apps_0015.get_model("reportes", "AreaPresupuesto")
        Proveedor = self.apps_0015.get_model("maestros", "Proveedor")
        Solicitud = self.apps_0015.get_model("compras", "SolicitudCompraDepartamental")
        user = Usuario.objects.create(username="migracion-compras")
        area = Area.objects.create(nombre="Migración compras", codigo="MIG_COMP")
        proveedor = Proveedor.objects.create(nombre="Proveedor histórico")
        solicitud = Solicitud.objects.create(
            folio="SCD-MIG-0001", area=area, solicitante=user,
            periodo=timezone.localdate().replace(day=1), estado="EN_ATENCION",
        )
        return user, proveedor, solicitud

    def _crear_linea(self, *, solicitud, proveedor, user, estado, folio):
        Item = self.apps_0015.get_model("compras", "ItemCompraDepartamental")
        Cotizacion = self.apps_0015.get_model("compras", "CotizacionCompraDepartamental")
        Orden = self.apps_0015.get_model("compras", "OrdenCompraDepartamental")
        Linea = self.apps_0015.get_model("compras", "LineaOrdenCompraDepartamental")
        Compromiso = self.apps_0015.get_model("compras", "CompromisoCompraDepartamental")
        item = Item.objects.create(solicitud=solicitud, descripcion=folio, estado=estado, cantidad=2)
        cotizacion = Cotizacion.objects.create(
            item=item, proveedor=proveedor, cantidad_ofertada=2,
            costo_unitario=Decimal("100"), seleccionada=True,
        )
        orden = Orden.objects.create(folio=folio, proveedor=proveedor, creado_por=user)
        linea = Linea.objects.create(
            orden=orden, item=item, cotizacion=cotizacion,
            cantidad=2, costo_unitario=Decimal("100"), total=Decimal("200"),
        )
        compromiso = Compromiso.objects.create(
            item=item, cotizacion=cotizacion, monto=Decimal("200"), activo=True,
            formalizado_en=timezone.now(),
        )
        return item, cotizacion, linea, compromiso

    def test_backfill_conserva_compra_compromisos_y_clasifica_entrega(self):
        user, proveedor, solicitud = self._datos_base()
        entregado, cotizacion, linea_entregada, compromiso = self._crear_linea(
            solicitud=solicitud, proveedor=proveedor, user=user,
            estado="PENDIENTE_CONFIRMACION", folio="OCD-MIG-1",
        )
        pendiente, _, linea_pendiente, _ = self._crear_linea(
            solicitud=solicitud, proveedor=proveedor, user=user,
            estado="COMPRADO", folio="OCD-MIG-2",
        )
        Compra = self.apps_0015.get_model("compras", "CompraRealizadaDepartamental")
        compra = Compra.objects.create(
            item=entregado, cotizacion=cotizacion, fecha_compra=timezone.localdate(),
            importe_final=Decimal("190"), comprobante="compras/historico.pdf", registrado_por=user,
        )
        Item = self.apps_0015.get_model("compras", "ItemCompraDepartamental")
        Cotizacion = self.apps_0015.get_model("compras", "CotizacionCompraDepartamental")
        Compromiso = self.apps_0015.get_model("compras", "CompromisoCompraDepartamental")
        preorden = Item.objects.create(solicitud=solicitud, descripcion="Reserva", estado="AUTORIZADO")
        cot_preorden = Cotizacion.objects.create(
            item=preorden, proveedor=proveedor, cantidad_ofertada=1, costo_unitario=Decimal("50"),
        )
        reserva = Compromiso.objects.create(
            item=preorden, cotizacion=cot_preorden, monto=Decimal("50"), activo=True,
        )

        self.executor = MigrationExecutor(connection)
        self.executor.migrate([self.migrate_to])
        apps = self.executor.loader.project_state([self.migrate_to]).apps
        Intento = apps.get_model("compras", "IntentoCompraDepartamental")
        LineaNueva = apps.get_model("compras", "LineaOrdenCompraDepartamental")
        CompraNueva = apps.get_model("compras", "CompraRealizadaDepartamental")
        CompromisoNuevo = apps.get_model("compras", "CompromisoCompraDepartamental")
        intento_entregado = Intento.objects.get(item_id=entregado.pk)
        intento_pendiente = Intento.objects.get(item_id=pendiente.pk)
        self.assertEqual((intento_entregado.estado, intento_entregado.numero), ("ENTREGADO", 1))
        self.assertEqual((intento_pendiente.estado, intento_pendiente.numero), ("VIGENTE", 1))
        self.assertEqual(intento_entregado.cotizacion_id, cotizacion.pk)
        self.assertEqual(LineaNueva.objects.get(pk=linea_entregada.pk).intento_id, intento_entregado.pk)
        self.assertEqual(LineaNueva.objects.get(pk=linea_pendiente.pk).intento_id, intento_pendiente.pk)
        self.assertEqual(CompraNueva.objects.get(pk=compra.pk).intento_id, intento_entregado.pk)
        self.assertEqual(CompraNueva.objects.get(pk=compra.pk).importe_final, Decimal("190"))
        self.assertEqual(CompromisoNuevo.objects.get(pk=compromiso.pk).intento_id, intento_entregado.pk)
        self.assertIsNone(CompromisoNuevo.objects.get(pk=reserva.pk).intento_id)

        MigrationExecutor(connection).migrate([self.migrate_from])
        MigrationExecutor(connection).migrate([self.migrate_to])
        self.assertEqual(Intento.objects.count(), 2)

    def test_backfill_rechaza_compra_sin_linea_antes_de_crear_intentos(self):
        user, proveedor, solicitud = self._datos_base()
        Item = self.apps_0015.get_model("compras", "ItemCompraDepartamental")
        Cotizacion = self.apps_0015.get_model("compras", "CotizacionCompraDepartamental")
        Compra = self.apps_0015.get_model("compras", "CompraRealizadaDepartamental")
        item = Item.objects.create(solicitud=solicitud, descripcion="Sin orden")
        cotizacion = Cotizacion.objects.create(
            item=item, proveedor=proveedor, cantidad_ofertada=1, costo_unitario=Decimal("100"),
        )
        compra = Compra.objects.create(
            item=item, cotizacion=cotizacion, fecha_compra=timezone.localdate(),
            importe_final=Decimal("100"), comprobante="compras/historico.pdf", registrado_por=user,
        )
        with self.assertRaisesMessage(RuntimeError, f"compra departamental {compra.pk}"):
            MigrationExecutor(connection).migrate([self.migrate_to])
        compra.delete()

    def test_backfill_rechaza_compra_con_otra_cotizacion_del_mismo_item(self):
        user, proveedor, solicitud = self._datos_base()
        item, _, _, _ = self._crear_linea(
            solicitud=solicitud, proveedor=proveedor, user=user,
            estado="COMPRADO", folio="OCD-MIG-6",
        )
        Cotizacion = self.apps_0015.get_model("compras", "CotizacionCompraDepartamental")
        Compra = self.apps_0015.get_model("compras", "CompraRealizadaDepartamental")
        alternativa = Cotizacion.objects.create(
            item=item, proveedor=proveedor, cantidad_ofertada=2, costo_unitario=Decimal("90"),
        )
        compra = Compra.objects.create(
            item=item, cotizacion=alternativa, fecha_compra=timezone.localdate(),
            importe_final=Decimal("180"), comprobante="compras/historico.pdf", registrado_por=user,
        )
        with self.assertRaisesMessage(RuntimeError, f"compra {compra.pk}"):
            MigrationExecutor(connection).migrate([self.migrate_to])
        compra.delete()

    def test_backfill_rechaza_compromiso_con_otra_cotizacion_del_mismo_item(self):
        user, proveedor, solicitud = self._datos_base()
        item, cotizacion, _, compromiso = self._crear_linea(
            solicitud=solicitud, proveedor=proveedor, user=user,
            estado="ORDENADO", folio="OCD-MIG-7",
        )
        Cotizacion = self.apps_0015.get_model("compras", "CotizacionCompraDepartamental")
        Compromiso = self.apps_0015.get_model("compras", "CompromisoCompraDepartamental")
        alternativa = Cotizacion.objects.create(
            item=item, proveedor=proveedor, cantidad_ofertada=2, costo_unitario=Decimal("90"),
        )
        Compromiso.objects.filter(pk=compromiso.pk).update(cotizacion_id=alternativa.pk)
        with self.assertRaisesMessage(RuntimeError, f"compromiso {compromiso.pk}"):
            MigrationExecutor(connection).migrate([self.migrate_to])
        Compromiso.objects.filter(pk=compromiso.pk).update(cotizacion_id=cotizacion.pk)

    def test_reversa_rechaza_reembolso_nuevo_que_0015_no_puede_conservar(self):
        user, proveedor, solicitud = self._datos_base()
        item, _, _, _ = self._crear_linea(
            solicitud=solicitud, proveedor=proveedor, user=user,
            estado="ORDENADO", folio="OCD-MIG-3",
        )
        executor = MigrationExecutor(connection)
        executor.migrate([self.migrate_to])
        apps = executor.loader.project_state([self.migrate_to]).apps
        Intento = apps.get_model("compras", "IntentoCompraDepartamental")
        Reembolso = apps.get_model("compras", "ReembolsoCompraDepartamental")
        reembolso = Reembolso.objects.create(
            intento=Intento.objects.get(item_id=item.pk), importe=Decimal("10"),
            fecha=timezone.localdate(), registrado_por_id=user.pk,
        )
        with self.assertRaisesMessage(RuntimeError, "reembolsos"):
            MigrationExecutor(connection).migrate([self.migrate_from])
        Reembolso.objects.filter(pk=reembolso.pk).delete()

    def test_reversa_rechaza_estado_entregado_que_item_no_reconstruye(self):
        user, proveedor, solicitud = self._datos_base()
        item, _, _, _ = self._crear_linea(
            solicitud=solicitud, proveedor=proveedor, user=user,
            estado="COMPRADO", folio="OCD-MIG-4",
        )
        executor = MigrationExecutor(connection)
        executor.migrate([self.migrate_to])
        Intento = executor.loader.project_state([self.migrate_to]).apps.get_model(
            "compras", "IntentoCompraDepartamental"
        )
        Intento.objects.filter(item_id=item.pk).update(estado="ENTREGADO")
        with self.assertRaisesMessage(RuntimeError, "estado"):
            MigrationExecutor(connection).migrate([self.migrate_from])

    def test_reversa_rechaza_numero_que_backfill_no_reconstruye(self):
        user, proveedor, solicitud = self._datos_base()
        item, _, _, _ = self._crear_linea(
            solicitud=solicitud, proveedor=proveedor, user=user,
            estado="COMPRADO", folio="OCD-MIG-5",
        )
        executor = MigrationExecutor(connection)
        executor.migrate([self.migrate_to])
        Intento = executor.loader.project_state([self.migrate_to]).apps.get_model(
            "compras", "IntentoCompraDepartamental"
        )
        Intento.objects.filter(item_id=item.pk).update(numero=2)
        with self.assertRaisesMessage(RuntimeError, "número"):
            MigrationExecutor(connection).migrate([self.migrate_from])
