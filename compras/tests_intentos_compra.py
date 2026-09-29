from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from django.core.exceptions import ValidationError
from django.core.files.base import ContentFile
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
    RecepcionItemDepartamental,
    EventoCompraDepartamental,
    ItemCompraDepartamental,
)
from compras.tests_edicion_compra import _CompraDepartamentalBase
from compras.services_departamentales import generar_ordenes_departamentales
from compras.services_intentos_compra import (
    cancelar_articulo_definitivamente, cancelar_intento_compra, registrar_reembolso_compra,
)
from compras.forms_intentos_compra import CancelarIntentoCompraForm, RegistrarReembolsoCompraForm


class OperacionesIntentoCompraTests(_CompraDepartamentalBase, TestCase):
    def _intento(self, *, pagado=False):
        generar_ordenes_departamentales([self.item], actor=self.user)
        intento = self.item.intento_vigente
        if pagado:
            CompraRealizadaDepartamental.objects.create(
                intento=intento, item=self.item, cotizacion=self.quote,
                fecha_compra=timezone.localdate(), importe_final=Decimal('180'),
                comprobante='compras/compra.pdf', registrado_por=self.user,
            )
            self.item.estado = ItemCompraDepartamental.ESTADO_COMPRADO
            self.item.save(update_fields=['estado'])
        return intento

    def _cancelar(self, intento, **kwargs):
        return cancelar_intento_compra(
            intento, version=kwargs.pop('version', intento.version),
            motivo=kwargs.pop('motivo', IntentoCompraDepartamental.MOTIVO_NO_ENTREGO),
            detalle=kwargs.pop('detalle', 'Proveedor confirmó que no entregará.'),
            actor=self.user, **kwargs,
        )

    def _solicitar_reembolso(self):
        intento = self._intento(pagado=True)
        self._cancelar(intento, reembolso_solicitado_en=timezone.localdate(),
                       reembolso_solicitado=Decimal('180'))
        intento.refresh_from_db()
        return intento

    def test_cancelar_sin_pago_libera_solo_su_compromiso_y_reabre_item(self):
        intento = self._intento()
        compromiso = CompromisoCompraDepartamental.objects.get(intento=intento)
        otro_item = self.solicitud.items.create(descripcion='Otro artículo', cantidad=1)
        otra_cotizacion = CotizacionCompraDepartamental.objects.create(
            item=otro_item, proveedor=self.proveedor,
            cantidad_ofertada=Decimal('1'), costo_unitario=Decimal('50'),
        )
        otro_compromiso = CompromisoCompraDepartamental.objects.create(
            item=otro_item, cotizacion=otra_cotizacion, monto=Decimal('50'), activo=True,
        )
        self._cancelar(intento)
        intento.refresh_from_db(); compromiso.refresh_from_db(); otro_compromiso.refresh_from_db()
        self.item.refresh_from_db(); self.quote.refresh_from_db(); self.solicitud.refresh_from_db()
        self.assertEqual((intento.estado, intento.version), ('CANCELADO_SIN_PAGO', 2))
        self.assertFalse(compromiso.activo)
        self.assertTrue(otro_compromiso.activo)
        self.assertIsNotNone(compromiso.liberado_en)
        self.assertFalse(self.quote.seleccionada)
        self.assertEqual((self.item.estado, self.item.siguiente_responsable), ('POR_COTIZAR', 'COMPRAS'))
        self.assertEqual(self.solicitud.estado, 'EN_ATENCION')
        self.assertTrue(EventoCompraDepartamental.objects.filter(item=self.item, tipo='INTENTO_CANCELADO').exists())
        self.assertTrue(LineaOrdenCompraDepartamental.objects.filter(intento=intento).exists())

    def test_cancelar_pagada_conserva_compromiso_y_registra_solicitud(self):
        intento = self._intento(pagado=True)
        compromiso = CompromisoCompraDepartamental.objects.get(intento=intento)
        self._cancelar(intento, reembolso_solicitado_en=timezone.localdate(),
                       reembolso_solicitado=Decimal('170'),
                       evidencia_solicitud_reembolso='compras/solicitud.pdf')
        intento.refresh_from_db(); compromiso.refresh_from_db(); self.item.refresh_from_db(); self.quote.refresh_from_db()
        self.assertEqual((intento.estado, intento.version), ('REEMBOLSO_SOLICITADO', 2))
        self.assertEqual(intento.reembolso_solicitado, Decimal('170'))
        self.assertEqual(intento.reembolso_solicitado_en, timezone.localdate())
        self.assertTrue(intento.evidencia_solicitud_reembolso)
        self.assertTrue(compromiso.activo)
        self.assertFalse(self.quote.seleccionada)
        self.assertEqual(self.item.estado, 'POR_COTIZAR')
        eventos = list(EventoCompraDepartamental.objects.filter(item=self.item).order_by('-pk')[:2])
        self.assertEqual({evento.tipo for evento in eventos}, {'INTENTO_CANCELADO', 'REEMBOLSO_SOLICITADO'})
        solicitud_evento = next(evento for evento in eventos if evento.tipo == 'REEMBOLSO_SOLICITADO')
        self.assertIn(timezone.localdate().isoformat(), solicitud_evento.detalle)
        self.assertIn('170', solicitud_evento.detalle)

    def test_cancelacion_actualiza_comentario_reciente_normalizado(self):
        intento = self._intento()
        self._cancelar(intento, detalle='  Proveedor no entregará.  ')
        self.item.refresh_from_db()
        self.assertEqual(self.item.comentario_reciente, 'Proveedor no entregó: Proveedor no entregará.')

    def test_solicitud_reembolso_rechaza_mas_de_dos_decimales_sin_mutar(self):
        intento = self._intento(pagado=True)
        compromiso = CompromisoCompraDepartamental.objects.get(intento=intento)
        with self.assertRaises(ValidationError):
            self._cancelar(intento, reembolso_solicitado_en=timezone.localdate(),
                           reembolso_solicitado=Decimal('100.005'))
        intento.refresh_from_db(); compromiso.refresh_from_db(); self.item.refresh_from_db()
        self.assertEqual((intento.estado, intento.version), ('VIGENTE', 1))
        self.assertIsNone(intento.reembolso_solicitado)
        self.assertTrue(compromiso.activo)
        self.assertEqual(self.item.estado, 'COMPRADO')

    def test_reembolso_rechaza_fraccion_de_centavo_sin_mutar(self):
        intento = self._intento(pagado=True)
        self._cancelar(intento, reembolso_solicitado_en=timezone.localdate(),
                       reembolso_solicitado=Decimal('100.00'))
        intento.refresh_from_db()
        compromiso = CompromisoCompraDepartamental.objects.get(intento=intento)
        with self.assertRaises(ValidationError):
            registrar_reembolso_compra(intento, version=2, fecha=timezone.localdate(),
                                      importe=Decimal('99.995'), actor=self.user)
        intento.refresh_from_db(); compromiso.refresh_from_db()
        self.assertEqual((intento.estado, intento.version), ('REEMBOLSO_SOLICITADO', 2))
        self.assertEqual(intento.reembolsos.count(), 0)
        self.assertTrue(compromiso.activo)

    def test_rollback_cancelacion_borra_solo_evidencia_nueva(self):
        intento = self._intento(pagado=True)
        campo = IntentoCompraDepartamental._meta.get_field('evidencia_solicitud_reembolso')
        compartido = campo.storage.save(
            campo.generate_filename(intento, 'solicitud.pdf'), ContentFile(b'archivo previo'),
        )
        evidencia = SimpleUploadedFile('solicitud.pdf', b'%PDF-1.4\n%%EOF')
        with patch('compras.services_intentos_compra.EventoCompraDepartamental.objects.create',
                   side_effect=RuntimeError('evento falló')):
            with self.assertRaisesMessage(RuntimeError, 'evento falló'):
                self._cancelar(intento, reembolso_solicitado_en=timezone.localdate(),
                               reembolso_solicitado=Decimal('100'),
                               evidencia_solicitud_reembolso=evidencia)
        intento.refresh_from_db(); self.item.refresh_from_db()
        self.assertEqual((intento.estado, intento.version), ('VIGENTE', 1))
        self.assertEqual(self.item.estado, 'COMPRADO')
        self.assertEqual(
            {str(path.relative_to(self.media.name)) for path in Path(self.media.name).rglob('*') if path.is_file()},
            {compartido},
        )

    def test_rollback_reembolso_borra_solo_comprobante_nuevo(self):
        intento = self._solicitar_reembolso()
        compromiso = CompromisoCompraDepartamental.objects.get(intento=intento)
        comprobante = SimpleUploadedFile('devolucion.pdf', b'%PDF-1.4\n%%EOF')
        with patch('compras.services_intentos_compra.EventoCompraDepartamental.objects.create',
                   side_effect=RuntimeError('evento falló')):
            with self.assertRaisesMessage(RuntimeError, 'evento falló'):
                registrar_reembolso_compra(intento, version=2, fecha=timezone.localdate(),
                                          importe=Decimal('180'), comprobante=comprobante,
                                          actor=self.user)
        intento.refresh_from_db(); compromiso.refresh_from_db()
        self.assertEqual((intento.estado, intento.version), ('REEMBOLSO_SOLICITADO', 2))
        self.assertEqual(intento.reembolsos.count(), 0)
        self.assertTrue(compromiso.activo)
        self.assertFalse(any(path.is_file() for path in Path(self.media.name).rglob('*')))

    def test_cancelacion_rechaza_recepcion_estado_version_y_datos_invalidos_sin_mutar(self):
        intento = self._intento()
        linea = LineaOrdenCompraDepartamental.objects.get(intento=intento)
        for changes in ({'version': 0}, {'motivo': 'INVENTADO'}, {'detalle': '  '},
                        {'reembolso_solicitado': Decimal('20')}):
            with self.subTest(changes=changes), self.assertRaises(ValidationError):
                self._cancelar(intento, **changes)
        self.assertEqual(IntentoCompraDepartamental.objects.get(pk=intento.pk).version, 1)
        RecepcionItemDepartamental.objects.create(
            linea_orden=linea, cantidad_recibida=Decimal('1'), registrado_por=self.user,
        )
        with self.assertRaises(ValidationError):
            self._cancelar(intento)
        self.assertEqual(IntentoCompraDepartamental.objects.get(pk=intento.pk).estado, 'VIGENTE')

    def test_cancelacion_pagada_rechaza_fechas_e_importes_invalidos(self):
        intento = self._intento(pagado=True)
        for changes in ({}, {'reembolso_solicitado_en': timezone.localdate()},
                        {'reembolso_solicitado': Decimal('10')},
                        {'reembolso_solicitado_en': timezone.localdate() + timedelta(days=1), 'reembolso_solicitado': Decimal('10')},
                        {'reembolso_solicitado_en': timezone.localdate(), 'reembolso_solicitado': Decimal('0')},
                        {'reembolso_solicitado_en': timezone.localdate(), 'reembolso_solicitado': Decimal('181')}):
            with self.subTest(changes=changes), self.assertRaises(ValidationError):
                self._cancelar(intento, **changes)
        self.assertEqual(IntentoCompraDepartamental.objects.get(pk=intento.pk).estado, 'VIGENTE')

    def test_reembolso_parcial_y_total_solo_liberan_al_completar(self):
        intento = self._solicitar_reembolso()
        compromiso = CompromisoCompraDepartamental.objects.get(intento=intento)
        primero = registrar_reembolso_compra(
            intento, version=2, fecha=timezone.localdate(), importe=Decimal('80'),
            referencia='Transferencia 1', actor=self.user,
        )
        intento.refresh_from_db(); compromiso.refresh_from_db()
        self.assertEqual((intento.estado, intento.version), ('REEMBOLSO_SOLICITADO', 3))
        self.assertTrue(compromiso.activo)
        self.assertEqual(intento.saldo_reembolso, Decimal('100'))
        with self.assertRaises(ValidationError):
            registrar_reembolso_compra(intento, version=2, fecha=timezone.localdate(),
                                      importe=Decimal('80'), actor=self.user)
        self.assertEqual(intento.reembolsos.count(), 1)
        segundo = registrar_reembolso_compra(intento, version=3, fecha=timezone.localdate(),
                                             importe=Decimal('100'), actor=self.user)
        intento.refresh_from_db(); compromiso.refresh_from_db()
        self.assertEqual((intento.estado, intento.version), ('REEMBOLSADO', 4))
        self.assertFalse(compromiso.activo)
        self.assertEqual(intento.total_reembolsado, Decimal('180'))
        self.assertNotEqual(primero.pk, segundo.pk)
        self.assertEqual(EventoCompraDepartamental.objects.filter(item=self.item, tipo='REEMBOLSO_RECIBIDO').count(), 2)

    def test_reembolso_rechaza_sobrepago_fecha_futura_y_estado(self):
        intento = self._solicitar_reembolso()
        for changes in ({'importe': Decimal('181')}, {'importe': Decimal('0')},
                        {'fecha': timezone.localdate() + timedelta(days=1)}):
            datos = {'version': 2, 'fecha': timezone.localdate(), 'importe': Decimal('1'), 'actor': self.user, **changes}
            with self.subTest(datos=datos), self.assertRaises(ValidationError):
                registrar_reembolso_compra(intento, **datos)
        self.assertEqual(intento.reembolsos.count(), 0)
        otro = IntentoCompraDepartamental.objects.get(pk=intento.pk)
        otro.estado = IntentoCompraDepartamental.ESTADO_REEMBOLSADO
        otro.save(update_fields=['estado'])
        with self.assertRaises(ValidationError):
            registrar_reembolso_compra(intento, version=2, fecha=timezone.localdate(),
                                      importe=Decimal('1'), actor=self.user)

    def test_cancelar_articulo_definitivo_conserva_historial_y_exige_saldo_cero(self):
        intento = self._solicitar_reembolso()
        with self.assertRaises(ValidationError):
            cancelar_articulo_definitivamente(self.item, motivo='Ya no se necesita', actor=self.user)
        registrar_reembolso_compra(intento, version=2, fecha=timezone.localdate(),
                                  importe=Decimal('180'), actor=self.user)
        cancelar_articulo_definitivamente(self.item, motivo='Ya no se necesita', actor=self.user)
        self.item.refresh_from_db(); self.solicitud.refresh_from_db()
        self.assertEqual((self.item.estado, self.item.siguiente_responsable), ('CANCELADO', 'NADIE'))
        self.assertEqual(self.solicitud.estado, 'COMPLETADA')
        self.assertTrue(CompraRealizadaDepartamental.objects.filter(intento=intento).exists())
        self.assertTrue(EventoCompraDepartamental.objects.filter(item=self.item, tipo='ARTICULO_CANCELADO').exists())

    def test_cancelar_articulo_rechaza_motivo_vacio_intento_vigente_y_recepcion(self):
        intento = self._intento()
        with self.assertRaises(ValidationError):
            cancelar_articulo_definitivamente(self.item, motivo=' ', actor=self.user)
        with self.assertRaises(ValidationError):
            cancelar_articulo_definitivamente(self.item, motivo='No procede', actor=self.user)
        linea = LineaOrdenCompraDepartamental.objects.get(intento=intento)
        RecepcionItemDepartamental.objects.create(
            linea_orden=linea, cantidad_recibida=Decimal('1'), registrado_por=self.user,
        )
        # La recepción histórica bloquea el cierre incluso si el estado del intento
        # ya fue corregido por una conciliación externa.
        IntentoCompraDepartamental.objects.filter(pk=intento.pk).update(
            estado=IntentoCompraDepartamental.ESTADO_CANCELADO_SIN_PAGO,
        )
        with self.assertRaises(ValidationError):
            cancelar_articulo_definitivamente(self.item, motivo='No procede', actor=self.user)
        self.item.refresh_from_db()
        self.assertNotEqual(self.item.estado, 'CANCELADO')

    def test_cancelacion_sin_pago_tolera_orden_legacy_sin_compromiso(self):
        intento = self._intento()
        CompromisoCompraDepartamental.objects.get(intento=intento).delete()
        self._cancelar(intento)
        intento.refresh_from_db()
        self.assertEqual(intento.estado, 'CANCELADO_SIN_PAGO')

    def test_cancelacion_pagada_exige_compromiso_financiero(self):
        intento = self._intento(pagado=True)
        CompromisoCompraDepartamental.objects.get(intento=intento).delete()
        with self.assertRaisesMessage(ValidationError, 'compromiso activo'):
            self._cancelar(intento, reembolso_solicitado_en=timezone.localdate(),
                           reembolso_solicitado=Decimal('180'))
        intento.refresh_from_db()
        self.assertEqual(intento.estado, 'VIGENTE')

    def test_cancelacion_rechaza_recepcion_total(self):
        intento = self._intento()
        linea = LineaOrdenCompraDepartamental.objects.get(intento=intento)
        RecepcionItemDepartamental.objects.create(
            linea_orden=linea, cantidad_recibida=linea.cantidad, registrado_por=self.user,
        )
        with self.assertRaises(ValidationError):
            self._cancelar(intento)
        self.assertEqual(IntentoCompraDepartamental.objects.get(pk=intento.pk).estado, 'ENTREGADO')


class FormulariosIntentoCompraTests(_CompraDepartamentalBase, TestCase):
    def _intento(self, *, pagado=False):
        generar_ordenes_departamentales([self.item], actor=self.user)
        intento = self.item.intento_vigente
        if pagado:
            CompraRealizadaDepartamental.objects.create(
                intento=intento, item=self.item, cotizacion=self.quote,
                fecha_compra=timezone.localdate(), importe_final=Decimal('180'),
                comprobante='compras/compra.pdf', registrado_por=self.user,
            )
        return intento

    def test_cancelar_form_oculta_reembolso_sin_compra_y_valida_campos(self):
        intento = self._intento()
        datos = {'version': '1', 'motivo': 'NO_ENTREGO', 'detalle': 'Proveedor no entregó'}
        form = CancelarIntentoCompraForm(data=datos, intento=intento)
        self.assertTrue(form.is_valid(), form.errors)
        self.assertNotIn('reembolso_solicitado', form.fields)
        self.assertFalse(CancelarIntentoCompraForm(data={**datos, 'motivo': 'OTRO', 'detalle': ' '}, intento=intento).is_valid())

    def test_cancelar_form_pagado_exige_solicitud_y_valida_comprobante(self):
        intento = self._intento(pagado=True)
        datos = {'version': '1', 'motivo': 'NO_ENTREGO', 'detalle': 'Proveedor no entregó',
                 'reembolso_solicitado_en': timezone.localdate().isoformat(), 'reembolso_solicitado': '180'}
        form = CancelarIntentoCompraForm(data=datos, intento=intento)
        self.assertTrue(form.is_valid(), form.errors)
        self.assertIn('reembolso_solicitado', form.fields)
        for cambios in ({'reembolso_solicitado': '181'},
                        {'reembolso_solicitado': '100.005'},
                        {'reembolso_solicitado_en': (timezone.localdate() + timedelta(days=1)).isoformat()},
                        {'reembolso_solicitado': ''}):
            invalido = CancelarIntentoCompraForm(data={**datos, **cambios}, intento=intento)
            self.assertFalse(invalido.is_valid())
            self.assertEqual(invalido.data['detalle'], datos['detalle'])
        archivo = SimpleUploadedFile('falso.pdf', b'no es PDF')
        invalido = CancelarIntentoCompraForm(data=datos, files={'evidencia_solicitud_reembolso': archivo}, intento=intento)
        self.assertFalse(invalido.is_valid())
        self.assertIn('evidencia_solicitud_reembolso', invalido.errors)

    def test_reembolso_form_exige_saldo_y_valida_archivo_y_fecha(self):
        intento = self._intento(pagado=True)
        intento.reembolso_solicitado = Decimal('180')
        intento.reembolso_solicitado_en = timezone.localdate()
        intento.estado = IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO
        intento.save(update_fields=['reembolso_solicitado', 'reembolso_solicitado_en', 'estado'])
        datos = {'version': '1', 'fecha': timezone.localdate().isoformat(), 'importe': '80', 'referencia': 'Banco'}
        form = RegistrarReembolsoCompraForm(data=datos, intento=intento)
        self.assertTrue(form.is_valid(), form.errors)
        self.assertFalse(RegistrarReembolsoCompraForm(data={**datos, 'importe': '181'}, intento=intento).is_valid())
        self.assertFalse(RegistrarReembolsoCompraForm(data={**datos, 'importe': '99.995'}, intento=intento).is_valid())
        self.assertFalse(RegistrarReembolsoCompraForm(data={**datos, 'fecha': (timezone.localdate() + timedelta(days=1)).isoformat()}, intento=intento).is_valid())
        invalido = RegistrarReembolsoCompraForm(data=datos, files={'comprobante': SimpleUploadedFile('falso.pdf', b'no es PDF')}, intento=intento)
        self.assertFalse(invalido.is_valid())
        self.assertIn('comprobante', invalido.errors)
        self.assertEqual(invalido.data['referencia'], 'Banco')


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

    def test_update_fields_generador_persiste_linea_compra_y_compromiso(self):
        intento = self.crear_intento()
        orden = OrdenCompraDepartamental.objects.create(proveedor=self.proveedor, creado_por=self.user)
        linea = LineaOrdenCompraDepartamental.objects.create(
            orden=orden, intento=intento, item=self.item, cotizacion=self.quote,
            cantidad=Decimal("2"), costo_unitario=Decimal("100"), total=Decimal("200"),
        )
        compromiso = CompromisoCompraDepartamental.objects.get(item=self.item)
        compromiso.intento = intento
        compromiso.save(update_fields=["intento"])
        compra = CompraRealizadaDepartamental.objects.create(
            intento=intento, item=self.item, cotizacion=self.quote,
            fecha_compra=timezone.localdate(), importe_final=Decimal("200"),
            comprobante="compras/prueba.pdf", registrado_por=self.user,
        )
        for obj, campo, valor in (
            (linea, "total", Decimal("190")),
            (compromiso, "monto", Decimal("190")),
            (compra, "importe_final", Decimal("190")),
        ):
            setattr(obj, campo, valor)
            obj.save(update_fields=(nombre for nombre in [campo]))
            obj.refresh_from_db()
            self.assertEqual(getattr(obj, campo), valor)

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

    def test_solicitud_no_puede_bajar_de_reembolso_ya_recibido(self):
        intento = self.crear_intento(IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO)
        intento.reembolso_solicitado = Decimal("1000")
        intento.save(update_fields=["reembolso_solicitado"])
        ReembolsoCompraDepartamental.objects.create(
            intento=intento, importe=Decimal("600"), fecha=timezone.localdate(),
            registrado_por=self.user,
        )
        intento.reembolso_solicitado = Decimal("500")
        with self.assertRaises(ValidationError):
            intento.save(update_fields=["reembolso_solicitado"])
        intento.refresh_from_db()
        self.assertEqual(intento.reembolso_solicitado, Decimal("1000"))
        intento.reembolso_solicitado = Decimal("600")
        intento.save(update_fields=["reembolso_solicitado"])
        intento.refresh_from_db()
        self.assertEqual(intento.reembolso_solicitado, Decimal("600"))

    def test_solicitud_omitida_en_update_fields_usa_valor_persistido(self):
        intento = self.crear_intento(IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO)
        intento.reembolso_solicitado = Decimal("1000")
        intento.save(update_fields=["reembolso_solicitado"])
        ReembolsoCompraDepartamental.objects.create(
            intento=intento, importe=Decimal("600"), fecha=timezone.localdate(),
            registrado_por=self.user,
        )
        intento.reembolso_solicitado = Decimal("500")
        intento.estado = IntentoCompraDepartamental.ESTADO_REEMBOLSADO
        intento.save(update_fields=["estado"])
        intento.refresh_from_db()
        self.assertEqual(intento.reembolso_solicitado, Decimal("1000"))

    def test_reembolso_bloquea_escrituras_masivas_incluido_update_conflicts(self):
        intento = self.crear_intento(IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO)
        datos = dict(intento=intento, importe=Decimal("100"), fecha=timezone.localdate(), registrado_por=self.user)
        nuevo = ReembolsoCompraDepartamental(**datos)
        with self.assertRaises(ValidationError):
            ReembolsoCompraDepartamental.objects.bulk_create([nuevo])
        with self.assertRaises(ValidationError):
            ReembolsoCompraDepartamental.objects.bulk_create(
                [nuevo], update_conflicts=True, update_fields=["importe"], unique_fields=["id"],
            )
        intento.reembolso_solicitado = Decimal("100")
        intento.save(update_fields=["reembolso_solicitado"])
        recibido = ReembolsoCompraDepartamental.objects.create(**datos)
        with self.assertRaises(ValidationError):
            ReembolsoCompraDepartamental.objects.bulk_create([
                ReembolsoCompraDepartamental(**{**datos, "importe": Decimal("200")}),
            ])
        recibido.importe = Decimal("200")
        with self.assertRaises(ValidationError):
            ReembolsoCompraDepartamental.objects.bulk_update([recibido], ["importe"])
        self.assertEqual(ReembolsoCompraDepartamental.objects.get(pk=recibido.pk).importe, Decimal("100"))

    def test_reembolso_normaliza_importes_de_cadena_y_float(self):
        intento = self.crear_intento(IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO)
        intento.reembolso_solicitado = Decimal("1000")
        intento.save(update_fields=["reembolso_solicitado"])
        datos = dict(intento=intento, fecha=timezone.localdate(), registrado_por=self.user)
        primero = ReembolsoCompraDepartamental.objects.create(importe="400.00", **datos)
        segundo = ReembolsoCompraDepartamental.objects.create(importe=600.0, **datos)
        self.assertEqual(primero.importe, Decimal("400.00"))
        self.assertEqual(segundo.importe, Decimal("600.0"))
        self.assertEqual(intento.total_reembolsado, Decimal("1000"))

    def test_reembolso_con_pk_existente_no_reescribe_el_historial(self):
        intento = self.crear_intento(IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO)
        intento.reembolso_solicitado = Decimal("1000")
        intento.save(update_fields=["reembolso_solicitado"])
        datos = dict(intento=intento, fecha=timezone.localdate(), registrado_por=self.user)
        original = ReembolsoCompraDepartamental.objects.create(importe=Decimal("400"), **datos)
        reemplazo = ReembolsoCompraDepartamental(pk=original.pk, importe=Decimal("900"), **datos)
        with self.assertRaises(ValidationError):
            reemplazo.save()
        original.refresh_from_db()
        self.assertEqual(original.importe, Decimal("400"))

    def test_actualizaciones_masivas_no_alteran_campos_financieros_del_intento(self):
        intento = self.crear_intento(IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO)
        intento.reembolso_solicitado = Decimal("1000")
        intento.save(update_fields=["reembolso_solicitado"])
        ReembolsoCompraDepartamental.objects.create(
            intento=intento, importe=Decimal("600"), fecha=timezone.localdate(),
            registrado_por=self.user,
        )
        with self.assertRaises(ValidationError):
            IntentoCompraDepartamental.objects.filter(pk=intento.pk).update(reembolso_solicitado=Decimal("500"))
        intento.reembolso_solicitado = Decimal("500")
        with self.assertRaises(ValidationError):
            IntentoCompraDepartamental.objects.bulk_update([intento], ["reembolso_solicitado"])
        for campo, valor in (
            ("reembolso_solicitado_en", timezone.localdate()),
            ("evidencia_solicitud_reembolso", "compras/solicitud.pdf"),
        ):
            with self.subTest(campo=campo), self.assertRaises(ValidationError):
                IntentoCompraDepartamental.objects.filter(pk=intento.pk).update(**{campo: valor})
        intento.refresh_from_db()
        self.assertEqual(intento.reembolso_solicitado, Decimal("1000"))
        IntentoCompraDepartamental.objects.filter(pk=intento.pk).update(estado=IntentoCompraDepartamental.ESTADO_REEMBOLSADO)
        intento.refresh_from_db()
        self.assertEqual(intento.estado, IntentoCompraDepartamental.ESTADO_REEMBOLSADO)

    def test_update_fields_generador_persiste_solicitud_validada(self):
        intento = self.crear_intento(IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO)
        intento.reembolso_solicitado = Decimal("1000")
        intento.save(update_fields=["reembolso_solicitado"])
        ReembolsoCompraDepartamental.objects.create(
            intento=intento, importe=Decimal("600"), fecha=timezone.localdate(),
            registrado_por=self.user,
        )
        intento.reembolso_solicitado = Decimal("700")
        intento.save(update_fields=(campo for campo in ("reembolso_solicitado",)))
        intento.refresh_from_db()
        self.assertEqual(intento.reembolso_solicitado, Decimal("700"))


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
