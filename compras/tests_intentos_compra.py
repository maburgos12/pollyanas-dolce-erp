from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from django.core.exceptions import ValidationError
from django.core.files.base import ContentFile
from django.core.files.uploadedfile import SimpleUploadedFile
from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import IntegrityError, connection, transaction
from django.db.migrations.executor import MigrationExecutor
from django.db.migrations.recorder import MigrationRecorder
from django.test import TestCase, TransactionTestCase
from django.test.utils import CaptureQueriesContext
from django.utils import timezone
from django.urls import reverse

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
    HistorialCompraDepartamental,
    AvisoCompraDepartamental,
    ItemCompraDepartamental,
    SolicitudCompraDepartamental,
)
from compras.tests_edicion_compra import _CompraDepartamentalBase
from compras.services_departamentales import evaluar_presupuesto_item, generar_ordenes_departamentales, seleccionar_cotizacion
from compras.services_intentos_compra import (
    cancelar_articulo_definitivamente, cancelar_intento_compra, registrar_reembolso_compra,
)
from compras.forms_intentos_compra import CancelarIntentoCompraForm, RegistrarReembolsoCompraForm
from reportes.models import AreaPresupuestoResponsable
from maestros.models import Proveedor


class OperacionesIntentoCompraTests(_CompraDepartamentalBase, TestCase):
    def test_reemplazo_conserva_exposicion_pendiente_sin_duplicar_su_reserva(self):
        intento = self._intento(pagado=True)
        compra = intento.compra
        compra.importe_final = Decimal('1000')
        compra.save(update_fields=['importe_final'])
        compromiso = intento.compromiso
        compromiso.monto = Decimal('1000')
        compromiso.save(update_fields=['monto'])
        self._cancelar(intento, reembolso_solicitado_en=timezone.localdate(),
                       reembolso_solicitado=Decimal('1000'))
        reemplazo = CotizacionCompraDepartamental.objects.create(
            item=self.item, proveedor=self.proveedor,
            cantidad_ofertada=Decimal('1'), costo_unitario=Decimal('500'),
        )
        resultado = seleccionar_cotizacion(reemplazo, actor=self.user)
        self.assertEqual(resultado.compromisos_previos, Decimal('1000'))
        self.assertEqual(resultado.disponible_despues, Decimal('8500'))
        reserva = CompromisoCompraDepartamental.objects.get(item=self.item, intento__isnull=True, activo=True)
        reevaluacion = evaluar_presupuesto_item(self.item, Decimal('500'), compromiso_excluido=reserva)
        self.assertEqual(reevaluacion.compromisos_previos, Decimal('1000'))
        self.assertEqual(reevaluacion.disponible_despues, Decimal('8500'))
        generar_ordenes_departamentales([self.item], actor=self.user)
        reemplazo_vigente = self.item.intento_vigente
        con_orden = evaluar_presupuesto_item(
            self.item, Decimal('500'), compromiso_excluido=reemplazo_vigente.compromiso,
        )
        self.assertEqual(con_orden.compromisos_previos, Decimal('1000'))
        self.assertEqual(con_orden.disponible_despues, Decimal('8500'))

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

    def test_conflictos_de_version_y_estado_tienen_codigo_estable(self):
        intento = self._intento()
        with self.assertRaises(ValidationError) as obsoleto:
            self._cancelar(intento, version=0)
        self.assertEqual(obsoleto.exception.error_list[0].code, 'conflict')
        with self.assertRaises(ValidationError) as vigente:
            cancelar_articulo_definitivamente(self.item, motivo='Ya no se requiere', actor=self.user)
        self.assertEqual(vigente.exception.error_list[0].code, 'conflict')
        self.assertEqual(EventoCompraDepartamental.objects.filter(item=self.item, tipo__in=['INTENTO_CANCELADO', 'ARTICULO_CANCELADO']).count(), 0)

    def test_reembolso_con_version_obsoleta_tiene_codigo_conflicto(self):
        intento = self._solicitar_reembolso()
        with self.assertRaises(ValidationError) as obsoleto:
            registrar_reembolso_compra(intento, version=1, fecha=timezone.localdate(),
                                      importe=Decimal('10'), actor=self.user)
        self.assertEqual(obsoleto.exception.error_list[0].code, 'conflict')
        self.assertEqual(ReembolsoCompraDepartamental.objects.filter(intento=intento).count(), 0)

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

    def test_cancelar_articulo_autorizado_libera_reserva_preorden(self):
        reserva = CompromisoCompraDepartamental.objects.get(
            item=self.item, intento__isnull=True, activo=True,
        )
        self.item.refresh_from_db()
        self.assertEqual(self.item.estado, 'AUTORIZADO')
        cancelar_articulo_definitivamente(self.item, motivo='  Ya no se requiere  ', actor=self.user)
        reserva.refresh_from_db(); self.item.refresh_from_db()
        self.assertEqual(self.item.estado, 'CANCELADO')
        self.assertFalse(reserva.activo)
        self.assertIsNotNone(reserva.liberado_en)
        self.assertEqual(self.item.comentario_reciente, 'Ya no se requiere')

    def test_cancelar_articulo_revierte_reserva_si_falla_evento(self):
        reserva = CompromisoCompraDepartamental.objects.get(
            item=self.item, intento__isnull=True, activo=True,
        )
        with patch('compras.services_intentos_compra.EventoCompraDepartamental.objects.create',
                   side_effect=RuntimeError('evento falló')):
            with self.assertRaisesMessage(RuntimeError, 'evento falló'):
                cancelar_articulo_definitivamente(self.item, motivo='Ya no se requiere', actor=self.user)
        reserva.refresh_from_db(); self.item.refresh_from_db()
        self.assertTrue(reserva.activo)
        self.assertIsNone(reserva.liberado_en)
        self.assertEqual(self.item.estado, 'AUTORIZADO')

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

    def test_cargos_adicionales_tienen_cero_predeterminado_y_admiten_total_documentado(self):
        intento = self.crear_intento(IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO)
        self.assertEqual(intento.reembolso_cargos_adicionales, Decimal("0.00"))

        intento.reembolso_solicitado = Decimal("318.56")
        intento.reembolso_cargos_adicionales = Decimal("119.00")
        intento.save(update_fields=["reembolso_solicitado", "reembolso_cargos_adicionales"])

        intento.refresh_from_db()
        self.assertEqual(intento.reembolso_solicitado, Decimal("318.56"))
        self.assertEqual(intento.reembolso_cargos_adicionales, Decimal("119.00"))

    def test_cargos_adicionales_rechazan_negativos_sin_total_y_mayores_al_total(self):
        intento = self.crear_intento(IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO)
        for total, cargos, mensaje in (
            (None, Decimal("-0.01"), "no pueden ser negativos"),
            (None, Decimal("1.00"), "requieren una solicitud de reembolso"),
            (Decimal("318.56"), Decimal("318.57"), "no pueden superar el total"),
        ):
            with self.subTest(total=total, cargos=cargos):
                intento.reembolso_solicitado = total
                intento.reembolso_cargos_adicionales = cargos
                with self.assertRaisesMessage(ValidationError, mensaje):
                    intento.save(update_fields=["reembolso_solicitado", "reembolso_cargos_adicionales"])

        intento.refresh_from_db()
        self.assertIsNone(intento.reembolso_solicitado)
        self.assertEqual(intento.reembolso_cargos_adicionales, Decimal("0.00"))

    def test_cargos_adicionales_none_se_rechaza_con_error_de_dominio(self):
        intento = self.crear_intento(IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO)
        intento.reembolso_solicitado = Decimal("100.00")
        intento.reembolso_cargos_adicionales = None

        with self.assertRaisesMessage(ValidationError, "no pueden ser negativos"):
            intento.save(update_fields=["reembolso_solicitado", "reembolso_cargos_adicionales"])

    def test_cargos_adicionales_no_admiten_update_ni_bulk_update(self):
        intento = self.crear_intento(IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO)
        with self.assertRaises(ValidationError):
            IntentoCompraDepartamental.objects.filter(pk=intento.pk).update(
                reembolso_cargos_adicionales=Decimal("1.00"),
            )

        intento.reembolso_cargos_adicionales = Decimal("1.00")
        with self.assertRaises(ValidationError):
            IntentoCompraDepartamental.objects.bulk_update(
                [intento], ["reembolso_cargos_adicionales"],
            )

        intento.refresh_from_db()
        self.assertEqual(intento.reembolso_cargos_adicionales, Decimal("0.00"))

    def test_restricciones_bd_rechazan_cargos_inconsistentes_sin_romper_conexion(self):
        intento = self.crear_intento(IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO)
        intento.reembolso_solicitado = Decimal("100.00")
        intento.save(update_fields=["reembolso_solicitado"])
        tabla = connection.ops.quote_name(IntentoCompraDepartamental._meta.db_table)
        casos = (
            (Decimal("100.00"), Decimal("-0.01")),
            (None, Decimal("1.00")),
            (Decimal("100.00"), Decimal("100.01")),
        )

        for total, cargos in casos:
            with self.subTest(total=total, cargos=cargos), self.assertRaises(IntegrityError):
                with transaction.atomic():
                    with connection.cursor() as cursor:
                        cursor.execute(
                            f"UPDATE {tabla} "
                            "SET reembolso_solicitado = %s, reembolso_cargos_adicionales = %s "
                            "WHERE id = %s",
                            [total, cargos, intento.pk],
                        )
            intento.refresh_from_db()
            self.assertEqual(intento.reembolso_solicitado, Decimal("100.00"))
            self.assertEqual(intento.reembolso_cargos_adicionales, Decimal("0.00"))

    def test_update_fields_generador_valida_valores_efectivos_de_total_y_cargos(self):
        intento = self.crear_intento(IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO)
        intento.reembolso_solicitado = Decimal("318.56")
        intento.reembolso_cargos_adicionales = Decimal("119.00")
        intento.save(update_fields=(campo for campo in (
            "reembolso_solicitado", "reembolso_cargos_adicionales",
        )))

        intento.reembolso_solicitado = Decimal("100.00")
        with self.assertRaisesMessage(ValidationError, "no pueden superar el total"):
            intento.save(update_fields=(campo for campo in ("reembolso_solicitado",)))

        intento.refresh_from_db()
        self.assertEqual(intento.reembolso_solicitado, Decimal("318.56"))
        self.assertEqual(intento.reembolso_cargos_adicionales, Decimal("119.00"))

        ReembolsoCompraDepartamental.objects.create(
            intento=intento, importe=Decimal("200.00"), fecha=timezone.localdate(),
            registrado_por=self.user,
        )
        intento.reembolso_cargos_adicionales = Decimal("100.00")
        intento.reembolso_solicitado = Decimal("199.00")
        with self.assertRaisesMessage(ValidationError, "no puede ser menor que lo reembolsado"):
            intento.save(update_fields=(campo for campo in ("reembolso_solicitado",)))

        intento.refresh_from_db()
        self.assertEqual(intento.reembolso_solicitado, Decimal("318.56"))
        self.assertEqual(intento.reembolso_cargos_adicionales, Decimal("119.00"))

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


class MigracionCargosReembolsoTests(TransactionTestCase):
    migrate_from = ("compras", "0016_intentos_compra_reembolsos")
    migrate_to = ("compras", "0017_intento_reembolso_cargos_adicionales")

    def setUp(self):
        super().setUp()
        self.executor = MigrationExecutor(connection)
        self.executor.migrate([self.migrate_from])
        self.apps_0016 = self.executor.loader.project_state([self.migrate_from]).apps

    def tearDown(self):
        MigrationExecutor(connection).migrate([self.migrate_to])
        super().tearDown()

    def test_migracion_asigna_cero_a_intento_historico(self):
        Usuario = self.apps_0016.get_model(*settings.AUTH_USER_MODEL.split("."))
        Area = self.apps_0016.get_model("reportes", "AreaPresupuesto")
        Proveedor = self.apps_0016.get_model("maestros", "Proveedor")
        Solicitud = self.apps_0016.get_model("compras", "SolicitudCompraDepartamental")
        Item = self.apps_0016.get_model("compras", "ItemCompraDepartamental")
        Cotizacion = self.apps_0016.get_model("compras", "CotizacionCompraDepartamental")
        Intento = self.apps_0016.get_model("compras", "IntentoCompraDepartamental")
        user = Usuario.objects.create(username="migracion-cargos-reembolso")
        area = Area.objects.create(nombre="Migración cargos", codigo="MIG_CARGOS")
        proveedor = Proveedor.objects.create(nombre="Proveedor histórico cargos")
        solicitud = Solicitud.objects.create(
            folio="SCD-MIG-CARGOS", area=area, solicitante=user,
            periodo=timezone.localdate().replace(day=1),
        )
        item = Item.objects.create(solicitud=solicitud, descripcion="Artículo histórico")
        cotizacion = Cotizacion.objects.create(
            item=item, proveedor=proveedor, cantidad_ofertada=Decimal("1"),
            costo_unitario=Decimal("100.00"),
        )
        intento = Intento.objects.create(
            item=item, cotizacion=cotizacion, numero=1,
            reembolso_solicitado=Decimal("100.00"),
        )

        self.executor = MigrationExecutor(connection)
        self.executor.migrate([self.migrate_to])
        apps_0017 = self.executor.loader.project_state([self.migrate_to]).apps
        IntentoMigrado = apps_0017.get_model("compras", "IntentoCompraDepartamental")

        self.assertEqual(
            IntentoMigrado.objects.get(pk=intento.pk).reembolso_cargos_adicionales,
            Decimal("0.00"),
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

    def test_backfill_rechaza_datos_no_asociables_sin_perder_atomicidad(self):
        user, proveedor, solicitud = self._datos_base()
        Item = self.apps_0015.get_model("compras", "ItemCompraDepartamental")
        Cotizacion = self.apps_0015.get_model("compras", "CotizacionCompraDepartamental")
        Compra = self.apps_0015.get_model("compras", "CompraRealizadaDepartamental")
        Compromiso = self.apps_0015.get_model("compras", "CompromisoCompraDepartamental")

        item = Item.objects.create(solicitud=solicitud, descripcion="Sin orden")
        cotizacion = Cotizacion.objects.create(
            item=item, proveedor=proveedor, cantidad_ofertada=1, costo_unitario=Decimal("100"),
        )
        compra = Compra.objects.create(
            item=item, cotizacion=cotizacion, fecha_compra=timezone.localdate(),
            importe_final=Decimal("100"), comprobante="compras/historico.pdf", registrado_por=user,
        )
        with self.subTest(caso="compra sin línea"):
            try:
                with self.assertRaisesMessage(RuntimeError, f"compra departamental {compra.pk}"):
                    MigrationExecutor(connection).migrate([self.migrate_to])
                self.assertNotIn(self.migrate_to, MigrationRecorder(connection).applied_migrations())
            finally:
                compra.delete()

        item, _, _, _ = self._crear_linea(
            solicitud=solicitud, proveedor=proveedor, user=user,
            estado="COMPRADO", folio="OCD-MIG-6",
        )
        alternativa = Cotizacion.objects.create(
            item=item, proveedor=proveedor, cantidad_ofertada=2, costo_unitario=Decimal("90"),
        )
        compra = Compra.objects.create(
            item=item, cotizacion=alternativa, fecha_compra=timezone.localdate(),
            importe_final=Decimal("180"), comprobante="compras/historico.pdf", registrado_por=user,
        )
        with self.subTest(caso="compra con otra cotización"):
            try:
                with self.assertRaisesMessage(RuntimeError, f"compra {compra.pk}"):
                    MigrationExecutor(connection).migrate([self.migrate_to])
                self.assertNotIn(self.migrate_to, MigrationRecorder(connection).applied_migrations())
            finally:
                compra.delete()

        item, cotizacion, _, compromiso = self._crear_linea(
            solicitud=solicitud, proveedor=proveedor, user=user,
            estado="ORDENADO", folio="OCD-MIG-7",
        )
        alternativa = Cotizacion.objects.create(
            item=item, proveedor=proveedor, cantidad_ofertada=2, costo_unitario=Decimal("90"),
        )
        Compromiso.objects.filter(pk=compromiso.pk).update(cotizacion_id=alternativa.pk)
        with self.subTest(caso="compromiso con otra cotización"):
            try:
                with self.assertRaisesMessage(RuntimeError, f"compromiso {compromiso.pk}"):
                    MigrationExecutor(connection).migrate([self.migrate_to])
                self.assertNotIn(self.migrate_to, MigrationRecorder(connection).applied_migrations())
            finally:
                Compromiso.objects.filter(pk=compromiso.pk).update(cotizacion_id=cotizacion.pk)

    def test_reversa_rechaza_datos_no_reconstruibles_sin_perder_atomicidad(self):
        user, proveedor, solicitud = self._datos_base()
        item_reembolso, _, _, _ = self._crear_linea(
            solicitud=solicitud, proveedor=proveedor, user=user,
            estado="ORDENADO", folio="OCD-MIG-3",
        )
        item_estado, _, _, _ = self._crear_linea(
            solicitud=solicitud, proveedor=proveedor, user=user,
            estado="COMPRADO", folio="OCD-MIG-4",
        )
        item_numero, _, _, _ = self._crear_linea(
            solicitud=solicitud, proveedor=proveedor, user=user,
            estado="COMPRADO", folio="OCD-MIG-5",
        )
        executor = MigrationExecutor(connection)
        executor.migrate([self.migrate_to])
        apps = executor.loader.project_state([self.migrate_to]).apps
        Intento = apps.get_model("compras", "IntentoCompraDepartamental")
        Reembolso = apps.get_model("compras", "ReembolsoCompraDepartamental")
        reembolso = Reembolso.objects.create(
            intento=Intento.objects.get(item_id=item_reembolso.pk), importe=Decimal("10"),
            fecha=timezone.localdate(), registrado_por_id=user.pk,
        )
        with self.subTest(caso="reembolso nuevo"):
            try:
                with self.assertRaisesMessage(RuntimeError, "reembolsos"):
                    MigrationExecutor(connection).migrate([self.migrate_from])
                self.assertIn(self.migrate_to, MigrationRecorder(connection).applied_migrations())
            finally:
                Reembolso.objects.filter(pk=reembolso.pk).delete()

        Intento.objects.filter(item_id=item_estado.pk).update(estado="ENTREGADO")
        with self.subTest(caso="estado no reconstruible"):
            try:
                with self.assertRaisesMessage(RuntimeError, "estado"):
                    MigrationExecutor(connection).migrate([self.migrate_from])
                self.assertIn(self.migrate_to, MigrationRecorder(connection).applied_migrations())
            finally:
                Intento.objects.filter(item_id=item_estado.pk).update(estado="VIGENTE")

        Intento.objects.filter(item_id=item_numero.pk).update(numero=2)
        with self.subTest(caso="número no reconstruible"):
            try:
                with self.assertRaisesMessage(RuntimeError, "número"):
                    MigrationExecutor(connection).migrate([self.migrate_from])
                self.assertIn(self.migrate_to, MigrationRecorder(connection).applied_migrations())
            finally:
                Intento.objects.filter(item_id=item_numero.pk).update(numero=1)


class AccionesIntentoCompraViewTests(_CompraDepartamentalBase, TestCase):
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

    def _cancelar_data(self, intento, **changes):
        data = {
            'version': str(intento.version), 'motivo': IntentoCompraDepartamental.MOTIVO_NO_ENTREGO,
            'detalle': 'Proveedor confirmó que no entregará.',
        }
        if CompraRealizadaDepartamental.objects.filter(intento=intento).exists():
            data.update(reembolso_solicitado_en=timezone.localdate().isoformat(), reembolso_solicitado='180.00')
        return {**data, **changes}

    def _reembolso_pendiente(self):
        intento = self._intento(pagado=True)
        cancelar_intento_compra(
            intento, version=intento.version, motivo=IntentoCompraDepartamental.MOTIVO_NO_ENTREGO,
            detalle='Proveedor no entregará.', actor=self.user,
            reembolso_solicitado_en=timezone.localdate(), reembolso_solicitado=Decimal('180'),
        )
        intento.refresh_from_db()
        return intento

    def test_area_no_puede_ver_ni_enviar_ninguna_accion(self):
        intento = self._intento()
        area_user = get_user_model().objects.create_user('solicitante-area', password='test')
        AreaPresupuestoResponsable.objects.create(area=self.area, usuario=area_user, puede_capturar=True)
        self.client.force_login(area_user)
        urls = [
            reverse('compras:departamental_intento_cancelar', args=[intento.pk]),
            reverse('compras:departamental_reembolso_registrar', args=[intento.pk]),
            reverse('compras:departamental_articulo_cancelar', args=[self.item.pk]),
        ]
        for url in urls:
            with self.subTest(url=url):
                self.assertEqual(self.client.get(url).status_code, 403)
                self.assertEqual(self.client.post(url, {}).status_code, 403)
        self.assertEqual(EventoCompraDepartamental.objects.filter(item=self.item, tipo__in=['INTENTO_CANCELADO', 'REEMBOLSO_RECIBIDO', 'ARTICULO_CANCELADO']).count(), 0)

    def test_get_compras_muestra_accion_version_contexto_y_confirmacion(self):
        intento = self._intento()
        self.item.refresh_from_db()
        casos = [
            (reverse('compras:departamental_intento_cancelar', args=[intento.pk]), 'Se cancelará este intento con el proveedor y se conservará todo su historial. ¿Continuar?', str(intento.version)),
            (reverse('compras:departamental_reembolso_registrar', args=[intento.pk]), 'Se registrará este reembolso recibido y se actualizará el saldo pendiente. ¿Continuar?', str(intento.version)),
            (reverse('compras:departamental_articulo_cancelar', args=[self.item.pk]), 'Este artículo dejará de buscarse y la solicitud puede cerrarse. ¿Continuar?', self.item.actualizado_en.isoformat()),
        ]
        for url, confirmacion, version in casos:
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 200)
                self.assertContains(response, f'action="{url}"')
                self.assertContains(response, 'data-async-action')
                self.assertContains(response, f'value="{version}"')
                self.assertContains(response, self.item.descripcion)
                self.assertContains(response, self.solicitud.folio)
                if confirmacion:
                    self.assertContains(response, confirmacion)

    def test_cancelar_intento_async_una_vez_y_segundo_post_409(self):
        intento = self._intento()
        url = reverse('compras:departamental_intento_cancelar', args=[intento.pk])
        data = self._cancelar_data(intento)
        first = self.client.post(url, data, **self.headers)
        self.assertEqual(first.status_code, 200, first.content)
        self.assertTrue(first.json()['ok'])
        self.assertTrue(first.json()['reload'])
        self.assertEqual(first.json()['redirect_url'], reverse('compras:departamental_detalle', args=[self.solicitud.pk]) + f'#item-{self.item.pk}')
        second = self.client.post(url, data, **self.headers)
        self.assertEqual(second.status_code, 409)
        self.assertFalse(second.json()['ok'])
        self.assertEqual(EventoCompraDepartamental.objects.filter(item=self.item, tipo='INTENTO_CANCELADO').count(), 1)

    def test_reembolso_async_una_vez_y_segundo_post_409(self):
        intento = self._reembolso_pendiente()
        url = reverse('compras:departamental_reembolso_registrar', args=[intento.pk])
        data = {'version': '2', 'fecha': timezone.localdate().isoformat(), 'importe': '180.00', 'referencia': 'Devolución banco'}
        first = self.client.post(url, data, **self.headers)
        self.assertEqual(first.status_code, 200, first.content)
        self.assertTrue(first.json()['reload'])
        self.assertEqual(first.json()['redirect_url'], reverse('compras:departamental_detalle', args=[self.solicitud.pk]) + f'#item-{self.item.pk}')
        self.assertEqual(self.client.post(url, data, **self.headers).status_code, 409)
        self.assertEqual(ReembolsoCompraDepartamental.objects.filter(intento=intento).count(), 1)

    def test_cancelar_intento_pagado_pide_reembolso_y_conserva_compra(self):
        intento = self._intento(pagado=True)
        compra = CompraRealizadaDepartamental.objects.get(intento=intento)
        url = reverse('compras:departamental_intento_cancelar', args=[intento.pk])
        get_response = self.client.get(url)
        self.assertContains(get_response, 'name="reembolso_solicitado_en"')
        self.assertContains(get_response, 'name="reembolso_solicitado"')
        response = self.client.post(url, self._cancelar_data(intento), **self.headers)
        self.assertEqual(response.status_code, 200, response.content)
        intento.refresh_from_db()
        self.assertEqual(intento.estado, IntentoCompraDepartamental.ESTADO_REEMBOLSO_SOLICITADO)
        self.assertEqual(CompraRealizadaDepartamental.objects.get(pk=compra.pk).intento_id, intento.pk)
        self.assertEqual(EventoCompraDepartamental.objects.filter(item=self.item, tipo='REEMBOLSO_SOLICITADO').count(), 1)

    def test_cancelar_intento_pagado_muestra_maximo_reembolsable(self):
        intento = self._intento(pagado=True)

        response = self.client.get(
            reverse('compras:departamental_intento_cancelar', args=[intento.pk]),
        )

        self.assertContains(response, 'max="180.00"')
        self.assertContains(response, 'Máximo reembolsable: $180.00')

    def test_cancelar_articulo_async_exige_motivo_y_segundo_post_409(self):
        url = reverse('compras:departamental_articulo_cancelar', args=[self.item.pk])
        self.item.refresh_from_db()
        version = self.item.actualizado_en.isoformat()
        bad = self.client.post(url, {'version': version, 'motivo': '  '}, **self.headers)
        self.assertEqual(bad.status_code, 400)
        self.item.refresh_from_db()
        self.assertNotEqual(self.item.estado, ItemCompraDepartamental.ESTADO_CANCELADO)
        data = {'version': version, 'motivo': 'Ya no se necesita este artículo.'}
        first = self.client.post(url, data, **self.headers)
        self.assertEqual(first.status_code, 200, first.content)
        self.assertEqual(first.json()['redirect_url'], reverse('compras:departamental_detalle', args=[self.solicitud.pk]) + f'#item-{self.item.pk}')
        self.assertTrue(first.json()['reload'])
        self.assertEqual(self.client.post(url, data, **self.headers).status_code, 409)
        self.assertEqual(EventoCompraDepartamental.objects.filter(item=self.item, tipo='ARTICULO_CANCELADO').count(), 1)

    def test_post_tradicional_redirige_al_articulo(self):
        intento = self._intento()
        response = self.client.post(reverse('compras:departamental_intento_cancelar', args=[intento.pk]), self._cancelar_data(intento))
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response['Location'], reverse('compras:departamental_detalle', args=[self.solicitud.pk]) + f'#item-{self.item.pk}')

    def test_formulario_invalido_conserva_valores_y_no_muta(self):
        intento = self._intento()
        url = reverse('compras:departamental_intento_cancelar', args=[intento.pk])
        response = self.client.post(url, self._cancelar_data(intento, detalle='  ', motivo='OTRO'))
        self.assertEqual(response.status_code, 400)
        self.assertContains(response, 'OTRO', status_code=400)
        self.assertTrue(response.context['form'].errors['detalle'])
        self.assertEqual(IntentoCompraDepartamental.objects.get(pk=intento.pk).version, 1)
        self.assertEqual(EventoCompraDepartamental.objects.filter(item=self.item, tipo='INTENTO_CANCELADO').count(), 0)

    def test_post_vacio_de_las_tres_acciones_muestra_errores_sin_mutar(self):
        intento = self._intento()
        for url in (
            reverse('compras:departamental_intento_cancelar', args=[intento.pk]),
            reverse('compras:departamental_reembolso_registrar', args=[intento.pk]),
            reverse('compras:departamental_articulo_cancelar', args=[self.item.pk]),
        ):
            with self.subTest(url=url):
                response = self.client.post(url, {})
                self.assertEqual(response.status_code, 400)
                self.assertTrue(response.context['form'].is_bound)
                self.assertTrue(response.context['form'].errors)
                self.assertContains(response, 'data-async-action', status_code=400)
        self.assertEqual(IntentoCompraDepartamental.objects.get(pk=intento.pk).version, 1)
        self.assertEqual(EventoCompraDepartamental.objects.filter(item=self.item, tipo__in=['INTENTO_CANCELADO', 'REEMBOLSO_RECIBIDO', 'ARTICULO_CANCELADO']).count(), 0)

    def test_cancelar_articulo_con_intento_vigente_es_409_sin_mutar(self):
        intento = self._intento()
        self.item.refresh_from_db()
        response = self.client.post(
            reverse('compras:departamental_articulo_cancelar', args=[self.item.pk]),
            {'version': self.item.actualizado_en.isoformat(), 'motivo': 'Ya no se necesita'},
            **self.headers,
        )
        self.assertEqual(response.status_code, 409)
        self.assertFalse(response.json()['ok'])
        self.assertEqual(EventoCompraDepartamental.objects.filter(item=self.item, tipo='ARTICULO_CANCELADO').count(), 0)
        self.assertEqual(IntentoCompraDepartamental.objects.get(pk=intento.pk).estado, 'VIGENTE')

    def test_sin_login_redirige_antes_de_ver_datos(self):
        intento = self._intento()
        self.client.logout()
        response = self.client.get(reverse('compras:departamental_intento_cancelar', args=[intento.pk]))
        self.assertEqual(response.status_code, 302)
        self.assertIn('/login/', response['Location'])

    def test_id_de_intento_y_articulo_no_cruza_solicitudes(self):
        intento = self._intento()
        otra_solicitud = SolicitudCompraDepartamental.objects.create(
            area=self.area, solicitante=self.user, periodo=self.solicitud.periodo, estado='ENVIADA',
        )
        otro_item = otra_solicitud.items.create(descripcion='Otro artículo', cantidad=1)
        response = self.client.get(reverse('compras:departamental_articulo_cancelar', args=[otro_item.pk]))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, otra_solicitud.folio)
        self.assertEqual(response.context['destino'], reverse('compras:departamental_detalle', args=[otra_solicitud.pk]) + f'#item-{otro_item.pk}')
        self.assertEqual(self.client.get(reverse('compras:departamental_intento_cancelar', args=[999999])).status_code, 404)
        self.assertEqual(self.client.get(reverse('compras:departamental_articulo_cancelar', args=[999999])).status_code, 404)
        self.assertEqual(self.client.get(reverse('compras:departamental_reembolso_registrar', args=[999999])).status_code, 404)


class HistorialIntentosDetalleTests(_CompraDepartamentalBase, TestCase):
    def detalle(self):
        return self.client.get(reverse('compras:departamental_detalle', args=[self.solicitud.pk]))

    def intento(self, *, pagado=False):
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

    def cancelar(self, intento, *, pagado=False):
        kwargs = {}
        if pagado:
            kwargs = {'reembolso_solicitado_en': timezone.localdate(),
                      'reembolso_solicitado': Decimal('180')}
        cancelar_intento_compra(
            intento, version=intento.version, motivo=IntentoCompraDepartamental.MOTIVO_NO_ENTREGO,
            detalle='El proveedor no entregará.', actor=self.user, **kwargs,
        )
        intento.refresh_from_db()

    def test_intento_vigente_sin_recepcion_ofrece_cancelacion_y_entrega_positiva(self):
        intento = self.intento()
        response = self.detalle()
        self.assertContains(response, 'Proveedor canceló / no entregó')
        self.assertContains(response, reverse('compras:departamental_intento_cancelar', args=[intento.pk]))
        self.assertContains(response, 'min="0.001"')
        self.assertNotContains(response, 'min="0"')
        area_user = get_user_model().objects.create_user('area-intento', password='test')
        AreaPresupuestoResponsable.objects.create(area=self.area, usuario=area_user, puede_capturar=True)
        self.client.force_login(area_user)
        self.assertNotContains(self.detalle(), reverse('compras:departamental_intento_cancelar', args=[intento.pk]))

    def test_entrega_total_identifica_a_compras_y_al_area_sin_ofrecer_otra_entrega(self):
        intento = self.intento()
        RecepcionItemDepartamental.objects.create(
            linea_orden=intento.linea_orden, cantidad_recibida=Decimal('2'), registrado_por=self.user,
        )
        response = self.detalle()
        self.assertContains(response, 'Entregado por Compras')
        self.assertContains(response, f'Pendiente de confirmación del área {self.area.nombre}.')
        self.assertNotContains(response, '<summary>Registrar entrega</summary>')
        self.assertNotContains(response, 'Proveedor canceló / no entregó')
        area_user = get_user_model().objects.create_user('area-detalle', password='test')
        AreaPresupuestoResponsable.objects.create(area=self.area, usuario=area_user, puede_capturar=True)
        self.client.force_login(area_user)
        self.assertContains(self.detalle(), 'Confirma lo que recibiste')

    def test_historial_con_reembolso_parcial_y_reemplazo_conserva_ambos_proveedores(self):
        primero = self.intento(pagado=True)
        self.cancelar(primero, pagado=True)
        registrar_reembolso_compra(
            primero, version=primero.version, fecha=timezone.localdate(), importe=Decimal('80'),
            referencia='Devolución parcial', actor=self.user,
        )
        segundo_proveedor = Proveedor.objects.create(nombre='Proveedor reemplazo')
        segunda_cotizacion = CotizacionCompraDepartamental.objects.create(
            item=self.item, proveedor=segundo_proveedor,
            cantidad_ofertada=Decimal('2'), costo_unitario=Decimal('90'),
        )
        seleccionar_cotizacion(segunda_cotizacion, actor=self.user)
        self.item.refresh_from_db()
        generar_ordenes_departamentales([self.item], actor=self.user)
        response = self.detalle()
        html = response.content.decode()
        self.assertLess(html.index('Intento 1'), html.index('Intento 2'))
        self.assertContains(response, 'Proveedor reemplazo')
        self.assertContains(response, 'Reembolso solicitado')
        self.assertContains(response, 'Recibido $80.00')
        self.assertContains(response, 'Saldo pendiente $100.00')
        self.assertContains(response, 'Registrar reembolso recibido')
        self.assertContains(response, reverse('compras:departamental_reembolso_registrar', args=[primero.pk]))

    def test_reembolso_total_muestra_saldo_cero_y_permite_cancelar_articulo(self):
        intento = self.intento(pagado=True)
        self.cancelar(intento, pagado=True)
        response = self.detalle()
        self.assertContains(response, 'Saldo pendiente $180.00')
        self.assertNotContains(response, 'Cancelar definitivamente el artículo')
        registrar_reembolso_compra(
            intento, version=intento.version, fecha=timezone.localdate(), importe=Decimal('180'),
            referencia='Devolución completa', actor=self.user,
        )
        response = self.detalle()
        self.assertContains(response, 'Reembolso completado')
        self.assertContains(response, 'Saldo pendiente $0.00')
        self.assertContains(response, 'Cancelar definitivamente el artículo')
        self.assertNotContains(response, 'Registrar reembolso recibido')

    def test_compra_historica_conserva_correccion_para_compras_en_ambos_estados_reembolso(self):
        intento = self.intento(pagado=True)
        compra = intento.compra
        url = reverse('compras:departamental_compra_corregir', args=[compra.pk])
        self.cancelar(intento, pagado=True)
        response = self.detalle()
        self.assertContains(response, 'Reembolso solicitado')
        self.assertContains(response, url)
        self.assertIn(url, response.content.decode().split(f'id="intento-{intento.pk}"', 1)[1].split('</ol>', 1)[0])

        area_user = get_user_model().objects.create_user('area-correccion-historica', password='test')
        AreaPresupuestoResponsable.objects.create(area=self.area, usuario=area_user, puede_capturar=True)
        self.client.force_login(area_user)
        self.assertNotContains(self.detalle(), url)
        self.client.force_login(self.user)

        registrar_reembolso_compra(
            intento, version=intento.version, fecha=timezone.localdate(), importe=Decimal('180'),
            referencia='Devolución completa', actor=self.user,
        )
        response = self.detalle()
        self.assertContains(response, 'Reembolsado')
        self.assertContains(response, url)

        self.client.force_login(area_user)
        self.assertNotContains(self.detalle(), url)

    def test_sin_compra_vigente_ni_entrega_permita_cancelacion_definitiva_solo_a_compras(self):
        intento = self.intento()
        self.cancelar(intento)
        url = reverse('compras:departamental_articulo_cancelar', args=[self.item.pk])
        response = self.detalle()
        self.assertContains(response, url)
        self.assertContains(response, 'Cancelar definitivamente el artículo')
        area_user = get_user_model().objects.create_user('area-historial', password='test')
        AreaPresupuestoResponsable.objects.create(area=self.area, usuario=area_user, puede_capturar=True)
        self.client.force_login(area_user)
        response = self.detalle()
        self.assertNotContains(response, url)
        self.assertNotContains(response, 'Proveedor canceló / no entregó')

    def test_confirmacion_final_indica_completado(self):
        intento = self.intento()
        RecepcionItemDepartamental.objects.create(
            linea_orden=intento.linea_orden, cantidad_recibida=Decimal('2'), registrado_por=self.user,
        )
        self.item.refresh_from_db()
        from compras.services_departamentales import confirmar_recepcion_departamental
        confirmar_recepcion_departamental(self.item, conforme=True, actor=self.user)
        response = self.detalle()
        self.assertContains(response, 'Entrega completada y confirmada')
        self.assertNotContains(response, 'Pendiente de confirmación del área')

    def test_varios_intentos_quedan_precargados_sin_consultas_al_recorrerlos(self):
        for numero in range(3):
            item = self.item if numero == 0 else self.solicitud.items.create(
                descripcion=f'Artículo {numero}', cantidad=1, rubro=self.rubro,
            )
            if numero:
                proveedor = Proveedor.objects.create(nombre=f'Proveedor {numero}')
                quote = CotizacionCompraDepartamental.objects.create(
                    item=item, proveedor=proveedor, cantidad_ofertada=1, costo_unitario=50,
                )
                seleccionar_cotizacion(quote, actor=self.user)
            generar_ordenes_departamentales([item], actor=self.user)
        response = self.detalle()
        self.assertEqual(response.status_code, 200)
        with self.assertNumQueries(0):
            for item in response.context['solicitud'].items.all():
                for intento in item.intentos_compra_prefetched:
                    intento.cotizacion.proveedor.nombre
                    intento.linea_orden.orden.folio
                    intento.reembolsos_visibles
                    intento.recepciones_visibles
                    intento.saldo_reembolso_visible

    def test_get_completo_no_agrega_consultas_por_intento_al_renderizar_historial(self):
        primero = self.intento(pagado=True)
        compra = primero.compra
        AvisoCompraDepartamental.objects.create(
            compra=compra, canal='CORREO', destinatario=self.user,
            destino='pruebas@example.com', estado='ENVIADO',
        )
        HistorialCompraDepartamental.objects.create(
            compra=compra, antes={'importe_final': '200'}, despues={'importe_final': '180'},
            motivo='Importe corregido', actor=self.user,
        )
        self.cancelar(primero, pagado=True)
        registrar_reembolso_compra(
            primero, version=primero.version, fecha=timezone.localdate(), importe=Decimal('80'),
            referencia='Parcial', actor=self.user,
        )
        with CaptureQueriesContext(connection) as inicial:
            self.assertEqual(self.detalle().status_code, 200)

        reemplazo = CotizacionCompraDepartamental.objects.create(
            item=self.item, proveedor=Proveedor.objects.create(nombre='Proveedor B'),
            cantidad_ofertada=2, costo_unitario=90,
        )
        seleccionar_cotizacion(reemplazo, actor=self.user)
        self.item.refresh_from_db()
        generar_ordenes_departamentales([self.item], actor=self.user)
        for numero in range(2):
            item = self.solicitud.items.create(
                descripcion=f'Artículo de rendimiento {numero}', cantidad=1, rubro=self.rubro,
            )
            quote = CotizacionCompraDepartamental.objects.create(
                item=item, proveedor=Proveedor.objects.create(nombre=f'Proveedor C{numero}'),
                cantidad_ofertada=1, costo_unitario=50,
            )
            seleccionar_cotizacion(quote, actor=self.user)
            generar_ordenes_departamentales([item], actor=self.user)
            intento = item.intento_vigente
            compra = CompraRealizadaDepartamental.objects.create(
                intento=intento, item=item, cotizacion=quote,
                fecha_compra=timezone.localdate(), importe_final=Decimal('50'),
                comprobante=f'compras/compra-{numero}.pdf', registrado_por=self.user,
            )
            AvisoCompraDepartamental.objects.create(
                compra=compra, canal='CORREO', destinatario=self.user,
                destino='pruebas@example.com', estado='ENVIADO',
            )
            HistorialCompraDepartamental.objects.create(
                compra=compra, antes={'importe_final': '55'}, despues={'importe_final': '50'},
                motivo='Importe corregido', actor=self.user,
            )
            item.estado = ItemCompraDepartamental.ESTADO_COMPRADO
            item.save(update_fields=['estado'])
        with CaptureQueriesContext(connection) as ampliado:
            response = self.detalle()
            self.assertEqual(response.status_code, 200)
            self.assertContains(response, 'Proveedor C1')

        tablas_historial = (
            'maestros_proveedor',
            'compras_intentocompradepartamental', 'compras_reembolsocompradepartamental',
            'compras_avisocompradepartamental', 'compras_historialcompradepartamental',
        )
        def consultas_historial(queries):
            return {tabla: sum(f'FROM "{tabla}"' in q['sql'] for q in queries) for tabla in tablas_historial}

        iniciales = consultas_historial(inicial)
        ampliadas = consultas_historial(ampliado)
        for tabla in tablas_historial:
            self.assertGreater(iniciales[tabla], 0, tabla)
            self.assertGreater(ampliadas[tabla], 0, tabla)
            self.assertLessEqual(ampliadas[tabla], iniciales[tabla], tabla)
        self.assertLessEqual(len(ampliado) - len(inicial), 12)
