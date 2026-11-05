from datetime import date
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied, ValidationError
from django.test import TestCase
from django.utils import timezone

from core.models import AuditLog, Sucursal
from sat_client.models import CfdiDescargado
from .models import CategoriaGasto, CentroCosto, GastoOperativoMensual
from .services_gastos_compromisos import validar_cfdi_soporte, vincular_cfdi_soporte
from .services_rentabilidad_gastos import leer_gastos_mensuales


class CfdiSoporteTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_superuser('soportes', password='test-only')
        branch = Sucursal.objects.create(codigo='SOP', nombre='Soportes')
        center = CentroCosto.objects.create(codigo='SOP', nombre='Soportes', tipo='SUCURSAL_VENTA', sucursal=branch)
        category = CategoriaGasto.objects.create(codigo='RENTA', nombre='Renta', capa_objetivo='SUCURSAL')
        self.gasto = GastoOperativoMensual.objects.create(periodo=date(2026, 10, 1), centro_costo=center, categoria_gasto=category, monto=100)
        self.cfdi = CfdiDescargado.objects.create(uuid='11111111-1111-1111-1111-111111111111', rfc_emisor='AAA010101AAA', rfc_receptor='BBB010101BBB', subtotal=100, total=100, moneda='MXN', tipo_comprobante='I', tipo_cfdi='recibido', fecha_emision=timezone.now(), estatus='vigente', xml_raw='''<cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4" Total="100.00" Moneda="MXN" TipoDeComprobante="I"><cfdi:Emisor Rfc="AAA010101AAA"/><cfdi:Receptor Rfc="BBB010101BBB"/><cfdi:Complemento><t:TimbreFiscalDigital xmlns:t="http://www.sat.gob.mx/TimbreFiscalDigital" UUID="11111111-1111-1111-1111-111111111111"/></cfdi:Complemento></cfdi:Comprobante>''')

    def vincular(self):
        return vincular_cfdi_soporte(self.gasto, self.cfdi, usuario=self.user, motivo='Proveedor, concepto y cobertura contrastados con el documento fuente.')

    def test_vinculo_unico_auditado_y_cancelacion_invalida_lector(self):
        self.vincular(); self.vincular()
        self.assertEqual(AuditLog.objects.filter(model='reportes.GastoOperativoMensual').count(), 1)
        self.gasto.refresh_from_db()
        self.assertEqual(self.gasto.monto, Decimal('100'))
        rows = leer_gastos_mensuales(self.gasto.periodo)['filas']
        self.assertEqual(rows[0]['estado'], 'COMPLETO')
        self.assertIn(self.cfdi.uuid, rows[0]['referencia_soporte'])
        self.cfdi.estatus = 'cancelado'; self.cfdi.save(update_fields=['estatus'])
        self.assertEqual(leer_gastos_mensuales(self.gasto.periodo)['filas'][0]['estado'], 'PARCIAL')

    def test_rechaza_xml_incongruente_y_cfdi_no_aplicable(self):
        for field, value in [('estatus', 'cancelado'), ('total', Decimal('99')), ('moneda', 'USD'), ('tipo_cfdi', 'emitido'), ('tipo_comprobante', 'P'), ('xml_raw', ''), ('xml_raw', '<broken'), ('xml_raw', '<!DOCTYPE x><x/>'), ('uuid', 'otro'), ('rfc_emisor', 'OTRO')]:
            original = getattr(self.cfdi, field)
            with self.subTest(field=field, value=value):
                setattr(self.cfdi, field, value)
                with self.assertRaises(ValidationError):
                    validar_cfdi_soporte(self.gasto, self.cfdi)
                setattr(self.cfdi, field, original)

    def test_no_duplica_comprobante_ni_permite_actor_sin_gestion(self):
        user = get_user_model().objects.create_user('sin-gestion')
        with self.assertRaises(PermissionDenied):
            vincular_cfdi_soporte(self.gasto, self.cfdi, usuario=user, motivo='Revisión')
        self.vincular()
        self.gasto.pk = None; self.gasto.periodo = date(2026, 11, 1); self.gasto.cfdi_soporte = None; self.gasto.save()
        with self.assertRaises(ValidationError):
            self.vincular()

    def test_texto_en_campo_archivo_no_demuestra_soporte(self):
        self.gasto.archivo_soporte = 'Excel octubre - revisado por administración'
        self.gasto.save(update_fields=['archivo_soporte'])
        self.assertEqual(leer_gastos_mensuales(self.gasto.periodo)['filas'][0]['estado'], 'PARCIAL')
