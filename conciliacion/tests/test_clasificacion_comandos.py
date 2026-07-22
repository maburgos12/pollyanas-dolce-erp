from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from io import StringIO

from django.core.management import call_command
from django.test import TestCase
from django.utils import timezone

from conciliacion.models import ConceptoConciliacion, ReglaClasificacionMovimiento
from syncfy_client.models import CuentaBancaria, MovimientoBancario


def _mov(cuenta, desc, tipo, monto, dia=5, **extra):
    return MovimientoBancario.objects.create(
        cuenta=cuenta,
        id_transaction=f"t-{desc[:20]}-{monto}-{dia}",
        descripcion=desc,
        monto=Decimal(monto),
        tipo=tipo,
        fecha_transaccion=timezone.make_aware(datetime(2026, 1, dia)),
        fecha_refresh=timezone.now(),
        **extra,
    )


class SeedYClasificacionTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        # conceptos base minimos que el seed espera (en prod ya existen)
        for codigo, familia, tipo in [
            ("VENTA_EFECTIVO_SUCURSAL", "venta", "abono"),
            ("VENTA_TARJETA_DEBITO", "tarjeta", "abono"),
            ("COMISION_TPV", "gasto", "cargo"),
            ("IVA_COMISION_TPV", "gasto", "cargo"),
            ("TRASPASO_ENTRE_CUENTAS", "balance", "ambos"),
            ("DISPOSICION_LINEA_CREDITO", "balance", "abono"),
            ("PAGO_TARJETA_CREDITO", "balance", "cargo"),
            ("PAGO_NOMINA_TIMBRADA", "nomina", "cargo"),
            ("PAGO_RENTA_CFDI", "gasto", "cargo"),
        ]:
            ConceptoConciliacion.objects.get_or_create(
                codigo=codigo,
                defaults={
                    "nombre": codigo, "familia": familia,
                    "tipo_movimiento": tipo, "cfdi_esperado": "ninguno",
                },
            )
        cls.cuenta = CuentaBancaria.objects.create(
            id_site_syncfy="", nombre_display="BanBajio Test", banco="banbajio",
        )

    def test_seed_es_idempotente_y_crea_concepto_proveedor(self):
        out = StringIO()
        call_command("seed_reglas_conciliacion", stdout=out)
        n1 = ReglaClasificacionMovimiento.objects.count()
        call_command("seed_reglas_conciliacion", stdout=out)
        self.assertEqual(ReglaClasificacionMovimiento.objects.count(), n1)
        self.assertGreaterEqual(n1, 10)
        self.assertTrue(ConceptoConciliacion.objects.filter(codigo="PAGO_PROVEEDOR_CFDI").exists())

    def test_clasificar_aplica_tipos_y_respeta_existentes(self):
        call_command("seed_reglas_conciliacion", stdout=StringIO())
        m_dep = _mov(self.cuenta, "DEPOSITO NEGOCIOS AFILIADOS (ADQUIRENTE)", "abono", "1000.00", dia=3)
        m_iva = _mov(self.cuenta, "IVA COMISION APLICACION DE TASAS DE DESCUENTO DE CR", "cargo", "3.17", dia=3)
        m_tras = _mov(self.cuenta, "TRASPASO DE RECURSOS A LA CUENTA CONECTA BANBAJIO# 410641890205", "cargo", "50926.81", dia=4)
        m_spei = _mov(self.cuenta, "ENVIO SPEI:FACTURA 8459325338(BI- )", "cargo", "76269.73", dia=4)
        m_conciliado = _mov(
            self.cuenta, "DEPOSITO NEGOCIOS AFILIADOS (ADQUIRENTE)", "abono", "2000.00",
            dia=6, conciliado=True,
        )
        m_ya_tipado = _mov(
            self.cuenta, "DEPOSITO NEGOCIOS AFILIADOS (ADQUIRENTE)", "abono", "3000.00",
            dia=7, tipo_conciliacion=MovimientoBancario.CONCILIACION_SOPORTE,
        )
        m_sin_regla = _mov(self.cuenta, "MOVIMIENTO RARO XYZ", "cargo", "1.00", dia=8)

        # dry-run no escribe
        call_command("clasificar_movimientos", "--periodo", "2026-01", stdout=StringIO())
        m_dep.refresh_from_db()
        self.assertEqual(m_dep.tipo_conciliacion, "")

        out = StringIO()
        call_command("clasificar_movimientos", "--periodo", "2026-01", "--aplicar", stdout=out)
        for m in (m_dep, m_iva, m_tras, m_spei, m_conciliado, m_ya_tipado, m_sin_regla):
            m.refresh_from_db()
        self.assertEqual(m_dep.tipo_conciliacion, MovimientoBancario.CONCILIACION_INGRESO_FACTURADO)
        self.assertEqual(m_iva.tipo_conciliacion, MovimientoBancario.CONCILIACION_COMISION)
        self.assertEqual(m_tras.tipo_conciliacion, MovimientoBancario.CONCILIACION_TRASPASO)
        self.assertEqual(m_spei.tipo_conciliacion, MovimientoBancario.CONCILIACION_CFDI)
        self.assertEqual(m_conciliado.tipo_conciliacion, "")  # conciliado: intocable
        self.assertEqual(m_ya_tipado.tipo_conciliacion, MovimientoBancario.CONCILIACION_SOPORTE)
        self.assertEqual(m_sin_regla.tipo_conciliacion, "")
        self.assertEqual(m_dep.extra_raw["clasificacion_auto"]["concepto"], "VENTA_TARJETA_DEBITO")
        self.assertIn("SIN REGLA", out.getvalue())
