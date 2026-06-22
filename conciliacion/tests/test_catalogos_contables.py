from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from django.test import TestCase
from django.utils import timezone

from conciliacion.models import (
    ConceptoConciliacion,
    ContraparteConciliacion,
    CuentaBancariaPropia,
    CuentaContableConciliacion,
    InstrumentoFinancieroConciliacion,
    ReglaClasificacionMovimiento,
)
from conciliacion.services.reglas_contables import propuestas_para_movimiento
from syncfy_client.models import CuentaBancaria, MovimientoBancario


class CatalogosContablesConciliacionTests(TestCase):
    def setUp(self):
        self.banco = CuentaContableConciliacion.objects.create(
            codigo="102-001",
            nombre="Banco BBVA",
            tipo=CuentaContableConciliacion.TIPO_ACTIVO,
            naturaleza=CuentaContableConciliacion.NATURALEZA_DEUDORA,
        )
        self.pasivo_credito = CuentaContableConciliacion.objects.create(
            codigo="203-001",
            nombre="Linea credito BBVA",
            tipo=CuentaContableConciliacion.TIPO_PASIVO,
            naturaleza=CuentaContableConciliacion.NATURALEZA_ACREEDORA,
        )
        self.cuenta = CuentaBancaria.objects.create(
            banco=CuentaBancaria.BANCO_BBVA,
            nombre_display="BBVA Empresas",
            id_site_syncfy="site-bbva",
            numero_cuenta="0127530844",
        )
        self.cuenta_propia = CuentaBancariaPropia.objects.create(
            cuenta_bancaria=self.cuenta,
            alias="BBVA Empresas principal",
            empresa_rfc="GEF211230KR2",
            clabe="012733001207530844",
            ultimos_digitos="0844",
            cuenta_contable=self.banco,
        )
        self.concepto_traspaso = ConceptoConciliacion.objects.get(codigo="TRASPASO_ENTRE_CUENTAS")
        self.concepto_credito = ConceptoConciliacion.objects.get(codigo="DISPOSICION_LINEA_CREDITO")
        self.concepto_tarjeta = ConceptoConciliacion.objects.get(codigo="PAGO_TARJETA_CREDITO")

    def test_cuenta_bancaria_propia_liga_banco_con_cuenta_contable(self):
        self.assertEqual(self.cuenta.catalogo_conciliacion, self.cuenta_propia)
        self.assertEqual(self.cuenta_propia.cuenta_contable.codigo, "102-001")
        self.assertEqual(str(self.cuenta_propia), "BBVA Empresas principal")

    def test_regla_detecta_traspaso_a_cuenta_propia_por_clabe(self):
        ReglaClasificacionMovimiento.objects.create(
            nombre="SPEI a cuenta propia",
            concepto=self.concepto_traspaso,
            tipo_movimiento=ReglaClasificacionMovimiento.TIPO_CARGO,
            patrones_descripcion=["SPEI", "CUENTA BENEFICIARIO"],
            requiere_cuenta_propia_destino=True,
            cuenta_debe_sugerida=self.banco,
            cuenta_haber_sugerida=self.banco,
            evidencia_requerida=["cuenta_origen", "cuenta_destino", "referencia_bancaria"],
            confianza_base=85,
        )
        movimiento = MovimientoBancario.objects.create(
            id_transaction="traspaso-propio",
            cuenta=self.cuenta,
            descripcion="SPEI Enviado Cuenta Beneficiario: 012733001207530844 Clave de Rastreo: BB2643314013215",
            monto=Decimal("100000.00"),
            tipo=MovimientoBancario.TIPO_CARGO,
            fecha_transaccion=timezone.make_aware(datetime(2026, 5, 30, 12, 0)),
            fecha_refresh=timezone.now(),
        )

        propuestas = propuestas_para_movimiento(movimiento)

        self.assertEqual(len(propuestas), 1)
        self.assertEqual(propuestas[0].regla.concepto.codigo, "TRASPASO_ENTRE_CUENTAS")
        self.assertIn("cuenta destino reconocida como propia", propuestas[0].razon)
        self.assertIn("referencia_bancaria", propuestas[0].evidencia_requerida)

    def test_regla_detecta_disposicion_linea_credito_por_instrumento(self):
        contraparte = ContraparteConciliacion.objects.create(
            tipo=ContraparteConciliacion.TIPO_LINEA_CREDITO,
            nombre="Linea credito BBVA",
            cuenta_contable=self.pasivo_credito,
        )
        InstrumentoFinancieroConciliacion.objects.create(
            tipo=InstrumentoFinancieroConciliacion.TIPO_LINEA_CREDITO,
            nombre="Credito revolvente BBVA",
            institucion="BBVA",
            numero_referencia="LC-9988",
            contraparte=contraparte,
            cuenta_contable_pasivo=self.pasivo_credito,
            patrones_descripcion=["DISPOSICION LINEA CREDITO", "LC-9988"],
            evidencia_requerida=["contrato", "tabla_amortizacion", "referencia_bancaria"],
        )
        ReglaClasificacionMovimiento.objects.create(
            nombre="Disposicion linea de credito",
            concepto=self.concepto_credito,
            tipo_movimiento=ReglaClasificacionMovimiento.TIPO_ABONO,
            patrones_descripcion=["DISPOSICION"],
            instrumento_tipo=InstrumentoFinancieroConciliacion.TIPO_LINEA_CREDITO,
            cuenta_debe_sugerida=self.banco,
            cuenta_haber_sugerida=self.pasivo_credito,
            confianza_base=80,
        )
        movimiento = MovimientoBancario.objects.create(
            id_transaction="disp-linea-credito",
            cuenta=self.cuenta,
            descripcion="DISPOSICION LINEA CREDITO LC-9988 BBVA",
            monto=Decimal("50000.00"),
            tipo=MovimientoBancario.TIPO_ABONO,
            fecha_transaccion=timezone.make_aware(datetime(2026, 5, 10, 12, 0)),
            fecha_refresh=timezone.now(),
        )

        propuestas = propuestas_para_movimiento(movimiento)

        self.assertEqual(len(propuestas), 1)
        self.assertEqual(propuestas[0].regla.concepto.codigo, "DISPOSICION_LINEA_CREDITO")
        self.assertIn("instrumento Linea de credito", propuestas[0].razon)

    def test_regla_detecta_pago_tarjeta_credito_por_instrumento(self):
        InstrumentoFinancieroConciliacion.objects.create(
            tipo=InstrumentoFinancieroConciliacion.TIPO_TARJETA_CREDITO,
            nombre="Tarjeta corporativa BBVA 1234",
            institucion="BBVA",
            numero_referencia="1234",
            cuenta_bancaria_pago=self.cuenta_propia,
            cuenta_contable_pasivo=self.pasivo_credito,
            patrones_descripcion=["PAGO TARJETA", "1234"],
            evidencia_requerida=["estado_cuenta_tarjeta", "cfdi_soporte"],
        )
        ReglaClasificacionMovimiento.objects.create(
            nombre="Pago tarjeta corporativa",
            concepto=self.concepto_tarjeta,
            tipo_movimiento=ReglaClasificacionMovimiento.TIPO_CARGO,
            patrones_descripcion=["PAGO TARJETA"],
            instrumento_tipo=InstrumentoFinancieroConciliacion.TIPO_TARJETA_CREDITO,
            cuenta_debe_sugerida=self.pasivo_credito,
            cuenta_haber_sugerida=self.banco,
            confianza_base=80,
        )
        movimiento = MovimientoBancario.objects.create(
            id_transaction="pago-tarjeta",
            cuenta=self.cuenta,
            descripcion="PAGO TARJETA CREDITO 1234",
            monto=Decimal("18500.00"),
            tipo=MovimientoBancario.TIPO_CARGO,
            fecha_transaccion=timezone.make_aware(datetime(2026, 5, 20, 12, 0)),
            fecha_refresh=timezone.now(),
        )

        propuestas = propuestas_para_movimiento(movimiento)

        self.assertEqual(len(propuestas), 1)
        self.assertEqual(propuestas[0].regla.concepto.codigo, "PAGO_TARJETA_CREDITO")
        self.assertIn("instrumento Tarjeta de credito", propuestas[0].razon)
