from django.test import TestCase

from conciliacion.models import ConceptoConciliacion


class ConceptoConciliacionTests(TestCase):
    def test_catalogo_base_incluye_conceptos_criticos(self):
        codigos = set(ConceptoConciliacion.objects.values_list("codigo", flat=True))

        self.assertIn("VENTA_EFECTIVO_SUCURSAL", codigos)
        self.assertIn("VENTA_TARJETA_DEBITO", codigos)
        self.assertIn("VENTA_TARJETA_CREDITO", codigos)
        self.assertIn("COMISION_TPV", codigos)
        self.assertIn("TRANSFERENCIA_CLIENTE_CFDI", codigos)
        self.assertIn("PAGO_PARCIAL_CLIENTE_REP", codigos)
        self.assertIn("DEVOLUCION_SAT_IVA", codigos)
        self.assertIn("PAGO_NOMINA_TIMBRADA", codigos)
        self.assertIn("PENDIENTE_ORIGEN", codigos)

    def test_efectivo_sucursal_requiere_cfdi_emitido_y_evidencia(self):
        concepto = ConceptoConciliacion.objects.get(codigo="VENTA_EFECTIVO_SUCURSAL")

        self.assertEqual(concepto.familia, ConceptoConciliacion.FAMILIA_VENTA)
        self.assertEqual(concepto.tipo_movimiento, ConceptoConciliacion.TIPO_ABONO)
        self.assertEqual(concepto.cfdi_esperado, ConceptoConciliacion.CFDI_EMITIDO)
        self.assertEqual(concepto.forma_pago_esperada, "01")
        self.assertTrue(concepto.requiere_evidencia_externa)
        self.assertFalse(concepto.permite_conciliacion_automatica)
        self.assertIn("sucursal", concepto.evidencia_requerida)

    def test_movimientos_sat_no_se_clasifican_como_venta(self):
        concepto = ConceptoConciliacion.objects.get(codigo="DEVOLUCION_SAT_IVA")

        self.assertEqual(concepto.familia, ConceptoConciliacion.FAMILIA_FISCAL)
        self.assertEqual(concepto.tipo_movimiento, ConceptoConciliacion.TIPO_ABONO)
        self.assertEqual(concepto.cfdi_esperado, ConceptoConciliacion.CFDI_NINGUNO)
        self.assertTrue(concepto.requiere_evidencia_externa)
        self.assertFalse(concepto.permite_conciliacion_automatica)

    def test_comision_tpv_requiere_cfdi_recibido(self):
        concepto = ConceptoConciliacion.objects.get(codigo="COMISION_TPV")

        self.assertEqual(concepto.familia, ConceptoConciliacion.FAMILIA_GASTO)
        self.assertEqual(concepto.tipo_movimiento, ConceptoConciliacion.TIPO_CARGO)
        self.assertEqual(concepto.cfdi_esperado, ConceptoConciliacion.CFDI_RECIBIDO)
        self.assertTrue(concepto.requiere_cfdi_recibido)
        self.assertIn("liquidacion_adquirente", concepto.evidencia_requerida)

    def test_pago_parcial_requiere_rep(self):
        concepto = ConceptoConciliacion.objects.get(codigo="PAGO_PARCIAL_CLIENTE_REP")

        self.assertEqual(concepto.cfdi_esperado, ConceptoConciliacion.CFDI_PAGO)
        self.assertEqual(concepto.forma_pago_esperada, "03")
        self.assertTrue(concepto.requiere_rep)
