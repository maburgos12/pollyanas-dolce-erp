from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch

from django.core.exceptions import ValidationError
from django.test import TestCase
from django.utils import timezone

from inventario.models import ConsumoInsumoMensual, ExistenciaInsumo, MovimientoInventario
from inventario.services_auditoria_insumos import ConsumoInsumoAuditService, CostoUnitarioAudit
from maestros.models import Insumo, UnidadMedida


class CorreccionesCosteoTests(TestCase):
    def setUp(self):
        self.unidad = UnidadMedida.objects.create(codigo="g", nombre="Gramo")
        self.insumo = Insumo.objects.create(nombre="Ingrediente", unidad_base=self.unidad)
        self.stock = ExistenciaInsumo.objects.create(insumo=self.insumo, almacen="CUARTO_FRIO", stock_actual=10)
        self.service = ConsumoInsumoAuditService()

    def movimiento(self, tipo, cantidad, **kwargs):
        return MovimientoInventario.objects.create(insumo=self.insumo, almacen="CUARTO_FRIO", tipo=tipo, cantidad=cantidad,
            fecha=timezone.make_aware(datetime(2026, 8, 25)), **kwargs)

    def resultado(self):
        return self.service.calcular_consumo_real(date(2026, 8, 1), date(2026, 8, 31), self.insumo)

    def test_saldo_inicial_positivo_o_negativo_no_es_consumo(self):
        for cantidad in (540, -540):
            with self.subTest(cantidad=cantidad):
                m = self.movimiento("AJUSTE", cantidad, trazabilidad={"source": "POINT_ALMACEN_BASELINE"})
                self.assertEqual(self.resultado()["consumo_real"], 0)
                m.delete()
        self.stock.refresh_from_db()
        self.assertEqual(self.stock.stock_actual, 10)

    def test_ajuste_operativo_y_salida_se_conservan(self):
        self.movimiento("AJUSTE", -540, trazabilidad={"source": "POINT_ALMACEN_BASELINE"})
        self.movimiento("AJUSTE", -2)
        self.movimiento("SALIDA", 7)
        self.assertEqual(self.resultado()["consumo_real"], 9)

    def excluido(self):
        return self.movimiento("CONSUMO", 850, source_hash="hash-captura", referencia="PROD-1", trazabilidad={"exclusion_costeo": {
            "cantidad": "840", "cantidad_fuente": "850", "source_hash": "hash-captura", "motivo": "Captura equivocada confirmada", "autorizado_por": "Director"}})

    def test_consumo_directo_conserva_otras_recetas_y_no_resta_saldo(self):
        self.excluido()
        self.movimiento("CONSUMO", 20)
        self.movimiento("AJUSTE", -540, trazabilidad={"source": "POINT_ALMACEN_BASELINE"})
        self.assertEqual(self.resultado()["consumo_real"], 30)
        self.assertEqual(self.resultado()["metodo_consumo"], "CONSUMO_DIRECTO")

    def test_exclusion_total_no_activa_diferencial(self):
        m = self.excluido(); m.trazabilidad['exclusion_costeo']['cantidad'] = '850'; m.save()
        self.assertEqual(self.resultado()["consumo_real"], 0)
        self.assertEqual(self.resultado()["metodo_consumo"], "CONSUMO_DIRECTO")

    def test_fuente_modificada_o_exclusion_invalida_bloquea_recalculo(self):
        m = self.excluido()
        for campo, valor in [('cantidad_fuente', '851'), ('cantidad', '-1'), ('cantidad', 'NaN'), ('source_hash', 'otro'), ('autorizado_por', '')]:
            with self.subTest(campo=campo, valor=valor):
                original = m.trazabilidad['exclusion_costeo'][campo]
                m.trazabilidad['exclusion_costeo'][campo] = valor; m.save()
                with self.assertRaises(ValidationError): self.resultado()
                m.trazabilidad['exclusion_costeo'][campo] = original; m.save()

    def test_recalculo_persiste_correccion_sin_modificar_fuentes(self):
        m = self.excluido()
        teorico = {self.insumo.pk: {"cantidad": Decimal(850), "costo_snapshot": 0, "lineas": 1, "unidad_incompatible_count": 0}}
        with patch.object(self.service, '_produccion_por_receta', return_value={}), patch.object(self.service, '_ventas_servicio_por_receta', return_value={}), patch.object(self.service, '_consumo_teorico_por_insumo', return_value=teorico), patch.object(self.service, '_costos_unitarios', return_value={self.insumo.pk: CostoUnitarioAudit(Decimal('.15'), 'g', 'TEST')}):
            for _ in range(2): self.service.calcular_periodo(date(2026, 8, 1))
        row = ConsumoInsumoMensual.objects.get(insumo=self.insumo)
        self.assertEqual(row.consumo_real, 10)
        self.assertEqual(row.consumo_teorico, 10)
        self.assertEqual(row.costo_real, Decimal('1.50'))
        self.assertEqual(row.metadata['consumo_excluido'], '840')
        m.refresh_from_db(); self.stock.refresh_from_db()
        self.assertEqual(m.cantidad, 850)
        self.assertEqual(self.stock.stock_actual, 10)

    def plan(self):
        mov = self.excluido()
        row = ConsumoInsumoMensual.objects.create(periodo=date(2026, 8, 1), insumo=self.insumo, consumo_real=850, consumo_teorico=850,
            costo_real=Decimal('127.50'), costo_teorico=Decimal('127.50'), metadata={'costo_unitario_usado': '.15'})
        # La exclusión se registra únicamente al aplicar el plan.
        mov.trazabilidad = {}; mov.save()
        from reportes.models import AreaPresupuesto, RubroPresupuesto, ReglaFuenteRubro, LineaPresupuestoMensual
        area, _ = AreaPresupuesto.objects.get_or_create(codigo='produccion', defaults={'nombre': 'Produccion'})
        rubro = RubroPresupuesto.objects.create(area=area, concepto='Costo prueba', tipo='COSTO')
        ReglaFuenteRubro.objects.create(rubro=rubro, tipo_fuente='CONSUMO_MP', filtros={'total_empresa': True})
        linea = LineaPresupuestoMensual.objects.create(rubro=rubro, periodo=row.periodo, monto_real=row.costo_real, fuente_real='AUTO:CONSUMO_MP')
        plan = {'periodo': '2026-08', 'motivo': 'Error confirmado', 'autorizado_por': 'Director',
            'exclusiones': [{'movimiento_id': mov.pk, 'source_hash': mov.source_hash, 'cantidad_fuente': '850', 'cantidad': '840'}],
            'consumos': [{'id': row.pk, 'consumo_anterior': '850', 'costo_anterior': '127.50', 'costo_unitario': '.15', 'consumo_corregido': '10'}]}
        return plan, mov, row, linea

    def ejecutar(self, plan, apply=False):
        import io, json
        from django.core.management import call_command
        output = io.StringIO()
        with patch('sys.stdin', io.StringIO(json.dumps(plan))):
            call_command('corregir_costeo_documentado', apply=apply, stdout=output)
        return json.loads(output.getvalue())

    def test_comando_simula_aplica_y_no_duplica(self):
        from core.models import AuditLog
        plan, mov, row, linea = self.plan()
        self.assertEqual(self.ejecutar(plan)['estado'], 'simulacion')
        mov.refresh_from_db(); row.refresh_from_db(); linea.refresh_from_db()
        self.assertEqual(mov.trazabilidad, {})
        self.assertEqual(row.costo_real, Decimal('127.50'))
        self.assertEqual(linea.monto_real, Decimal('127.50'))
        self.assertFalse(AuditLog.objects.filter(action='CORRECCION_COSTEO').exists())
        result = self.ejecutar(plan, True)
        self.assertEqual(result['estado'], 'aplicado')
        row.refresh_from_db(); linea.refresh_from_db()
        self.assertEqual(row.costo_real, Decimal('1.50'))
        self.assertEqual(linea.monto_real, Decimal('1.50'))
        self.assertEqual(self.ejecutar(plan, True)['estado'], 'ya_aplicado')
        self.assertEqual(AuditLog.objects.filter(action='CORRECCION_COSTEO').count(), 1)

    def test_plan_obsoleto_revierte_toda_la_transaccion(self):
        from django.core.management.base import CommandError
        plan, mov, row, linea = self.plan()
        plan['consumos'][0]['costo_anterior'] = '1.00'
        with self.assertRaises(CommandError): self.ejecutar(plan, True)
        mov.refresh_from_db(); row.refresh_from_db(); linea.refresh_from_db()
        self.assertEqual(mov.trazabilidad, {})
        self.assertEqual(row.costo_real, Decimal('127.50'))
        self.assertEqual(linea.monto_real, Decimal('127.50'))

    def test_presupuesto_manual_se_conserva(self):
        plan, mov, row, linea = self.plan()
        linea.fuente_real = 'MANUAL:direccion'; linea.save()
        self.ejecutar(plan, True)
        linea.refresh_from_db()
        self.assertEqual(linea.monto_real, Decimal('127.50'))
