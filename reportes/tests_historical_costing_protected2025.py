from datetime import date
from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from maestros.models import Insumo, UnidadMedida
from pos_bridge.models import PointProduct
from recetas.models import Receta
from reportes.models import (
    InsumoCostoHistoricoMensual,
    ProductoReventaCostoHistoricoMensual,
    RecetaCostoHistoricoMensual,
)
from reportes.services_historical_costing import MonthlyHistoricalCostingService
from reportes.tasks import snapshot_historical_costing_task


class HistoricalCostingProtected2025Tests(TestCase):
    models = (InsumoCostoHistoricoMensual, RecetaCostoHistoricoMensual,
              ProductoReventaCostoHistoricoMensual)

    def snapshot(self):
        return [list(model.objects.order_by('pk').values()) for model in self.models]

    def test_all_2025_months_are_protected_even_without_rows(self):
        service = MonthlyHistoricalCostingService()
        with patch.object(service, '_build_recipe_monthly_cost') as recipes, \
                patch.object(service, '_build_resale_product_monthly_cost') as resale, \
                patch.object(service, '_build_insumo_monthly_cost') as supplies:
            for month in range(1, 13):
                with self.subTest(month=month), self.assertRaisesRegex(ValueError, '2025.*protegido'):
                    service.build_period(period_start=date(2025, month, 1))
            recipes.assert_not_called()
            resale.assert_not_called()
            supplies.assert_not_called()
        self.assertEqual(self.snapshot(), [[], [], []])

    def test_documentary_and_stale_rows_remain_exactly_unchanged(self):
        unit = UnidadMedida.objects.create(codigo='pza', nombre='Pieza',
                                          tipo=UnidadMedida.TIPO_PIEZA, factor_to_base=1)
        supply = Insumo.objects.create(codigo='CAP', nombre='Capacillo', unidad_base=unit)
        recipe = Receta.objects.create(nombre='Producto documental', hash_contenido='protected-2025')
        product = PointProduct.objects.create(external_id=259, sku='01BLN01', name='Bolitas')
        for period in (date(2025, 1, 1), date(2025, 12, 1), date(2026, 1, 1)):
            InsumoCostoHistoricoMensual.objects.create(
                periodo=period, insumo=supply, costo_unitario='0.400294',
                metodo=InsumoCostoHistoricoMensual.METODO_PROMEDIO_MENSUAL)
            RecetaCostoHistoricoMensual.objects.create(
                periodo=period, receta=recipe, costo_total='81.234567',
                metadata={'source': 'documentary', 'audit_id': 43350})
            ProductoReventaCostoHistoricoMensual.objects.create(
                periodo=period, producto_point=product, costo_promedio='46.608800',
                metadata={'source': 'documentary', 'audit_id': 43344})
        before = self.snapshot()
        for month in (1, 12):
            with self.assertRaisesRegex(ValueError, '2025.*protegido'):
                MonthlyHistoricalCostingService().build_period(period_start=date(2025, month, 1))
        self.assertEqual(self.snapshot(), before)

    def test_cli_reports_protection_without_success(self):
        output = StringIO()
        with self.assertRaisesRegex(CommandError, 'fuentes históricas'):
            call_command('snapshot_historical_costing', period='2025-12', stdout=output)
        self.assertEqual(output.getvalue(), '')

    def test_cli_does_not_hide_unrelated_errors(self):
        with patch.object(MonthlyHistoricalCostingService, 'build_period', side_effect=RuntimeError('unexpected')):
            with self.assertRaisesRegex(RuntimeError, 'unexpected'):
                call_command('snapshot_historical_costing', period='2026-01')

    def test_task_propagates_protection_for_december_2025(self):
        with patch('reportes.tasks.date') as clock:
            clock.today.return_value = date(2026, 1, 1)
            with self.assertRaisesRegex(ValueError, '2025.*protegido'):
                snapshot_historical_costing_task.run()

    def test_other_years_keep_empty_period_behavior(self):
        for year in (2024, 2026):
            with self.subTest(year=year):
                result = MonthlyHistoricalCostingService().build_period(period_start=date(year, 1, 1))
                self.assertEqual(result.period_start, date(year, 1, 1))
                self.assertEqual(result.receta_rows, 0)
                self.assertEqual(result.producto_reventa_rows, 0)
