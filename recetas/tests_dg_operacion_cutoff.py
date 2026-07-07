from datetime import date, datetime
from unittest.mock import patch

from django.test import SimpleTestCase

from recetas.views.plan import _latest_point_operational_cutoff_date


class DgOperacionCutoffTests(SimpleTestCase):
    def test_prefiere_fecha_de_ventas_si_existe(self):
        with (
            patch("recetas.views.plan._point_official_sales_stage_max_date", return_value=date(2026, 7, 6)),
            patch("recetas.views.plan._point_recent_sales_stage_max_date", return_value=date(2026, 7, 7)),
        ):
            self.assertEqual(_latest_point_operational_cutoff_date(), date(2026, 7, 7))

    def test_sin_ventas_usa_la_fuente_mas_reciente_restante(self):
        class _Manager:
            def __init__(self, value):
                self.value = value

            def aggregate(self, **kwargs):
                return {"max_date": self.value}

            def filter(self, **kwargs):
                return self

        aware_dt = datetime(2026, 7, 7, 8, 30)
        with (
            patch("recetas.views.plan._point_official_sales_stage_max_date", return_value=None),
            patch("recetas.views.plan._point_recent_sales_stage_max_date", return_value=None),
            patch("recetas.views.plan.PointDailyBranchIndicator.objects", _Manager(date(2026, 7, 5))),
            patch("recetas.views.plan.PointProductionLine.objects", _Manager(date(2026, 7, 4))),
            patch("recetas.views.plan.PointWasteLine.objects", _Manager(aware_dt)),
            patch("recetas.views.plan.PointTransferLine.objects", _Manager(date(2026, 7, 3))),
            patch("recetas.views.plan.PointInventorySnapshot.objects", _Manager(date(2026, 7, 2))),
        ):
            self.assertEqual(_latest_point_operational_cutoff_date(), date(2026, 7, 7))
