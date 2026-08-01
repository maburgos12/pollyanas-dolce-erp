from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.test import TestCase

from reportes.models import ProyectoInversion, ProyectoInversionGasto
from scripts import erp_doctor


class InvestmentProjectConsistencyDoctorTests(TestCase):
    def test_planned_capex_is_not_compared_against_actual_investment(self):
        project = ProyectoInversion.objects.create(
            id=erp_doctor.GUAMUCHIL_PROJECT_ID,
            nombre_proyecto="Apertura Bamoa 2026",
            fecha_inicio=date(2026, 5, 1),
            monto_inversion_planeado=Decimal("194000.00"),
            monto_inversion_real=Decimal("0.00"),
        )
        ProyectoInversionGasto.objects.create(
            proyecto=project,
            tipo_registro=ProyectoInversionGasto.TIPO_PLANEADO,
            fecha=date(2026, 5, 1),
            categoria=ProyectoInversionGasto.CATEGORIA_OBRA_CIVIL,
            descripcion="Remodelacion",
            monto=Decimal("194000.00"),
            monto_total=Decimal("194000.00"),
        )

        with patch.multiple(
            erp_doctor,
            GUAMUCHIL_EXPECTED_INVESTMENT=Decimal("0.00"),
            GUAMUCHIL_EXPECTED_RECON_ROWS=0,
        ):
            result = erp_doctor.check_investment_project_consistency()

        self.assertEqual(result.status, "OK")
        self.assertEqual(result.details, [])
