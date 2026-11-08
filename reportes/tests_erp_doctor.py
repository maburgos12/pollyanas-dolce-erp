import glob
import os
from contextlib import ExitStack
from datetime import date
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.test import SimpleTestCase, TestCase

from reportes.models import ProyectoInversion, ProyectoInversionGasto
from scripts import erp_doctor


class TemplateInventoryDoctorTests(SimpleTestCase):
    def test_quick_inventory_counts_project_templates_without_entering_nas(self):
        with TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            for relative_path in (
                "templates/base.html",
                "templates/shared/header.html",
                "reportes/templates/report.html",
                "reportes/templates/reportes/detail.html",
                "reportes/unrelated.html",
                "storage/nas/templates/remote.html",
            ):
                path = root / relative_path
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("<html></html>", encoding="utf-8")
            nas = root / "storage" / "nas"
            real_scandir = os.scandir

            def guarded_scandir(path):
                self.assertFalse(Path(path).is_relative_to(nas), "Inventory entered NAS")
                return real_scandir(path)

            with ExitStack() as stack:
                stack.enter_context(patch.object(erp_doctor, "ROOT", root))
                stack.enter_context(patch("os.scandir", side_effect=guarded_scandir))
                # Python 3.13 caches scandir in glob; Python 3.12 uses os.scandir directly.
                if hasattr(glob, "_StringGlobber"):
                    stack.enter_context(patch.object(glob._StringGlobber, "scandir", side_effect=guarded_scandir))
                checks = erp_doctor.check_templates(full=False)

        self.assertEqual(len(checks), 1)
        self.assertEqual(checks[0].status, "OK")
        self.assertTrue(checks[0].summary.startswith("4 templates HTML detectados;"))

    def test_full_check_delegates_to_djlint_without_inventory(self):
        result = erp_doctor.CheckResult(name="Templates: djlint", severity="OK", status="OK")
        with patch.object(erp_doctor, "check_optional_tool", return_value=result) as optional_tool:
            with patch.object(Path, "glob", side_effect=AssertionError("Full check ran inventory")):
                checks = erp_doctor.check_templates(full=True)

        self.assertEqual(checks, [result])
        self.assertEqual(optional_tool.call_args.args[1], "djlint templates */templates --check --extension html")


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
