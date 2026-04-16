from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
from django.core.management.base import BaseCommand, CommandError

from pos_bridge.models import PointBranch, PointDailyBranchIndicator


class Command(BaseCommand):
    help = "Importa un XLS de historial de ventas por sucursal y lo materializa como indicador diario por sucursal."

    def add_arguments(self, parser):
        parser.add_argument("path", help="Ruta al XLS exportado desde Point.")
        parser.add_argument("--branch-id", dest="branch_id", help="external_id Point de la sucursal.")
        parser.add_argument("--branch-name", dest="branch_name", help="Nombre de la sucursal en Point/ERP.")
        parser.add_argument("--sheet", dest="sheet_name", default=None, help="Hoja a leer si el XLS tiene más de una.")

    def handle(self, *args, **options):
        path = Path(options["path"]).expanduser()
        if not path.exists():
            raise CommandError(f"No existe el archivo: {path}")

        branch = self._resolve_branch(branch_id=options.get("branch_id"), branch_name=options.get("branch_name"))
        sheet_name = options.get("sheet_name")
        imported = self._import_file(path=path, branch=branch, sheet_name=sheet_name)
        self.stdout.write(self.style.SUCCESS(f"Indicadores importados/actualizados: {imported}"))

    def _resolve_branch(self, *, branch_id: str | None, branch_name: str | None) -> PointBranch:
        qs = PointBranch.objects.filter(status=PointBranch.STATUS_ACTIVE).select_related("erp_branch")
        branch = None
        if branch_id:
            branch = qs.filter(external_id=str(branch_id)).first()
        if branch is None and branch_name:
            branch = qs.filter(name__iexact=str(branch_name)).first() or qs.filter(erp_branch__nombre__iexact=str(branch_name)).first()
        if branch is None:
            branch = qs.filter(external_id="13").first() or qs.filter(name__iexact="Guamuchil").first()
        if branch is None:
            raise CommandError("No pude resolver la sucursal Point a actualizar.")
        return branch

    def _import_file(self, *, path: Path, branch: PointBranch, sheet_name: str | None) -> int:
        workbook = pd.ExcelFile(path)
        target_sheet = sheet_name or workbook.sheet_names[0]
        df = workbook.parse(target_sheet, header=None)
        rows = df.iloc[6:, [4, 5, 6, 9]].copy()
        rows.columns = ["day", "sale", "tickets", "avg_ticket"]
        rows = rows.dropna(subset=["day"])

        imported = 0
        for _, row in rows.iterrows():
            day = self._to_date(row["day"])
            sale = self._to_decimal(row["sale"])
            tickets = int(Decimal(str(row["tickets"] or 0)))
            avg_ticket = self._to_decimal(row["avg_ticket"])
            PointDailyBranchIndicator.objects.update_or_create(
                branch=branch,
                indicator_date=day,
                defaults={
                    "contado_amount": sale,
                    "credito_amount": Decimal("0"),
                    "contado_tickets": tickets,
                    "credito_tickets": 0,
                    "contado_avg_ticket": avg_ticket,
                    "credito_avg_ticket": Decimal("0"),
                    "total_amount": sale,
                    "total_tickets": tickets,
                    "total_avg_ticket": avg_ticket,
                    "source_endpoint": "/Report/HistorialVentasSucursal",
                    "raw_payload": {
                        "import_path": str(path),
                        "sheet_name": target_sheet,
                        "imported_from_history_summary": True,
                    },
                },
            )
            imported += 1
        return imported

    @staticmethod
    def _to_date(value) -> datetime.date:
        if hasattr(value, "date"):
            return value.date()
        return datetime.fromisoformat(str(value)).date()

    @staticmethod
    def _to_decimal(value) -> Decimal:
        return Decimal(str(value or 0))
