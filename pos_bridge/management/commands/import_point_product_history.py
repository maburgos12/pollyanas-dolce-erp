from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from pos_bridge.services.product_history_import_service import PointProductHistoryImportService


class Command(BaseCommand):
    help = "Importa un Excel de historial de movimientos por producto desde Point a staging y reconciliación."

    def add_arguments(self, parser):
        parser.add_argument("--report-path", required=True, help="Ruta absoluta o relativa al archivo XLS/XLSX descargado desde Point.")
        parser.add_argument(
            "--allow-reimport",
            action="store_true",
            help="Si el archivo ya fue importado por hash, lo reprocesa y reemplaza sus filas.",
        )

    def handle(self, *args, **options):
        report_path = str(options["report_path"] or "").strip()
        if not report_path:
            raise CommandError("Debes indicar --report-path.")

        import_record, created = PointProductHistoryImportService().import_report(
            report_path=report_path,
            allow_reimport=bool(options["allow_reimport"]),
        )
        reconciliation = getattr(import_record, "reconciliation", None)

        self.stdout.write("Historial Point importado")
        self.stdout.write(f"Creado: {'SI' if created else 'NO'}")
        self.stdout.write(f"Producto: {import_record.product_name}")
        self.stdout.write(f"Sucursal: {import_record.branch_name or 'N/D'}")
        self.stdout.write(f"Fecha reporte: {import_record.report_date or 'N/D'}")
        self.stdout.write(f"Filas: {import_record.row_count}")
        self.stdout.write(f"Costo Point: {import_record.latest_unit_cost}")
        self.stdout.write(f"Receta ERP: {import_record.receta.nombre if import_record.receta_id else 'NO_MATCH'}")
        if reconciliation is not None:
            self.stdout.write(f"Costo ERP: {reconciliation.erp_unit_cost}")
            self.stdout.write(f"Varianza: {reconciliation.variance_amount}")
            self.stdout.write(f"Estatus: {reconciliation.status}")
