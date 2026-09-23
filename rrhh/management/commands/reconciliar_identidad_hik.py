import json
from datetime import date

from django.core.management.base import BaseCommand, CommandError

from rrhh.services_hik_reconciliacion import (
    ReconciliacionHikError,
    aplicar_reconciliacion,
    preparar_reconciliacion,
)


class Command(BaseCommand):
    help = "Previsualiza o corrige una reasignación errónea de identidad Hik sin borrar historial."

    def add_arguments(self, parser):
        parser.add_argument("--codigo-afectado", required=True)
        parser.add_argument("--codigo-origen", required=True)
        parser.add_argument("--empleado-origen-id", type=int, required=True)
        parser.add_argument("--empleado-destino-id", type=int, required=True)
        parser.add_argument("--desde", type=date.fromisoformat, required=True)
        parser.add_argument("--hasta", type=date.fromisoformat, required=True)
        parser.add_argument("--backup-dir", default="/opt/backups/erp/rrhh-hik")
        parser.add_argument("--apply", action="store_true")

    def handle(self, *args, **options):
        try:
            desde = options["desde"]
            hasta = options["hasta"]
            if isinstance(desde, str):
                desde = date.fromisoformat(desde)
            if isinstance(hasta, str):
                hasta = date.fromisoformat(hasta)
            plan = preparar_reconciliacion(
                codigo_afectado=options["codigo_afectado"],
                codigo_origen=options["codigo_origen"],
                empleado_origen_id=options["empleado_origen_id"],
                empleado_destino_id=options["empleado_destino_id"],
                desde=desde,
                hasta=hasta,
            )
            result = (
                aplicar_reconciliacion(plan, backup_dir=options["backup_dir"])
                if options["apply"]
                else plan.resumen(modo="dry-run")
            )
        except (ReconciliacionHikError, OSError) as exc:
            raise CommandError(str(exc)) from exc
        self.stdout.write(json.dumps(result, ensure_ascii=False, sort_keys=True))
