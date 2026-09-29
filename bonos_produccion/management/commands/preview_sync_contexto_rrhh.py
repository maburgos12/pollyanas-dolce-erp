import json

from django.core.management.base import BaseCommand, CommandError

from bonos_produccion.models import ConfigBonoPeriodo
from bonos_produccion.services_preview import generar_preview_contexto_rrhh


class Command(BaseCommand):
    help = "Simula la sincronización RRHH de bonos sin guardar cambios."

    def add_arguments(self, parser):
        parser.add_argument("--periodo-id", type=int, required=True)
        parser.add_argument("--json", action="store_true", dest="as_json")

    def handle(self, *args, **options):
        try:
            periodo = ConfigBonoPeriodo.objects.get(pk=options["periodo_id"])
        except ConfigBonoPeriodo.DoesNotExist as exc:
            raise CommandError("No existe el periodo solicitado.") from exc

        preview = generar_preview_contexto_rrhh(periodo)
        if options["as_json"]:
            self.stdout.write(json.dumps(preview, ensure_ascii=False, sort_keys=True))
            return

        self.stdout.write(
            f"Preview periodo {periodo.id} ({periodo.fecha_inicio} a {periodo.fecha_fin}) · "
            f"manuales intactos: {'sí' if preview['manuales_intactos'] else 'NO'}"
        )
        for fila in preview["filas"]:
            self.stdout.write(
                f"{fila['empleado']} | faltas {fila['faltas_actuales']} -> "
                f"{fila['faltas_rrhh_propuestas']} | total ${fila['total_actual']} -> "
                f"${fila['total_propuesto']} | cancela {fila['cancela_actual']} -> "
                f"{fila['cancela_propuesto']}"
            )
        self.stdout.write(f"Hash manual: {preview['hash_manual_antes']}")
