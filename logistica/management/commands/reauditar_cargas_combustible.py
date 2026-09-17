from django.core.management.base import BaseCommand

from logistica.models import CargaCombustibleUnidad
from logistica.services_combustible_auditoria import auditar_carga_combustible


class Command(BaseCommand):
    help = "Vuelve a correr la auditoría de tickets de combustible sobre cargas ya registradas."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Solo reporta, no guarda.")

    def handle(self, *args, **options):
        cargas = CargaCombustibleUnidad.objects.exclude(foto_ticket="").order_by("id")
        cambios = 0
        for carga in cargas:
            antes = (carga.auditoria_estado, carga.auditoria_score)
            if options["dry_run"]:
                continue
            resultado = auditar_carga_combustible(carga.id)
            despues = (resultado["estado"], resultado["score"])
            if antes != despues:
                cambios += 1
                self.stdout.write(f"#{carga.id}: {antes[0]}·{antes[1]} → {despues[0]}·{despues[1]}")
        self.stdout.write(self.style.SUCCESS(f"Cargas revisadas: {cargas.count()} · cambiadas: {cambios}"))
