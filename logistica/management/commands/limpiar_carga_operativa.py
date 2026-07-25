from django.core.management.base import BaseCommand, CommandError

from logistica.carga_operativa import limpiar_carga_operativa_rutas_abiertas
from logistica.models import RutaEntrega


class Command(BaseCommand):
    help = (
        "Audita rutas abiertas y, con --ejecutar, reconstruye su carga operativa "
        "usando únicamente Enviado de Point."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--ruta",
            action="append",
            dest="folios",
            help="Folio de ruta a revisar. Puede repetirse.",
        )
        parser.add_argument(
            "--ejecutar",
            action="store_true",
            help="Aplica la limpieza. Sin esta bandera el comando es solo auditoría.",
        )

    def handle(self, *args, **options):
        folios = options.get("folios") or []
        ruta_ids = None
        if folios:
            rutas = RutaEntrega.objects.filter(folio__in=folios)
            encontrados = set(rutas.values_list("folio", flat=True))
            faltantes = sorted(set(folios) - encontrados)
            if faltantes:
                raise CommandError(f"No existen las rutas: {', '.join(faltantes)}")
            ruta_ids = list(rutas.values_list("id", flat=True))

        resultado = limpiar_carga_operativa_rutas_abiertas(
            ruta_ids=ruta_ids,
            ejecutar=bool(options["ejecutar"]),
        )
        modo = "EJECUTADA" if resultado.ejecutada else "AUDITORIA"
        self.stdout.write(f"MODO: {modo}")
        self.stdout.write(f"Rutas abiertas: {resultado.rutas}")
        self.stdout.write(f"Líneas activas antes: {resultado.lineas_activas}")
        self.stdout.write(f"Solicitudes activas antes: {resultado.solicitudes_activas}")
        self.stdout.write(f"Duplicados activos antes: {resultado.duplicados_activos}")
        self.stdout.write(f"Líneas activas después: {resultado.lineas_activas_despues}")
        self.stdout.write(
            f"Solicitudes activas después: {resultado.solicitudes_activas_despues}"
        )
        self.stdout.write(
            f"Duplicados activos después: {resultado.duplicados_activos_despues}"
        )
        if not resultado.ejecutada:
            self.stdout.write("No se modificaron datos. Usa --ejecutar para aplicar.")
