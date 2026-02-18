from pathlib import Path
import os

from django.core.management.base import BaseCommand
from django.core.management import call_command
from recetas.utils.importador import ImportadorCosteo
from recetas.utils.reportes import generar_reportes
from django.conf import settings

class Command(BaseCommand):
    help = "Importa costos y recetas desde un Excel (COSTEO_Prueba.xlsx o similar)."

    def add_arguments(self, parser):
        parser.add_argument("filepath", type=str, help="Ruta del archivo Excel")
        parser.add_argument("--dry-run", action="store_true", help="Solo simular (no implementado aún)")

    def handle(self, *args, **options):
        filepath = options["filepath"]
        if not os.path.exists(filepath):
            self.stdout.write(self.style.ERROR(f"Archivo no encontrado: {filepath}"))
            return

        self.stdout.write(self.style.SUCCESS(f"Iniciando importación: {filepath}"))
        importador = ImportadorCosteo(filepath)
        resultado = importador.procesar_completo()

        resumen = {
            "catalogo_importado": resultado.catalogo_importado,
            "insumos_creados": resultado.insumos_creados,
            "costos_creados": resultado.costos_creados,
            "recetas_creadas": resultado.recetas_creadas,
            "recetas_actualizadas": resultado.recetas_actualizadas,
            "lineas_creadas": resultado.lineas_creadas,
            "errores": len(resultado.errores),
            "matches_pendientes": len(resultado.matches_pendientes),
        }

        self.stdout.write(self.style.SUCCESS("Sincronizando costos/insumos derivados..."))
        call_command("sync_insumos_derivados", verbosity=0)

        self.stdout.write(self.style.SUCCESS("✅ Importación completada"))
        for k, v in resumen.items():
            self.stdout.write(f"  - {k}: {v}")

        paths = generar_reportes(Path(settings.BASE_DIR), resumen, resultado.errores, resultado.matches_pendientes)
        self.stdout.write(self.style.SUCCESS("📊 Reportes generados en /logs"))
