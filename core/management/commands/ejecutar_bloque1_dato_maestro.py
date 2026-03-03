from __future__ import annotations

from datetime import datetime
from pathlib import Path

from django.core.management import BaseCommand, CommandError, call_command


class Command(BaseCommand):
    help = (
        "Ejecuta el Bloque 1 de dato maestro único: "
        "1) sincronización Point -> ERP (opcional), "
        "2) consolidación de diccionario maestro e inconsistencias."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--point-dir",
            default="/Users/mauricioburgos/Downloads/INFORMACION POINT",
            help="Carpeta con exports .xls de Point para homologación automática.",
        )
        parser.add_argument(
            "--output-dir",
            default="logs",
            help="Carpeta base de salida para reportes del bloque 1.",
        )
        parser.add_argument(
            "--skip-point",
            action="store_true",
            help="Omite la fase de sync Point y solo ejecuta consolidación de diccionario.",
        )
        parser.add_argument(
            "--fuzzy-threshold",
            type=int,
            default=90,
            help="Umbral fuzzy para homologación Point (0-100).",
        )
        parser.add_argument(
            "--runs-lookback",
            type=int,
            default=20,
            help="Corridas históricas de almacén a revisar en consolidación.",
        )
        parser.add_argument(
            "--apply-point",
            action="store_true",
            help=(
                "Aplica homologación automática en Point sync: "
                "proveedores, insumos, productos, aliases e insumos faltantes."
            ),
        )
        parser.add_argument(
            "--apply-point-name-aliases",
            action="store_true",
            help="En consolidación, crea alias desde nombre_point cuando no hay conflicto.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Simula cambios y ejecuta rollback en fase Point. Consolidación sí genera reportes.",
        )

    def handle(self, *args, **options):
        base_output = Path(str(options["output_dir"])).expanduser().resolve()
        base_output.mkdir(parents=True, exist_ok=True)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        block_output = base_output / f"bloque1_dato_maestro_{ts}"
        point_output = block_output / "point_sync"
        dict_output = block_output / "diccionario"
        block_output.mkdir(parents=True, exist_ok=True)
        point_output.mkdir(parents=True, exist_ok=True)
        dict_output.mkdir(parents=True, exist_ok=True)

        skip_point = bool(options["skip_point"])
        apply_point = bool(options["apply_point"])
        dry_run = bool(options["dry_run"])
        point_dir = Path(str(options["point_dir"])).expanduser().resolve()

        self.stdout.write(self.style.SUCCESS("Bloque 1 - Dato maestro único"))
        self.stdout.write(f"  - salida base: {block_output}")
        self.stdout.write(f"  - dry-run: {'SI' if dry_run else 'NO'}")

        ran_point = False
        if not skip_point:
            if not point_dir.exists():
                raise CommandError(
                    f"No existe point-dir: {point_dir}. Usa --skip-point o corrige la ruta."
                )
            self.stdout.write("")
            self.stdout.write(self.style.WARNING("Fase 1/2: Point sync catalogs"))
            call_command(
                "sync_point_catalogs",
                str(point_dir),
                output_dir=str(point_output),
                fuzzy_threshold=int(options["fuzzy_threshold"]),
                apply_proveedores=apply_point,
                apply_insumos=apply_point,
                apply_productos=apply_point,
                create_aliases=apply_point,
                create_missing_insumos=apply_point,
                dry_run=dry_run,
            )
            ran_point = True
        else:
            self.stdout.write("")
            self.stdout.write(self.style.WARNING("Fase 1/2: Point sync omitido (--skip-point)"))

        self.stdout.write("")
        self.stdout.write(self.style.WARNING("Fase 2/2: Consolidación diccionario maestro"))
        call_command(
            "consolidar_diccionario_nombres",
            output_dir=str(dict_output),
            runs_lookback=int(options["runs_lookback"]),
            apply_point_name_aliases=bool(options["apply_point_name_aliases"]),
        )

        summary_path = block_output / "resumen_bloque1.md"
        summary_lines = [
            "# Resumen Bloque 1 - Dato Maestro",
            "",
            f"- timestamp: {ts}",
            f"- output_dir: {block_output}",
            f"- dry_run: {'SI' if dry_run else 'NO'}",
            f"- point_sync_ejecutado: {'SI' if ran_point else 'NO'}",
            f"- point_dir: {point_dir}",
            f"- point_output: {point_output}",
            f"- diccionario_output: {dict_output}",
            "",
            "## Notas",
            "- Si dry-run=SI, la fase Point se ejecuta con rollback.",
            "- La fase de consolidación siempre genera reportes CSV de salida.",
            "- Este comando no modifica estructura de DB ni instala dependencias.",
        ]
        summary_path.write_text("\n".join(summary_lines), encoding="utf-8")

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("Bloque 1 completado"))
        self.stdout.write(f"  - resumen: {summary_path}")
        self.stdout.write(f"  - reportes Point: {point_output}")
        self.stdout.write(f"  - reportes Diccionario: {dict_output}")
