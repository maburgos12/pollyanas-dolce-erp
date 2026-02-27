from __future__ import annotations

from datetime import datetime
from pathlib import Path

from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = (
        "Orquesta proceso mensual de stock por sucursal: "
        "1) extracción/normalización desde XLSX legado, "
        "2) generación de plantilla operativa, "
        "3) importación de políticas de stock al ERP."
    )

    def add_arguments(self, parser):
        parser.add_argument("archivo", type=str, help="Ruta XLSX fuente (plantilla legacy de stocks por sucursal).")
        parser.add_argument(
            "--periodo",
            type=str,
            default="",
            help="Etiqueta de periodo para nombres de salida (ej. 2026-02).",
        )
        parser.add_argument(
            "--output-dir",
            type=str,
            default="output/spreadsheet",
            help="Directorio de salida para artefactos generados.",
        )
        parser.add_argument(
            "--strategy",
            choices=["max", "lv", "sd"],
            default="max",
            help="Estrategia para consolidar LV/SD al guardar políticas.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="No persiste políticas (sí genera archivos).",
        )

    def handle(self, *args, **options):
        input_path = Path(options["archivo"]).expanduser()
        if not input_path.exists():
            raise CommandError(f"No existe archivo fuente: {input_path}")
        if input_path.suffix.lower() not in {".xlsx", ".xlsm"}:
            raise CommandError("Formato no soportado. Usa archivo .xlsx/.xlsm.")

        out_dir = Path(options.get("output_dir") or "output/spreadsheet")
        out_dir.mkdir(parents=True, exist_ok=True)

        periodo_raw = (options.get("periodo") or "").strip()
        if periodo_raw:
            periodo_safe = periodo_raw.replace("/", "-").replace(" ", "_")
        else:
            now = datetime.now()
            periodo_safe = f"{now.year:04d}-{now.month:02d}"
        suffix = f"{periodo_safe}_{datetime.now().strftime('%H%M%S')}"

        self.stdout.write(self.style.NOTICE("Paso 1/2: extrayendo stock mínimo por sucursal..."))
        call_command(
            "extraer_stock_minimos_sucursales",
            str(input_path),
            output_dir=str(out_dir),
            output_suffix=suffix,
        )

        consolidated_xlsx = out_dir / f"stock_minimo_sucursales_{suffix}.xlsx"
        if not consolidated_xlsx.exists():
            raise CommandError(f"No se generó consolidado esperado: {consolidated_xlsx}")

        self.stdout.write(self.style.NOTICE("Paso 2/2: importando políticas al ERP..."))
        import_kwargs = {
            "sheet": "stock_minimo_abastecimiento",
            "strategy": (options.get("strategy") or "max").strip().lower(),
        }
        if bool(options.get("dry_run")):
            import_kwargs["dry_run"] = True

        call_command(
            "importar_politicas_stock_minimo",
            str(consolidated_xlsx),
            **import_kwargs,
        )

        plantillas_dir = out_dir / "plantillas_sucursales"
        call_command(
            "generar_plantillas_reabasto_sucursales",
            str(consolidated_xlsx),
            sheet="stock_minimo_abastecimiento",
            output_dir=str(plantillas_dir),
            periodo=periodo_raw,
        )

        self.stdout.write(self.style.SUCCESS("Proceso mensual completado"))
        self.stdout.write(f"  - fuente: {input_path}")
        self.stdout.write(f"  - consolidado: {consolidated_xlsx}")
        self.stdout.write(f"  - plantilla captura: {out_dir / f'plantilla_captura_reabasto_sucursales_{suffix}.xlsx'}")
        self.stdout.write(f"  - csv consolidado: {out_dir / f'stock_minimo_sucursales_{suffix}.csv'}")
        self.stdout.write(f"  - plantillas por sucursal: {plantillas_dir}")
