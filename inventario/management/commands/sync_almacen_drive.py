from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError

from inventario.utils.google_drive_sync import sync_almacen_from_drive


VALID_SOURCES = {"inventario", "entradas", "salidas", "merma"}


class Command(BaseCommand):
    help = "Sincroniza archivos de almacén desde Google Drive por carpeta mensual y aplica importación." 

    def add_arguments(self, parser):
        parser.add_argument(
            "--sources",
            type=str,
            default="inventario,entradas,salidas,merma",
            help="Fuentes a importar (coma separada). Ejemplo: inventario,entradas",
        )
        parser.add_argument(
            "--month",
            type=str,
            default="",
            help="Mes objetivo en formato YYYY-MM (ejemplo: 2026-02). Si se omite, usa mes actual.",
        )
        parser.add_argument(
            "--no-fallback-previous",
            action="store_true",
            help="No hacer fallback al mes anterior cuando no exista carpeta del mes objetivo.",
        )
        parser.add_argument(
            "--fuzzy-threshold",
            type=int,
            default=96,
            help="Score mínimo para aceptar match FUZZY (default: 96)",
        )
        parser.add_argument(
            "--create-aliases",
            action="store_true",
            help="Crear/actualizar alias automáticamente cuando un match FUZZY sea confiable.",
        )
        parser.add_argument(
            "--create-missing-insumos",
            action="store_true",
            help="Crear insumos faltantes automáticamente cuando no haya match.",
        )
        parser.add_argument(
            "--alias-threshold",
            type=int,
            default=95,
            help="Score mínimo para crear alias automático (default: 95)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Simula importación sin persistir cambios.",
        )

    def handle(self, *args, **options):
        requested = {x.strip().lower() for x in options["sources"].split(",") if x.strip()}
        invalid = sorted(requested - VALID_SOURCES)
        if invalid:
            raise CommandError(f"Fuentes inválidas: {', '.join(invalid)}")
        if not requested:
            raise CommandError("Debes indicar al menos una fuente en --sources")

        try:
            result = sync_almacen_from_drive(
                include_sources=requested,
                month_override=options["month"].strip() or None,
                fallback_previous=not bool(options["no_fallback_previous"]),
                fuzzy_threshold=int(options["fuzzy_threshold"]),
                create_aliases=bool(options["create_aliases"]),
                alias_threshold=int(options["alias_threshold"]),
                create_missing_insumos=bool(options["create_missing_insumos"]),
                dry_run=bool(options["dry_run"]),
            )
        except Exception as exc:
            raise CommandError(str(exc)) from exc

        summary = result.summary
        mode = "DRY-RUN" if options["dry_run"] else "APLICADO"

        self.stdout.write(self.style.SUCCESS(f"Sync de Google Drive completado ({mode})"))
        self.stdout.write(f"  - carpeta usada: {result.folder_name} ({result.folder_id})")
        self.stdout.write(f"  - mes objetivo: {result.target_month}")
        self.stdout.write(f"  - fallback aplicado: {'sí' if result.used_fallback_month else 'no'}")
        self.stdout.write(f"  - fuentes descargadas: {', '.join(result.downloaded_sources) if result.downloaded_sources else '-'}")
        self.stdout.write(f"  - filas inventario leídas: {summary.rows_stock_read}")
        self.stdout.write(f"  - filas movimientos leídas: {summary.rows_mov_read}")
        self.stdout.write(f"  - matches: {summary.matched}")
        self.stdout.write(f"  - sin match: {summary.unmatched}")
        self.stdout.write(f"  - insumos creados: {summary.insumos_created}")
        self.stdout.write(f"  - existencias actualizadas: {summary.existencias_updated}")
        self.stdout.write(f"  - movimientos creados: {summary.movimientos_created}")
        self.stdout.write(f"  - movimientos omitidos (duplicado): {summary.movimientos_skipped_duplicate}")
        self.stdout.write(f"  - aliases creados/actualizados: {summary.aliases_created}")

        if result.skipped_files:
            self.stdout.write("  - archivos omitidos:")
            for item in result.skipped_files[:30]:
                self.stdout.write(f"    * {item}")
            if len(result.skipped_files) > 30:
                self.stdout.write(f"    * ... y {len(result.skipped_files) - 30} más")
