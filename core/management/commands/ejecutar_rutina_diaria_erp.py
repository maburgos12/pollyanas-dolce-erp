from __future__ import annotations

from datetime import datetime
from pathlib import Path

from django.core.management import BaseCommand, CommandError, call_command
from django.utils import timezone


class Command(BaseCommand):
    help = (
        "Ejecuta la rutina diaria operativa del ERP en orden: "
        "1) sync almacén (Drive), 2) sync Point, 3) hardening post-import, "
        "4) rematch de recetas, 5) auditoría global."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--month",
            default=timezone.localdate().strftime("%Y-%m"),
            help="Mes objetivo YYYY-MM para sync de Drive (default: mes actual local).",
        )
        parser.add_argument(
            "--point-dir",
            default="/Users/mauricioburgos/Downloads/INFORMACION POINT",
            help="Carpeta con exports .xls de Point.",
        )
        parser.add_argument(
            "--output-dir",
            default="logs",
            help="Carpeta base para resumen de ejecución.",
        )
        parser.add_argument(
            "--skip-drive",
            action="store_true",
            help="Omite sincronización de almacén por Drive.",
        )
        parser.add_argument(
            "--skip-point",
            action="store_true",
            help="Omite sincronización de catálogos Point.",
        )
        parser.add_argument(
            "--skip-hardening",
            action="store_true",
            help="Omite hardening post-import.",
        )
        parser.add_argument(
            "--skip-audit",
            action="store_true",
            help="Omite auditoría final.",
        )
        parser.add_argument(
            "--skip-rematch",
            action="store_true",
            help="Omite rematch de líneas de receta.",
        )
        parser.add_argument(
            "--drive-fuzzy-threshold",
            type=int,
            default=88,
            help="Umbral fuzzy para sync de Drive (default: 88).",
        )
        parser.add_argument(
            "--point-fuzzy-threshold",
            type=int,
            default=85,
            help="Umbral fuzzy para sync de Point (default: 85).",
        )
        parser.add_argument(
            "--rematch-limit",
            type=int,
            default=500,
            help="Límite de líneas por corrida de rematch recetas (default: 500).",
        )
        parser.add_argument(
            "--rematch-offset",
            type=int,
            default=0,
            help="Offset de líneas para rematch recetas (default: 0).",
        )
        parser.add_argument(
            "--rematch-progress-every",
            type=int,
            default=100,
            help="Progreso cada N líneas en rematch recetas (default: 100).",
        )
        parser.add_argument(
            "--rematch-recipe",
            default="",
            help="Filtro contains por nombre de receta para rematch.",
        )
        parser.add_argument(
            "--rematch-include-needs-review",
            action="store_true",
            default=True,
            help="Incluye NEEDS_REVIEW en rematch recetas (default: true).",
        )
        parser.add_argument(
            "--no-rematch-include-needs-review",
            action="store_false",
            dest="rematch_include_needs_review",
            help="Excluye NEEDS_REVIEW en rematch recetas.",
        )
        parser.add_argument(
            "--drive-create-aliases",
            action="store_true",
            help="Permite creación/actualización de aliases durante sync Drive.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Simula operaciones en sync Drive/Point. Hardening/auditoría se omiten.",
        )
        parser.add_argument(
            "--continue-on-error",
            action="store_true",
            help="Continúa con siguientes pasos aunque uno falle y reporta resumen de errores.",
        )

    def handle(self, *args, **options):
        started_at = timezone.now()
        ts = started_at.strftime("%Y%m%d_%H%M%S")
        output_dir = Path(str(options["output_dir"])).expanduser().resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
        summary_path = output_dir / f"rutina_diaria_erp_{ts}.md"

        dry_run = bool(options["dry_run"])
        skip_drive = bool(options["skip_drive"])
        skip_point = bool(options["skip_point"])
        skip_hardening = bool(options["skip_hardening"]) or dry_run
        skip_rematch = bool(options["skip_rematch"])
        skip_audit = bool(options["skip_audit"]) or dry_run

        summary_lines = [
            "# Rutina diaria ERP",
            "",
            f"- started_at: {started_at.isoformat()}",
            f"- month: {options['month']}",
            f"- dry_run: {'SI' if dry_run else 'NO'}",
            f"- continue_on_error: {'SI' if options['continue_on_error'] else 'NO'}",
            "",
            "## Pasos",
        ]

        self.stdout.write(self.style.SUCCESS("Rutina diaria ERP"))
        self.stdout.write(f"  - month: {options['month']}")
        self.stdout.write(f"  - dry_run: {'SI' if dry_run else 'NO'}")
        self.stdout.write(f"  - continue_on_error: {'SI' if options['continue_on_error'] else 'NO'}")

        errors: list[str] = []

        def run_step(step_name: str, callback):
            try:
                callback()
                summary_lines.append(f"- [OK] {step_name}")
            except Exception as exc:
                msg = f"{step_name}: {exc.__class__.__name__}: {exc}"
                errors.append(msg)
                summary_lines.append(f"- [ERROR] {msg}")
                self.stderr.write(self.style.ERROR(msg))
                if not options["continue_on_error"]:
                    raise

        if not skip_drive:
            self.stdout.write("1) Sync almacén Drive")

            def _run_drive():
                call_command(
                    "sync_almacen_drive",
                    month=str(options["month"]),
                    fuzzy_threshold=int(options["drive_fuzzy_threshold"]),
                    create_aliases=bool(options["drive_create_aliases"]),
                    dry_run=dry_run,
                )

            run_step(
                f"sync_almacen_drive month={options['month']} fuzzy={options['drive_fuzzy_threshold']}",
                _run_drive,
            )
        else:
            self.stdout.write("1) Sync almacén Drive omitido")
            summary_lines.append("- [SKIP] sync_almacen_drive")

        if not skip_point:
            point_dir = Path(str(options["point_dir"])).expanduser().resolve()
            self.stdout.write("2) Sync catálogos Point")

            def _run_point():
                if not point_dir.exists():
                    raise CommandError(f"No existe point-dir: {point_dir}")
                call_command(
                    "sync_point_catalogs",
                    str(point_dir),
                    fuzzy_threshold=int(options["point_fuzzy_threshold"]),
                    apply_proveedores=not dry_run,
                    apply_insumos=not dry_run,
                    apply_productos=not dry_run,
                    create_aliases=not dry_run,
                    dry_run=dry_run,
                )

            run_step(
                f"sync_point_catalogs dir={point_dir} fuzzy={options['point_fuzzy_threshold']}",
                _run_point,
            )
        else:
            self.stdout.write("2) Sync catálogos Point omitido")
            summary_lines.append("- [SKIP] sync_point_catalogs")

        if not skip_hardening:
            self.stdout.write("3) Hardening post-import")
            run_step("ejecutar_hardening_post_import", lambda: call_command("ejecutar_hardening_post_import"))
        else:
            self.stdout.write("3) Hardening post-import omitido")
            summary_lines.append("- [SKIP] ejecutar_hardening_post_import")

        if not skip_rematch:
            self.stdout.write("4) Rematch recetas")

            def _run_rematch():
                kwargs = {
                    "limit": int(options["rematch_limit"]),
                    "offset": int(options["rematch_offset"]),
                    "progress_every": int(options["rematch_progress_every"]),
                    "apply": not dry_run,
                }
                receta_filter = str(options.get("rematch_recipe") or "").strip()
                if receta_filter:
                    kwargs["receta"] = receta_filter
                if bool(options.get("rematch_include_needs_review", True)):
                    kwargs["include_needs_review"] = True
                call_command("rematch_lineas_receta", **kwargs)

            run_step(
                "rematch_lineas_receta "
                f"limit={options['rematch_limit']} offset={options['rematch_offset']} "
                f"include_needs_review={'SI' if options['rematch_include_needs_review'] else 'NO'}",
                _run_rematch,
            )
        else:
            self.stdout.write("4) Rematch recetas omitido")
            summary_lines.append("- [SKIP] rematch_lineas_receta")

        if not skip_audit:
            self.stdout.write("5) Auditoría final")
            run_step("auditar_flujo_erp", lambda: call_command("auditar_flujo_erp"))
        else:
            self.stdout.write("5) Auditoría final omitida")
            summary_lines.append("- [SKIP] auditar_flujo_erp")

        finished_at = timezone.now()
        summary_lines.extend(
            [
                "",
                "## Tiempos",
                f"- finished_at: {finished_at.isoformat()}",
                f"- duration_sec: {(finished_at - started_at).total_seconds():.2f}",
            ]
        )
        if errors:
            summary_lines.extend(["", "## Errores", *[f"- {e}" for e in errors]])
        summary_path.write_text("\n".join(summary_lines), encoding="utf-8")

        if errors:
            self.stdout.write(self.style.WARNING("Rutina diaria completada con errores"))
        else:
            self.stdout.write(self.style.SUCCESS("Rutina diaria completada"))
        self.stdout.write(f"  - resumen: {summary_path}")
