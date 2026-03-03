from __future__ import annotations

from datetime import datetime
from pathlib import Path

from django.core.management import BaseCommand, CommandError, call_command
from django.utils import timezone


class Command(BaseCommand):
    help = (
        "Ejecuta la rutina diaria operativa del ERP en orden: "
        "1) sync almacén (Drive), 2) sync Point, 3) hardening post-import, "
        "4) auditoría global."
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
            "--drive-create-aliases",
            action="store_true",
            help="Permite creación/actualización de aliases durante sync Drive.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Simula operaciones en sync Drive/Point. Hardening/auditoría se omiten.",
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
        skip_audit = bool(options["skip_audit"]) or dry_run

        summary_lines = [
            "# Rutina diaria ERP",
            "",
            f"- started_at: {started_at.isoformat()}",
            f"- month: {options['month']}",
            f"- dry_run: {'SI' if dry_run else 'NO'}",
            "",
            "## Pasos",
        ]

        self.stdout.write(self.style.SUCCESS("Rutina diaria ERP"))
        self.stdout.write(f"  - month: {options['month']}")
        self.stdout.write(f"  - dry_run: {'SI' if dry_run else 'NO'}")

        if not skip_drive:
            self.stdout.write("1) Sync almacén Drive")
            call_command(
                "sync_almacen_drive",
                month=str(options["month"]),
                fuzzy_threshold=int(options["drive_fuzzy_threshold"]),
                create_aliases=bool(options["drive_create_aliases"]),
                dry_run=dry_run,
            )
            summary_lines.append(
                f"- [OK] sync_almacen_drive month={options['month']} fuzzy={options['drive_fuzzy_threshold']}"
            )
        else:
            self.stdout.write("1) Sync almacén Drive omitido")
            summary_lines.append("- [SKIP] sync_almacen_drive")

        if not skip_point:
            point_dir = Path(str(options["point_dir"])).expanduser().resolve()
            if not point_dir.exists():
                raise CommandError(f"No existe point-dir: {point_dir}")
            self.stdout.write("2) Sync catálogos Point")
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
            summary_lines.append(
                f"- [OK] sync_point_catalogs dir={point_dir} fuzzy={options['point_fuzzy_threshold']}"
            )
        else:
            self.stdout.write("2) Sync catálogos Point omitido")
            summary_lines.append("- [SKIP] sync_point_catalogs")

        if not skip_hardening:
            self.stdout.write("3) Hardening post-import")
            call_command("ejecutar_hardening_post_import")
            summary_lines.append("- [OK] ejecutar_hardening_post_import")
        else:
            self.stdout.write("3) Hardening post-import omitido")
            summary_lines.append("- [SKIP] ejecutar_hardening_post_import")

        if not skip_audit:
            self.stdout.write("4) Auditoría final")
            call_command("auditar_flujo_erp")
            summary_lines.append("- [OK] auditar_flujo_erp")
        else:
            self.stdout.write("4) Auditoría final omitida")
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
        summary_path.write_text("\n".join(summary_lines), encoding="utf-8")

        self.stdout.write(self.style.SUCCESS("Rutina diaria completada"))
        self.stdout.write(f"  - resumen: {summary_path}")
