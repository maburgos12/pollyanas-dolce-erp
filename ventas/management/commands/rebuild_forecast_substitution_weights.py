from __future__ import annotations

from django.core.management.base import BaseCommand

from ventas.services.substitution_learning import rebuild_substitution_weights


class Command(BaseCommand):
    help = (
        "Recalcula la matriz aprendida de sustitución comercial (V7.2) por grupo competitivo "
        "usando historia real y persiste pesos branch/global auditables."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--lookback-days",
            type=int,
            default=180,
            help="Ventana histórica a usar para aprender pesos de sustitución.",
        )
        parser.add_argument(
            "--window-days",
            type=int,
            default=7,
            help="Tamaño de bucket temporal para aprender cambios de share.",
        )
        parser.add_argument(
            "--branch-id",
            action="append",
            dest="branch_ids",
            type=int,
            help="Sucursal específica. Repetible.",
        )
        parser.add_argument(
            "--family",
            type=str,
            help="Filtra el aprendizaje a una familia comercial específica.",
        )
        parser.add_argument(
            "--category",
            type=str,
            help="Filtra el aprendizaje a una categoría comercial específica.",
        )
        parser.add_argument(
            "--weights-version",
            type=str,
            default="v7.2-learned",
            help="Versión lógica del snapshot de pesos aprendidos.",
        )
        parser.add_argument(
            "--keep-existing",
            action="store_true",
            help="No borrar pesos existentes antes de reconstruir el snapshot.",
        )

    def handle(self, *args, **options):
        result = rebuild_substitution_weights(
            lookback_days=int(options["lookback_days"]),
            window_days=int(options["window_days"]),
            branch_ids=options.get("branch_ids") or None,
            family=(options.get("family") or "").strip() or None,
            category=(options.get("category") or "").strip() or None,
            clear_existing=not bool(options.get("keep_existing")),
            version=(options.get("weights_version") or "v7.2-learned").strip(),
        )
        self.stdout.write(
            self.style.SUCCESS(
                "Pesos aprendidos recalculados. "
                f"rows={result['created']} groups={result['groups']} scopes={result['scopes']} rows_seen={result['rows_seen']}"
            )
        )
