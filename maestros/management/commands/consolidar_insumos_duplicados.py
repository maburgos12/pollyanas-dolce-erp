from __future__ import annotations

from collections import defaultdict
from typing import Dict, List

from django.core.management.base import BaseCommand

from maestros.models import Insumo
from recetas.utils.normalizacion import normalizar_nombre


class Command(BaseCommand):
    help = (
        "Consolida duplicados de insumos por nombre_normalizado. "
        "Por defecto solo audita (dry-run). Con --apply desactiva duplicados y conserva canónico."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Aplica cambios (desactiva duplicados).",
        )

    def handle(self, *args, **options):
        apply_changes = bool(options.get("apply"))

        qs = (
            Insumo.objects.filter(activo=True)
            .select_related("unidad_base", "proveedor_principal")
            .order_by("nombre", "id")
        )

        grouped: Dict[str, List[Insumo]] = defaultdict(list)
        for insumo in qs:
            key = (insumo.nombre_normalizado or normalizar_nombre(insumo.nombre or "")).strip()
            if key:
                grouped[key].append(insumo)

        dup_groups = {k: v for k, v in grouped.items() if len(v) > 1}

        if not dup_groups:
            self.stdout.write(self.style.SUCCESS("No se detectaron duplicados activos por nombre_normalizado."))
            return

        def score(i: Insumo) -> int:
            s = 0
            code = (i.codigo or "")
            if code.startswith("DERIVADO:RECETA:"):
                s += 300
                if ":PREPARACION" in code:
                    s += 50
            if (i.codigo_point or "").strip():
                s += 80
            if i.unidad_base_id:
                s += 40
            if i.proveedor_principal_id:
                s += 20
            if normalizar_nombre(i.nombre).startswith("qa flow"):
                s -= 120
            return s

        affected = 0
        total_deactivated = 0

        self.stdout.write(f"Grupos duplicados detectados: {len(dup_groups)}")
        for key, rows in sorted(dup_groups.items(), key=lambda x: len(x[1]), reverse=True):
            ordered = sorted(rows, key=lambda x: (score(x), -x.id), reverse=True)
            winner = ordered[0]
            losers = ordered[1:]
            affected += 1

            self.stdout.write("-")
            self.stdout.write(f"[{key}] canónico -> #{winner.id} {winner.nombre}")
            for loser in losers:
                self.stdout.write(
                    f"  dup -> #{loser.id} {loser.nombre} "
                    f"(codigo={loser.codigo or '-'}, unidad={(loser.unidad_base.codigo if loser.unidad_base else '-')})"
                )

            if apply_changes:
                for loser in losers:
                    loser.activo = False
                    loser.save(update_fields=["activo"])
                    total_deactivated += 1

        if apply_changes:
            self.stdout.write(self.style.SUCCESS(
                f"Consolidación aplicada. Grupos: {affected}. Insumos desactivados: {total_deactivated}."
            ))
        else:
            self.stdout.write(self.style.WARNING(
                "Dry-run: no se aplicaron cambios. Usa --apply para desactivar duplicados."
            ))
