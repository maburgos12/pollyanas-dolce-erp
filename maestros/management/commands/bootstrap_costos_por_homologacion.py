from __future__ import annotations

import hashlib
from datetime import date
from decimal import Decimal

from django.core.management.base import BaseCommand
from django.db.models import Q
from rapidfuzz import fuzz

from maestros.models import CostoInsumo, Insumo, Proveedor
from recetas.models import LineaReceta


class Command(BaseCommand):
    help = (
        "Genera costos para insumos sin costo clonando el costo más reciente de un insumo "
        "homologado por similitud (regla estricta de score y unicidad)."
    )

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true", help="Aplica cambios. Por default dry-run.")
        parser.add_argument(
            "--min-score",
            type=float,
            default=95.0,
            help="Score mínimo de similitud token_set_ratio (default: 95).",
        )
        parser.add_argument(
            "--min-gap",
            type=float,
            default=3.0,
            help="Diferencia mínima entre mejor y segundo score para aceptar (default: 3).",
        )
        parser.add_argument(
            "--proveedor-auto",
            type=str,
            default="AUTO HOMOLOGACION",
            help="Proveedor a usar en costos auto por homologación.",
        )

    def handle(self, *args, **options):
        min_score = float(options["min_score"])
        min_gap = float(options["min_gap"])
        proveedor_nombre = (options["proveedor_auto"] or "AUTO HOMOLOGACION").strip()

        # Solo insumos que hoy afectan líneas sin snapshot.
        target_ids = set(
            LineaReceta.objects.filter(insumo__isnull=False)
            .filter(Q(costo_unitario_snapshot__isnull=True) | Q(costo_unitario_snapshot__lte=0))
            .values_list("insumo_id", flat=True)
        )

        insumos_sin_costo = Insumo.objects.filter(id__in=target_ids).exclude(
            id__in=CostoInsumo.objects.values_list("insumo_id", flat=True).distinct()
        )

        costed = list(
            Insumo.objects.filter(id__in=CostoInsumo.objects.values_list("insumo_id", flat=True).distinct())
            .values_list("id", "nombre", "nombre_normalizado")
            .order_by("nombre")
        )

        proposals: list[tuple[Insumo, int, str, float, Decimal]] = []
        for insumo in insumos_sin_costo.order_by("nombre"):
            source_norm = insumo.nombre_normalizado or " ".join((insumo.nombre or "").lower().split())
            scored = []
            for cid, cname, cnorm in costed:
                score = float(fuzz.token_set_ratio(source_norm, cnorm))
                if score >= min_score:
                    scored.append((score, cid, cname))
            if not scored:
                continue
            scored.sort(key=lambda x: (-x[0], x[2]))
            best_score, best_id, best_name = scored[0]
            second_score = scored[1][0] if len(scored) > 1 else 0.0
            if (best_score - second_score) < min_gap:
                continue

            latest = (
                CostoInsumo.objects.filter(insumo_id=best_id)
                .order_by("-fecha", "-id")
                .values_list("costo_unitario", flat=True)
                .first()
            )
            if latest is None:
                continue
            proposals.append((insumo, best_id, best_name, best_score, Decimal(str(latest))))

        self.stdout.write("Bootstrap costos por homologación")
        self.stdout.write(f"  - insumos target (sin snapshot): {len(target_ids)}")
        self.stdout.write(f"  - insumos sin costo evaluados: {insumos_sin_costo.count()}")
        self.stdout.write(f"  - propuestas aceptadas: {len(proposals)}")
        if proposals:
            self.stdout.write("  - muestra:")
            for insumo, _, best_name, score, costo in proposals[:20]:
                self.stdout.write(f"    * {insumo.nombre} <= {best_name} | score={score:.1f} | costo={costo}")

        if not options["apply"]:
            self.stdout.write("Dry-run: no se crearon costos. Usa --apply para confirmar.")
            return

        proveedor, _ = Proveedor.objects.get_or_create(nombre=proveedor_nombre, defaults={"activo": True})
        today = date.today()
        created = 0
        for insumo, best_id, best_name, score, costo in proposals:
            source_hash = hashlib.sha256(
                f"AUTO_HOMOLOGA:{insumo.id}:{best_id}:{today.isoformat()}:{costo}".encode("utf-8")
            ).hexdigest()
            _, was_created = CostoInsumo.objects.get_or_create(
                source_hash=source_hash,
                defaults={
                    "insumo": insumo,
                    "proveedor": proveedor,
                    "fecha": today,
                    "moneda": "MXN",
                    "costo_unitario": costo,
                    "raw": {
                        "fuente": "AUTO_HOMOLOGACION_TOKEN_SET",
                        "match_nombre": best_name,
                        "match_score": score,
                    },
                },
            )
            if was_created:
                created += 1

        self.stdout.write(self.style.SUCCESS(f"Costos creados: {created}"))
