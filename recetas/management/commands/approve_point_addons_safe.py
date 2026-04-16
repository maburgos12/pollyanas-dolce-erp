from __future__ import annotations

import json
from decimal import Decimal

from django.core.management.base import BaseCommand
from django.db.models import Count
from django.utils import timezone

from pos_bridge.config import load_point_bridge_settings
from pos_bridge.models import PointProduct
from recetas.models import Receta, RecetaAgrupacionAddon
from recetas.utils.addon_grouping import calculate_grouped_addon_cost, upsert_addon_rule
from recetas.utils.commercial_composition import (
    EXPLICIT_DUPLICATE_ALLOWED_CODES,
    KNOWN_BLOCKED_CODES,
    SAFE_APPROVAL_SPECS,
    ensure_curated_commercial_mappings,
)


class Command(BaseCommand):
    help = (
        "Aprueba de forma curada e idempotente addons base+addon ya validados de negocio, "
        "saltando sku ambiguos o recetas faltantes."
    )

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        dry_run = bool(options.get("dry_run"))
        settings = load_point_bridge_settings()
        ensure_curated_commercial_mappings()
        duplicate_skus = {
            row["sku"]
            for row in PointProduct.objects.values("sku")
            .annotate(total=Count("id"))
            .filter(total__gt=1)
        }

        report: dict[str, object] = {
            "generated_at": timezone.now().isoformat(),
            "dry_run": dry_run,
            "approved": [],
            "skipped": [],
        }

        for addon_code, reason in KNOWN_BLOCKED_CODES.items():
            if addon_code in duplicate_skus:
                report["skipped"].append(
                    {
                        "addon_codigo_point": addon_code,
                        "base_codigo_point": "",
                        "reason": reason,
                    }
                )

        for item in SAFE_APPROVAL_SPECS:
            addon_code = item.addon_codigo_point.strip().upper()
            base_code = item.base_codigo_point.strip().upper()
            if addon_code in duplicate_skus and addon_code not in EXPLICIT_DUPLICATE_ALLOWED_CODES:
                report["skipped"].append(
                    {
                        "addon_codigo_point": addon_code,
                        "base_codigo_point": base_code,
                        "reason": "SKU duplicado en Point; requiere identidad por external_id.",
                    }
                )
                continue

            addon_receta = Receta.objects.filter(codigo_point__iexact=addon_code).order_by("id").first()
            if addon_receta is None:
                report["skipped"].append(
                    {
                        "addon_codigo_point": addon_code,
                        "base_codigo_point": base_code,
                        "reason": "Receta addon no encontrada en ERP.",
                    }
                )
                continue
            if addon_code in EXPLICIT_DUPLICATE_ALLOWED_CODES:
                addon_receta.temporalidad = Receta.TEMPORALIDAD_TEMPORAL
                addon_receta.temporalidad_detalle = "Temporada manzana"
                addon_receta.save(update_fields=["temporalidad", "temporalidad_detalle"])

            base_receta = Receta.objects.filter(codigo_point__iexact=base_code).order_by("id").first()
            if base_receta is None:
                report["skipped"].append(
                    {
                        "addon_codigo_point": addon_code,
                        "base_codigo_point": base_code,
                        "reason": "Receta base no encontrada en ERP.",
                    }
                )
                continue

            existing = RecetaAgrupacionAddon.objects.filter(
                base_receta=base_receta,
                addon_codigo_point=addon_code,
                activo=True,
            ).order_by("id").first()
            if dry_run:
                if existing is None:
                    rule_status = "WOULD_CREATE"
                    grouped_cost = None
                    base_cost = None
                    addon_cost = None
                else:
                    rule_status = existing.status
                    if existing.addon_receta_id:
                        grouped = calculate_grouped_addon_cost(rule=existing)
                        grouped_cost = str(grouped.grouped_cost)
                        base_cost = str(grouped.base_cost)
                        addon_cost = str(grouped.addon_cost)
                    else:
                        grouped_cost = None
                        base_cost = None
                        addon_cost = None
                report["approved"].append(
                    {
                        "addon_codigo_point": addon_code,
                        "addon_nombre": addon_receta.nombre,
                        "base_codigo_point": base_code,
                        "base_nombre": base_receta.nombre,
                        "status": rule_status,
                        "reason": item.reason,
                        "base_cost": base_cost,
                        "addon_cost": addon_cost,
                        "grouped_cost": grouped_cost,
                    }
                )
                continue

            rule = upsert_addon_rule(
                base_receta=base_receta,
                addon_receta=addon_receta,
                addon_codigo_point=addon_code,
                addon_nombre_point=addon_receta.nombre,
                addon_familia=addon_receta.familia,
                addon_categoria=addon_receta.categoria,
                status=RecetaAgrupacionAddon.STATUS_APPROVED,
                notas=f"Aprobación curada DG. {item.reason}",
            )
            grouped = calculate_grouped_addon_cost(rule=rule)
            report["approved"].append(
                {
                    "addon_codigo_point": addon_code,
                    "addon_nombre": addon_receta.nombre,
                    "base_codigo_point": base_code,
                    "base_nombre": base_receta.nombre,
                    "status": rule.status,
                    "confidence_score": str(rule.confidence_score),
                    "cooccurrence_qty": str(rule.cooccurrence_qty),
                    "base_cost": str(grouped.base_cost),
                    "addon_cost": str(grouped.addon_cost),
                    "grouped_cost": str(grouped.grouped_cost),
                    "reason": item.reason,
                }
            )

        reports_dir = settings.storage_root / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        report_path = reports_dir / f"{timezone.now().strftime('%Y%m%d_%H%M%S')}_point_addon_safe_approvals.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        report["report_path"] = str(report_path)
        self.stdout.write(json.dumps(report, ensure_ascii=False, indent=2))
