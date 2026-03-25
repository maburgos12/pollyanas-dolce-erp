from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal

from django.utils import timezone

from pos_bridge.config import load_point_bridge_settings
from pos_bridge.models import PointDailySale, PointProduct
from pos_bridge.services.product_recipe_sync_service import PointProductRecipeSyncService
from pos_bridge.services.sales_matching_service import PointSalesMatchingService
from recetas.models import Receta, RecetaAgrupacionAddon
from recetas.utils.addon_grouping import upsert_addon_rule


ZERO = Decimal("0")


@dataclass(slots=True)
class AddonCandidate:
    sku: str
    name: str
    category: str
    total_qty: Decimal
    rows: int


class PointAddonDetectionService:
    def __init__(self, *, settings=None, recipe_sync_service=None):
        self.settings = settings or load_point_bridge_settings()
        self.recipe_sync_service = recipe_sync_service or PointProductRecipeSyncService(self.settings)
        self.matcher = PointSalesMatchingService()

    def detect_and_stage(
        self,
        *,
        branch_hint: str | None = None,
        limit: int | None = None,
        auto_sync_missing: bool = True,
        top_per_addon: int = 3,
    ) -> dict:
        candidates = self._list_addon_candidates(limit=limit)
        synced_codes: list[str] = []
        if auto_sync_missing and candidates:
            synced_codes = [candidate.sku for candidate in candidates]
            self.recipe_sync_service.sync(branch_hint=branch_hint, product_codes=synced_codes)

        detected_rules = []
        for candidate in candidates:
            addon_receta = self.matcher.resolve_receta(codigo_point=candidate.sku, point_name=candidate.name)
            if addon_receta is None:
                continue
            for base in self._rank_base_candidates(candidate=candidate)[:top_per_addon]:
                base_receta = self.matcher.resolve_receta(codigo_point=base["sku"], point_name=base["name"])
                if base_receta is None:
                    continue
                rule = upsert_addon_rule(
                    base_receta=base_receta,
                    addon_receta=addon_receta,
                    addon_codigo_point=candidate.sku,
                    addon_nombre_point=candidate.name,
                    addon_categoria=candidate.category,
                    status=RecetaAgrupacionAddon.STATUS_DETECTED,
                    notas=(
                        "Detección automática por coocurrencia Point. "
                        f"coverage={base['coverage']}% qty={base['qty']}"
                    ),
                )
                detected_rules.append(
                    {
                        "base_codigo_point": base_receta.codigo_point,
                        "base_nombre": base_receta.nombre,
                        "addon_codigo_point": candidate.sku,
                        "addon_nombre": candidate.name,
                        "coverage": str(base["coverage"]),
                        "qty": str(base["qty"]),
                        "rule_id": rule.id,
                        "status": rule.status,
                    }
                )

        report = {
            "generated_at": timezone.now().isoformat(),
            "candidates": [
                {
                    "sku": candidate.sku,
                    "name": candidate.name,
                    "category": candidate.category,
                    "total_qty": str(candidate.total_qty),
                    "rows": candidate.rows,
                }
                for candidate in candidates
            ],
            "synced_codes": synced_codes,
            "detected_rules": detected_rules,
        }
        reports_dir = self.settings.storage_root / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        path = reports_dir / f"{timezone.now().strftime('%Y%m%d_%H%M%S')}_point_addon_detection.json"
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        report["report_path"] = str(path)
        return report

    def _list_addon_candidates(self, *, limit: int | None = None) -> list[AddonCandidate]:
        grouped = {}
        qs = PointDailySale.objects.select_related("product").order_by("product_id")
        for sale in qs:
            key = sale.product_id
            bucket = grouped.setdefault(
                key,
                {
                    "sku": sale.product.sku,
                    "name": sale.product.name,
                    "category": sale.product.category,
                    "sales": ZERO,
                    "qty": ZERO,
                    "rows": 0,
                },
            )
            bucket["sales"] += Decimal(str(sale.total_amount or 0))
            bucket["qty"] += Decimal(str(sale.quantity or 0))
            bucket["rows"] += 1

        candidates: list[AddonCandidate] = []
        for item in grouped.values():
            name = (item["name"] or "").lower()
            sku = (item["sku"] or "").upper()
            if item["qty"] <= 0:
                continue
            if item["sales"] != 0:
                continue
            if not (("sabor" in name) or ("topping" in name) or sku.startswith("S")):
                continue
            candidates.append(
                AddonCandidate(
                    sku=item["sku"],
                    name=item["name"],
                    category=item["category"],
                    total_qty=item["qty"],
                    rows=item["rows"],
                )
            )
        candidates.sort(key=lambda item: (item.category, -item.total_qty, item.name))
        return candidates[:limit] if limit else candidates

    def _rank_base_candidates(self, *, candidate: AddonCandidate) -> list[dict]:
        addon_qs = PointDailySale.objects.select_related("product").filter(product__sku__iexact=candidate.sku)
        addon_pairs = set(addon_qs.values_list("sale_date", "branch_id").distinct())
        if not addon_pairs:
            return []

        base_qs = (
            PointDailySale.objects.select_related("product")
            .filter(product__category__iexact=candidate.category, total_amount__gt=0)
            .exclude(product__sku__iexact=candidate.sku)
        )
        grouped: dict[int, dict] = {}
        for sale in base_qs:
            bucket = grouped.setdefault(
                sale.product_id,
                {
                    "sku": sale.product.sku,
                    "name": sale.product.name,
                    "pairs": set(),
                    "qty": ZERO,
                },
            )
            bucket["pairs"].add((sale.sale_date, sale.branch_id))
            bucket["qty"] += Decimal(str(sale.quantity or 0))

        ranked = []
        for bucket in grouped.values():
            overlap = addon_pairs & bucket["pairs"]
            if not overlap:
                continue
            coverage = (Decimal(len(overlap)) / Decimal(len(addon_pairs))) * Decimal("100")
            ranked.append(
                {
                    "sku": bucket["sku"],
                    "name": bucket["name"],
                    "qty": bucket["qty"],
                    "coverage": coverage.quantize(Decimal("0.0001")),
                }
            )
        ranked.sort(key=lambda item: (-item["coverage"], -item["qty"], item["name"]))
        return ranked
