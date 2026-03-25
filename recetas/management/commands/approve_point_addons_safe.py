from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal

from django.core.management.base import BaseCommand
from django.db.models import Count
from django.utils import timezone

from pos_bridge.config import load_point_bridge_settings
from pos_bridge.models import PointProduct
from recetas.models import Receta, RecetaAgrupacionAddon
from recetas.utils.addon_grouping import calculate_grouped_addon_cost, upsert_addon_rule


@dataclass(frozen=True, slots=True)
class CuratedAddonApproval:
    addon_codigo_point: str
    base_codigo_point: str
    reason: str


SAFE_APPROVALS: tuple[CuratedAddonApproval, ...] = (
    CuratedAddonApproval("SFRESAG", "0001", "Pay de queso grande con sabor fresa."),
    CuratedAddonApproval("03SPOREB", "0003", "Pay de queso rebanada con sabor oreo."),
    CuratedAddonApproval("SMANZANAREB", "0003", "Pay de queso rebanada con sabor manzana."),
    CuratedAddonApproval("SOREOG", "0001", "Pay de queso grande con sabor oreo."),
    CuratedAddonApproval("SOREOM", "0002", "Pay de queso mediano con sabor oreo."),
    CuratedAddonApproval("SBROWNIEG", "0001", "Pay de queso grande con sabor brownie."),
    CuratedAddonApproval("SBROWNIEM", "0002", "Pay de queso mediano con sabor brownie."),
    CuratedAddonApproval("SFRESAPC", "0101", "Pastel de fresas con crema chico con topping fresa."),
    CuratedAddonApproval("SFRESAPG", "0099", "Pastel de fresas con crema grande con topping fresa."),
    CuratedAddonApproval("SFRESAPM", "0100", "Pastel de fresas con crema mediano con topping fresa."),
    CuratedAddonApproval("SFRESAPMINI", "PFCMINI", "Pastel fresas con crema mini con topping fresa."),
    CuratedAddonApproval("1412", "0056", "Pastel de Snickers chico con topping Snickers."),
    CuratedAddonApproval("21254", "0054", "Pastel de Snickers grande con topping Snickers."),
    CuratedAddonApproval("22145", "0055", "Pastel de Snickers mediano con topping Snickers."),
    CuratedAddonApproval("214541", "0061", "Pastel de Crunch chico con topping Crunch."),
    CuratedAddonApproval("21455", "0059", "Pastel de Crunch grande con topping Crunch."),
    CuratedAddonApproval("21125", "0060", "Pastel de Crunch mediano con topping Crunch."),
    CuratedAddonApproval("1125", "0064", "Pastel de zanahoria grande con topping zanahoria."),
    CuratedAddonApproval("21445", "0065", "Pastel de zanahoria mediano con topping zanahoria."),
    CuratedAddonApproval("21245", "0105", "Pastel de 3 leches mediano con topping 3 leches."),
)

KNOWN_BLOCKED_CODES: dict[str, str] = {
    "1254": "SKU duplicado en Point entre TOPPING ZANAHORIA C y TOPPING CRUNCH MINI.",
}

EXPLICIT_DUPLICATE_ALLOWED_CODES: dict[str, str] = {
    "SMANZANAREB": "DG confirmó que corresponde a Pay de queso rebanada con sabor manzana de temporada.",
}


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

        for item in SAFE_APPROVALS:
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
