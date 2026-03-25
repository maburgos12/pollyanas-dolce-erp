from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from django.db import transaction
from rapidfuzz import fuzz

from recetas.models import Receta, RecetaPresentacionDerivada
from recetas.utils.normalizacion import normalizar_nombre


@dataclass(slots=True)
class PointDerivedPresentationSyncResult:
    summary: dict
    report_path: str


class PointDerivedPresentationSyncService:
    SHEET_NAME = "AUTO_POINT_DERIVED_PRESENTATION"
    PARENT_CODE_OVERRIDES = {
        "0003": "0001",
        "0007": "0005",
        "0058": "0055",
        "0063": "0060",
        "0068": "0065",
        "0103": "0100",
        "0106": "0105",
        "0110": "0108",
    }
    NOISE_TOKENS = {"de", "del", "la", "las", "el", "los", "con", "y", "r", "rebanada"}
    SIZE_TOKENS = {"mediano", "grande", "chico", "individual", "mini", "media plancha", "1/2 plancha"}
    SEASONAL_TOKENS = {
        "san valentin",
        "san valentín",
        "navideno",
        "navideño",
        "edicion",
        "edición",
        "dia del padre",
        "día del padre",
        "dia de la madre",
        "día de la madre",
        "halloween",
        "navidad",
    }

    def __init__(self, *, storage_root: Path):
        self.storage_root = storage_root

    @property
    def reports_dir(self) -> Path:
        return self.storage_root / "reports"

    def latest_report_path(self) -> Path:
        files = sorted(self.reports_dir.glob("*_point_recipe_gap_audit.json"))
        if not files:
            raise FileNotFoundError("No existe un reporte de auditoría de recetas faltantes en storage/pos_bridge/reports.")
        return files[-1]

    def sync(
        self,
        *,
        report_path: str | None = None,
        create_missing_recipes: bool = True,
    ) -> PointDerivedPresentationSyncResult:
        resolved_report_path = Path(report_path).expanduser().resolve() if report_path else self.latest_report_path()
        payload = json.loads(resolved_report_path.read_text(encoding="utf-8"))
        items = list(payload.get("items") or [])
        derived_items = [item for item in items if item.get("status") == "DERIVED_PRESENTATION"]
        summary = {
            "report_path": str(resolved_report_path),
            "items_seen": len(items),
            "derived_items_seen": len(derived_items),
            "relations_created": 0,
            "relations_updated": 0,
            "derived_recipes_created": 0,
            "unresolved_parent_matches": 0,
            "unresolved_derived_matches": 0,
        }

        for item in derived_items:
            result = self._sync_item(item=item, create_missing_recipes=create_missing_recipes)
            for key in summary:
                if key in result:
                    summary[key] += result[key]

        return PointDerivedPresentationSyncResult(summary=summary, report_path=str(resolved_report_path))

    def _base_name(self, product_name: str) -> str:
        norm = normalizar_nombre(product_name)
        tokens = [token for token in norm.split() if token not in self.NOISE_TOKENS]
        return " ".join(tokens).strip()

    def _resolve_parent_recipe(self, *, product: dict, derived_rule: dict) -> Receta | None:
        derived_code = (product.get("codigo") or "").strip()
        override_code = self.PARENT_CODE_OVERRIDES.get(derived_code)
        if override_code:
            recipe = Receta.objects.filter(codigo_point__iexact=override_code).order_by("id").first()
            if recipe is not None:
                return recipe
        base_name = self._base_name(product.get("nombre") or "")
        size_hint = normalizar_nombre(derived_rule.get("parent_size_hint") or "")
        family = normalizar_nombre(product.get("familia") or "")
        candidates = list(Receta.objects.filter(tipo=Receta.TIPO_PRODUCTO_FINAL).order_by("nombre"))
        ranked: list[tuple[int, Receta]] = []
        for receta in candidates:
            name_norm = normalizar_nombre(receta.nombre)
            score = 0
            base_tokens = [token for token in base_name.split() if token not in self.NOISE_TOKENS]
            candidate_tokens = [token for token in name_norm.split() if token not in self.NOISE_TOKENS]
            if base_tokens and all(token in name_norm for token in base_tokens):
                score += 100
            elif base_name and base_name in name_norm:
                score += 80
            elif base_name:
                score += int(fuzz.token_set_ratio(base_name, name_norm) * 0.7)
            if size_hint and size_hint in name_norm:
                score += 150
            elif size_hint and any(token in name_norm for token in self.SIZE_TOKENS if token != size_hint):
                score -= 80
            if family == "pastel" and "pastel" in name_norm:
                score += 10
            if family == "pay" and "pay" in name_norm:
                score += 10
            if "rebanada" in name_norm:
                score -= 100
            if any(token in name_norm for token in self.SEASONAL_TOKENS):
                score -= 60
            extra_tokens = [
                token for token in candidate_tokens
                if token not in base_tokens and token not in self.SIZE_TOKENS
            ]
            score -= len(extra_tokens) * 12
            if score > 0:
                ranked.append((score, receta))
        ranked.sort(key=lambda item: (item[0], item[1].id), reverse=True)
        return ranked[0][1] if ranked else None

    def _placeholder_hash(self, *, product: dict) -> str:
        raw = f"POINT_DERIVED|{product.get('codigo') or ''}|{product.get('nombre') or ''}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _resolve_or_create_derived_recipe(self, *, product: dict, create_missing_recipes: bool) -> tuple[Receta | None, bool]:
        code = (product.get("codigo") or "").strip()
        recipe = None
        if code:
            recipe = Receta.objects.filter(codigo_point__iexact=code).order_by("id").first()
        if recipe is None:
            recipe = Receta.objects.filter(nombre_normalizado=normalizar_nombre(product.get("nombre") or "")).order_by("id").first()
        if recipe is not None or not create_missing_recipes:
            return recipe, False

        recipe = Receta.objects.create(
            nombre=(product.get("nombre") or "Producto derivado Point")[:250],
            codigo_point=code[:80],
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            familia=(product.get("familia") or "")[:120],
            categoria=(product.get("categoria") or "")[:120],
            sheet_name=self.SHEET_NAME,
            hash_contenido=self._placeholder_hash(product=product),
        )
        return recipe, True

    @transaction.atomic
    def _sync_item(self, *, item: dict, create_missing_recipes: bool) -> dict:
        product = item.get("product") or {}
        derived_rule = item.get("derived_rule") or {}
        summary = {
            "relations_created": 0,
            "relations_updated": 0,
            "derived_recipes_created": 0,
            "unresolved_parent_matches": 0,
            "unresolved_derived_matches": 0,
        }
        parent = self._resolve_parent_recipe(product=product, derived_rule=derived_rule)
        if parent is None:
            summary["unresolved_parent_matches"] += 1
            return summary

        derived_recipe, created_recipe = self._resolve_or_create_derived_recipe(
            product=product,
            create_missing_recipes=create_missing_recipes,
        )
        if derived_recipe is None:
            summary["unresolved_derived_matches"] += 1
            return summary
        if created_recipe:
            summary["derived_recipes_created"] += 1

        code = (product.get("codigo") or "")[:80]
        RecetaPresentacionDerivada.objects.exclude(receta_padre=parent).filter(codigo_point_derivado=code).delete()
        relation, created = RecetaPresentacionDerivada.objects.update_or_create(
            codigo_point_derivado=code,
            defaults={
                "receta_padre": parent,
                "receta_derivada": derived_recipe,
                "nombre_derivado": (product.get("nombre") or derived_recipe.nombre)[:250],
                "tipo_derivado": RecetaPresentacionDerivada.TIPO_REBANADA,
                "unidades_por_padre": Decimal(str(derived_rule.get("units_per_parent") or "0")),
                "padre_size_hint": (derived_rule.get("parent_size_hint") or "")[:40],
                "requiere_componentes_directos": bool(derived_rule.get("requires_direct_components")),
                "fuente": "POINT_AUDIT",
                "notas": (derived_rule.get("recommended_action") or "")[:1000],
                "activo": True,
            },
        )
        if created:
            summary["relations_created"] += 1
        else:
            summary["relations_updated"] += 1
        return summary
