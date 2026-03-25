from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path

from django.utils import timezone
from rapidfuzz import fuzz

from maestros.models import Insumo
from pos_bridge.config import load_point_bridge_settings
from pos_bridge.services.point_http_client import PointHttpSessionClient
from pos_bridge.utils.helpers import normalize_text, write_json_file
from recetas.models import Receta, normalizar_codigo_point

NON_RECIPE_FAMILIES = {
    "wilton",
    "accesorios",
    "plasticos",
    "plásticos",
    "regalos",
    "velas",
    "bebidas",
    "hielo",
}

NON_RECIPE_CATEGORIES = {
    "accesorios de reposteria",
    "accesorios de repostería",
    "granmark",
    "alegria",
    "alegría",
    "plasticos",
    "plásticos",
    "regalos",
    "letreros",
    "velas sparklers",
    "industrias lec",
    "te",
    "té",
    "hielo y agua mar de cortez",
}


@dataclass(slots=True)
class PointRecipeGapAuditResult:
    summary: dict
    report_path: str
    raw_export_path: str


class PointRecipeGapAuditService:
    """Audita productos sin BOM en Point y busca evidencia en catálogos de insumos."""

    STATUS_DERIVED_PRESENTATION = "DERIVED_PRESENTATION"
    STATUS_CORROBORATED = "CORROBORATED_FROM_INSUMO_CATALOG"
    STATUS_REVIEW = "POSSIBLE_MATCH_REQUIRES_REVIEW"
    STATUS_CANDIDATE_WITHOUT_BOM = "INTERNAL_CANDIDATE_WITHOUT_BOM"
    STATUS_MISSING = "MISSING_IN_POINT"

    def __init__(self, settings=None, *, http_client_factory=None):
        self.settings = settings or load_point_bridge_settings()
        self.http_client_factory = http_client_factory or (lambda: PointHttpSessionClient(self.settings))

    @property
    def reports_dir(self) -> Path:
        path = self.settings.storage_root / "reports"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def audit(
        self,
        *,
        branch_hint: str | None = None,
        product_codes: list[str] | None = None,
        limit: int | None = None,
    ) -> PointRecipeGapAuditResult:
        selected_codes = {normalizar_codigo_point(code) for code in (product_codes or []) if (code or "").strip()}
        audited_items: list[dict] = []
        summary = {
            "workspace": "",
            "products_seen": 0,
            "products_skipped_non_recipe": 0,
            "products_with_product_bom": 0,
            "products_missing_recipe_in_point": 0,
            "products_audited": 0,
            "products_derived_presentations": 0,
            "corroborated_from_insumos": 0,
            "possible_matches_requiring_review": 0,
            "internal_candidates_without_bom": 0,
            "missing_without_candidates": 0,
        }

        with self.http_client_factory() as client:
            workspace = client.login(branch_hint=branch_hint)
            summary["workspace"] = workspace["branch_name"]
            products = client.get_products()
            summary["products_seen"] = len(products)

            for product in products:
                product_code = normalizar_codigo_point(product.get("Codigo") or "")
                if selected_codes and product_code not in selected_codes:
                    continue
                if self._is_non_recipe_product(product):
                    summary["products_skipped_non_recipe"] += 1
                    continue

                bom_rows = client.get_product_bom(product["PK_Producto"])
                if bom_rows:
                    summary["products_with_product_bom"] += 1
                    continue

                detail = client.get_product_detail(product["PK_Producto"])
                audit_item = self._audit_missing_product(client=client, product=product, detail=detail)
                audited_items.append(audit_item)
                summary["products_missing_recipe_in_point"] += 1
                summary["products_audited"] += 1
                self._apply_status_to_summary(summary, audit_item["status"])

                if limit and summary["products_audited"] >= limit:
                    break

        generated_at = timezone.now()
        raw_payload = {
            "generated_at": generated_at.isoformat(),
            "summary": summary,
            "items": audited_items,
        }
        raw_export_path = self.reports_dir / f"{generated_at.strftime('%Y%m%d_%H%M%S')}_point_recipe_gap_audit.json"
        write_json_file(raw_export_path, raw_payload)

        report_path = self.reports_dir / f"{generated_at.strftime('%Y%m%d_%H%M%S')}_point_recipe_gap_audit.csv"
        self._write_csv(report_path, audited_items)
        return PointRecipeGapAuditResult(
            summary=summary,
            report_path=str(report_path),
            raw_export_path=str(raw_export_path),
        )

    def _is_non_recipe_product(self, product: dict) -> bool:
        family = normalize_text(product.get("Familia") or "")
        category = normalize_text(product.get("Categoria") or "")
        return family in NON_RECIPE_FAMILIES or category in NON_RECIPE_CATEGORIES

    def _apply_status_to_summary(self, summary: dict, status: str) -> None:
        if status == self.STATUS_DERIVED_PRESENTATION:
            summary["products_derived_presentations"] += 1
        elif status == self.STATUS_CORROBORATED:
            summary["corroborated_from_insumos"] += 1
        elif status == self.STATUS_REVIEW:
            summary["possible_matches_requiring_review"] += 1
        elif status == self.STATUS_CANDIDATE_WITHOUT_BOM:
            summary["internal_candidates_without_bom"] += 1
        elif status == self.STATUS_MISSING:
            summary["missing_without_candidates"] += 1

    def _audit_missing_product(self, *, client, product: dict, detail: dict) -> dict:
        derived_rule = self._infer_derived_presentation_rule(product=product, detail=detail)
        candidates = self._find_internal_candidates(client=client, product=product, detail=detail)
        status = self._classify_candidates(candidates, derived_rule=derived_rule)
        best_candidate = candidates[0] if candidates else None
        return {
            "product": {
                "pk_producto": product.get("PK_Producto"),
                "codigo": (product.get("Codigo") or detail.get("Codigo") or "").strip(),
                "nombre": (product.get("Nombre") or detail.get("Nombre") or "").strip(),
                "familia": (product.get("Familia") or "").strip(),
                "categoria": (product.get("Categoria") or "").strip(),
                "has_receta_flag": bool(product.get("hasReceta")),
            },
            "status": status,
            "derived_rule": derived_rule,
            "best_candidate": best_candidate,
            "candidates": candidates,
        }

    def _build_search_terms(self, *, product: dict, detail: dict) -> list[str]:
        code = (product.get("Codigo") or detail.get("Codigo") or "").strip()
        name = (product.get("Nombre") or detail.get("Nombre") or "").strip()
        normalized_name = normalize_text(name)
        noise_words = {
            "de",
            "del",
            "la",
            "las",
            "el",
            "los",
            "mini",
            "mediano",
            "mediana",
            "grande",
            "personal",
            "vaso",
            "rebanada",
            "pza",
            "pz",
            "por",
        }
        focus_tokens = [token for token in normalized_name.split() if len(token) > 2 and token not in noise_words]
        if focus_tokens and focus_tokens[-1] == "r":
            focus_tokens = focus_tokens[:-1]
        search_terms: list[str] = []
        base_name = " ".join(token for token in focus_tokens if token not in {"rebanada"})
        for value in (code, name, base_name, " ".join(focus_tokens[:4])):
            value = str(value or "").strip()
            if value and normalize_text(value) not in {normalize_text(item) for item in search_terms}:
                search_terms.append(value)
        return search_terms

    def _infer_derived_presentation_rule(self, *, product: dict, detail: dict) -> dict | None:
        name = normalize_text(product.get("Nombre") or detail.get("Nombre") or "")
        family = normalize_text(product.get("Familia") or "")
        category = normalize_text(product.get("Categoria") or "")
        if "rebanada" not in name and "rebanada" not in category:
            return None

        units_per_parent = None
        parent_size_hint = ""
        if "3 leches" in name or "tres leches" in name:
            units_per_parent = 6
            parent_size_hint = "MEDIANO"
        elif family == "pastel":
            units_per_parent = 10
            parent_size_hint = "MEDIANO"
        elif family == "pay":
            units_per_parent = 8
            parent_size_hint = "GRANDE"

        return {
            "kind": "SLICE_FROM_PARENT",
            "units_per_parent": units_per_parent,
            "parent_size_hint": parent_size_hint,
            "requires_direct_components": True,
            "recommended_action": "Ligar a receta padre y mantener empaque/etiqueta como componentes directos del SKU derivado.",
        }

    def _score_candidate(self, *, product_code: str, product_name: str, candidate_code: str, candidate_name: str) -> float:
        name_score = max(
            float(fuzz.token_set_ratio(product_name, candidate_name)),
            float(fuzz.partial_ratio(product_name, candidate_name)),
        )
        if product_code and normalizar_codigo_point(product_code) == normalizar_codigo_point(candidate_code):
            name_score = min(100.0, name_score + 20.0)
        return round(name_score, 2)

    def _find_internal_candidates(self, *, client, product: dict, detail: dict) -> list[dict]:
        product_code = (product.get("Codigo") or detail.get("Codigo") or "").strip()
        product_name = (product.get("Nombre") or detail.get("Nombre") or "").strip()
        found: dict[int, dict] = {}

        for search_term in self._build_search_terms(product=product, detail=detail):
            for row in client.get_articulos(search=search_term):
                pk_articulo = row.get("PK_Articulo")
                if pk_articulo in (None, ""):
                    continue
                candidate_code = (row.get("Codigo_Articulo") or "").strip()
                candidate_name = (row.get("Nombre_Articulo") or "").strip()
                score = self._score_candidate(
                    product_code=product_code,
                    product_name=product_name,
                    candidate_code=candidate_code,
                    candidate_name=candidate_name,
                )
                entry = found.setdefault(
                    int(pk_articulo),
                    {
                        "pk_articulo": int(pk_articulo),
                        "codigo": candidate_code,
                        "nombre": candidate_name,
                        "categoria": (row.get("Categoria") or "").strip(),
                        "has_recipe_flag": bool(row.get("HasReceta")),
                        "score": score,
                        "search_terms": [search_term],
                    },
                )
                if score > entry["score"]:
                    entry["score"] = score
                if search_term not in entry["search_terms"]:
                    entry["search_terms"].append(search_term)
                entry["has_recipe_flag"] = entry["has_recipe_flag"] or bool(row.get("HasReceta"))

        ranked = sorted(
            found.values(),
            key=lambda item: (
                item["score"],
                1 if item["has_recipe_flag"] else 0,
                item["nombre"],
            ),
            reverse=True,
        )
        for candidate in ranked[:5]:
            if not candidate["has_recipe_flag"] and candidate["score"] < 70:
                candidate["bom_count"] = 0
                candidate["erp_insumo_id"] = None
                candidate["erp_insumo_nombre"] = ""
                candidate["erp_preparation_recipe_exists"] = False
                continue
            detail_row = client.get_articulo_detail(candidate["pk_articulo"])
            bom_rows = list(detail_row.get("BOM") or [])
            erp_insumo = Insumo.objects.filter(codigo_point__iexact=candidate["codigo"]).order_by("id").first()
            candidate["bom_count"] = len(bom_rows)
            candidate["bom_preview"] = [
                {
                    "codigo": (row.get("CodigoInsumo") or "").strip(),
                    "nombre": (row.get("Nombre") or "").strip(),
                    "cantidad": row.get("Cantidad"),
                    "unidad": ((row.get("UnidadVenta") or {}).get("Abreviacion") or "").strip(),
                }
                for row in bom_rows[:3]
            ]
            candidate["erp_insumo_id"] = erp_insumo.id if erp_insumo else None
            candidate["erp_insumo_nombre"] = erp_insumo.nombre if erp_insumo else ""
            candidate["erp_preparation_recipe_exists"] = bool(
                Receta.objects.filter(
                    codigo_point__iexact=candidate["codigo"],
                    tipo=Receta.TIPO_PREPARACION,
                ).exists()
            )

        return ranked[:5]

    def _classify_candidates(self, candidates: list[dict], *, derived_rule: dict | None = None) -> str:
        if derived_rule is not None:
            return self.STATUS_DERIVED_PRESENTATION
        if not candidates:
            return self.STATUS_MISSING
        with_bom = [candidate for candidate in candidates if int(candidate.get("bom_count") or 0) > 0]
        if len(with_bom) == 1 and float(with_bom[0].get("score") or 0) >= 70:
            return self.STATUS_CORROBORATED
        if with_bom:
            return self.STATUS_REVIEW
        if any(float(candidate.get("score") or 0) >= 60 for candidate in candidates):
            return self.STATUS_CANDIDATE_WITHOUT_BOM
        return self.STATUS_MISSING

    def _write_csv(self, path: Path, items: list[dict]) -> None:
        headers = [
            "product_code",
            "product_name",
            "product_family",
            "product_category",
            "status",
            "derived_kind",
            "units_per_parent",
            "parent_size_hint",
            "requires_direct_components",
            "candidate_code",
            "candidate_name",
            "candidate_score",
            "candidate_has_recipe_flag",
            "candidate_bom_count",
            "erp_insumo_id",
            "erp_insumo_nombre",
            "erp_preparation_recipe_exists",
            "search_terms",
        ]
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=headers)
            writer.writeheader()
            for item in items:
                product = item["product"]
                derived = item.get("derived_rule") or {}
                candidate = item.get("best_candidate") or {}
                writer.writerow(
                    {
                        "product_code": product["codigo"],
                        "product_name": product["nombre"],
                        "product_family": product["familia"],
                        "product_category": product["categoria"],
                        "status": item["status"],
                        "derived_kind": derived.get("kind", ""),
                        "units_per_parent": derived.get("units_per_parent", ""),
                        "parent_size_hint": derived.get("parent_size_hint", ""),
                        "requires_direct_components": derived.get("requires_direct_components", False),
                        "candidate_code": candidate.get("codigo", ""),
                        "candidate_name": candidate.get("nombre", ""),
                        "candidate_score": candidate.get("score", ""),
                        "candidate_has_recipe_flag": candidate.get("has_recipe_flag", False),
                        "candidate_bom_count": candidate.get("bom_count", ""),
                        "erp_insumo_id": candidate.get("erp_insumo_id", ""),
                        "erp_insumo_nombre": candidate.get("erp_insumo_nombre", ""),
                        "erp_preparation_recipe_exists": candidate.get("erp_preparation_recipe_exists", False),
                        "search_terms": " | ".join(candidate.get("search_terms") or []),
                    }
                )
