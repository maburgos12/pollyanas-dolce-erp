from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from maestros.models import CostoInsumo, Insumo, Proveedor
from pos_bridge.utils.exceptions import ExtractionError


ARTICLE_ALIASES = {
    "COLORANTE ROJO REDVELVET": "Colorante Rojo",
}


@dataclass(slots=True)
class PointPurchaseCostImportResult:
    created: int
    existing: int
    unresolved: int
    imported_articles: list[str]
    unresolved_articles: list[str]


class PointPurchaseCostImportService:
    def _load_payload(self, path: str | Path) -> Any:
        return json.loads(Path(path).read_text(encoding="utf-8"))

    def _summary_index(self, summary_payload: Any) -> dict[str, dict[str, str]]:
        try:
            rows = summary_payload["js_results"][0]["rows"]
        except Exception as exc:  # noqa: BLE001
            raise ExtractionError("Formato inválido en resumen de compras Point.") from exc

        index: dict[str, dict[str, str]] = {}
        for row in rows:
            purchase_id = str(row.get("compra_id") or "").strip()
            cells = row.get("cells") or []
            if not purchase_id or len(cells) < 4:
                continue
            raw_date = str(cells[3] or "").strip()
            compact = "".join(ch for ch in raw_date if ch.isdigit())
            purchase_date = compact[:8] if len(compact) >= 8 else ""
            index[purchase_id] = {
                "folio": str(cells[0] or "").strip(),
                "branch": str(cells[1] or "").strip(),
                "supplier": str(cells[2] or "").strip(),
                "purchase_date": purchase_date,
            }
        return index

    def _resolve_insumo(self, article_name: str) -> Insumo | None:
        exact = Insumo.objects.filter(nombre__iexact=article_name).first()
        if exact:
            return exact
        alias_target = ARTICLE_ALIASES.get(article_name.upper())
        if alias_target:
            return Insumo.objects.filter(nombre__iexact=alias_target).first()
        return None

    @staticmethod
    def _parse_date(raw_value: str) -> date | None:
        if len(raw_value) != 8 or not raw_value.isdigit():
            return None
        return date(int(raw_value[:4]), int(raw_value[4:6]), int(raw_value[6:8]))

    def import_from_browser_exports(
        self,
        *,
        summary_path: str | Path,
        details_path: str | Path,
    ) -> PointPurchaseCostImportResult:
        summary_payload = self._load_payload(summary_path)
        details_payload = self._load_payload(details_path)
        summary_index = self._summary_index(summary_payload)

        try:
            purchase_rows = details_payload["js_results"][0]
        except Exception as exc:  # noqa: BLE001
            raise ExtractionError("Formato inválido en detalle de compras Point.") from exc

        created = 0
        existing = 0
        unresolved = 0
        imported_articles: set[str] = set()
        unresolved_articles: set[str] = set()

        for purchase in purchase_rows:
            purchase_id = str(purchase.get("purchase_id") or "").strip()
            purchase_meta = summary_index.get(purchase_id)
            if not purchase_id or not purchase_meta:
                continue
            purchase_date = self._parse_date(purchase_meta["purchase_date"])
            supplier_name = purchase_meta["supplier"] or "POINT COMPRAS"
            supplier, _ = Proveedor.objects.get_or_create(nombre=supplier_name, defaults={"activo": True})

            for match in purchase.get("matches") or []:
                article_name = str(match.get("articulo") or "").strip()
                insumo = self._resolve_insumo(article_name)
                if insumo is None:
                    unresolved += 1
                    unresolved_articles.add(article_name)
                    continue

                source_key = (
                    f"POINT_COMPRAS_HISTORICAS|{purchase_id}|{article_name}|"
                    f"{match.get('cantidad')}|{match.get('unidad')}|{match.get('costo_unitario')}"
                )
                source_hash = hashlib.sha256(source_key.encode("utf-8")).hexdigest()
                _, was_created = CostoInsumo.objects.get_or_create(
                    source_hash=source_hash,
                    defaults={
                        "insumo": insumo,
                        "proveedor": supplier,
                        "fecha": purchase_date or date.today(),
                        "moneda": "MXN",
                        "costo_unitario": match.get("costo_unitario") or 0,
                        "raw": {
                            "source": "POINT_COMPRAS_HISTORICAS",
                            "purchase_id": purchase_id,
                            "folio": purchase_meta["folio"],
                            "branch": purchase_meta["branch"],
                            "supplier": supplier_name,
                            "article_name": article_name,
                            "quantity": match.get("cantidad"),
                            "unit": match.get("unidad"),
                            "unit_cost": match.get("costo_unitario"),
                            "total_cost": match.get("costo_total"),
                            "raw": match.get("raw"),
                        },
                    },
                )
                if was_created:
                    created += 1
                else:
                    existing += 1
                imported_articles.add(article_name)

        return PointPurchaseCostImportResult(
            created=created,
            existing=existing,
            unresolved=unresolved,
            imported_articles=sorted(imported_articles),
            unresolved_articles=sorted(unresolved_articles),
        )
