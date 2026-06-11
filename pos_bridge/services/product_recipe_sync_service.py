from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, InvalidOperation

from django.db import transaction
from django.utils import timezone

from core.cache_versions import bump_cache_scopes
from maestros.models import Insumo, UnidadMedida, seed_unidades_basicas
from pos_bridge.config import load_point_bridge_settings
from pos_bridge.models import PointExtractionLog, PointProduct, PointRecipeExtractionRun, PointRecipeNode, PointRecipeNodeLine, PointSyncJob
from pos_bridge.services.point_http_client import PointHttpSessionClient
from pos_bridge.services.recipe_identity_service import PointRecipeIdentityService, ResolvedInsumo
from pos_bridge.utils.helpers import sanitize_sensitive_data
from pos_bridge.utils.logger import get_job_logger
from reportes.analytics_service import mark_analytics_dirty_for_range
from recetas.models import LineaReceta, Receta
from recetas.utils.matching import clasificar_match
from recetas.utils.normalizacion import normalizar_nombre
from recetas.utils.temporalidad import inferir_temporalidad_receta


@dataclass(slots=True)
class PointRecipeSyncResult:
    summary: dict
    raw_export_path: str


@dataclass(slots=True)
class ComponentResolution:
    insumo: Insumo | None
    receta: Receta | None
    child_node: PointRecipeNode | None
    classification: str
    match_method: str
    match_score: float


class PointProductRecipeSyncService:
    SHEET_NAME = "POINT_PRODUCT_BOM"
    PREPARATION_SHEET_NAME = "POINT_INSUMO_BOM"
    DISCOVERY_CORE_FAMILIES = {
        "pastel",
        "pay",
        "vasos preparados",
        "vaso preparado",
        "galletas",
        "galleta",
        "bollo",
        "bollos",
        "empanadas",
        "otros postres",
    }
    DISCOVERY_CORE_CATEGORY_KEYWORDS = (
        "pastel",
        "pay",
        "vaso preparado",
        "vasos preparados",
        "galleta",
        "bollo",
        "empanada",
        "individual",
    )
    DISCOVERY_ADDON_NAME_KEYWORDS = (
        "topping",
        "sabor",
        "extra",
    )

    def __init__(self, settings=None, *, http_client_factory=None, identity_service=None):
        self.settings = settings or load_point_bridge_settings()
        self.http_client_factory = http_client_factory
        self.identity_service = identity_service or PointRecipeIdentityService()

    def _build_http_client(self, *, sync_job=None):
        if self.http_client_factory is not None:
            return self.http_client_factory()
        return PointHttpSessionClient(self.settings, audit_callback=self._make_audit_callback(sync_job=sync_job))

    def _make_audit_callback(self, *, sync_job=None):
        if sync_job is None:
            return None

        def _callback(*, event: str, message: str, context: dict | None = None) -> None:
            safe_context = sanitize_sensitive_data({"event": event, **(context or {})})
            PointExtractionLog.objects.create(
                sync_job=sync_job,
                level=PointExtractionLog.LEVEL_WARNING,
                message=message,
                context=safe_context,
            )
            get_job_logger(sync_job.id).warning("%s | %s", message, safe_context)

        return _callback

    def sync(
        self,
        *,
        branch_hint: str | None = None,
        product_codes: list[str] | None = None,
        limit: int | None = None,
        include_without_recipe: bool = False,
        sync_job=None,
        max_depth: int = 3,
    ) -> PointRecipeSyncResult:
        seed_unidades_basicas()
        selected_codes = {self._norm_code(code) for code in (product_codes or []) if (code or "").strip()}
        summary = {
            "workspace": "",
            "products_seen": 0,
            "products_selected": 0,
            "products_without_recipe_in_point": 0,
            "recipes_created": 0,
            "recipes_updated": 0,
            "recipes_unchanged": 0,
            "preparations_created": 0,
            "preparations_updated": 0,
            "preparations_unchanged": 0,
            "lineas_created": 0,
            "lineas_auto": 0,
            "lineas_needs_review": 0,
            "lineas_rejected": 0,
            "graph_nodes": 0,
            "graph_lines": 0,
            "aliases_synced": 0,
            "internal_insumos_created": 0,
            "catalog_insumos_created": 0,
            "new_products_imported": 0,
            "new_preparations_imported": 0,
            "recursive_nodes_created": 0,
            "recipes_completed_successfully": 0,
            "recipes_with_unresolved_inputs": 0,
            "unresolved_inputs_count": 0,
            "imported_products_status": [],
        }
        node_outcomes: dict[str, dict[str, object]] = {}

        with self._build_http_client(sync_job=sync_job) as client:
            workspace = client.login(branch_hint=branch_hint)
            summary["workspace"] = workspace["branch_name"]
            run = PointRecipeExtractionRun.objects.create(
                sync_job=sync_job,
                workspace=workspace["branch_name"],
                branch_hint=branch_hint or "",
                root_codes=sorted(selected_codes),
                max_depth=max(1, int(max_depth or 3)),
            )
            products = client.get_products()
            products = self._hydrate_selected_products(client=client, products=products, selected_codes=selected_codes)
            summary["products_seen"] = len(products)
            visited: dict[str, PointRecipeNode] = {}

            for product in products:
                code_norm = self._norm_code(product.get("Codigo") or "")
                if selected_codes and code_norm not in selected_codes:
                    continue
                if not include_without_recipe and not product.get("hasReceta"):
                    continue
                summary["products_selected"] += 1
                self._extract_product_node(
                    client=client,
                    run=run,
                    product=product,
                    depth=0,
                    max_depth=max_depth,
                    visited=visited,
                    summary=summary,
                    node_outcomes=node_outcomes,
                )
                root_node = visited.get(
                    self._identity_key(
                        source_type=PointRecipeNode.SOURCE_PRODUCT,
                        point_code=product.get("Codigo") or "",
                        point_pk=product.get("PK_Producto") or "",
                        point_name=product.get("Nombre") or "",
                    )
                )
                if root_node is not None:
                    product_status = self._build_product_import_status(root_node=root_node, node_outcomes=node_outcomes)
                    summary["imported_products_status"].append(product_status)
                    if product_status["status"] == "SUCCESS_COMPLETE":
                        summary["recipes_completed_successfully"] += 1
                    else:
                        summary["recipes_with_unresolved_inputs"] += 1
                    summary["unresolved_inputs_count"] += len(product_status["unresolved_inputs"])
                if limit and summary["products_selected"] >= limit:
                    break

            summary["new_products_imported"] = sum(
                1
                for outcome in node_outcomes.values()
                if outcome.get("source_type") == PointRecipeNode.SOURCE_PRODUCT and outcome.get("recipe_change") == "created"
            )
            summary["new_preparations_imported"] = sum(
                1
                for outcome in node_outcomes.values()
                if outcome.get("node_kind") == PointRecipeNode.KIND_PREPARED_INPUT and outcome.get("recipe_change") == "created"
            )
            summary["recursive_nodes_created"] = sum(
                1
                for outcome in node_outcomes.values()
                if outcome.get("node_kind") == PointRecipeNode.KIND_PREPARED_INPUT and outcome.get("node_created")
            )
            run.summary = summary
            run.save(update_fields=["summary", "updated_at"])

        raw_export_path = self._write_raw_export(run=run, payload=self._serialize_run(run))
        summary["run_id"] = run.id
        return PointRecipeSyncResult(summary=summary, raw_export_path=str(raw_export_path))

    def discover_new_product_codes(
        self,
        *,
        branch_hint: str | None = None,
        include_without_recipe: bool = False,
    ) -> dict[str, object]:
        with self._build_http_client() as client:
            workspace = client.login(branch_hint=branch_hint)
            products = client.get_products()
            discovery_baseline_at = self._discovery_baseline_at()
            products = self._hydrate_recent_discovery_products(
                client=client,
                products=products,
                discovery_baseline_at=discovery_baseline_at,
            )
            code_occurrences: dict[str, int] = {}
            for product in products:
                point_code = (product.get("Codigo") or "").strip().upper()
                if point_code:
                    code_occurrences[point_code] = code_occurrences.get(point_code, 0) + 1
            new_candidates: list[dict[str, str]] = []
            blocked_candidates: list[dict[str, str]] = []
            ignored_candidates_count = 0
            for product in products:
                point_code = (product.get("Codigo") or "").strip().upper()
                point_name = (product.get("Nombre") or "").strip()
                if not point_code:
                    continue
                if self.identity_service.resolve_recipe(point_code=point_code, point_name=point_name) is not None:
                    continue
                if not self._is_discovery_new_candidate(product=product, discovery_baseline_at=discovery_baseline_at):
                    ignored_candidates_count += 1
                    continue
                if code_occurrences.get(point_code, 0) > 1:
                    blocked_candidates.append(
                        self._build_discovery_candidate(
                            product=product,
                            detection_reason="DUPLICATE_POINT_CODE",
                            message="Point reutiliza el mismo código para más de un producto; requiere revisión manual antes de sincronizar.",
                        )
                    )
                    continue
                has_recipe_flag = bool(product.get("hasReceta"))
                should_probe_bom = self._looks_like_recipe_catalog_candidate(product=product)
                bom_rows: list[dict] = []

                if has_recipe_flag:
                    new_candidates.append(
                        self._build_discovery_candidate(
                            product=product,
                            detection_reason="HAS_RECETA_FLAG",
                        )
                    )
                    continue

                if should_probe_bom and product.get("PK_Producto"):
                    try:
                        bom_rows = client.get_product_bom(product.get("PK_Producto"))
                    except Exception:
                        bom_rows = []

                if bom_rows:
                    new_candidates.append(
                        self._build_discovery_candidate(
                            product=product,
                            detection_reason="BOM_PROBE_POSITIVE",
                            bom_lines=len(bom_rows),
                        )
                    )
                    continue

                if should_probe_bom or include_without_recipe:
                    blocked_candidates.append(
                        self._build_discovery_candidate(
                            product=product,
                            detection_reason="POINT_NO_BOM",
                            message="Point no reportó receta/BOM para este código y no puede incorporarse automáticamente.",
                        )
                    )
                    continue

                ignored_candidates_count += 1
        new_candidates.sort(key=lambda item: (item["familia"], item["categoria"], item["nombre"], item["codigo_point"]))
        blocked_candidates.sort(key=lambda item: (item["familia"], item["categoria"], item["nombre"], item["codigo_point"]))
        return {
            "workspace": workspace["branch_name"],
            "products_seen": len(products),
            "new_candidates": new_candidates,
            "new_codes": [item["codigo_point"] for item in new_candidates],
            "blocked_candidates": blocked_candidates,
            "blocked_codes": [item["codigo_point"] for item in blocked_candidates],
            "importable_candidates_count": len(new_candidates),
            "blocked_candidates_count": len(blocked_candidates),
            "ignored_candidates_count": ignored_candidates_count,
            "discovery_baseline_at": discovery_baseline_at.isoformat(),
        }

    def _build_discovery_candidate(
        self,
        *,
        product: dict,
        detection_reason: str,
        message: str = "",
        bom_lines: int = 0,
    ) -> dict[str, str | int]:
        return {
            "codigo_point": ((product.get("Codigo") or "").strip().upper())[:80],
            "nombre": (product.get("Nombre") or "").strip()[:250],
            "familia": (product.get("Familia") or "")[:120],
            "categoria": (product.get("Categoria") or "")[:120],
            "detection_reason": detection_reason,
            "message": message[:280],
            "bom_lines": max(int(bom_lines or 0), 0),
            "point_external_id": str(product.get("PK_Producto") or product.get("external_id") or "")[:120],
        }

    def _looks_like_recipe_catalog_candidate(self, *, product: dict) -> bool:
        family_norm = normalizar_nombre((product.get("Familia") or "").strip())
        category_norm = normalizar_nombre((product.get("Categoria") or "").strip())
        name_norm = normalizar_nombre((product.get("Nombre") or "").strip())

        if family_norm in self.DISCOVERY_CORE_FAMILIES:
            return True
        if any(keyword in category_norm for keyword in self.DISCOVERY_CORE_CATEGORY_KEYWORDS):
            return True
        if any(name_norm.startswith(keyword) or f" {keyword} " in f" {name_norm} " for keyword in self.DISCOVERY_ADDON_NAME_KEYWORDS):
            return True
        return False

    def _discovery_baseline_at(self):
        last_success = (
            PointSyncJob.objects.filter(
                job_type=PointSyncJob.JOB_TYPE_RECIPES,
                status=PointSyncJob.STATUS_SUCCESS,
                parameters__action="SYNC_ONLY_NEW_PRODUCTS",
            )
            .exclude(finished_at__isnull=True)
            .order_by("-finished_at", "-id")
            .first()
        )
        if last_success is not None and last_success.finished_at is not None:
            return last_success.finished_at
        fallback_days = max(int(getattr(self.settings, "recipe_discovery_fallback_days", 7) or 7), 1)
        return timezone.now() - timedelta(days=fallback_days)

    def _is_discovery_new_candidate(self, *, product: dict, discovery_baseline_at) -> bool:
        external_id = str(product.get("PK_Producto") or product.get("external_id") or "").strip()
        point_code = (product.get("Codigo") or "").strip().upper()

        qs = PointProduct.objects.all()
        if external_id:
            point_product = qs.filter(external_id=external_id).order_by("created_at", "id").first()
            if point_product is not None:
                return bool(point_product.created_at and point_product.created_at >= discovery_baseline_at)
        if point_code:
            point_product = qs.filter(sku__iexact=point_code).order_by("created_at", "id").first()
            if point_product is not None:
                return bool(point_product.created_at and point_product.created_at >= discovery_baseline_at)
        return True

    def _hydrate_recent_discovery_products(self, *, client, products: list[dict], discovery_baseline_at) -> list[dict]:
        hydrated = list(products)
        seen_codes = {
            self._norm_code(item.get("Codigo") or "")
            for item in hydrated
            if (item.get("Codigo") or "").strip()
        }
        seen_external_ids = {
            str(item.get("PK_Producto") or item.get("external_id") or "").strip()
            for item in hydrated
            if str(item.get("PK_Producto") or item.get("external_id") or "").strip()
        }
        recent_products = PointProduct.objects.filter(created_at__gte=discovery_baseline_at).order_by("-created_at", "id")
        for point_product in recent_products:
            external_id = (point_product.external_id or "").strip()
            sku_norm = self._norm_code(point_product.sku or "")
            if not external_id and not sku_norm:
                continue
            if external_id in seen_external_ids or (sku_norm and sku_norm in seen_codes):
                continue
            try:
                detail = client.get_product_detail(external_id)
                bom_rows = client.get_product_bom(external_id)
            except Exception:
                continue
            hydrated.append(
                {
                    "PK_Producto": external_id,
                    "Codigo": detail.get("Codigo") or point_product.sku or external_id,
                    "Nombre": detail.get("Nombre") or point_product.name,
                    "Familia": detail.get("Familia") or point_product.metadata.get("family") or "",
                    "Categoria": detail.get("Categoria") or point_product.category or "",
                    "hasReceta": bool(bom_rows),
                    "external_id": external_id,
                }
            )
            if sku_norm:
                seen_codes.add(sku_norm)
            seen_external_ids.add(external_id)
        return hydrated

    def _hydrate_selected_products(self, *, client, products: list[dict], selected_codes: set[str]) -> list[dict]:
        if not selected_codes:
            return list(products)

        hydrated = list(products)
        seen_codes = {
            self._norm_code(item.get("Codigo") or "")
            for item in hydrated
            if (item.get("Codigo") or "").strip()
        }
        missing_codes = [code for code in sorted(selected_codes) if code and code not in seen_codes]
        if not missing_codes:
            return hydrated

        local_candidates = [
            point_product
            for point_product in PointProduct.objects.order_by("id")
            if self._norm_code(point_product.sku or "") in missing_codes
        ]
        for point_product in local_candidates:
            external_id = (point_product.external_id or "").strip()
            if not external_id:
                continue
            try:
                detail = client.get_product_detail(external_id)
                bom_rows = client.get_product_bom(external_id)
            except Exception:
                continue
            hydrated.append(
                {
                    "PK_Producto": external_id,
                    "Codigo": detail.get("Codigo") or point_product.sku or external_id,
                    "Nombre": detail.get("Nombre") or point_product.name,
                    "Familia": detail.get("Familia") or point_product.metadata.get("family") or "",
                    "Categoria": detail.get("Categoria") or point_product.category or "",
                    "hasReceta": bool(bom_rows),
                }
            )
        return hydrated

    def _norm_code(self, value: str) -> str:
        return "".join(ch for ch in (value or "").strip().upper() if ch.isalnum())

    def _write_raw_export(self, *, run: PointRecipeExtractionRun, payload: dict):
        timestamp = timezone.now().strftime("%Y%m%d_%H%M%S")
        path = self.settings.raw_exports_dir / f"{timestamp}_point_product_recipes_graph_{run.id}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def _serialize_run(self, run: PointRecipeExtractionRun) -> dict:
        items = []
        queryset = (
            run.nodes.select_related("yield_unit", "erp_recipe", "erp_insumo")
            .prefetch_related("lines__unit", "lines__erp_insumo", "lines__erp_recipe", "lines__child_node")
            .order_by("depth", "point_name", "id")
        )
        for node in queryset:
            items.append(
                {
                    "identity_key": node.identity_key,
                    "source_type": node.source_type,
                    "node_kind": node.node_kind,
                    "point_pk": node.point_pk,
                    "point_code": node.point_code,
                    "point_name": node.point_name,
                    "depth": node.depth,
                    "family": node.family,
                    "category": node.category,
                    "yield_mode": node.yield_mode,
                    "yield_quantity": self._serialize_decimal(node.yield_quantity),
                    "yield_unit": node.yield_unit.codigo if node.yield_unit_id else node.yield_unit_text,
                    "erp_recipe": node.erp_recipe.nombre if node.erp_recipe_id else "",
                    "erp_insumo": node.erp_insumo.nombre if node.erp_insumo_id else "",
                    "lines": [
                        {
                            "position": line.position,
                            "point_code": line.point_code,
                            "point_name": line.point_name,
                            "quantity": self._serialize_decimal(line.quantity),
                            "unit": line.unit.codigo if line.unit_id else line.unit_text,
                            "classification": line.classification,
                            "erp_insumo": line.erp_insumo.nombre if line.erp_insumo_id else "",
                            "erp_recipe": line.erp_recipe.nombre if line.erp_recipe_id else "",
                            "child_identity_key": line.child_node.identity_key if line.child_node_id else "",
                            "match_method": line.match_method,
                            "match_score": line.match_score,
                        }
                        for line in node.lines.all().order_by("position", "id")
                    ],
                }
            )
        return {
            "generated_at": timezone.now().isoformat(),
            "run_id": run.id,
            "summary": run.summary,
            "items": items,
        }

    def _extract_product_node(
        self,
        *,
        client,
        run,
        product: dict,
        depth: int,
        max_depth: int,
        visited: dict[str, PointRecipeNode],
        summary: dict,
        node_outcomes: dict[str, dict[str, object]],
    ) -> PointRecipeNode:
        identity_key = self._identity_key(
            source_type=PointRecipeNode.SOURCE_PRODUCT,
            point_code=product.get("Codigo") or "",
            point_pk=product.get("PK_Producto") or "",
            point_name=product.get("Nombre") or "",
        )
        if identity_key in visited:
            return visited[identity_key]

        detail = client.get_product_detail(product["PK_Producto"])
        bom_rows = client.get_product_bom(product["PK_Producto"])
        if not bom_rows:
            summary["products_without_recipe_in_point"] += 1
            identity_key = self._identity_key(
                source_type=PointRecipeNode.SOURCE_PRODUCT,
                point_code=product.get("Codigo") or "",
                point_pk=product.get("PK_Producto") or "",
                point_name=product.get("Nombre") or "",
            )
            node, created = PointRecipeNode.objects.update_or_create(
                run=run,
                identity_key=identity_key,
                defaults={
                    "source_type": PointRecipeNode.SOURCE_PRODUCT,
                    "node_kind": PointRecipeNode.KIND_FINAL_PRODUCT,
                    "point_pk": str(product.get("PK_Producto") or ""),
                    "point_code": (product.get("Codigo") or detail.get("Codigo") or "")[:80],
                    "point_name": (product.get("Nombre") or detail.get("Nombre") or f"Producto {product.get('PK_Producto')}")[:255],
                    "family": (product.get("Familia") or "")[:120],
                    "category": (product.get("Categoria") or "")[:120],
                    "has_recipe_flag": bool(product.get("hasReceta")),
                    "depth": depth,
                    "yield_mode": PointRecipeNode.YIELD_UNKNOWN,
                    "yield_quantity": None,
                    "yield_unit": None,
                    "yield_unit_text": "",
                    "erp_recipe": None,
                    "raw_detail": detail,
                    "raw_bom": bom_rows,
                },
            )
            if created:
                summary["graph_nodes"] += 1
            visited[identity_key] = node
            self._register_node_outcome(
                node_outcomes=node_outcomes,
                identity_key=identity_key,
                source_type=PointRecipeNode.SOURCE_PRODUCT,
                node_kind=PointRecipeNode.KIND_FINAL_PRODUCT,
                point_code=node.point_code,
                point_name=node.point_name,
                recipe_change=None,
                node_created=created,
                blocked_reason="POINT_BOM_EMPTY",
            )
            return node

        receta, change = self._upsert_recipe_record(
            point_code=(product.get("Codigo") or detail.get("Codigo") or "").strip(),
            point_name=(product.get("Nombre") or detail.get("Nombre") or "").strip(),
            family=(product.get("Familia") or "").strip(),
            category=(product.get("Categoria") or "").strip(),
            tipo=Receta.TIPO_PRODUCTO_FINAL,
            sheet_name=self.SHEET_NAME,
            yield_quantity=None,
            yield_unit=None,
            yield_unit_text="",
            raw_context={"detail": detail, "bom_rows": bom_rows},
        )
        summary_key = f"recipes_{change}"
        summary[summary_key] += 1
        summary["aliases_synced"] += self.identity_service.sync_recipe_point_identity(
            receta=receta,
            point_code=(product.get("Codigo") or detail.get("Codigo") or ""),
            point_name=(product.get("Nombre") or detail.get("Nombre") or ""),
        )

        node, created = PointRecipeNode.objects.update_or_create(
            run=run,
            identity_key=identity_key,
            defaults={
                "source_type": PointRecipeNode.SOURCE_PRODUCT,
                "node_kind": PointRecipeNode.KIND_FINAL_PRODUCT,
                "point_pk": str(product.get("PK_Producto") or ""),
                "point_code": (product.get("Codigo") or detail.get("Codigo") or "")[:80],
                "point_name": (product.get("Nombre") or detail.get("Nombre") or f"Producto {product.get('PK_Producto')}")[:255],
                "family": (product.get("Familia") or "")[:120],
                "category": (product.get("Categoria") or "")[:120],
                "has_recipe_flag": bool(product.get("hasReceta")) or bool(bom_rows),
                "depth": depth,
                "yield_mode": PointRecipeNode.YIELD_UNKNOWN,
                "yield_quantity": None,
                "yield_unit": None,
                "yield_unit_text": "",
                "erp_recipe": receta,
                "raw_detail": detail,
                "raw_bom": bom_rows,
            },
        )
        if created:
            summary["graph_nodes"] += 1
        visited[identity_key] = node
        self._register_node_outcome(
            node_outcomes=node_outcomes,
            identity_key=identity_key,
            source_type=PointRecipeNode.SOURCE_PRODUCT,
            node_kind=PointRecipeNode.KIND_FINAL_PRODUCT,
            point_code=node.point_code,
            point_name=node.point_name,
            recipe_change=change,
            node_created=created,
        )
        self._materialize_node_lines(
            client=client,
            run=run,
            node=node,
            receta=receta,
            bom_rows=bom_rows,
            depth=depth,
            max_depth=max_depth,
            visited=visited,
            summary=summary,
            node_outcomes=node_outcomes,
        )
        return node

    def _extract_insumo_node(
        self,
        *,
        client,
        run,
        articulo_row: dict,
        depth: int,
        max_depth: int,
        visited: dict[str, PointRecipeNode],
        summary: dict,
        node_outcomes: dict[str, dict[str, object]],
    ) -> PointRecipeNode:
        point_code = (articulo_row.get("Codigo_Articulo") or articulo_row.get("CodigoInsumo") or "").strip()
        point_name = (articulo_row.get("Nombre_Articulo") or articulo_row.get("Nombre") or "").strip()
        point_pk = articulo_row.get("PK_Articulo") or articulo_row.get("PKInsumo") or ""
        identity_key = self._identity_key(
            source_type=PointRecipeNode.SOURCE_INSUMO,
            point_code=point_code,
            point_pk=point_pk,
            point_name=point_name,
        )
        if identity_key in visited:
            return visited[identity_key]

        detail = client.get_articulo_detail(point_pk)
        bom_rows = list(detail.get("BOM") or [])
        yield_quantity, yield_unit, yield_unit_text, yield_mode = self._infer_yield(detail)
        insumo_preparado, insumo_created = self._resolve_or_create_prepared_insumo(
            point_code=point_code,
            point_name=point_name,
            yield_unit=yield_unit,
            category=articulo_row.get("Categoria") or detail.get("Categoria") or "",
        )
        if insumo_created:
            summary["internal_insumos_created"] += 1
        summary["aliases_synced"] += self.identity_service.sync_insumo_point_identity(
            insumo=insumo_preparado,
            point_code=point_code,
            point_name=point_name,
        )
        receta, change = self._upsert_recipe_record(
            point_code=point_code,
            point_name=point_name,
            family=(articulo_row.get("Categoria") or detail.get("Categoria") or "")[:120],
            category=(articulo_row.get("Categoria") or detail.get("Categoria") or "")[:120],
            tipo=Receta.TIPO_PREPARACION,
            sheet_name=self.PREPARATION_SHEET_NAME,
            yield_quantity=yield_quantity,
            yield_unit=yield_unit,
            yield_unit_text=yield_unit_text,
            raw_context={"detail": detail, "bom_rows": bom_rows},
        )
        summary_key = f"preparations_{change}"
        summary[summary_key] += 1
        summary["aliases_synced"] += self.identity_service.sync_recipe_point_identity(
            receta=receta,
            point_code=point_code,
            point_name=point_name,
        )

        node, created = PointRecipeNode.objects.update_or_create(
            run=run,
            identity_key=identity_key,
            defaults={
                "source_type": PointRecipeNode.SOURCE_INSUMO,
                "node_kind": PointRecipeNode.KIND_PREPARED_INPUT,
                "point_pk": str(point_pk or ""),
                "point_code": point_code[:80],
                "point_name": point_name[:255] or f"Insumo {point_pk}",
                "family": "",
                "category": (articulo_row.get("Categoria") or detail.get("Categoria") or "")[:120],
                "has_recipe_flag": bool(articulo_row.get("HasReceta")) or bool(bom_rows),
                "depth": depth,
                "yield_mode": yield_mode,
                "yield_quantity": yield_quantity,
                "yield_unit": yield_unit,
                "yield_unit_text": yield_unit_text[:40],
                "erp_recipe": receta,
                "erp_insumo": insumo_preparado,
                "raw_detail": detail,
                "raw_bom": bom_rows,
            },
        )
        if created:
            summary["graph_nodes"] += 1
        visited[identity_key] = node
        self._register_node_outcome(
            node_outcomes=node_outcomes,
            identity_key=identity_key,
            source_type=PointRecipeNode.SOURCE_INSUMO,
            node_kind=PointRecipeNode.KIND_PREPARED_INPUT,
            point_code=node.point_code,
            point_name=node.point_name,
            recipe_change=change,
            node_created=created,
        )
        self._materialize_node_lines(
            client=client,
            run=run,
            node=node,
            receta=receta,
            bom_rows=bom_rows,
            depth=depth,
            max_depth=max_depth,
            visited=visited,
            summary=summary,
            node_outcomes=node_outcomes,
        )
        return node

    def _materialize_node_lines(
        self,
        *,
        client,
        run,
        node: PointRecipeNode,
        receta: Receta,
        bom_rows: list[dict],
        depth: int,
        max_depth: int,
        visited: dict[str, PointRecipeNode],
        summary: dict,
        node_outcomes: dict[str, dict[str, object]],
    ) -> None:
        receta.lineas.all().delete()
        node.lines.all().delete()

        recipe_lines: list[LineaReceta] = []
        graph_lines: list[PointRecipeNodeLine] = []
        for position, row in enumerate(bom_rows, start=1):
            resolution = self._resolve_component(
                client=client,
                row=row,
                run=run,
                depth=depth + 1,
                max_depth=max_depth,
                visited=visited,
                summary=summary,
                node_outcomes=node_outcomes,
            )
            status = LineaReceta.STATUS_REJECTED
            if resolution.insumo is not None:
                status = clasificar_match(resolution.match_score)
                if self._should_exclude_derived_slice_consumable(receta=receta, row=row):
                    status = LineaReceta.STATUS_REJECTED
                if status == LineaReceta.STATUS_AUTO:
                    summary["lineas_auto"] += 1
                elif status == LineaReceta.STATUS_NEEDS_REVIEW:
                    summary["lineas_needs_review"] += 1
                else:
                    summary["lineas_rejected"] += 1
            else:
                summary["lineas_rejected"] += 1

            unit_raw = self._unit_text(row.get("Unidad_corto") or row.get("Unidad") or row.get("UnidadVenta") or "")
            recipe_lines.append(
                LineaReceta(
                    receta=receta,
                    posicion=position,
                    tipo_linea=LineaReceta.TIPO_NORMAL,
                    insumo=resolution.insumo,
                    insumo_texto=((row.get("Articulo") or row.get("Nombre") or row.get("Nombre_Articulo") or row.get("Codigo_Articulo") or row.get("CodigoInsumo") or f"Artículo {position}")[:250]),
                    cantidad=self._decimal(row.get("Cantidad")),
                    unidad_texto=unit_raw[:40],
                    unidad=self.identity_service.resolve_unit(unit_raw),
                    match_score=float(resolution.match_score or 0),
                    match_method=(resolution.match_method or LineaReceta.MATCH_NONE)[:20],
                    match_status=status,
                )
            )
            graph_lines.append(
                PointRecipeNodeLine(
                    node=node,
                    child_node=resolution.child_node,
                    position=position,
                    point_code=((row.get("Codigo_Articulo") or row.get("CodigoInsumo") or "")[:80]),
                    point_name=((row.get("Articulo") or row.get("Nombre") or row.get("Nombre_Articulo") or row.get("Codigo_Articulo") or row.get("CodigoInsumo") or f"Artículo {position}")[:255]),
                    quantity=self._decimal(row.get("Cantidad")),
                    unit_text=unit_raw[:40],
                    unit=self.identity_service.resolve_unit(unit_raw),
                    classification=resolution.classification,
                    erp_insumo=resolution.insumo,
                    erp_recipe=resolution.receta,
                    match_method=(resolution.match_method or "")[:32],
                    match_score=float(resolution.match_score or 0),
                    raw_payload=row,
                )
            )

        if recipe_lines:
            LineaReceta.objects.bulk_create(recipe_lines, batch_size=200)
            bump_cache_scopes("dashboard")
            today = timezone.localdate()
            mark_analytics_dirty_for_range(
                start_date=today,
                end_date=today,
                include_production=True,
                reason="product_recipe_sync_service",
            )
        if graph_lines:
            PointRecipeNodeLine.objects.bulk_create(graph_lines, batch_size=200)
        summary["lineas_created"] += len(recipe_lines)
        summary["graph_lines"] += len(graph_lines)

    def _should_exclude_derived_slice_consumable(self, *, receta: Receta, row: dict) -> bool:
        from recetas.utils.derived_product_presentations import get_active_derived_relation

        relation = get_active_derived_relation(receta)
        if relation is None or not relation.activo or relation.tipo_derivado != "REBANADA":
            return False

        point_code = str(row.get("Codigo_Articulo") or row.get("CodigoInsumo") or "").strip()
        point_name = normalizar_nombre(
            row.get("Articulo") or row.get("Nombre") or row.get("Nombre_Articulo") or ""
        )
        return point_code == "068" or point_name == "servilleta"

    def _resolve_component(
        self,
        *,
        client,
        row: dict,
        run,
        depth: int,
        max_depth: int,
        visited: dict[str, PointRecipeNode],
        summary: dict,
        node_outcomes: dict[str, dict[str, object]],
    ) -> ComponentResolution:
        point_code = (row.get("Codigo_Articulo") or row.get("CodigoInsumo") or "").strip()
        point_name = (row.get("Articulo") or row.get("Nombre") or row.get("Nombre_Articulo") or "").strip()
        resolved = self.identity_service.resolve_insumo(point_code=point_code, point_name=point_name)
        articulo_row = None

        child_node = None
        receta = None
        classification = PointRecipeNodeLine.COMPONENT_UNRESOLVED
        match_method = resolved.method or LineaReceta.MATCH_NONE
        match_score = float(resolved.score or 0)

        if depth <= max_depth:
            articulo_row = self._lookup_articulo_catalog_row(client=client, point_code=point_code, point_name=point_name)
            if articulo_row and articulo_row.get("HasReceta"):
                child_node = self._extract_insumo_node(
                    client=client,
                    run=run,
                    articulo_row=articulo_row,
                    depth=depth,
                    max_depth=max_depth,
                    visited=visited,
                    summary=summary,
                    node_outcomes=node_outcomes,
                )
                receta = child_node.erp_recipe
                prepared_insumo = child_node.erp_insumo
                if prepared_insumo is None:
                    insumo_before = self.identity_service.resolve_insumo(point_code=point_code, point_name=point_name).insumo
                    prepared_insumo = self.identity_service.get_or_create_internal_insumo(
                        point_code=point_code,
                        point_name=point_name,
                        unidad_base=child_node.yield_unit,
                        categoria=child_node.category,
                    )
                    if insumo_before is None:
                        summary["internal_insumos_created"] += 1
                resolved = ResolvedInsumo(insumo=prepared_insumo, score=100.0, method="PREPARED_INPUT")
                classification = PointRecipeNodeLine.COMPONENT_PREPARED_INPUT
                match_method = "PREPARED_INPUT"
                match_score = 100.0

        if classification != PointRecipeNodeLine.COMPONENT_PREPARED_INPUT:
            resolved, created_direct = self._resolve_or_create_direct_catalog_insumo(
                client=client,
                row=row,
                articulo_row=articulo_row,
            )
            if created_direct:
                summary["catalog_insumos_created"] += 1
            match_method = resolved.method or match_method
            match_score = float(resolved.score or match_score or 0)
            if resolved.insumo is not None:
                classification = (
                    PointRecipeNodeLine.COMPONENT_PACKAGING
                    if resolved.insumo.tipo_item == Insumo.TIPO_EMPAQUE
                    else PointRecipeNodeLine.COMPONENT_DIRECT_INPUT
                )
                self.identity_service.sync_insumo_point_identity(
                    insumo=resolved.insumo,
                    point_code=point_code,
                    point_name=point_name,
                    category=((articulo_row or {}).get("Categoria") or row.get("Categoria") or ""),
                )
            else:
                classification = PointRecipeNodeLine.COMPONENT_UNRESOLVED

        return ComponentResolution(
            insumo=resolved.insumo,
            receta=receta,
            child_node=child_node,
            classification=classification,
            match_method=match_method,
            match_score=match_score,
        )

    def _register_node_outcome(
        self,
        *,
        node_outcomes: dict[str, dict[str, object]],
        identity_key: str,
        source_type: str,
        node_kind: str,
        point_code: str,
        point_name: str,
        recipe_change: str | None,
        node_created: bool,
        blocked_reason: str = "",
    ) -> None:
        node_outcomes[identity_key] = {
            "source_type": source_type,
            "node_kind": node_kind,
            "point_code": (point_code or "").strip(),
            "point_name": (point_name or "").strip(),
            "recipe_change": recipe_change,
            "node_created": bool(node_created),
            "blocked_reason": blocked_reason or "",
        }

    def _build_product_import_status(self, *, root_node: PointRecipeNode, node_outcomes: dict[str, dict[str, object]]) -> dict[str, object]:
        unresolved_inputs: list[dict[str, str]] = []
        created_preparations: dict[str, dict[str, str]] = {}
        visited_node_ids: set[int] = set()

        def walk(node: PointRecipeNode) -> None:
            if node.id in visited_node_ids:
                return
            visited_node_ids.add(node.id)
            for line in node.lines.select_related("child_node").order_by("position", "id"):
                if line.classification == PointRecipeNodeLine.COMPONENT_UNRESOLVED:
                    unresolved_inputs.append(
                        {
                            "codigo_point": (line.point_code or "").strip(),
                            "nombre": (line.point_name or "").strip(),
                            "classification": line.classification,
                        }
                    )
                child_node = line.child_node
                if child_node is None:
                    continue
                child_outcome = node_outcomes.get(child_node.identity_key) or {}
                if (
                    child_node.node_kind == PointRecipeNode.KIND_PREPARED_INPUT
                    and child_outcome.get("recipe_change") == "created"
                ):
                    created_preparations[child_node.identity_key] = {
                        "codigo_point": (child_node.point_code or "").strip(),
                        "nombre": (child_node.point_name or "").strip(),
                    }
                walk(child_node)

        walk(root_node)
        root_outcome = node_outcomes.get(root_node.identity_key) or {}
        root_lines = list(root_node.lines.all())
        unresolved_count = len(unresolved_inputs)
        resolved_lines = sum(1 for line in root_lines if line.classification != PointRecipeNodeLine.COMPONENT_UNRESOLVED)

        if root_node.erp_recipe_id is None or (root_lines and resolved_lines == 0 and unresolved_count > 0):
            status = "BLOCKED_UNRESOLVED"
        elif unresolved_count > 0:
            status = "SUCCESS_WITH_WARNINGS"
        else:
            status = "SUCCESS_COMPLETE"

        created_preparations_list = sorted(
            created_preparations.values(),
            key=lambda item: (item["nombre"], item["codigo_point"]),
        )
        return {
            "codigo_point": (root_node.point_code or "").strip(),
            "nombre": (root_node.point_name or "").strip(),
            "status": status,
            "recipe_change": root_outcome.get("recipe_change"),
            "is_new_product": root_outcome.get("recipe_change") == "created",
            "unresolved_inputs": unresolved_inputs,
            "created_preparations": created_preparations_list,
            "message": self._build_product_import_message(
                root_node=root_node,
                status=status,
                unresolved_count=unresolved_count,
                created_preparations_count=len(created_preparations_list),
                blocked_reason=str(root_outcome.get("blocked_reason") or ""),
            ),
        }

    def _build_product_import_message(
        self,
        *,
        root_node: PointRecipeNode,
        status: str,
        unresolved_count: int,
        created_preparations_count: int,
        blocked_reason: str,
    ) -> str:
        product_name = (root_node.point_name or root_node.point_code or "Producto Point").strip()
        if status == "SUCCESS_COMPLETE":
            if created_preparations_count:
                return (
                    f"Se importó {product_name} correctamente con toda su receta y "
                    f"{created_preparations_count} preparación(es) hija(s) automática(s)."
                )
            return f"Se importó {product_name} correctamente con toda su receta."
        if status == "SUCCESS_WITH_WARNINGS":
            message = (
                f"Se importó {product_name}, pero quedaron {unresolved_count} insumo(s) pendiente(s) por resolver."
            )
            if created_preparations_count:
                message += f" Además se crearon {created_preparations_count} preparación(es) hija(s)."
            return message
        if blocked_reason == "POINT_BOM_EMPTY":
            return f"No se pudo cerrar {product_name}: Point no reportó receta/BOM para materializarlo."
        return (
            f"No se pudo cerrar {product_name}: quedaron {unresolved_count} insumo(s) sin resolver para liberar la receta."
        )

    def _resolve_or_create_direct_catalog_insumo(self, *, client, row: dict, articulo_row: dict | None) -> tuple[ResolvedInsumo, bool]:
        point_code = (row.get("Codigo_Articulo") or row.get("CodigoInsumo") or "").strip()
        point_name = (row.get("Articulo") or row.get("Nombre") or row.get("Nombre_Articulo") or "").strip()
        if not point_code and not point_name:
            return ResolvedInsumo(insumo=None, score=0.0, method="NO_MATCH"), False

        resolved = self.identity_service.resolve_insumo(point_code=point_code, point_name=point_name)
        target_code_norm = self._norm_code(point_code)
        resolved_code_norm = self._norm_code(resolved.insumo.codigo_point or "") if resolved.insumo is not None else ""
        normalized_point_name = normalizar_nombre(point_name)
        if resolved.insumo is not None and (
            not target_code_norm
            or resolved_code_norm == target_code_norm
            or (not resolved_code_norm and resolved.method in {"EXACT", "ALIAS"})
        ):
            if target_code_norm and resolved_code_norm == target_code_norm:
                update_fields: list[str] = []
                if point_name and resolved.insumo.nombre_point != point_name:
                    resolved.insumo.nombre_point = point_name[:250]
                    update_fields.append("nombre_point")
                if (
                    point_name
                    and normalized_point_name
                    and normalizar_nombre(resolved.insumo.nombre) != normalized_point_name
                    and normalizar_nombre(resolved.insumo.nombre_point or point_name) == normalized_point_name
                ):
                    resolved.insumo.nombre = point_name[:250]
                    update_fields.append("nombre")
                if update_fields:
                    resolved.insumo.save(update_fields=update_fields)
            return resolved, False

        articulo_detail = None
        category = ""
        unit_raw = self._unit_text(row.get("Unidad_corto") or row.get("Unidad") or row.get("UnidadVenta") or "")
        pk_articulo = row.get("PK_Articulo") or row.get("PKInsumo")
        if articulo_row is not None:
            category = str(articulo_row.get("Categoria") or "").strip()
        if pk_articulo not in (None, ""):
            try:
                articulo_detail = client.get_articulo_detail(pk_articulo)
            except Exception:
                articulo_detail = None
        if articulo_detail:
            category_payload = articulo_detail.get("Categoria")
            if isinstance(category_payload, dict):
                category = str(category_payload.get("Categoria") or category_payload.get("Abreviacion") or category).strip()
            else:
                category = str(category_payload or category).strip()
            unit_raw = self._unit_text(articulo_detail.get("UnidadVenta") or articulo_detail.get("UnidadBase") or unit_raw)
            point_code = (articulo_detail.get("Codigo_Articulo") or articulo_detail.get("CodigoInsumo") or articulo_detail.get("Codigo") or point_code).strip()
            point_name = (articulo_detail.get("Nombre_Articulo") or articulo_detail.get("Nombre") or point_name).strip()

        unit = self.identity_service.resolve_unit(unit_raw)
        tipo_item = self._infer_direct_input_type(category=category)
        insumo = Insumo.objects.create(
            codigo_point=point_code[:80],
            nombre_point=point_name[:250],
            nombre=(point_name or point_code or "Insumo Point")[:250],
            tipo_item=tipo_item,
            categoria=(category or "")[:120],
            unidad_base=unit,
            activo=True,
        )
        self.identity_service.sync_insumo_point_identity(
            insumo=insumo,
            point_code=point_code,
            point_name=point_name,
            category=category,
        )
        method = "POINT_CODE_AUTO_CREATE_EMPAQUE" if tipo_item == Insumo.TIPO_EMPAQUE else "POINT_CODE_AUTO_CREATE"
        return ResolvedInsumo(insumo=insumo, score=100.0, method=method), True

    def _infer_direct_input_type(self, *, category: str) -> str:
        normalized = normalizar_nombre(category or "")
        if normalized in {"desechables", "empaque", "empaques", "plasticos", "plasticos y desechables"}:
            return Insumo.TIPO_EMPAQUE
        return Insumo.TIPO_MATERIA_PRIMA

    def _lookup_articulo_catalog_row(self, *, client, point_code: str, point_name: str) -> dict | None:
        searches = []
        if point_code:
            searches.append(point_code)
        if point_name and normalizar_nombre(point_name) not in {normalizar_nombre(term) for term in searches}:
            searches.append(point_name)

        exact_name_key = normalizar_nombre(point_name)
        for search_term in searches:
            try:
                rows = client.get_articulos(search=search_term)
            except Exception:
                continue
            exact_code = None
            exact_name = None
            for row in rows:
                row_code = (row.get("Codigo_Articulo") or "").strip()
                row_name = (row.get("Nombre_Articulo") or "").strip()
                if point_code and self._norm_code(row_code) == self._norm_code(point_code):
                    exact_code = row
                    break
                if exact_name_key and normalizar_nombre(row_name) == exact_name_key:
                    exact_name = row
            if exact_code is not None:
                return exact_code
            if exact_name is not None:
                return exact_name
        return None

    def _resolve_or_create_prepared_insumo(self, *, point_code: str, point_name: str, yield_unit: UnidadMedida | None, category: str = "") -> tuple[Insumo, bool]:
        point_code = (point_code or "").strip().upper()
        point_name = (point_name or "").strip()
        normalized_name = normalizar_nombre(point_name)
        category = (category or "").strip()

        internal_qs = Insumo.objects.filter(tipo_item=Insumo.TIPO_INTERNO, activo=True).order_by("id")
        stale_internal = None
        if point_code:
            internal = internal_qs.filter(codigo_point__iexact=point_code).first()
            if internal is not None:
                if normalized_name and internal.nombre_normalizado != normalized_name:
                    named_internal = internal_qs.filter(nombre_normalizado=normalized_name).first()
                    if named_internal is not None and named_internal.id != internal.id:
                        stale_internal = internal
                        internal = named_internal
                updates: list[str] = []
                if point_code and internal.codigo_point != point_code:
                    internal.codigo_point = point_code[:80]
                    updates.append("codigo_point")
                if point_name and internal.nombre_point != point_name:
                    internal.nombre_point = point_name[:250]
                    updates.append("nombre_point")
                if category and internal.categoria != category[:120]:
                    internal.categoria = category[:120]
                    updates.append("categoria")
                if yield_unit and internal.unidad_base_id != yield_unit.id:
                    internal.unidad_base = yield_unit
                    updates.append("unidad_base")
                if updates:
                    internal.save(update_fields=updates)
                if stale_internal is not None:
                    stale_updates: list[str] = []
                    if stale_internal.codigo_point:
                        stale_internal.codigo_point = ""
                        stale_updates.append("codigo_point")
                    if stale_internal.nombre_point:
                        stale_internal.nombre_point = ""
                        stale_updates.append("nombre_point")
                    if stale_updates:
                        stale_internal.save(update_fields=stale_updates)
                return internal, False

        if normalized_name:
            internal = internal_qs.filter(nombre_normalizado=normalized_name).first()
            if internal is not None:
                updates: list[str] = []
                if point_code and internal.codigo_point != point_code:
                    internal.codigo_point = point_code[:80]
                    updates.append("codigo_point")
                if point_name and internal.nombre_point != point_name:
                    internal.nombre_point = point_name[:250]
                    updates.append("nombre_point")
                if category and internal.categoria != category[:120]:
                    internal.categoria = category[:120]
                    updates.append("categoria")
                if yield_unit and internal.unidad_base_id != yield_unit.id:
                    internal.unidad_base = yield_unit
                    updates.append("unidad_base")
                if updates:
                    internal.save(update_fields=updates)
                return internal, False

        resolved_by_name = self.identity_service.resolve_insumo(point_name=point_name)
        if resolved_by_name.insumo is not None and resolved_by_name.insumo.tipo_item == Insumo.TIPO_INTERNO:
            internal = resolved_by_name.insumo
            updates: list[str] = []
            if point_code and internal.codigo_point != point_code:
                internal.codigo_point = point_code[:80]
                updates.append("codigo_point")
            if point_name and internal.nombre_point != point_name:
                internal.nombre_point = point_name[:250]
                updates.append("nombre_point")
            if category and internal.categoria != category[:120]:
                internal.categoria = category[:120]
                updates.append("categoria")
            if yield_unit and internal.unidad_base_id != yield_unit.id:
                internal.unidad_base = yield_unit
                updates.append("unidad_base")
            if updates:
                internal.save(update_fields=updates)
            return internal, False

        insumo = Insumo.objects.create(
            codigo_point=point_code[:80],
            nombre_point=point_name[:250],
            nombre=(point_name or point_code or "Insumo interno Point")[:250],
            tipo_item=Insumo.TIPO_INTERNO,
            categoria=category[:120],
            unidad_base=yield_unit,
            activo=True,
        )
        self.identity_service.sync_insumo_point_identity(
            insumo=insumo,
            point_code=point_code,
            point_name=point_name,
            category=category,
        )
        return insumo, True

    def _infer_yield(self, detail: dict) -> tuple[Decimal | None, UnidadMedida | None, str, str]:
        unit_text = self._unit_text(detail.get("UnidadBase") or detail.get("UnidadVenta") or "")
        unit = self.identity_service.resolve_unit(unit_text)
        if unit is None:
            return None, None, unit_text, PointRecipeNode.YIELD_UNKNOWN
        if unit.tipo == UnidadMedida.TIPO_MASA:
            yield_mode = PointRecipeNode.YIELD_WEIGHT
        elif unit.tipo == UnidadMedida.TIPO_VOLUMEN:
            yield_mode = PointRecipeNode.YIELD_VOLUME
        else:
            yield_mode = PointRecipeNode.YIELD_UNIT

        purchase_conversion = self._decimal(detail.get("ConvUnidadCompra"))
        if purchase_conversion is not None and purchase_conversion > 0:
            return purchase_conversion, unit, unit_text, yield_mode
        return Decimal("1"), unit, unit_text, yield_mode

    def _upsert_recipe_record(
        self,
        *,
        point_code: str,
        point_name: str,
        family: str,
        category: str,
        tipo: str,
        sheet_name: str,
        yield_quantity: Decimal | None,
        yield_unit: UnidadMedida | None,
        yield_unit_text: str,
        raw_context: dict,
    ) -> tuple[Receta, str]:
        receta_hash = self._build_recipe_hash(
            point_code=point_code,
            point_name=point_name,
            family=family,
            category=category,
            tipo=tipo,
            yield_quantity=yield_quantity,
            yield_unit=yield_unit,
            yield_unit_text=yield_unit_text,
            raw_context=raw_context,
        )
        temporalidad, temporalidad_detalle = inferir_temporalidad_receta(point_name or "")
        receta = self.identity_service.resolve_recipe(point_code=point_code, point_name=point_name)
        created = False
        change = "updated"
        if receta is None:
            receta = Receta(
                nombre=(point_name or point_code or "Receta Point")[:250],
                codigo_point=(point_code or "")[:80],
                tipo=tipo,
                familia=(family or "")[:120],
                categoria=(category or "")[:120],
                temporalidad=temporalidad,
                temporalidad_detalle=temporalidad_detalle[:120],
                sheet_name=sheet_name,
                rendimiento_cantidad=yield_quantity,
                rendimiento_unidad=yield_unit,
                hash_contenido=receta_hash,
            )
            receta.save()
            created = True
            change = "created"
        else:
            if receta.hash_contenido == receta_hash:
                change = "unchanged"
            receta.nombre = (point_name or receta.nombre)[:250]
            receta.codigo_point = (point_code or receta.codigo_point)[:80]
            receta.tipo = tipo
            receta.familia = (family or receta.familia)[:120]
            receta.categoria = (category or receta.categoria)[:120]
            receta.temporalidad = temporalidad
            receta.temporalidad_detalle = temporalidad_detalle[:120]
            receta.sheet_name = sheet_name
            receta.rendimiento_cantidad = yield_quantity
            receta.rendimiento_unidad = yield_unit
            receta.hash_contenido = receta_hash
            receta.save()
        if created:
            change = "created"
        return receta, change

    def _build_recipe_hash(
        self,
        *,
        point_code: str,
        point_name: str,
        family: str,
        category: str,
        tipo: str,
        yield_quantity: Decimal | None,
        yield_unit: UnidadMedida | None,
        yield_unit_text: str,
        raw_context: dict,
    ) -> str:
        bom_rows = list(raw_context.get("bom_rows") or [])
        payload = {
            "point_code": point_code or "",
            "point_name": point_name or "",
            "family": family or "",
            "category": category or "",
            "tipo": tipo,
            "yield_quantity": self._serialize_decimal(yield_quantity),
            "yield_unit": yield_unit.codigo if yield_unit else yield_unit_text,
            "bom": [
                {
                    "codigo": row.get("Codigo_Articulo") or row.get("CodigoInsumo") or "",
                    "nombre": row.get("Articulo") or row.get("Nombre") or row.get("Nombre_Articulo") or "",
                    "cantidad": str(row.get("Cantidad") or ""),
                    "unidad": self._unit_text(row.get("Unidad_corto") or row.get("Unidad") or row.get("UnidadVenta") or ""),
                }
                for row in bom_rows
            ],
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()

    def _identity_key(self, *, source_type: str, point_code: str, point_pk, point_name: str) -> str:
        code_key = self._norm_code(point_code)
        if code_key:
            return f"{source_type}:{code_key}"
        if point_pk not in (None, ""):
            return f"{source_type}:PK:{point_pk}"
        return f"{source_type}:NAME:{normalizar_nombre(point_name)}"

    def _serialize_decimal(self, value):
        if isinstance(value, Decimal):
            return str(value)
        return value

    def _decimal(self, value) -> Decimal | None:
        if value in (None, ""):
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return None

    def _unit_text(self, raw_value) -> str:
        if raw_value is None:
            return ""
        if isinstance(raw_value, dict):
            return str(raw_value.get("Abreviacion") or raw_value.get("Nombre") or "").strip()
        return str(raw_value).strip()
