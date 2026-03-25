from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from django.db import transaction
from django.utils import timezone

from maestros.models import Insumo, UnidadMedida, seed_unidades_basicas
from pos_bridge.config import load_point_bridge_settings
from pos_bridge.models import PointProduct, PointRecipeExtractionRun, PointRecipeNode, PointRecipeNodeLine
from pos_bridge.services.point_http_client import PointHttpSessionClient
from pos_bridge.services.recipe_identity_service import PointRecipeIdentityService, ResolvedInsumo
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

    def __init__(self, settings=None, *, http_client_factory=None, identity_service=None):
        self.settings = settings or load_point_bridge_settings()
        self.http_client_factory = http_client_factory or (lambda: PointHttpSessionClient(self.settings))
        self.identity_service = identity_service or PointRecipeIdentityService()

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
        }

        with self.http_client_factory() as client:
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
                )
                if limit and summary["products_selected"] >= limit:
                    break

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
        with self.http_client_factory() as client:
            workspace = client.login(branch_hint=branch_hint)
            products = client.get_products()
            new_candidates: list[dict[str, str]] = []
            for product in products:
                if not include_without_recipe and not product.get("hasReceta"):
                    continue
                point_code = (product.get("Codigo") or "").strip().upper()
                point_name = (product.get("Nombre") or "").strip()
                if not point_code:
                    continue
                if self.identity_service.resolve_recipe(point_code=point_code, point_name=point_name) is not None:
                    continue
                new_candidates.append(
                    {
                        "codigo_point": point_code,
                        "nombre": point_name,
                        "familia": (product.get("Familia") or "")[:120],
                        "categoria": (product.get("Categoria") or "")[:120],
                    }
                )
        new_candidates.sort(key=lambda item: (item["familia"], item["categoria"], item["nombre"], item["codigo_point"]))
        return {
            "workspace": workspace["branch_name"],
            "products_seen": len(products),
            "new_candidates": new_candidates,
            "new_codes": [item["codigo_point"] for item in new_candidates],
        }

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

        local_candidates = (
            PointProduct.objects.filter(sku__in=missing_codes).order_by("id")
        )
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

    def _extract_product_node(self, *, client, run, product: dict, depth: int, max_depth: int, visited: dict[str, PointRecipeNode], summary: dict) -> PointRecipeNode:
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
        )
        return node

    def _extract_insumo_node(self, *, client, run, articulo_row: dict, depth: int, max_depth: int, visited: dict[str, PointRecipeNode], summary: dict) -> PointRecipeNode:
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
            )
            status = LineaReceta.STATUS_REJECTED
            if resolution.insumo is not None:
                status = clasificar_match(resolution.match_score)
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
        if graph_lines:
            PointRecipeNodeLine.objects.bulk_create(graph_lines, batch_size=200)
        summary["lineas_created"] += len(recipe_lines)
        summary["graph_lines"] += len(graph_lines)

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
    ) -> ComponentResolution:
        point_code = (row.get("Codigo_Articulo") or row.get("CodigoInsumo") or "").strip()
        point_name = (row.get("Articulo") or row.get("Nombre") or row.get("Nombre_Articulo") or "").strip()
        resolved = self.identity_service.resolve_insumo(point_code=point_code, point_name=point_name)

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

        internal_qs = Insumo.objects.filter(tipo_item=Insumo.TIPO_INTERNO, activo=True).order_by("id")
        if point_code:
            internal = internal_qs.filter(codigo_point__iexact=point_code).first()
            if internal is not None:
                if yield_unit and internal.unidad_base_id != yield_unit.id:
                    internal.unidad_base = yield_unit
                    internal.save(update_fields=["unidad_base"])
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
            categoria=(category or "")[:120],
            unidad_base=yield_unit,
            activo=True,
        )
        self.identity_service.sync_insumo_point_identity(
            insumo=insumo,
            point_code=point_code,
            point_name=point_name,
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
