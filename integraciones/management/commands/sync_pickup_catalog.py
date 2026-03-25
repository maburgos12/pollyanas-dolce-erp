from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from core.audit import log_event
from core.models import Sucursal
from pos_bridge.models import PointBranch, PointInventorySnapshot
from recetas.models import Receta, RecetaCodigoPointAlias, normalizar_codigo_point
from recetas.utils.normalizacion import normalizar_nombre


TARGET_BRANCH_CODES = [
    "MATRIZ",
    "CRUCERO",
    "COLOSIO",
    "LAS_GLORIAS",
    "LEYVA",
    "PAYAN",
    "PLAZA_NIO",
    "EL_TUNEL",
]


@dataclass(slots=True)
class PickupCatalogRowResult:
    product_id: str
    product_name: str
    store_code: str
    mapping_type: str
    receta_id: int | None
    receta_nombre: str
    canonical_codigo_point: str
    notes: str = ""


class Command(BaseCommand):
    help = "Sincroniza el catálogo pickup web contra Receta/aliases y reporta faltantes ERP."

    def add_arguments(self, parser):
        parser.add_argument("csv_path", help="Ruta al CSV pickup_erp_catalog_mapping.csv de la tienda.")
        parser.add_argument(
            "--report-path",
            default="",
            help="Ruta opcional para persistir el reporte JSON resultante.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Analiza y arma el reporte sin persistir cambios de catálogo.",
        )

    def handle(self, *args, **options):
        csv_path = Path((options.get("csv_path") or "").strip()).expanduser()
        if not csv_path.exists():
            raise CommandError(f"No existe el CSV: {csv_path}")

        report_path_raw = (options.get("report_path") or "").strip()
        report_path = Path(report_path_raw).expanduser() if report_path_raw else None
        dry_run = bool(options.get("dry_run"))

        rows = self._load_active_rows(csv_path)
        freshness_seconds = max(int(getattr(settings, "PICKUP_AVAILABILITY_FRESHNESS_MINUTES", 20)), 1) * 60

        duplicates_in_csv: list[dict] = []
        direct_mappings: list[PickupCatalogRowResult] = []
        alias_mappings: list[PickupCatalogRowResult] = []
        missing_in_erp: list[PickupCatalogRowResult] = []
        conflicts: list[PickupCatalogRowResult] = []

        seen_codes: set[str] = set()

        with transaction.atomic():
            for row in rows:
                store_code = ((row.get("internal_code") or row.get("sku") or "")).strip()[:80]
                code_norm = normalizar_codigo_point(store_code)
                if code_norm in seen_codes:
                    duplicates_in_csv.append(
                        {
                            "product_id": str(row.get("product_id") or ""),
                            "product_name": (row.get("name") or "").strip(),
                            "store_code": store_code,
                        }
                    )
                    continue
                seen_codes.add(code_norm)

                result = self._process_row(row=row, dry_run=dry_run)
                if result.mapping_type in {"direct", "assigned_codigo_point"}:
                    direct_mappings.append(result)
                elif result.mapping_type in {"alias", "reactivated_alias"}:
                    alias_mappings.append(result)
                elif result.mapping_type == "conflict":
                    conflicts.append(result)
                else:
                    missing_in_erp.append(result)

            if dry_run:
                transaction.set_rollback(True)

        branch_status = self._build_branch_status_report(freshness_seconds=freshness_seconds)
        summary = {
            "source_csv": str(csv_path),
            "generated_at": timezone.now().isoformat(),
            "dry_run": dry_run,
            "counts": {
                "active_rows": len(rows),
                "direct_mappings": len(direct_mappings),
                "alias_mappings": len(alias_mappings),
                "missing_in_erp": len(missing_in_erp),
                "conflicts": len(conflicts),
                "duplicates_in_csv": len(duplicates_in_csv),
            },
            "direct_mappings": [asdict(item) for item in direct_mappings],
            "alias_mappings": [asdict(item) for item in alias_mappings],
            "missing_in_erp": [asdict(item) for item in missing_in_erp],
            "conflicts": [asdict(item) for item in conflicts],
            "duplicates_in_csv": duplicates_in_csv,
            "branches": branch_status,
        }

        if report_path is not None:
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
            summary["report_path"] = str(report_path)

        if not dry_run:
            log_event(
                None,
                "IMPORT",
                "integraciones.PickupCatalogSync",
                str(csv_path),
                payload={
                    "counts": summary["counts"],
                    "report_path": str(report_path) if report_path else "",
                },
            )

        self.stdout.write(json.dumps(summary, ensure_ascii=False, indent=2))

    def _load_active_rows(self, csv_path: Path) -> list[dict]:
        with csv_path.open(newline="", encoding="utf-8-sig") as csv_file:
            reader = csv.DictReader(csv_file)
            return [
                row
                for row in reader
                if str(row.get("is_active") or "").strip().lower() == "true"
            ]

    def _process_row(self, *, row: dict, dry_run: bool) -> PickupCatalogRowResult:
        product_id = str(row.get("product_id") or "")
        product_name = (row.get("name") or "").strip()
        store_code = ((row.get("internal_code") or row.get("sku") or "")).strip()[:80]
        if not store_code:
            return PickupCatalogRowResult(
                product_id=product_id,
                product_name=product_name,
                store_code="",
                mapping_type="missing",
                receta_id=None,
                receta_nombre="",
                canonical_codigo_point="",
                notes="missing_store_code",
            )

        receta_direct = (
            Receta.objects.filter(tipo=Receta.TIPO_PRODUCTO_FINAL, codigo_point__iexact=store_code)
            .order_by("id")
            .first()
        )
        if receta_direct is not None:
            return PickupCatalogRowResult(
                product_id=product_id,
                product_name=product_name,
                store_code=store_code,
                mapping_type="direct",
                receta_id=receta_direct.id,
                receta_nombre=receta_direct.nombre,
                canonical_codigo_point=receta_direct.codigo_point or "",
            )

        preparacion_direct = (
            Receta.objects.filter(tipo=Receta.TIPO_PREPARACION, codigo_point__iexact=store_code)
            .order_by("id")
            .first()
        )
        if preparacion_direct is not None:
            if self._strict_name_compatible(product_name, preparacion_direct.nombre) or self._loose_name_subset_match(product_name, preparacion_direct.nombre):
                if not dry_run:
                    preparacion_direct.tipo = Receta.TIPO_PRODUCTO_FINAL
                    preparacion_direct.save(update_fields=["tipo"])
                return PickupCatalogRowResult(
                    product_id=product_id,
                    product_name=product_name,
                    store_code=store_code,
                    mapping_type="direct",
                    receta_id=preparacion_direct.id,
                    receta_nombre=preparacion_direct.nombre,
                    canonical_codigo_point=preparacion_direct.codigo_point or "",
                    notes="retyped_preparacion_direct_to_producto_final",
                )

        store_code_norm = normalizar_codigo_point(store_code)
        preferred_recipe = self._find_preferred_recipe_by_name(product_name)
        existing_alias = (
            RecetaCodigoPointAlias.objects.filter(codigo_point_normalizado=store_code_norm, activo=True)
            .select_related("receta")
            .order_by("id")
            .first()
        )
        if existing_alias is not None and existing_alias.receta_id:
            if preferred_recipe is not None:
                preferred_resolution = self._apply_preferred_recipe_resolution(
                    product_id=product_id,
                    product_name=product_name,
                    store_code=store_code,
                    preferred_recipe=preferred_recipe,
                    existing_alias=existing_alias,
                    dry_run=dry_run,
                )
                if preferred_resolution is not None:
                    return preferred_resolution

            if self._strict_name_compatible(product_name, existing_alias.receta.nombre) or self._loose_name_subset_match(product_name, existing_alias.receta.nombre):
                if existing_alias.receta.tipo != Receta.TIPO_PRODUCTO_FINAL:
                    if not dry_run:
                        existing_alias.receta.tipo = Receta.TIPO_PRODUCTO_FINAL
                        existing_alias.receta.save(update_fields=["tipo"])
                    if (existing_alias.receta.codigo_point or "").strip().lower() == store_code.lower():
                        return PickupCatalogRowResult(
                            product_id=product_id,
                            product_name=product_name,
                            store_code=store_code,
                            mapping_type="direct",
                            receta_id=existing_alias.receta_id,
                            receta_nombre=existing_alias.receta.nombre,
                            canonical_codigo_point=existing_alias.receta.codigo_point or "",
                            notes="retyped_preparacion_to_producto_final",
                        )
                return PickupCatalogRowResult(
                    product_id=product_id,
                    product_name=product_name,
                    store_code=store_code,
                    mapping_type="alias",
                    receta_id=existing_alias.receta_id,
                    receta_nombre=existing_alias.receta.nombre,
                    canonical_codigo_point=existing_alias.receta.codigo_point or "",
                    notes="existing_alias_strict_name_match",
                )

            if not dry_run and existing_alias.activo:
                existing_alias.activo = False
                existing_alias.save(update_fields=["activo"])
            return PickupCatalogRowResult(
                product_id=product_id,
                product_name=product_name,
                store_code=store_code,
                mapping_type="missing",
                receta_id=None,
                receta_nombre="",
                canonical_codigo_point="",
                notes="invalid_existing_alias_deactivated",
            )

        normalized_name = normalizar_nombre(product_name)
        candidate_qs = Receta.objects.filter(tipo=Receta.TIPO_PRODUCTO_FINAL, nombre_normalizado=normalized_name).order_by("id")
        candidates = list(candidate_qs.only("id", "nombre", "codigo_point"))
        if len(candidates) != 1:
            notes = "not_found"
            if len(candidates) > 1:
                notes = "ambiguous_exact_name_match"
            return PickupCatalogRowResult(
                product_id=product_id,
                product_name=product_name,
                store_code=store_code,
                mapping_type="missing",
                receta_id=None,
                receta_nombre="",
                canonical_codigo_point="",
                notes=notes,
            )

        receta = candidates[0]
        if not (receta.codigo_point or "").strip():
            if not dry_run:
                receta.codigo_point = store_code
                receta.save(update_fields=["codigo_point"])
            return PickupCatalogRowResult(
                product_id=product_id,
                product_name=product_name,
                store_code=store_code,
                mapping_type="assigned_codigo_point",
                receta_id=receta.id,
                receta_nombre=receta.nombre,
                canonical_codigo_point=store_code,
                notes="exact_name_match_with_empty_codigo_point",
            )

        existing_alias_any = (
            RecetaCodigoPointAlias.objects.filter(codigo_point_normalizado=store_code_norm)
            .select_related("receta")
            .order_by("id")
            .first()
        )
        if existing_alias_any is not None and existing_alias_any.receta_id != receta.id:
            return PickupCatalogRowResult(
                product_id=product_id,
                product_name=product_name,
                store_code=store_code,
                mapping_type="conflict",
                receta_id=existing_alias_any.receta_id,
                receta_nombre=existing_alias_any.receta.nombre,
                canonical_codigo_point=existing_alias_any.receta.codigo_point or "",
                notes="alias_conflict_with_other_receta",
            )

        if not dry_run:
            alias, created = RecetaCodigoPointAlias.objects.get_or_create(
                codigo_point_normalizado=store_code_norm,
                defaults={
                    "receta": receta,
                    "codigo_point": store_code,
                    "nombre_point": product_name[:250],
                    "activo": True,
                },
            )
            if not created:
                updated_fields = []
                if alias.receta_id != receta.id:
                    return PickupCatalogRowResult(
                        product_id=product_id,
                        product_name=product_name,
                        store_code=store_code,
                        mapping_type="conflict",
                        receta_id=alias.receta_id,
                        receta_nombre=alias.receta.nombre if alias.receta_id else "",
                        canonical_codigo_point=alias.receta.codigo_point if alias.receta_id else "",
                        notes="alias_conflict_with_other_receta",
                    )
                if alias.codigo_point != store_code:
                    alias.codigo_point = store_code
                    updated_fields.append("codigo_point")
                if alias.nombre_point != product_name[:250]:
                    alias.nombre_point = product_name[:250]
                    updated_fields.append("nombre_point")
                if not alias.activo:
                    alias.activo = True
                    updated_fields.append("activo")
                if updated_fields:
                    alias.save(update_fields=updated_fields)

        mapping_type = "reactivated_alias" if existing_alias_any is not None and not existing_alias_any.activo else "alias"
        return PickupCatalogRowResult(
            product_id=product_id,
            product_name=product_name,
            store_code=store_code,
            mapping_type=mapping_type,
            receta_id=receta.id,
            receta_nombre=receta.nombre,
            canonical_codigo_point=receta.codigo_point or "",
            notes="exact_name_match_with_alias",
        )

    def _apply_preferred_recipe_resolution(
        self,
        *,
        product_id: str,
        product_name: str,
        store_code: str,
        preferred_recipe: Receta,
        existing_alias: RecetaCodigoPointAlias | None,
        dry_run: bool,
    ) -> PickupCatalogRowResult | None:
        store_code_norm = normalizar_codigo_point(store_code)
        existing_target_id = existing_alias.receta_id if existing_alias is not None else None
        target_changed = existing_target_id not in {None, preferred_recipe.id}

        if (preferred_recipe.codigo_point or "").strip().lower() == store_code.lower():
            return PickupCatalogRowResult(
                product_id=product_id,
                product_name=product_name,
                store_code=store_code,
                mapping_type="direct",
                receta_id=preferred_recipe.id,
                receta_nombre=preferred_recipe.nombre,
                canonical_codigo_point=preferred_recipe.codigo_point or "",
                notes="preferred_name_match",
            )

        if not (preferred_recipe.codigo_point or "").strip():
            if not dry_run:
                preferred_recipe.codigo_point = store_code
                preferred_recipe.save(update_fields=["codigo_point"])
                if existing_alias is not None:
                    updated_fields = []
                    if existing_alias.receta_id != preferred_recipe.id:
                        existing_alias.receta = preferred_recipe
                        updated_fields.append("receta")
                    if existing_alias.codigo_point != store_code:
                        existing_alias.codigo_point = store_code
                        updated_fields.append("codigo_point")
                    if existing_alias.nombre_point != product_name[:250]:
                        existing_alias.nombre_point = product_name[:250]
                        updated_fields.append("nombre_point")
                    if not existing_alias.activo:
                        existing_alias.activo = True
                        updated_fields.append("activo")
                    if updated_fields:
                        existing_alias.save(update_fields=updated_fields)
            return PickupCatalogRowResult(
                product_id=product_id,
                product_name=product_name,
                store_code=store_code,
                mapping_type="assigned_codigo_point",
                receta_id=preferred_recipe.id,
                receta_nombre=preferred_recipe.nombre,
                canonical_codigo_point=store_code,
                notes="preferred_name_match_assigned_codigo_point" if not target_changed else "preferred_name_match_retargeted_and_assigned_codigo_point",
            )

        conflicting_alias = (
            RecetaCodigoPointAlias.objects.filter(codigo_point_normalizado=store_code_norm)
            .exclude(id=existing_alias.id if existing_alias is not None else None)
            .select_related("receta")
            .order_by("id")
            .first()
        )
        if conflicting_alias is not None and conflicting_alias.receta_id != preferred_recipe.id:
            return PickupCatalogRowResult(
                product_id=product_id,
                product_name=product_name,
                store_code=store_code,
                mapping_type="conflict",
                receta_id=conflicting_alias.receta_id,
                receta_nombre=conflicting_alias.receta.nombre,
                canonical_codigo_point=conflicting_alias.receta.codigo_point or "",
                notes="preferred_name_match_alias_conflict",
            )

        if not dry_run:
            alias = existing_alias
            if alias is None:
                alias, _ = RecetaCodigoPointAlias.objects.get_or_create(
                    codigo_point_normalizado=store_code_norm,
                    defaults={
                        "receta": preferred_recipe,
                        "codigo_point": store_code,
                        "nombre_point": product_name[:250],
                        "activo": True,
                    },
                )
            updated_fields = []
            if alias.receta_id != preferred_recipe.id:
                alias.receta = preferred_recipe
                updated_fields.append("receta")
            if alias.codigo_point != store_code:
                alias.codigo_point = store_code
                updated_fields.append("codigo_point")
            if alias.nombre_point != product_name[:250]:
                alias.nombre_point = product_name[:250]
                updated_fields.append("nombre_point")
            if not alias.activo:
                alias.activo = True
                updated_fields.append("activo")
            if updated_fields:
                alias.save(update_fields=updated_fields)

        return PickupCatalogRowResult(
            product_id=product_id,
            product_name=product_name,
            store_code=store_code,
            mapping_type="alias",
            receta_id=preferred_recipe.id,
            receta_nombre=preferred_recipe.nombre,
            canonical_codigo_point=preferred_recipe.codigo_point or "",
            notes="preferred_name_match_alias" if not target_changed else "preferred_name_match_retargeted_alias",
        )

    def _find_preferred_recipe_by_name(self, product_name: str) -> Receta | None:
        strict_variants = self._pickup_name_variants(product_name, broad=False)
        if strict_variants:
            strict_matches: list[Receta] = []
            for recipe in Receta.objects.filter(tipo=Receta.TIPO_PRODUCTO_FINAL).only("id", "nombre", "codigo_point"):
                recipe_strict_variants = self._pickup_name_variants(recipe.nombre, broad=False)
                if strict_variants.intersection(recipe_strict_variants):
                    strict_matches.append(recipe)
            if len(strict_matches) == 1:
                return strict_matches[0]

        broad_variants = self._pickup_name_variants(product_name, broad=True)
        if not broad_variants:
            return None

        matches: list[Receta] = []
        for recipe in Receta.objects.filter(tipo=Receta.TIPO_PRODUCTO_FINAL).only("id", "nombre", "codigo_point"):
            recipe_variants = self._pickup_name_variants(recipe.nombre, broad=True)
            if broad_variants.intersection(recipe_variants):
                matches.append(recipe)

        if len(matches) == 1:
            return matches[0]
        return None

    def _pickup_name_variants(self, raw_name: str, *, broad: bool) -> set[str]:
        base = normalizar_nombre(raw_name)
        if not base:
            return set()

        variants = {base}
        sanitized = " ".join(re.sub(r"[^a-z0-9]+", " ", base).split())
        if sanitized:
            variants.add(sanitized)

        def _add_token_variant(text: str) -> None:
            tokens = [token for token in text.split() if token]
            if not tokens:
                return
            variants.add(" ".join(tokens))

            singular_tokens = ["cheesecake" if token == "cheesecakes" else token for token in tokens]
            variants.add(" ".join(singular_tokens))

            expanded_tokens = singular_tokens[:]
            expansions = {"r": "rebanada", "reb": "rebanada", "m": "mediano", "g": "grande", "i": "individual"}
            if expanded_tokens and expanded_tokens[-1] in expansions:
                expanded_tokens = expanded_tokens[:-1] + [expansions[expanded_tokens[-1]]]
                variants.add(" ".join(expanded_tokens))

            without_de = [token for token in expanded_tokens if token != "de"]
            if without_de:
                variants.add(" ".join(without_de))

            if broad:
                without_three_milks = []
                skip_next = 0
                for idx, token in enumerate(without_de):
                    if skip_next:
                        skip_next -= 1
                        continue
                    if idx + 1 < len(without_de) and token == "3" and without_de[idx + 1] == "leches":
                        skip_next = 1
                        continue
                    without_three_milks.append(token)
                if without_three_milks:
                    variants.add(" ".join(without_three_milks))

        _add_token_variant(base)
        if sanitized != base:
            _add_token_variant(sanitized)

        return {item for item in variants if item}

    def _strict_name_compatible(self, left: str, right: str) -> bool:
        left_variants = self._pickup_name_variants(left, broad=False)
        right_variants = self._pickup_name_variants(right, broad=False)
        return bool(left_variants and right_variants and left_variants.intersection(right_variants))

    def _loose_name_subset_match(self, left: str, right: str) -> bool:
        left_tokens = set(self._meaningful_tokens(left))
        right_tokens = set(self._meaningful_tokens(right))
        return bool(left_tokens and right_tokens and left_tokens.issubset(right_tokens))

    def _meaningful_tokens(self, value: str) -> list[str]:
        sanitized = " ".join(re.sub(r"[^a-z0-9]+", " ", normalizar_nombre(value)).split())
        expansions = {"r": "rebanada", "reb": "rebanada", "m": "mediano", "g": "grande", "i": "individual"}
        stopwords = {"de"}
        tokens: list[str] = []
        for token in sanitized.split():
            if token in stopwords:
                continue
            token = expansions.get(token, token)
            tokens.append(token)
        return tokens

    def _build_branch_status_report(self, *, freshness_seconds: int) -> list[dict]:
        now = timezone.now()
        rows = []
        for branch_code in TARGET_BRANCH_CODES:
            sucursal = Sucursal.objects.filter(codigo__iexact=branch_code, activa=True).first()
            point_branch = PointBranch.objects.filter(erp_branch=sucursal).order_by("id").first() if sucursal else None
            snapshot = (
                PointInventorySnapshot.objects.filter(branch=point_branch).order_by("-captured_at", "-id").first()
                if point_branch is not None
                else None
            )
            snapshot_age_seconds = int((now - snapshot.captured_at).total_seconds()) if snapshot is not None else None
            rows.append(
                {
                    "branch_code": branch_code,
                    "erp_branch_found": sucursal is not None,
                    "point_branch_found": point_branch is not None,
                    "point_branch_name": point_branch.name if point_branch is not None else "",
                    "snapshot_captured_at": snapshot.captured_at.isoformat() if snapshot is not None else None,
                    "snapshot_age_seconds": snapshot_age_seconds,
                    "snapshot_is_fresh": snapshot_age_seconds is not None and snapshot_age_seconds <= freshness_seconds,
                }
            )
        return rows
