"""Complete the costs of imported recipes using attributable Point purchases."""

from datetime import timedelta
from decimal import Decimal, InvalidOperation

from django.db import transaction
from django.utils import timezone
from core.audit import log_event
from maestros.models import Insumo, UnidadMedida
from recetas.models import LineaReceta
from recetas.utils.costeo_snapshot import (
    resolve_line_snapshot_cost,
    resolve_preparation_recipe_for_insumo,
    POINT_UNIT_ALIASES,
    convert_unit_cost,
)
from pos_bridge.config import load_point_bridge_settings
from pos_bridge.services.point_http_client import PointHttpSessionClient
from pos_bridge.services.point_account_session_lock import point_account_session_lock
from pos_bridge.services.point_purchase_extraction_service import (
    _epoch_ms,
    _parse_purchase_date,
)
from pos_bridge.services.point_purchase_cost_import_service import (
    PointPurchaseCostImportService,
)
from pos_bridge.services.catalog_recipe_execution import remaining_seconds


def validate_recipe_costs(recipe_ids):
    missing = []
    checked = set()
    missing_inputs = set()

    def visit(recipe_id, path):
        if recipe_id in path:
            missing.append(
                {
                    "recipe_id": recipe_id,
                    "name": "Preparación circular",
                    "reason": "RECETA_CICLICA",
                }
            )
            return
        if recipe_id in checked:
            return
        checked.add(recipe_id)
        lines = list(
            LineaReceta.objects.filter(receta_id=recipe_id)
            .exclude(tipo_linea=LineaReceta.TIPO_SUBSECCION)
            .select_related("insumo", "unidad", "insumo__unidad_base")
        )
        if not lines:
            missing.append(
                {
                    "recipe_id": recipe_id,
                    "name": "Receta sin componentes",
                    "reason": "SIN_COMPONENTES",
                }
            )
        for line in lines:
            prep = resolve_preparation_recipe_for_insumo(line.insumo)
            if prep:
                visit(prep.id, (*path, recipe_id))
            cost, source = resolve_line_snapshot_cost(line)
            if (
                line.cantidad <= 0
                or not cost
                or cost <= 0
                or "SNAPSHOT" in source
                or "INCOMPATIBLE" in source
            ):
                missing.append(
                    {
                        "recipe_id": recipe_id,
                        "insumo_id": line.insumo_id,
                        "name": line.insumo.nombre
                        if line.insumo
                        else line.insumo_texto,
                        "reason": source if line.cantidad > 0 else "CANTIDAD_INVALIDA",
                    }
                )
                if line.insumo_id and not prep:
                    missing_inputs.add(line.insumo_id)

    for recipe_id in sorted(set(recipe_ids)):
        visit(recipe_id, ())
    for item in missing:
        reason = item["reason"]
        item["message"] = (
            "Vincular el ingrediente con Point."
            if reason == "NO_INSUMO"
            else "Corregir la cantidad en Point."
            if reason == "CANTIDAD_INVALIDA"
            else "Revisar las unidades de compra y receta."
            if "UNIDAD" in reason
            else "Revisar la composición en Point."
            if reason in {"RECETA_CICLICA", "SIN_COMPONENTES"}
            else "Falta un costo de compra verificable."
        )
    return {
        "recipe_ids": sorted(checked),
        "missing": missing,
        "missing_count": len(missing),
        "missing_insumo_ids": sorted(missing_inputs),
    }


def complete_recipe_costs(*, recipe_ids, job):
    validation = validate_recipe_costs(recipe_ids)
    wanted = set(validation["missing_insumo_ids"])
    imported = 0
    searched = 0
    search_error = ""
    today = timezone.localdate()
    if wanted:
        importer = PointPurchaseCostImportService()
        try:
            with (
                point_account_session_lock(wait=True),
                PointHttpSessionClient(load_point_bridge_settings()) as client,
            ):
                client.login(branch_hint="MATRIZ")
                rows = client._catalog_rows(
                    "/InventoryPurchases/GetCompras",
                    params={
                        "fechaInicio": _epoch_ms(today - timedelta(days=90)),
                        "fechaFin": _epoch_ms(today + timedelta(days=1)),
                        "fkproveedor": "",
                        "fkSucursal": "null",
                    },
                )
                rows = sorted(
                    rows,
                    key=lambda row: (
                        str(row.get("Fecha_compra") or ""),
                        str(row.get("FK_Movimiento") or ""),
                    ),
                    reverse=True,
                )
                for row in rows[:500]:
                    remaining_seconds()
                    if not wanted:
                        break
                    purchase_id = row.get("FK_Movimiento")
                    purchase_date = _parse_purchase_date(row.get("Fecha_compra"))
                    if not purchase_id or not purchase_date or purchase_date > today:
                        continue
                    details = client._catalog_rows(
                        "/InventoryPurchases/GetComprabyId",
                        params={"fkCompra": purchase_id},
                    )
                    searched += 1
                    lines = []
                    matched = set()
                    for detail in details:
                        name = str(detail.get("Articulo") or "").strip()
                        insumo = importer._resolve_insumo(name)
                        if not insumo or insumo.id not in wanted:
                            continue
                        # Never choose an arbitrary homonym or guess a purchase unit.
                        if (
                            Insumo.objects.filter(nombre__iexact=insumo.nombre).count()
                            != 1
                        ):
                            continue
                        unit_code = POINT_UNIT_ALIASES.get(
                            str(detail.get("Unidad") or "").strip().lower()
                        )
                        unit = (
                            UnidadMedida.objects.filter(
                                codigo__iexact=unit_code
                            ).first()
                            if unit_code
                            else None
                        )
                        try:
                            quantity = Decimal(str(detail.get("Cantidad")))
                            cost = Decimal(str(detail.get("Costo_unitario")))
                        except (InvalidOperation, TypeError):
                            continue
                        if (
                            not quantity.is_finite()
                            or not cost.is_finite()
                            or quantity <= 0
                            or cost <= 0
                        ):
                            continue
                        if (
                            convert_unit_cost(
                                cost, source_unit=unit, target_unit=insumo.unidad_base
                            )
                            is None
                        ):
                            continue
                        lines.append(
                            {
                                "articulo": name,
                                "cantidad": detail["Cantidad"],
                                "unidad": detail["Unidad"],
                                "costo_unitario": detail["Costo_unitario"],
                                "costo_total": detail.get("Costo_total"),
                                "raw": detail,
                            }
                        )
                        matched.add(insumo.id)
                    if lines:
                        with transaction.atomic():
                            result = importer.persist_purchases(
                                [
                                    {
                                        "purchase_id": str(purchase_id),
                                        "folio": row.get("Folio"),
                                        "supplier": row.get("Proveedor"),
                                        "branch": row.get("Sucursal"),
                                        "purchase_date": purchase_date,
                                        "lines": lines,
                                    }
                                ]
                            )
                            log_event(
                                job.triggered_by,
                                "POINT_RECIPE_PURCHASE_COSTS",
                                "pos_bridge.PointSyncJob",
                                job.id,
                                {
                                    "purchase_id": str(purchase_id),
                                    "created": result.created,
                                    "existing": result.existing,
                                    "insumo_ids": sorted(matched),
                                },
                            )
                        imported += result.created
                        wanted -= matched
                    job.parameters = {
                        **job.parameters,
                        "progress": {
                            "stage": "PURCHASES",
                            "detail": f"Revisadas {searched} compras; {len(wanted)} insumos aún sin costo verificado.",
                        },
                    }
                    job.save(update_fields=["parameters", "updated_at"])
        except Exception as exc:
            # Preserve previously imported purchases; a retry is idempotent.
            search_error = str(exc)
    validation = validate_recipe_costs(recipe_ids)
    validation.update(
        {
            "purchases_imported": imported,
            "purchases_checked": searched,
            "lookback_days": 90,
            "search_error": search_error,
        }
    )
    return validation
