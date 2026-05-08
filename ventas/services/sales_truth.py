from __future__ import annotations

from calendar import monthrange
from datetime import date
from decimal import Decimal
from functools import lru_cache
import re
from typing import Any
import warnings

import pandas as pd
from django.db.models import Q, Sum
from unidecode import unidecode

from core.models import Sucursal
from pos_bridge.models import PointDailySale, PointSalesDailyProductFact
from recetas.models import Receta, RecetaCodigoPointAlias, VentaHistorica, normalizar_codigo_point
from ventas.models import VentaAutoritativaPoint

ZERO = Decimal('0')
OFFICIAL_POINT_SOURCE = "/Report/PrintReportes?idreporte=3"
LEGACY_POINT_SOURCE = "/Report/VentasCategorias"


def _normalize_name(value: str) -> str:
    raw = unidecode(str(value or '')).lower().strip()
    return ' '.join(raw.split())


def _decimal(value: Any) -> Decimal:
    if value is None:
        return ZERO
    return Decimal(str(value or 0))


@lru_cache(maxsize=2048)
def recipe_point_codes(recipe_id: int) -> tuple[str, ...]:
    recipe = Receta.objects.filter(pk=recipe_id).first()
    if not recipe:
        return tuple()
    codes: set[str] = set()
    if (recipe.codigo_point or '').strip():
        codes.add((recipe.codigo_point or '').strip())
    codes.update(
        code
        for code in RecetaCodigoPointAlias.objects.filter(receta_id=recipe_id, activo=True)
        .values_list('codigo_point', flat=True)
        if (code or '').strip()
    )
    return tuple(sorted(codes))


@lru_cache(maxsize=4096)
def authoritative_daily_total(recipe_id: int, branch_id: int | None, target_day: date) -> Decimal:
    qs = VentaAutoritativaPoint.objects.filter(product_id=recipe_id, sale_date=target_day)
    if branch_id:
        qs = qs.filter(branch_id=branch_id)
    total = qs.aggregate(total=Sum('quantity')).get('total')
    return Decimal(str(total or 0))


@lru_cache(maxsize=4096)
def authoritative_day_loaded(branch_id: int | None, target_day: date) -> bool:
    qs = VentaAutoritativaPoint.objects.filter(sale_date=target_day)
    if branch_id:
        qs = qs.filter(branch_id=branch_id)
    return qs.exists()


@lru_cache(maxsize=4096)
def historical_daily_total(recipe_id: int, branch_id: int | None, target_day: date) -> Decimal:
    qs = VentaHistorica.objects.filter(receta_id=recipe_id, fecha=target_day)
    if branch_id:
        qs = qs.filter(sucursal_id=branch_id)
    total = qs.aggregate(total=Sum('cantidad')).get('total')
    return Decimal(str(total or 0))


def _point_recipe_filters(*, recipe_id: int, codes: tuple[str, ...]) -> Q:
    filters = Q(receta_id=recipe_id)
    code_filter = Q()
    for code in codes:
        code_filter |= Q(raw_payload__Codigo=code)
    if codes:
        fallback_filter = Q(receta_id=recipe_id) & (Q(raw_payload__Codigo__isnull=True) | Q(raw_payload__Codigo=''))
        filters = code_filter | fallback_filter
    return filters


def _prefer_single_point_source(qs):
    official_qs = qs.filter(source_endpoint=OFFICIAL_POINT_SOURCE)
    if official_qs.exists():
        return official_qs
    legacy_qs = qs.filter(source_endpoint=LEGACY_POINT_SOURCE)
    if legacy_qs.exists():
        return legacy_qs
    return qs


def _verified_point_daily_metrics(recipe_id: int, branch_id: int | None, target_day: date) -> tuple[Decimal, Decimal]:
    fact_qs = PointSalesDailyProductFact.objects.filter(receta_id=recipe_id, sale_date=target_day)
    if branch_id:
        fact_qs = fact_qs.filter(branch__erp_branch_id=branch_id)
    fact_totals = fact_qs.aggregate(qty=Sum('total_cantidad'), sales=Sum('total_venta'))
    fact_qty = Decimal(str(fact_totals.get('qty') or 0))
    fact_sales = Decimal(str(fact_totals.get('sales') or 0))
    if fact_qs.exists():
        return fact_qty, fact_sales

    codes = recipe_point_codes(recipe_id)
    qs = PointDailySale.objects.filter(sale_date=target_day)
    if branch_id:
        qs = qs.filter(branch__erp_branch_id=branch_id)
    qs = _prefer_single_point_source(qs.filter(_point_recipe_filters(recipe_id=recipe_id, codes=codes)))
    totals = qs.aggregate(qty=Sum('quantity'), sales=Sum('total_amount'))
    return Decimal(str(totals.get('qty') or 0)), Decimal(str(totals.get('sales') or 0))


@lru_cache(maxsize=4096)
def verified_point_daily_total(recipe_id: int, branch_id: int | None, target_day: date) -> Decimal:
    qty, _sales = _verified_point_daily_metrics(recipe_id, branch_id, target_day)
    return qty


@lru_cache(maxsize=2048)
def authoritative_sales_aggregate(recipe_id: int, start_date: date, end_date: date) -> tuple[Decimal, Decimal]:
    aggregate = VentaAutoritativaPoint.objects.filter(product_id=recipe_id, sale_date__range=(start_date, end_date)).aggregate(
        qty=Sum('quantity'),
        sales=Sum('total_amount'),
    )
    return Decimal(str(aggregate.get('qty') or 0)), Decimal(str(aggregate.get('sales') or 0))


@lru_cache(maxsize=2048)
def verified_point_sales_aggregate(recipe_id: int, start_date: date, end_date: date) -> tuple[Decimal, Decimal]:
    total_qty = ZERO
    total_sales = ZERO
    current_day = start_date
    while current_day <= end_date:
        day_qty, day_sales = _verified_point_daily_metrics(recipe_id, None, current_day)
        total_qty += day_qty
        total_sales += day_sales
        current_day = date.fromordinal(current_day.toordinal() + 1)
    return total_qty, total_sales


@lru_cache(maxsize=512)
def _resolve_recipe_for_authoritative_row(product_code: str, point_name: str) -> int | None:
    code = (product_code or '').strip()
    name = (point_name or '').strip()
    by_code = None
    if code:
        by_code = Receta.objects.filter(codigo_point__iexact=code).order_by('id').first()
        if by_code is None:
            code_norm = normalizar_codigo_point(code)
            if code_norm:
                alias = RecetaCodigoPointAlias.objects.filter(codigo_point_normalizado=code_norm, activo=True).select_related('receta').first()
                if alias and alias.receta_id:
                    by_code = alias.receta
    by_name = None
    if name:
        key = _normalize_name(name)
        by_name = Receta.objects.filter(nombre_normalizado=key).order_by('id').first()
        if by_name is None:
            alias = RecetaCodigoPointAlias.objects.filter(activo=True, nombre_point__iexact=name).select_related('receta').first()
            if alias and alias.receta_id:
                by_name = alias.receta
    if by_code and by_name and by_code.id != by_name.id:
        if _normalize_name(by_name.nombre) == _normalize_name(name):
            return by_name.id
        return None
    if by_name:
        return by_name.id
    if by_code:
        if not name:
            return by_code.id
        # Be conservative: if the Point name is clearly different from the recipe name, do not force-link it.
        if _normalize_name(by_code.nombre) == _normalize_name(name):
            return by_code.id
        return None
    return None


def _period_bounds(periodo: str) -> tuple[date, date]:
    try:
        year_raw, month_raw = (periodo or "").split("-", 1)
        year = int(year_raw)
        month = int(month_raw)
        start_date = date(year, month, 1)
    except (TypeError, ValueError) as exc:
        raise ValueError("periodo debe tener formato YYYY-MM") from exc
    return start_date, date(year, month, monthrange(year, month)[1])


def _authoritative_product_code(row: PointSalesDailyProductFact) -> str:
    if row.point_product_id and row.point_product and (row.point_product.external_id or "").strip():
        return (row.point_product.external_id or "").strip()
    if row.receta_id and row.receta and (row.receta.codigo_point or "").strip():
        return (row.receta.codigo_point or "").strip()
    # Gap documentado: PointSalesDailyProductFact no tiene una columna
    # `product_code` propia cuando no hay catálogo Point ligado. Se usa una
    # clave estable por nombre/categoría solo para respetar la unicidad.
    name_key = re.sub(r"[^A-Za-z0-9_-]+", "-", row.producto_nombre_historico or "").strip("-")
    category_key = re.sub(r"[^A-Za-z0-9_-]+", "-", row.categoria or "").strip("-")
    return f"FACT-{category_key[:24]}-{name_key[:40]}"[:80]


def sync_authoritative_from_vps(periodo: str, sucursal_id: int | None = None, *, dry_run: bool = False) -> dict[str, Any]:
    """
    Puebla VentaAutoritativaPoint desde PointSalesDailyProductFact.

    Gaps conocidos del mapeo:
    - VentaAutoritativaPoint.gross_amount no tiene equivalente directo en
      PointSalesDailyProductFact; queda en default 0.
    - VentaAutoritativaPoint.source_sheet no aplica al sync VPS_DB; se marca
      como `PointSalesDailyProductFact`.
    """
    start_date, end_date = _period_bounds(periodo)
    rows = (
        PointSalesDailyProductFact.objects.select_related("branch", "branch__erp_branch", "point_product", "receta")
        .filter(sale_date__gte=start_date, sale_date__lte=end_date, branch__erp_branch_id__isnull=False)
        .order_by("sale_date", "branch_id", "categoria", "producto_nombre_historico", "id")
    )
    if sucursal_id:
        rows = rows.filter(branch__erp_branch_id=sucursal_id)

    errors: list[str] = []
    examples: list[dict[str, Any]] = []
    objects_by_key: dict[tuple[int, date, str], VentaAutoritativaPoint] = {}


    for row in rows:
        branch = row.branch.erp_branch if row.branch_id and row.branch else None
        if branch is None:
            errors.append(f"{row.id}: sin sucursal ERP ligada")
            continue
        product_code = _authoritative_product_code(row)
        product_id = row.receta_id or _resolve_recipe_for_authoritative_row(
            product_code,
            row.producto_nombre_historico or "",
        )
        obj = VentaAutoritativaPoint(
            branch=branch,
            sale_date=row.sale_date,
            product_code=product_code,
            category=row.categoria or "",
            point_name=row.producto_nombre_historico or "",
            product_id=product_id,
            quantity=_decimal(row.total_cantidad),
            gross_amount=ZERO,
            discount_amount=_decimal(row.total_descuento),
            total_amount=_decimal(row.total_venta),
            tax_amount=_decimal(row.total_impuestos),
            net_amount=_decimal(row.total_venta_neta),
            source_file=row.source_file or "VPS_DB:PointSalesDailyProductFact",
            source_sheet="PointSalesDailyProductFact",
            raw_payload={
                "source_model": "pos_bridge.PointSalesDailyProductFact",
                "source_id": row.id,
                "source_hash": row.source_hash,
                "categoria": row.categoria,
                "producto_nombre_historico": row.producto_nombre_historico,
                "point_product_id": row.point_product_id,
                "receta_id": row.receta_id,
                "source_granularity": row.source_granularity,
            },
        )
        key = (int(branch.id), row.sale_date, product_code)
        existing_obj = objects_by_key.get(key)
        if existing_obj is None:
            objects_by_key[key] = obj
        else:
            existing_obj.quantity += obj.quantity
            existing_obj.discount_amount += obj.discount_amount
            existing_obj.total_amount += obj.total_amount
            existing_obj.tax_amount += obj.tax_amount
            existing_obj.net_amount += obj.net_amount
            existing_obj.raw_payload.setdefault("merged_source_ids", []).append(row.id)
        if len(examples) < 5:
            examples.append(
                {
                    "branch": branch.codigo,
                    "sale_date": row.sale_date.isoformat(),
                    "product_code": product_code,
                    "point_name": obj.point_name,
                    "quantity": str(obj.quantity),
                    "total_amount": str(obj.total_amount),
                }
            )

    existing_keys = set(
        VentaAutoritativaPoint.objects.filter(
            sale_date__gte=start_date,
            sale_date__lte=end_date,
            branch_id__in={key[0] for key in objects_by_key} or {-1},
            product_code__in={key[2] for key in objects_by_key} or {""},
        ).values_list("branch_id", "sale_date", "product_code")
    )
    updated = sum(1 for key in objects_by_key if key in existing_keys)
    created = len(objects_by_key) - updated

    if not dry_run and objects_by_key:
        VentaAutoritativaPoint.objects.bulk_create(
            list(objects_by_key.values()),
            batch_size=1000,
            update_conflicts=True,
            update_fields=[
                "category",
                "point_name",
                "product",
                "quantity",
                "gross_amount",
                "discount_amount",
                "total_amount",
                "tax_amount",
                "net_amount",
                "source_file",
                "source_sheet",
                "raw_payload",
            ],
            unique_fields=["branch", "sale_date", "product_code"],
        )

    if not dry_run:
        authoritative_daily_total.cache_clear()
        authoritative_day_loaded.cache_clear()
        authoritative_sales_aggregate.cache_clear()
    return {
        "periodo": periodo,
        "sucursal_id": sucursal_id,
        "source_count": rows.count(),
        "creados": created,
        "actualizados": updated,
        "errores": errors,
        "examples": examples,
        "dry_run": dry_run,
    }


def import_authoritative_point_category_report(path: str) -> dict[str, Any]:
    """
    DEPRECADO — usar sync_authoritative_from_vps() para flujo automático.
    Esta función se mantiene como importador manual de respaldo para períodos
    históricos o cuando el sync de Point no esté disponible.
    """
    warnings.warn(
        "import_authoritative_point_category_report está deprecado. "
        "Usar sync_ventas_autoritativas management command.",
        DeprecationWarning,
        stacklevel=2,
    )
    raw = pd.read_excel(path, header=None, sheet_name=0)
    title = ""
    for row_idx in range(min(6, len(raw.index))):
        for col_idx in range(min(6, len(raw.columns))):
            candidate = str(raw.iloc[row_idx, col_idx] or "").strip()
            if "VENTA POR CATEGORÍA" in candidate.upper():
                title = candidate
                break
        if title:
            break
    lines = [line.strip() for line in title.splitlines() if line.strip()]
    if len(lines) < 3:
        raise ValueError('No se pudo identificar sucursal/fecha en el reporte autoritativo.')
    branch_name = lines[1]
    date_range = lines[2]
    match = re.search(r"(\d{1,2})/([A-Za-z]{3})/(\d{4})", date_range)
    if not match:
        raise ValueError('No se pudo identificar la fecha del reporte autoritativo.')
    day_txt, month_txt, year_txt = match.groups()
    months = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
    }
    sale_date = date(int(year_txt), months[month_txt.lower()], int(day_txt))
    branch = Sucursal.objects.filter(nombre__iexact=branch_name).order_by('id').first()
    if branch is None:
        raise ValueError(f'Sucursal no encontrada para reporte autoritativo: {branch_name}')

    df = pd.read_excel(path, header=5, sheet_name=0)
    current_category = ''
    created = 0
    updated = 0
    unresolved: list[str] = []
    for _, row in df.iterrows():
        category = str(row.get('CATEGORÍA') or '').strip()
        if category and category.lower() != 'total general':
            current_category = category
        product_name = str(row.get('PRODUCTO') or '').strip()
        product_code = str(row.get('CÓDIGO') or '').strip()
        if not product_name or product_name.lower().startswith('total'):
            continue
        recipe_id = _resolve_recipe_for_authoritative_row(product_code, product_name)
        payload = {
            'category': current_category,
            'product_code': product_code,
            'point_name': product_name,
            'product_id': recipe_id,
            'quantity': Decimal(str(row.get('CANTIDAD') or 0)),
            'gross_amount': Decimal(str(row.get('BRUTO') or 0)),
            'discount_amount': Decimal(str(row.get('DESCUENTOS') or 0)),
            'total_amount': Decimal(str(row.get('VENTA') or 0)),
            'tax_amount': Decimal(str(row.get('IMPUESTOS') or 0)),
            'net_amount': Decimal(str(row.get('VENTA NETA') or 0)),
            'source_file': path,
            'source_sheet': 'Sheet1',
            'raw_payload': {
                'categoria': current_category,
                'codigo': product_code,
                'producto': product_name,
            },
        }
        obj, was_created = VentaAutoritativaPoint.objects.update_or_create(
            branch=branch,
            sale_date=sale_date,
            product_code=product_code,
            defaults=payload,
        )
        if was_created:
            created += 1
        else:
            updated += 1
        if obj.product_id is None:
            unresolved.append(f'{product_code} · {product_name}')
    authoritative_daily_total.cache_clear()
    authoritative_day_loaded.cache_clear()
    authoritative_sales_aggregate.cache_clear()
    return {
        'branch': branch.nombre,
        'sale_date': sale_date.isoformat(),
        'created': created,
        'updated': updated,
        'unresolved': unresolved,
    }
