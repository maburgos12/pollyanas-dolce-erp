from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from django.db import IntegrityError, transaction
from unidecode import unidecode

from recetas.models import CostoDriver, Receta, RecetaCostoVersion


Q6 = Decimal("0.000001")
ZERO = Decimal("0")


@dataclass(frozen=True)
class CostBreakdown:
    receta: Receta
    lote_referencia: Decimal
    driver: CostoDriver | None
    costo_mp: Decimal
    costo_mo: Decimal
    costo_indirecto: Decimal
    costo_total: Decimal
    costo_por_unidad_rendimiento: Decimal | None
    hash_snapshot: str
    snapshot_payload: dict[str, Any]


def _normalize(text: str | None) -> str:
    return " ".join(unidecode((text or "")).lower().strip().split())


def _dec(value: Any, default: Decimal = ZERO) -> Decimal:
    if value is None:
        return default
    try:
        return Decimal(str(value))
    except Exception:
        return default


def _q6(value: Decimal) -> Decimal:
    return _dec(value).quantize(Q6, rounding=ROUND_HALF_UP)


def _recipe_family_key(receta: Receta) -> str:
    # Hoja es la aproximación más estable de "familia" en la data importada.
    return _normalize(receta.sheet_name) or _normalize(receta.nombre)


def _line_snapshot(linea) -> dict[str, Any]:
    return {
        "id": linea.id,
        "pos": int(linea.posicion or 0),
        "tipo": linea.tipo_linea,
        "etapa": _normalize(linea.etapa),
        "insumo_id": linea.insumo_id,
        "insumo_txt": _normalize(linea.insumo_texto),
        "cantidad": str(_q6(_dec(linea.cantidad))),
        "unidad": _normalize(linea.unidad_texto),
        "costo_excel": str(_q6(_dec(linea.costo_linea_excel))),
        "costo_snapshot": str(_q6(_dec(linea.costo_unitario_snapshot))),
        "match": linea.match_status,
    }


def _line_cost(linea) -> Decimal:
    estimated = linea.costo_total_estimado
    if estimated is None:
        return ZERO
    return _q6(_dec(estimated))


def resolve_cost_driver(receta: Receta, lote_referencia: Decimal = Decimal("1")) -> CostoDriver | None:
    family_key = _recipe_family_key(receta)
    lote = _dec(lote_referencia, Decimal("1"))

    candidates: list[tuple[int, int, int, CostoDriver]] = []

    for driver in CostoDriver.objects.filter(activo=True).select_related("receta").order_by("prioridad", "id"):
        score = 0
        valid = False

        if driver.scope == CostoDriver.SCOPE_PRODUCTO:
            valid = driver.receta_id == receta.id
            score = 400
        elif driver.scope == CostoDriver.SCOPE_FAMILIA:
            valid = bool(driver.familia_normalizada) and driver.familia_normalizada == family_key
            score = 300
        elif driver.scope == CostoDriver.SCOPE_LOTE:
            in_range = True
            if driver.lote_desde is not None and lote < _dec(driver.lote_desde):
                in_range = False
            if driver.lote_hasta is not None and lote > _dec(driver.lote_hasta):
                in_range = False

            match_producto = (driver.receta_id is None) or (driver.receta_id == receta.id)
            match_familia = (not driver.familia_normalizada) or (driver.familia_normalizada == family_key)
            valid = in_range and match_producto and match_familia
            score = 200
            if driver.receta_id == receta.id:
                score += 40
            if driver.familia_normalizada and driver.familia_normalizada == family_key:
                score += 20
        elif driver.scope == CostoDriver.SCOPE_GLOBAL:
            valid = True
            score = 100

        if valid:
            candidates.append((score, -int(driver.prioridad), -int(driver.id), driver))

    if not candidates:
        return None

    # score alto primero, luego menor prioridad (guardada invertida), luego id más viejo
    candidates.sort(reverse=True)
    return candidates[0][3]


def calcular_costeo_receta(receta: Receta, lote_referencia: Decimal = Decimal("1")) -> CostBreakdown:
    lineas = list(receta.lineas.select_related("insumo", "unidad").order_by("id"))
    driver = resolve_cost_driver(receta, lote_referencia=lote_referencia)

    costo_mp = _q6(sum((_line_cost(linea) for linea in lineas), ZERO))

    mo_pct = _dec(driver.mo_pct if driver else ZERO)
    indirecto_pct = _dec(driver.indirecto_pct if driver else ZERO)
    mo_fijo = _dec(driver.mo_fijo if driver else ZERO)
    indirecto_fijo = _dec(driver.indirecto_fijo if driver else ZERO)

    costo_mo = _q6((costo_mp * (mo_pct / Decimal("100"))) + mo_fijo)
    costo_indirecto = _q6((costo_mp * (indirecto_pct / Decimal("100"))) + indirecto_fijo)
    costo_total = _q6(costo_mp + costo_mo + costo_indirecto)

    costo_por_unidad: Decimal | None = None
    rendimiento_qty = _dec(receta.rendimiento_cantidad)
    if rendimiento_qty > 0:
        costo_por_unidad = _q6(costo_total / rendimiento_qty)

    payload = {
        "receta_id": receta.id,
        "receta_nombre": _normalize(receta.nombre),
        "tipo": receta.tipo,
        "sheet": _normalize(receta.sheet_name),
        "usa_presentaciones": bool(receta.usa_presentaciones),
        "rend_qty": str(_q6(_dec(receta.rendimiento_cantidad))),
        "rend_unit": receta.rendimiento_unidad.codigo if receta.rendimiento_unidad_id and receta.rendimiento_unidad else "",
        "lote": str(_q6(_dec(lote_referencia, Decimal("1")))),
        "driver": {
            "id": driver.id if driver else None,
            "scope": driver.scope if driver else "",
            "nombre": driver.nombre if driver else "",
            "mo_pct": str(_q6(mo_pct)),
            "ind_pct": str(_q6(indirecto_pct)),
            "mo_fijo": str(_q6(mo_fijo)),
            "ind_fijo": str(_q6(indirecto_fijo)),
        },
        "costos": {
            "mp": str(costo_mp),
            "mo": str(costo_mo),
            "indirecto": str(costo_indirecto),
            "total": str(costo_total),
            "unidad": str(costo_por_unidad) if costo_por_unidad is not None else "",
        },
        "lineas": [_line_snapshot(linea) for linea in lineas],
    }
    hash_snapshot = hashlib.sha256(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

    return CostBreakdown(
        receta=receta,
        lote_referencia=_q6(_dec(lote_referencia, Decimal("1"))),
        driver=driver,
        costo_mp=costo_mp,
        costo_mo=costo_mo,
        costo_indirecto=costo_indirecto,
        costo_total=costo_total,
        costo_por_unidad_rendimiento=costo_por_unidad,
        hash_snapshot=hash_snapshot,
        snapshot_payload=payload,
    )


def asegurar_version_costeo(
    receta: Receta,
    lote_referencia: Decimal = Decimal("1"),
    *,
    fuente: str = "AUTO",
) -> tuple[RecetaCostoVersion, bool]:
    snapshot = calcular_costeo_receta(receta, lote_referencia=lote_referencia)

    with transaction.atomic():
        latest = (
            RecetaCostoVersion.objects.select_for_update()
            .filter(receta=receta)
            .order_by("-version_num", "-id")
            .first()
        )

        if latest and latest.hash_snapshot == snapshot.hash_snapshot:
            return latest, False

        next_version = 1 if not latest else latest.version_num + 1
        try:
            created = RecetaCostoVersion.objects.create(
                receta=receta,
                version_num=next_version,
                hash_snapshot=snapshot.hash_snapshot,
                lote_referencia=snapshot.lote_referencia,
                driver_scope=snapshot.driver.scope if snapshot.driver else "",
                driver_nombre=snapshot.driver.nombre if snapshot.driver else "",
                mo_pct=snapshot.driver.mo_pct if snapshot.driver else Decimal("0"),
                indirecto_pct=snapshot.driver.indirecto_pct if snapshot.driver else Decimal("0"),
                mo_fijo=snapshot.driver.mo_fijo if snapshot.driver else Decimal("0"),
                indirecto_fijo=snapshot.driver.indirecto_fijo if snapshot.driver else Decimal("0"),
                costo_mp=snapshot.costo_mp,
                costo_mo=snapshot.costo_mo,
                costo_indirecto=snapshot.costo_indirecto,
                costo_total=snapshot.costo_total,
                rendimiento_cantidad=receta.rendimiento_cantidad,
                rendimiento_unidad=(receta.rendimiento_unidad.codigo if receta.rendimiento_unidad_id and receta.rendimiento_unidad else ""),
                costo_por_unidad_rendimiento=snapshot.costo_por_unidad_rendimiento,
                fuente=(fuente or "AUTO")[:40],
            )
            return created, True
        except IntegrityError:
            existing = (
                RecetaCostoVersion.objects.filter(receta=receta, hash_snapshot=snapshot.hash_snapshot)
                .order_by("-version_num", "-id")
                .first()
            )
            if existing:
                return existing, False
            raise


def comparativo_versiones(versiones: list[RecetaCostoVersion]) -> dict[str, Decimal | int] | None:
    if len(versiones) < 2:
        return None

    actual = versiones[0]
    previa = versiones[1]
    delta = _q6(_dec(actual.costo_total) - _dec(previa.costo_total))
    pct = ZERO
    if _dec(previa.costo_total) > 0:
        pct = _q6((delta / _dec(previa.costo_total)) * Decimal("100"))

    return {
        "version_actual": actual.version_num,
        "version_previa": previa.version_num,
        "delta_total": delta,
        "delta_pct": pct,
    }
