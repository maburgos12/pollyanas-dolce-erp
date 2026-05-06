from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal

from celery import shared_task

from ventas.models import PronosticoGuardado
from ventas.services.pronostico_engine import calcular_pronostico
from ventas.services.sales_truth import sync_authoritative_from_vps


def _json_ready(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, defaultdict):
        return {str(key): _json_ready(val) for key, val in value.items()}
    if isinstance(value, dict):
        return {str(key): _json_ready(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


@shared_task(name="ventas.sync_ventas_autoritativas")
def sync_ventas_autoritativas_task(periodo: str | None = None) -> dict:
    if periodo is None:
        from datetime import date

        today = date.today()
        periodo = f"{today.year - 1}-12" if today.month == 1 else f"{today.year}-{today.month - 1:02d}"
    return sync_authoritative_from_vps(periodo)


@shared_task(bind=True, time_limit=300)
def calcular_y_guardar_pronostico(
    self,
    nombre,
    fecha_inicio_str,
    fecha_fin_str,
    sucursal_ids,
    usuario_id,
    skus_incluidos=None,
):
    fecha_inicio = date.fromisoformat(fecha_inicio_str)
    fecha_fin = date.fromisoformat(fecha_fin_str)
    sucursal_ids = [int(value) for value in (sucursal_ids or [])]
    skus_incluidos = [str(value).strip() for value in (skus_incluidos or []) if str(value).strip()]

    resultado = calcular_pronostico(fecha_inicio, fecha_fin, set(sucursal_ids), skus_incluidos=skus_incluidos or None)

    from django.contrib.auth import get_user_model

    User = get_user_model()
    usuario = User.objects.get(id=usuario_id)

    resumen = resultado.get("resumen") or {}
    pronostico = PronosticoGuardado.objects.create(
        nombre=nombre,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        resultado_json=_json_ready(resultado),
        total_piezas=resumen.get("total_piezas") or 0,
        total_ingreso=resumen.get("total_ingreso") or 0,
        creado_por=usuario,
    )
    if sucursal_ids:
        from core.models import Sucursal

        pronostico.sucursales.set(Sucursal.objects.filter(id__in=sucursal_ids))

    return pronostico.id
