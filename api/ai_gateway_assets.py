"""Bounded asset reads; existing asset and maintenance policies remain authoritative."""
from __future__ import annotations

import json
import re
from datetime import timedelta

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.exceptions import PermissionDenied
from django.core.serializers.json import DjangoJSONEncoder
from django.core.validators import RegexValidator
from django.db.models import Q
from django.utils import timezone
from rest_framework import serializers
from rest_framework.schemas.openapi import AutoSchema

from activos.models import OrdenMantenimiento, PlanMantenimiento
from activos.services_pasaporte import (
    FALLA_ESTATUS_ABIERTOS, MAX_EVENTOS, _falla_publica, _sucursal_operativa,
    activos_autorizados, construir_pasaporte,
)
from fallas.models import ReporteFalla
from mantenimiento.services_access import authorized_branch_ids, can_access_mantenimiento, can_view_costs

READ_SHADOW_KEYS = frozenset({"erp.search_assets", "erp.get_asset_context", "erp.get_pending_maintenance"})


class StrictIntegerField(serializers.IntegerField):
    def to_internal_value(self, data):
        if type(data) is not int:
            self.fail("invalid")
        return super().to_internal_value(data)


class StrictStringField(serializers.CharField):
    def to_internal_value(self, data):
        if not isinstance(data, str):
            self.fail("invalid")
        return super().to_internal_value(data)


class StrictDateField(serializers.DateField):
    pattern = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"

    def __init__(self, **kwargs):
        super().__init__(input_formats=["%Y-%m-%d"], validators=[RegexValidator(self.pattern)], **kwargs)

    def to_internal_value(self, data):
        if not isinstance(data, str) or not re.fullmatch(self.pattern, data):
            self.fail("invalid", format="YYYY-MM-DD")
        return super().to_internal_value(data)


class StrictArguments(serializers.Serializer):
    def to_internal_value(self, data):
        if not isinstance(data, dict):
            raise serializers.ValidationError({"arguments": "arguments debe ser un objeto JSON."})
        unexpected = set(data) - set(self.fields)
        if unexpected:
            raise serializers.ValidationError({"arguments": "Claves no permitidas."})
        return super().to_internal_value(data)

    @classmethod
    def argument_schema(cls):
        schema = AutoSchema().map_serializer(cls())
        schema["additionalProperties"] = False
        return schema


class SearchAssetsArguments(StrictArguments):
    q = StrictStringField(required=False, allow_blank=True, max_length=180, default="")
    sucursal_id = StrictIntegerField(required=False, min_value=1, max_value=9223372036854775807)
    limit = StrictIntegerField(required=False, min_value=1, max_value=50, default=20)


class AssetContextArguments(StrictArguments):
    activo_id = StrictIntegerField(min_value=1, max_value=9223372036854775807)


class PendingMaintenanceArguments(StrictArguments):
    sucursal_id = StrictIntegerField(required=False, min_value=1, max_value=9223372036854775807)
    fecha_hasta = StrictDateField(required=False)
    limit = StrictIntegerField(required=False, min_value=1, max_value=50, default=20)


def json_dto(value):
    """Dates become ISO strings and Decimal remains exact, never a float."""
    return json.loads(json.dumps(value, cls=DjangoJSONEncoder))


def fresh_asset_user(user):
    if not user or not getattr(user, "is_authenticated", False) or not getattr(user, "pk", None):
        return None
    # A new instance also removes cached groups, explicit ACL and profile relations.
    return get_user_model().objects.filter(pk=user.pk, is_active=True).first()


def asset_branch_scope(user):
    if can_access_mantenimiento(user):
        return authorized_branch_ids(user)
    branch = _sucursal_operativa(user)
    return [branch.pk] if branch else []


def can_read_assets(user):
    if getattr(settings, "AI_GATEWAY_ASSETS_ENABLED", False) is not True:
        return False
    user = fresh_asset_user(user)
    from orquestacion.services.agent_pilot import is_pilot_participant
    return bool(is_pilot_participant(user) and asset_branch_scope(user) != [])


def _assets(user, arguments):
    if not can_read_assets(user):
        raise PermissionDenied("No tienes acceso a las lecturas de activos del ERP AI Gateway.")
    queryset = activos_autorizados(user).select_related("sucursal", "proveedor_compra")
    if "sucursal_id" in arguments:
        queryset = queryset.filter(sucursal_id=arguments["sucursal_id"])
    return queryset


def _asset_choice(asset):
    return {
        "id": asset.pk, "codigo": asset.codigo, "nombre": asset.nombre,
        "sucursal_id": asset.sucursal_id,
        "sucursal": asset.sucursal.nombre if asset.sucursal_id else None,
        "categoria": asset.categoria, "ubicacion": asset.ubicacion,
        "estado": asset.estado, "vigente": asset.activo,
    }


def _result(status, sources, arguments, payload):
    return json_dto({
        "status": status, "sources": sources, "filters": arguments,
        "as_of": timezone.now(), "timezone": settings.TIME_ZONE,
        "unit_of_analysis": "asset" if sources == ["activos.Activo"] else "maintenance_plan" if sources == ["activos.PlanMantenimiento"] else "asset_context",
        "data_is_authority": False,
        "payload": payload,
    })


def search_assets(user, arguments):
    queryset = _assets(user, arguments)
    if arguments["q"]:
        q = arguments["q"]
        queryset = queryset.filter(Q(codigo__icontains=q) | Q(nombre__icontains=q) | Q(numero_serie__icontains=q))
    rows = list(queryset.order_by("codigo", "id")[:arguments["limit"] + 1])
    items = [_asset_choice(asset) for asset in rows[:arguments["limit"]]]
    status = "no_data" if not rows else "ambiguous" if len(rows) > 1 else "ok"
    return _result(status, ["activos.Activo"], arguments, {
        "items": items, "returned": len(items), "truncated": len(rows) > arguments["limit"],
        "selection_required": len(rows) > 1,
    })


def _fields(row, keys):
    return {key: row[key] for key in keys if key in row}


def _order_dto(row, *, con_costos):
    if row is None:
        return None
    keys = ("id", "folio", "tipo", "estatus", "estatus_label", "fecha", "descripcion", "responsable")
    if con_costos:
        keys += ("costo_total", "numero_factura", "proveedor_servicio")
    return _fields(row, keys)


def get_asset_context(user, arguments):
    asset = _assets(user, arguments).filter(pk=arguments["activo_id"]).first()
    sources = ["activos.Activo", "fallas.ReporteFalla", "activos.OrdenMantenimiento", "activos.PlanMantenimiento"]
    if asset is None:
        return _result("no_data", sources, arguments, {})
    passport = construir_pasaporte(asset, user)
    # Reports retain their historical branch when the asset moves. Asset scope
    # alone must never disclose another branch's incident to a limited reader.
    reports = ReporteFalla.objects.filter(
        activo_relacionado=asset, estatus__in=FALLA_ESTATUS_ABIERTOS, duplicado_de__isnull=True,
    )
    branch_ids = asset_branch_scope(user)
    if branch_ids is not None:
        reports = reports.filter(sucursal_id__in=branch_ids)
    failures = list(reports.order_by("-fecha_reporte", "-id")[:MAX_EVENTOS + 1])
    con_costos = can_view_costs(user)
    payload = {
        "identidad": _fields(passport["identidad"], (
            "codigo", "nombre", "categoria", "sucursal", "ubicacion", "marca", "modelo",
            "numero_serie", "estado", "estado_label", "criticidad_label", "vigente",
        )),
        "ordenes_recientes": [_order_dto(row, con_costos=con_costos) for row in passport["ordenes_recientes"]],
        "ultimo_mantenimiento": _order_dto(passport["ultimo_mantenimiento"], con_costos=con_costos),
        "proximo_plan": _fields(passport["proximo_plan"], ("nombre", "tipo", "proxima_ejecucion", "responsable")) if passport["proximo_plan"] else None,
        "puede_ver_costos": con_costos,
    }
    payload["activo"] = _asset_choice(asset)
    payload["fallas_abiertas"] = [_fields(_falla_publica(f), ("id", "titulo", "prioridad", "prioridad_label", "estatus", "estatus_label", "fecha_reporte")) for f in failures[:MAX_EVENTOS]]
    payload["history"] = {
        "complete": False, "event_limit": MAX_EVENTOS,
        "failures_truncated": len(failures) > MAX_EVENTOS,
        "orders_truncated": OrdenMantenimiento.objects.filter(activo_ref=asset).count() > MAX_EVENTOS,
        "absence_of_plan_is_not_absence_of_service": True,
    }
    if con_costos:
        payload["costos"] = _fields(passport["costos"], ("adquisicion", "mantenimiento_total", "valor_reposicion"))
        for key in ("garantia_hasta", "proveedor_compra"):
            payload[key] = passport[key]
        # File paths/URLs and document contents are never included.
        payload["facturas"] = [{k: row[k] for k in ("folio_orden", "numero", "fecha")} for row in passport["facturas"]]
    return _result("ok", sources, arguments, payload)


def get_pending_maintenance(user, arguments):
    today = timezone.localdate()
    horizon = arguments.get("fecha_hasta", today + timedelta(days=30))
    plans = PlanMantenimiento.objects.filter(activo_ref__in=_assets(user, arguments)).select_related("activo_ref", "activo_ref__sucursal")
    active = plans.filter(activo=True, estatus=PlanMantenimiento.ESTATUS_ACTIVO)
    buckets = {
        "overdue": active.filter(proxima_ejecucion__lt=today),
        "upcoming": active.filter(proxima_ejecucion__gte=today, proxima_ejecucion__lte=horizon),
        "missing_schedule": active.filter(proxima_ejecucion__isnull=True),
        "inactive_paused": plans.filter(Q(activo=False) | Q(estatus=PlanMantenimiento.ESTATUS_PAUSADO)),
    }
    payload = {"today": today, "fecha_hasta": horizon, "per_bucket_limit": arguments["limit"], "truncated": {}, "absence_of_plan_is_not_absence_of_service": True}
    for name, queryset in buckets.items():
        rows = list(queryset.order_by("proxima_ejecucion", "id")[:arguments["limit"] + 1])
        payload[name] = [{
            "id": plan.pk, "activo": _asset_choice(plan.activo_ref),
            "nombre": plan.nombre, "tipo": plan.tipo, "estatus": plan.estatus,
            "vigente": plan.activo, "ultima_ejecucion": plan.ultima_ejecucion,
            "proxima_ejecucion": plan.proxima_ejecucion, "responsable": plan.responsable,
        } for plan in rows[:arguments["limit"]]]
        payload["truncated"][name] = len(rows) > arguments["limit"]
    return _result("ok" if any(payload[name] for name in buckets) else "no_data", ["activos.PlanMantenimiento"], arguments, payload)


ASSET_READERS = {
    "erp.search_assets": (SearchAssetsArguments, search_assets, "Buscar activos", "Devuelve opciones de activos autorizados; una coincidencia ambigua nunca selecciona un equipo."),
    "erp.get_asset_context": (AssetContextArguments, get_asset_context, "Consultar contexto de activo", "Ficha autorizada con hasta diez fallas abiertas y diez órdenes recientes; historial parcial y costos según permiso vigente."),
    "erp.get_pending_maintenance": (PendingMaintenanceArguments, get_pending_maintenance, "Consultar planes de mantenimiento", "Separa vencidos, próximos hasta fecha_hasta (30 días por defecto), sin fecha e inactivos/pausados; limit por grupo, máximo 50. Órdenes abiertas no son planes."),
}
