from __future__ import annotations

import csv
import json
from datetime import date, timedelta

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Count, Max, Q
from django.db.models.functions import TruncDate
from django.http import HttpResponse
from django.urls import reverse
from django.shortcuts import redirect, render
from django.utils import timezone

from core.access import can_view_audit
from core.audit import log_event
from core.models import AuditLog
from inventario.models import AlmacenSyncRun
from maestros.models import Insumo, InsumoAlias, PointPendingMatch, Proveedor
from recetas.models import LineaReceta, Receta, RecetaCodigoPointAlias
from recetas.utils.normalizacion import normalizar_nombre

from .models import PublicApiAccessLog, PublicApiClient


def _export_logs_csv(logs_qs) -> HttpResponse:
    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = 'attachment; filename="integraciones_api_logs.csv"'
    writer = csv.writer(response)
    writer.writerow(["fecha", "cliente", "metodo", "endpoint", "status_code"])
    for row in logs_qs:
        writer.writerow(
            [
                row.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                row.client.nombre if row.client_id else "",
                row.method,
                row.endpoint,
                row.status_code,
            ]
        )
    return response


def _export_health_csv(
    requests_24h: int,
    errors_24h: int,
    requests_prev_24h: int,
    errors_prev_24h: int,
    requests_delta_pct: float,
    errors_delta_pct: float,
    integracion_point: dict,
    alertas_operativas: list[dict],
) -> HttpResponse:
    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = 'attachment; filename="integraciones_estado_operativo.csv"'
    writer = csv.writer(response)
    writer.writerow(["kpi", "value"])
    writer.writerow(["requests_24h", requests_24h])
    writer.writerow(["errors_24h", errors_24h])
    writer.writerow(["requests_prev_24h", requests_prev_24h])
    writer.writerow(["errors_prev_24h", errors_prev_24h])
    writer.writerow(["requests_delta_pct", requests_delta_pct])
    writer.writerow(["errors_delta_pct", errors_delta_pct])
    writer.writerow(["insumos_activos", integracion_point["insumos"]["activos"]])
    writer.writerow(["insumos_con_codigo_point", integracion_point["insumos"]["con_codigo_point"]])
    writer.writerow(["insumos_sin_codigo_point", integracion_point["insumos"]["sin_codigo_point"]])
    writer.writerow(["insumos_cobertura_pct", integracion_point["insumos"]["cobertura_pct"]])
    writer.writerow(["recetas_total", integracion_point["recetas"]["total"]])
    writer.writerow(["recetas_homologadas", integracion_point["recetas"]["homologadas"]])
    writer.writerow(["recetas_sin_homologar", integracion_point["recetas"]["sin_homologar"]])
    writer.writerow(["recetas_cobertura_pct", integracion_point["recetas"]["cobertura_pct"]])
    writer.writerow(["point_pending_total", integracion_point["point_pending"]["total"]])
    writer.writerow(["point_pending_insumo", integracion_point["point_pending"]["por_tipo"].get(PointPendingMatch.TIPO_INSUMO, 0)])
    writer.writerow(["point_pending_producto", integracion_point["point_pending"]["por_tipo"].get(PointPendingMatch.TIPO_PRODUCTO, 0)])
    writer.writerow(
        ["point_pending_proveedor", integracion_point["point_pending"]["por_tipo"].get(PointPendingMatch.TIPO_PROVEEDOR, 0)]
    )
    writer.writerow(["recetas_pending_match", integracion_point["inventario"]["recetas_pending_match"]])
    writer.writerow(["almacen_pending_preview", integracion_point["inventario"]["almacen_pending_preview"]])
    writer.writerow([])
    writer.writerow(["alerta_nivel", "alerta_titulo", "alerta_detalle"])
    for alerta in alertas_operativas:
        writer.writerow([alerta.get("nivel", ""), alerta.get("titulo", ""), alerta.get("detalle", "")])
    return response


def _export_errors_csv(
    errors_by_endpoint_24h: list[dict],
    errors_by_client_24h: list[dict],
) -> HttpResponse:
    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = 'attachment; filename="integraciones_errors_24h.csv"'
    writer = csv.writer(response)
    writer.writerow(["tipo", "clave", "total_errores_24h", "clientes_distintos", "ultimo_error"])
    for row in errors_by_endpoint_24h:
        writer.writerow(
            [
                "endpoint",
                row.get("endpoint", "") or "-",
                row.get("total", 0),
                row.get("clientes", 0),
                row.get("last_at").strftime("%Y-%m-%d %H:%M:%S") if row.get("last_at") else "",
            ]
        )
    writer.writerow([])
    for row in errors_by_client_24h:
        writer.writerow(
            [
                "cliente",
                row.get("client__nombre", "") or "-",
                row.get("total", 0),
                "",
                row.get("last_at").strftime("%Y-%m-%d %H:%M:%S") if row.get("last_at") else "",
            ]
        )
    return response


def _export_audit_csv(audit_rows) -> HttpResponse:
    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = 'attachment; filename="integraciones_audit_acciones.csv"'
    writer = csv.writer(response)
    writer.writerow(["fecha", "usuario", "accion", "modelo", "object_id", "payload"])
    for row in audit_rows:
        writer.writerow(
            [
                row.timestamp.strftime("%Y-%m-%d %H:%M:%S") if row.timestamp else "",
                row.user.username if getattr(row, "user", None) else "",
                row.action,
                row.model,
                row.object_id,
                json.dumps(row.payload or {}, ensure_ascii=False),
            ]
        )
    return response


def _build_api_daily_trend(days: int = 7) -> list[dict]:
    days = max(1, min(int(days or 7), 31))
    today = timezone.localdate()
    start_date = today - timedelta(days=days - 1)
    raw_rows = list(
        PublicApiAccessLog.objects.filter(created_at__date__gte=start_date)
        .annotate(day=TruncDate("created_at"))
        .values("day")
        .annotate(
            total=Count("id"),
            errors=Count("id", filter=Q(status_code__gte=400)),
        )
        .order_by("day")
    )
    by_day = {row["day"]: row for row in raw_rows}
    trend = []
    for day_index in range(days):
        day = start_date + timedelta(days=day_index)
        row = by_day.get(day, {})
        total = int(row.get("total") or 0)
        errors = int(row.get("errors") or 0)
        trend.append(
            {
                "day": day,
                "total": total,
                "errors": errors,
                "error_rate_pct": round((errors * 100.0 / total), 2) if total else 0.0,
            }
        )
    return trend


def _export_trend_csv(api_daily_trend: list[dict]) -> HttpResponse:
    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = 'attachment; filename="integraciones_api_tendencia_7d.csv"'
    writer = csv.writer(response)
    writer.writerow(["fecha", "requests", "errors", "error_rate_pct"])
    for row in api_daily_trend:
        writer.writerow(
            [
                row.get("day"),
                row.get("total", 0),
                row.get("errors", 0),
                row.get("error_rate_pct", 0),
            ]
        )
    return response


def _build_client_usage_maps(client_ids: list[int]) -> dict[str, dict[int, int]]:
    if not client_ids:
        return {"24h": {}, "7d": {}, "30d": {}}
    now = timezone.now()
    windows = {
        "24h": now - timedelta(hours=24),
        "7d": now - timedelta(days=7),
        "30d": now - timedelta(days=30),
    }
    result: dict[str, dict[int, int]] = {}
    for key, since in windows.items():
        rows = (
            PublicApiAccessLog.objects.filter(client_id__in=client_ids, created_at__gte=since)
            .values("client_id")
            .annotate(total=Count("id"))
        )
        result[key] = {int(row["client_id"]): int(row["total"] or 0) for row in rows}
    return result


def _export_clients_csv(client_metrics: list[dict]) -> HttpResponse:
    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = 'attachment; filename="integraciones_clientes_api.csv"'
    writer = csv.writer(response)
    writer.writerow(["cliente", "activo", "prefijo", "requests_24h", "requests_7d", "requests_30d", "last_used_at"])
    for row in client_metrics:
        client = row["client"]
        writer.writerow(
            [
                client.nombre,
                "1" if client.activo else "0",
                client.clave_prefijo,
                row.get("requests_24h", 0),
                row.get("requests_7d", 0),
                row.get("requests_30d", 0),
                client.last_used_at.strftime("%Y-%m-%d %H:%M:%S") if client.last_used_at else "",
            ]
        )
    return response


def _to_float(raw, default=0.0) -> float:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def _to_int(raw, default=0) -> int:
    try:
        return int(raw)
    except (TypeError, ValueError):
        return int(default)


def _pct_change(current: int, previous: int) -> float:
    current_i = int(current or 0)
    previous_i = int(previous or 0)
    if previous_i <= 0:
        return 100.0 if current_i > 0 else 0.0
    return round(((current_i - previous_i) * 100.0 / previous_i), 2)


def _parse_iso_date(raw: str | None) -> date | None:
    if not raw:
        return None
    value = str(raw).strip()
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _resolve_point_pending_insumos(min_score: float, limit: int, create_aliases: bool) -> dict:
    queryset = PointPendingMatch.objects.filter(tipo=PointPendingMatch.TIPO_INSUMO).order_by("-fuzzy_score", "point_nombre", "id")
    selected = list(queryset[:limit])

    resolved = 0
    conflicts = 0
    skipped_low_score = 0
    skipped_no_suggestion = 0
    skipped_no_target = 0
    aliases_created = 0

    for pending in selected:
        if float(pending.fuzzy_score or 0.0) < min_score:
            skipped_low_score += 1
            continue

        sugerencia_norm = normalizar_nombre(pending.fuzzy_sugerencia or "")
        if not sugerencia_norm:
            skipped_no_suggestion += 1
            continue

        target = Insumo.objects.filter(
            activo=True,
            nombre_normalizado=sugerencia_norm,
        ).only("id", "codigo_point", "nombre_point", "nombre_normalizado").first()
        if not target:
            skipped_no_target += 1
            continue

        point_code = (pending.point_codigo or "").strip()
        if point_code and target.codigo_point and target.codigo_point != point_code:
            conflicts += 1
            continue

        changed_fields = []
        if point_code and target.codigo_point != point_code:
            target.codigo_point = point_code
            changed_fields.append("codigo_point")
        if target.nombre_point != pending.point_nombre:
            target.nombre_point = pending.point_nombre
            changed_fields.append("nombre_point")
        if changed_fields:
            target.save(update_fields=changed_fields)

        if create_aliases:
            alias_norm = normalizar_nombre(pending.point_nombre or "")
            if alias_norm and alias_norm != target.nombre_normalizado:
                alias, was_created = InsumoAlias.objects.get_or_create(
                    nombre_normalizado=alias_norm,
                    defaults={"nombre": (pending.point_nombre or "")[:250], "insumo": target},
                )
                if not was_created and alias.insumo_id != target.id:
                    alias.insumo = target
                    alias.save(update_fields=["insumo"])
                if was_created:
                    aliases_created += 1

        pending.delete()
        resolved += 1

    return {
        "seleccionados": len(selected),
        "resueltos": resolved,
        "conflictos": conflicts,
        "score_bajo": skipped_low_score,
        "sin_sugerencia": skipped_no_suggestion,
        "sin_objetivo": skipped_no_target,
        "aliases_creados": aliases_created,
        "pendientes_restantes": PointPendingMatch.objects.filter(tipo=PointPendingMatch.TIPO_INSUMO).count(),
    }


def _deactivate_idle_api_clients(idle_days: int, limit: int) -> dict:
    idle_days = max(1, min(int(idle_days or 30), 365))
    limit = max(1, min(int(limit or 100), 500))
    cutoff = timezone.now() - timedelta(days=idle_days)
    recent_client_ids = set(
        PublicApiAccessLog.objects.filter(created_at__gte=cutoff)
        .values_list("client_id", flat=True)
        .distinct()
    )
    candidates = list(
        PublicApiClient.objects.filter(activo=True)
        .exclude(id__in=recent_client_ids)
        .order_by("id")[:limit]
    )
    candidate_ids = [int(client.id) for client in candidates]
    updated = 0
    if candidate_ids:
        updated = PublicApiClient.objects.filter(id__in=candidate_ids, activo=True).update(
            activo=False,
            updated_at=timezone.now(),
        )
    return {
        "idle_days": idle_days,
        "limit": limit,
        "candidates": len(candidates),
        "deactivated": int(updated),
        "cutoff": cutoff.isoformat(),
    }


def _purge_api_logs(retain_days: int, max_delete: int) -> dict:
    retain_days = max(1, min(int(retain_days or 90), 3650))
    max_delete = max(1, min(int(max_delete or 5000), 50000))
    cutoff = timezone.now() - timedelta(days=retain_days)
    candidates_qs = PublicApiAccessLog.objects.filter(created_at__lt=cutoff).order_by("id")
    total_candidates = candidates_qs.count()
    delete_ids = list(candidates_qs.values_list("id", flat=True)[:max_delete])
    deleted = 0
    if delete_ids:
        deleted, _detail = PublicApiAccessLog.objects.filter(id__in=delete_ids).delete()
    return {
        "retain_days": retain_days,
        "max_delete": max_delete,
        "cutoff": cutoff.isoformat(),
        "candidates": int(total_candidates),
        "deleted": int(deleted),
        "remaining_candidates": max(int(total_candidates) - int(deleted), 0),
    }


@login_required
def panel(request):
    if not can_view_audit(request.user):
        raise PermissionDenied("No tienes permisos para gestionar integraciones.")

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        if action == "resolve_point_sugerencias_insumos":
            min_score = max(0.0, min(100.0, _to_float(request.POST.get("auto_score_min"), 90.0)))
            limit = max(1, min(2000, _to_int(request.POST.get("auto_limit"), 250)))
            create_aliases = request.POST.get("create_aliases") == "on"
            summary = _resolve_point_pending_insumos(
                min_score=min_score,
                limit=limit,
                create_aliases=create_aliases,
            )
            log_event(
                request.user,
                "AUTO_RESOLVE_POINT_INSUMOS",
                "maestros.PointPendingMatch",
                "",
                payload={
                    "score_min": min_score,
                    "limit": limit,
                    "create_aliases": create_aliases,
                    **summary,
                },
            )
            messages.success(
                request,
                (
                    "Auto-resolución de pendientes Point (insumos): "
                    f"{summary['resueltos']} resueltos de {summary['seleccionados']} evaluados. "
                    f"Aliases creados: {summary['aliases_creados']}."
                ),
            )
            if summary["conflictos"] or summary["score_bajo"] or summary["sin_sugerencia"] or summary["sin_objetivo"]:
                messages.warning(
                    request,
                    (
                        "No procesados: "
                        f"conflicto código Point {summary['conflictos']}, "
                        f"score bajo {summary['score_bajo']}, "
                        f"sin sugerencia {summary['sin_sugerencia']}, "
                        f"sugerencia sin insumo activo {summary['sin_objetivo']}."
                    ),
                )
            return redirect("integraciones:panel")

        if action == "create":
            nombre = (request.POST.get("nombre") or "").strip()
            descripcion = (request.POST.get("descripcion") or "").strip()
            if not nombre:
                messages.error(request, "El nombre del cliente es obligatorio.")
                return redirect("integraciones:panel")
            client, raw_key = PublicApiClient.create_with_generated_key(
                nombre=nombre,
                descripcion=descripcion,
                created_by=request.user,
            )
            log_event(
                request.user,
                "CREATE",
                "integraciones.PublicApiClient",
                str(client.id),
                payload={
                    "nombre": client.nombre,
                    "clave_prefijo": client.clave_prefijo,
                    "activo": client.activo,
                },
            )
            request.session["integraciones_last_api_key"] = raw_key
            messages.success(request, f"Cliente creado: {client.nombre}")
            return redirect("integraciones:panel")

        if action == "rotate":
            client_id = (request.POST.get("client_id") or "").strip()
            client = PublicApiClient.objects.filter(id=client_id).first() if client_id.isdigit() else None
            if not client:
                messages.error(request, "Cliente no encontrado para rotación.")
                return redirect("integraciones:panel")
            raw_key = client.rotate_key()
            log_event(
                request.user,
                "ROTATE_KEY",
                "integraciones.PublicApiClient",
                str(client.id),
                payload={"nombre": client.nombre, "clave_prefijo": client.clave_prefijo},
            )
            request.session["integraciones_last_api_key"] = raw_key
            messages.success(request, f"API key rotada para: {client.nombre}")
            return redirect("integraciones:panel")

        if action == "toggle":
            client_id = (request.POST.get("client_id") or "").strip()
            client = PublicApiClient.objects.filter(id=client_id).first() if client_id.isdigit() else None
            if not client:
                messages.error(request, "Cliente no encontrado.")
                return redirect("integraciones:panel")
            old_status = client.activo
            client.activo = not client.activo
            client.save(update_fields=["activo", "updated_at"])
            log_event(
                request.user,
                "TOGGLE_ACTIVE",
                "integraciones.PublicApiClient",
                str(client.id),
                payload={"nombre": client.nombre, "old_activo": old_status, "new_activo": client.activo},
            )
            label = "activado" if client.activo else "desactivado"
            messages.success(request, f"Cliente {label}: {client.nombre}")
            return redirect("integraciones:panel")

        if action == "deactivate_idle_clients":
            idle_days = max(1, min(365, _to_int(request.POST.get("idle_days"), 30)))
            limit = max(1, min(500, _to_int(request.POST.get("idle_limit"), 100)))
            summary = _deactivate_idle_api_clients(idle_days=idle_days, limit=limit)
            log_event(
                request.user,
                "DEACTIVATE_IDLE_API_CLIENTS",
                "integraciones.PublicApiClient",
                "",
                payload=summary,
            )
            messages.success(
                request,
                (
                    "Limpieza API ejecutada: "
                    f"{summary['deactivated']} cliente(s) desactivados "
                    f"de {summary['candidates']} candidatos (ventana {summary['idle_days']} días)."
                ),
            )
            return redirect("integraciones:panel")

        if action == "purge_api_logs":
            retain_days = max(1, min(3650, _to_int(request.POST.get("retain_days"), 90)))
            max_delete = max(1, min(50000, _to_int(request.POST.get("max_delete"), 5000)))
            summary = _purge_api_logs(retain_days=retain_days, max_delete=max_delete)
            log_event(
                request.user,
                "PURGE_API_LOGS",
                "integraciones.PublicApiAccessLog",
                "",
                payload=summary,
            )
            messages.success(
                request,
                (
                    "Limpieza de logs API completada: "
                    f"{summary['deleted']} eliminados de {summary['candidates']} candidatos "
                    f"(retención {summary['retain_days']} días)."
                ),
            )
            if summary["remaining_candidates"] > 0:
                messages.warning(
                    request,
                    f"Quedaron {summary['remaining_candidates']} logs por encima del límite de borrado."
                )
            return redirect("integraciones:panel")

    last_generated_api_key = request.session.pop("integraciones_last_api_key", "")
    clients = list(PublicApiClient.objects.order_by("nombre", "id"))
    client_ids = [int(client.id) for client in clients]
    client_usage_maps = _build_client_usage_maps(client_ids)
    client_metrics = []
    for client in clients:
        requests_24h_client = int(client_usage_maps["24h"].get(client.id, 0))
        requests_7d_client = int(client_usage_maps["7d"].get(client.id, 0))
        requests_30d_client = int(client_usage_maps["30d"].get(client.id, 0))
        client_metrics.append(
            {
                "client": client,
                "requests_24h": requests_24h_client,
                "requests_7d": requests_7d_client,
                "requests_30d": requests_30d_client,
            }
        )
    clients_inactive = sum(1 for client in clients if not client.activo)
    clients_unused_30d = sum(1 for row in client_metrics if row["requests_30d"] == 0)
    total_api_logs = PublicApiAccessLog.objects.count()
    oldest_api_log = PublicApiAccessLog.objects.order_by("created_at").values_list("created_at", flat=True).first()

    filter_client_id = (request.GET.get("client") or "").strip()
    filter_status = (request.GET.get("status") or "all").strip().lower()
    filter_q = (request.GET.get("q") or "").strip()
    filter_from = _parse_iso_date(request.GET.get("from"))
    filter_to = _parse_iso_date(request.GET.get("to"))
    export_mode = (request.GET.get("export") or "").strip().lower()
    if export_mode == "clients_csv":
        return _export_clients_csv(client_metrics)

    logs_qs = PublicApiAccessLog.objects.select_related("client")
    if filter_client_id.isdigit():
        logs_qs = logs_qs.filter(client_id=int(filter_client_id))
    if filter_status == "ok":
        logs_qs = logs_qs.filter(status_code__lt=400)
    elif filter_status == "error":
        logs_qs = logs_qs.filter(status_code__gte=400)
    if filter_q:
        logs_qs = logs_qs.filter(endpoint__icontains=filter_q)
    if filter_from:
        logs_qs = logs_qs.filter(created_at__date__gte=filter_from)
    if filter_to:
        logs_qs = logs_qs.filter(created_at__date__lte=filter_to)

    logs_qs = logs_qs.order_by("-created_at", "-id")
    if export_mode == "csv":
        return _export_logs_csv(logs_qs[:5000])
    logs = list(logs_qs[:120])

    audit_qs = (
        AuditLog.objects.select_related("user")
        .filter(
            Q(model="integraciones.PublicApiClient")
            | Q(action="AUTO_RESOLVE_POINT_INSUMOS")
            | Q(model="maestros.PointPendingMatch")
        )
        .order_by("-timestamp", "-id")
    )
    if export_mode == "audit_csv":
        return _export_audit_csv(audit_qs[:5000])
    audit_rows = list(audit_qs[:60])

    now_dt = timezone.now()
    since_24h = now_dt - timedelta(hours=24)
    since_48h = now_dt - timedelta(hours=48)
    top_clients_24h = list(
        PublicApiAccessLog.objects.filter(created_at__gte=since_24h)
        .values("client__nombre")
        .annotate(
            total=Count("id"),
            errores=Count("id", filter=Q(status_code__gte=400)),
        )
        .order_by("-total", "client__nombre")[:10]
    )

    current_window_qs = PublicApiAccessLog.objects.filter(created_at__gte=since_24h)
    previous_window_qs = PublicApiAccessLog.objects.filter(created_at__gte=since_48h, created_at__lt=since_24h)
    requests_24h = current_window_qs.count()
    errors_24h = current_window_qs.filter(status_code__gte=400).count()
    requests_prev_24h = previous_window_qs.count()
    errors_prev_24h = previous_window_qs.filter(status_code__gte=400).count()
    requests_delta_pct = _pct_change(requests_24h, requests_prev_24h)
    errors_delta_pct = _pct_change(errors_24h, errors_prev_24h)
    errors_by_endpoint_24h = list(
        PublicApiAccessLog.objects.filter(created_at__gte=since_24h, status_code__gte=400)
        .values("endpoint")
        .annotate(
            total=Count("id"),
            clientes=Count("client_id", distinct=True),
            last_at=Max("created_at"),
        )
        .order_by("-total", "endpoint")[:12]
    )
    errors_by_client_24h = list(
        PublicApiAccessLog.objects.filter(created_at__gte=since_24h, status_code__gte=400)
        .values("client__nombre")
        .annotate(
            total=Count("id"),
            last_at=Max("created_at"),
        )
        .order_by("-total", "client__nombre")[:12]
    )
    api_daily_trend = _build_api_daily_trend(days=7)
    api_7d_requests = sum(int(row.get("total") or 0) for row in api_daily_trend)
    api_7d_errors = sum(int(row.get("errors") or 0) for row in api_daily_trend)
    api_7d_error_rate = round((api_7d_errors * 100.0 / api_7d_requests), 2) if api_7d_requests else 0.0

    insumos_activos_qs = Insumo.objects.filter(activo=True)
    insumos_activos = insumos_activos_qs.count()
    insumos_con_codigo = insumos_activos_qs.exclude(Q(codigo_point="") | Q(codigo_point__isnull=True)).count()
    insumos_sin_codigo = max(insumos_activos - insumos_con_codigo, 0)
    insumos_cobertura = round((insumos_con_codigo * 100.0 / insumos_activos), 2) if insumos_activos else 100.0

    recetas_total = Receta.objects.count()
    receta_ids_primary = set(
        Receta.objects.exclude(Q(codigo_point="") | Q(codigo_point__isnull=True)).values_list("id", flat=True)
    )
    receta_ids_alias = set(RecetaCodigoPointAlias.objects.filter(activo=True).values_list("receta_id", flat=True))
    recetas_homologadas = len(receta_ids_primary.union(receta_ids_alias))
    recetas_sin_homologar = max(recetas_total - recetas_homologadas, 0)
    recetas_cobertura = round((recetas_homologadas * 100.0 / recetas_total), 2) if recetas_total else 100.0

    point_pending_by_tipo = {
        row["tipo"]: row["count"]
        for row in (
            PointPendingMatch.objects.values("tipo")
            .annotate(count=Count("id"))
            .order_by("tipo")
        )
    }
    point_pending_total = sum(point_pending_by_tipo.values())
    point_pending_recent = list(
        PointPendingMatch.objects.order_by("-actualizado_en", "-id")[:12]
    )

    recetas_pending_qs = (
        LineaReceta.objects.filter(insumo__isnull=True)
        .filter(match_status__in=[LineaReceta.STATUS_NEEDS_REVIEW, LineaReceta.STATUS_REJECTED])
        .select_related("receta")
        .order_by("-match_score", "receta__nombre", "posicion")
    )
    recetas_pending_total = recetas_pending_qs.count()
    recetas_pending_recent = list(recetas_pending_qs[:12])

    proveedores_activos = Proveedor.objects.filter(activo=True).count()

    latest_run = AlmacenSyncRun.objects.only("id", "started_at", "pending_preview").order_by("-started_at").first()
    almacen_pending_count = len((latest_run.pending_preview or [])) if latest_run else 0
    almacen_pending_preview = (latest_run.pending_preview or [])[:12] if latest_run else []
    stale_limit = timezone.now() - timedelta(hours=24)

    alertas_operativas = []
    if errors_24h:
        alertas_operativas.append(
            {
                "nivel": "danger",
                "titulo": "Errores API en últimas 24h",
                "detalle": f"{errors_24h} requests con status >= 400.",
                "cta_label": "Ver log API",
                "cta_url": "#log-api",
            }
        )
    if errors_24h >= 5 and errors_delta_pct >= 50:
        alertas_operativas.append(
            {
                "nivel": "danger",
                "titulo": "Spike de errores API (24h)",
                "detalle": (
                    f"Errores 24h: {errors_24h} vs {errors_prev_24h} previos "
                    f"({errors_delta_pct:+.2f}%)."
                ),
                "cta_label": "Ver log API",
                "cta_url": "#log-api",
            }
        )
    if point_pending_total:
        alertas_operativas.append(
            {
                "nivel": "warning",
                "titulo": "Pendientes Point abiertos",
                "detalle": (
                    f"Total {point_pending_total}. "
                    f"Insumos {point_pending_by_tipo.get(PointPendingMatch.TIPO_INSUMO, 0)}, "
                    f"productos {point_pending_by_tipo.get(PointPendingMatch.TIPO_PRODUCTO, 0)}, "
                    f"proveedores {point_pending_by_tipo.get(PointPendingMatch.TIPO_PROVEEDOR, 0)}."
                ),
                "cta_label": "Resolver en Maestros",
                "cta_url": reverse("maestros:point_pending_review"),
            }
        )
    if clients_unused_30d:
        alertas_operativas.append(
            {
                "nivel": "warning",
                "titulo": "Clientes API sin uso (30 días)",
                "detalle": f"{clients_unused_30d} cliente(s) no registran requests en 30 días.",
                "cta_label": "Revisar clientes API",
                "cta_url": "#clientes-api",
            }
        )
    if clients_inactive:
        alertas_operativas.append(
            {
                "nivel": "warning",
                "titulo": "Clientes API inactivos",
                "detalle": f"{clients_inactive} cliente(s) están desactivados.",
                "cta_label": "Revisar clientes API",
                "cta_url": "#clientes-api",
            }
        )
    if recetas_pending_total:
        alertas_operativas.append(
            {
                "nivel": "warning",
                "titulo": "Líneas receta sin match",
                "detalle": f"{recetas_pending_total} líneas requieren homologación interna.",
                "cta_label": "Revisar matching",
                "cta_url": reverse("recetas:matching_pendientes"),
            }
        )
    if not latest_run:
        alertas_operativas.append(
            {
                "nivel": "warning",
                "titulo": "Sync de almacén no ejecutado",
                "detalle": "No hay corridas de sincronización registradas.",
                "cta_label": "Ir a Carga Almacén",
                "cta_url": reverse("inventario:carga_almacen"),
            }
        )
    elif latest_run.started_at and latest_run.started_at < stale_limit:
        alertas_operativas.append(
            {
                "nivel": "warning",
                "titulo": "Sync de almacén desactualizado",
                "detalle": f"Último sync: {latest_run.started_at:%Y-%m-%d %H:%M}.",
                "cta_label": "Ir a Carga Almacén",
                "cta_url": reverse("inventario:carga_almacen"),
            }
        )

    if not alertas_operativas:
        alertas_operativas.append(
            {
                "nivel": "ok",
                "titulo": "Operación estable",
                "detalle": "Sin alertas críticas en integración, match y sincronización.",
                "cta_label": "",
                "cta_url": "",
            }
        )

    context = {
        "clients": clients,
        "client_metrics": client_metrics,
        "clients_inactive": clients_inactive,
        "clients_unused_30d": clients_unused_30d,
        "total_api_logs": total_api_logs,
        "oldest_api_log": oldest_api_log,
        "logs": logs,
        "last_generated_api_key": last_generated_api_key,
        "top_clients_24h": top_clients_24h,
        "requests_24h": requests_24h,
        "errors_24h": errors_24h,
        "requests_prev_24h": requests_prev_24h,
        "errors_prev_24h": errors_prev_24h,
        "requests_delta_pct": requests_delta_pct,
        "errors_delta_pct": errors_delta_pct,
        "filter_client_id": filter_client_id,
        "filter_status": filter_status,
        "filter_q": filter_q,
        "filter_from": filter_from.isoformat() if filter_from else "",
        "filter_to": filter_to.isoformat() if filter_to else "",
        "integracion_point": {
            "insumos": {
                "activos": insumos_activos,
                "con_codigo_point": insumos_con_codigo,
                "sin_codigo_point": insumos_sin_codigo,
                "cobertura_pct": insumos_cobertura,
            },
            "recetas": {
                "total": recetas_total,
                "homologadas": recetas_homologadas,
                "sin_homologar": recetas_sin_homologar,
                "cobertura_pct": recetas_cobertura,
            },
            "proveedores": {"activos": proveedores_activos},
            "point_pending": {
                "total": point_pending_total,
                "por_tipo": point_pending_by_tipo,
            },
            "inventario": {
                "almacen_pending_preview": almacen_pending_count,
                "almacen_latest_run_id": latest_run.id if latest_run else None,
                "almacen_latest_run_at": latest_run.started_at if latest_run else None,
                "recetas_pending_match": recetas_pending_total,
            },
        },
        "point_pending_recent": point_pending_recent,
        "recetas_pending_recent": recetas_pending_recent,
        "almacen_pending_preview": almacen_pending_preview,
        "point_pending_insumo": int(point_pending_by_tipo.get(PointPendingMatch.TIPO_INSUMO, 0)),
        "point_pending_producto": int(point_pending_by_tipo.get(PointPendingMatch.TIPO_PRODUCTO, 0)),
        "point_pending_proveedor": int(point_pending_by_tipo.get(PointPendingMatch.TIPO_PROVEEDOR, 0)),
        "alertas_operativas": alertas_operativas,
        "errors_by_endpoint_24h": errors_by_endpoint_24h,
        "errors_by_client_24h": errors_by_client_24h,
        "api_daily_trend": api_daily_trend,
        "api_7d_requests": api_7d_requests,
        "api_7d_errors": api_7d_errors,
        "api_7d_error_rate": api_7d_error_rate,
        "audit_rows": audit_rows,
    }
    if export_mode == "health_csv":
        return _export_health_csv(
            requests_24h=requests_24h,
            errors_24h=errors_24h,
            requests_prev_24h=requests_prev_24h,
            errors_prev_24h=errors_prev_24h,
            requests_delta_pct=requests_delta_pct,
            errors_delta_pct=errors_delta_pct,
            integracion_point=context["integracion_point"],
            alertas_operativas=alertas_operativas,
        )
    if export_mode == "errors_csv":
        return _export_errors_csv(
            errors_by_endpoint_24h=errors_by_endpoint_24h,
            errors_by_client_24h=errors_by_client_24h,
        )
    if export_mode == "trend_csv":
        return _export_trend_csv(api_daily_trend=api_daily_trend)
    return render(request, "integraciones/panel.html", context)
