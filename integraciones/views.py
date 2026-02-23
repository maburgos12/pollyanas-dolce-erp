from __future__ import annotations

import csv
from datetime import timedelta

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Count, Q
from django.http import HttpResponse
from django.shortcuts import redirect, render
from django.utils import timezone

from core.access import can_view_audit
from core.audit import log_event
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
    integracion_point: dict,
    alertas_operativas: list[dict],
) -> HttpResponse:
    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = 'attachment; filename="integraciones_estado_operativo.csv"'
    writer = csv.writer(response)
    writer.writerow(["kpi", "value"])
    writer.writerow(["requests_24h", requests_24h])
    writer.writerow(["errors_24h", errors_24h])
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

    last_generated_api_key = request.session.pop("integraciones_last_api_key", "")
    clients = list(PublicApiClient.objects.order_by("nombre", "id"))

    filter_client_id = (request.GET.get("client") or "").strip()
    filter_status = (request.GET.get("status") or "all").strip().lower()
    filter_q = (request.GET.get("q") or "").strip()
    export_mode = (request.GET.get("export") or "").strip().lower()

    logs_qs = PublicApiAccessLog.objects.select_related("client")
    if filter_client_id.isdigit():
        logs_qs = logs_qs.filter(client_id=int(filter_client_id))
    if filter_status == "ok":
        logs_qs = logs_qs.filter(status_code__lt=400)
    elif filter_status == "error":
        logs_qs = logs_qs.filter(status_code__gte=400)
    if filter_q:
        logs_qs = logs_qs.filter(endpoint__icontains=filter_q)

    logs_qs = logs_qs.order_by("-created_at", "-id")
    if export_mode == "csv":
        return _export_logs_csv(logs_qs[:5000])
    logs = list(logs_qs[:120])

    since_24h = timezone.now() - timedelta(hours=24)
    top_clients_24h = list(
        PublicApiAccessLog.objects.filter(created_at__gte=since_24h)
        .values("client__nombre")
        .annotate(
            total=Count("id"),
            errores=Count("id", filter=Q(status_code__gte=400)),
        )
        .order_by("-total", "client__nombre")[:10]
    )

    requests_24h = PublicApiAccessLog.objects.filter(created_at__gte=since_24h).count()
    errors_24h = PublicApiAccessLog.objects.filter(created_at__gte=since_24h, status_code__gte=400).count()

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
                "cta_url": "/maestros/point-pendientes/",
            }
        )
    if recetas_pending_total:
        alertas_operativas.append(
            {
                "nivel": "warning",
                "titulo": "Líneas receta sin match",
                "detalle": f"{recetas_pending_total} líneas requieren homologación interna.",
                "cta_label": "Revisar matching",
                "cta_url": "/recetas/revisar-matching/",
            }
        )
    if not latest_run:
        alertas_operativas.append(
            {
                "nivel": "warning",
                "titulo": "Sync de almacén no ejecutado",
                "detalle": "No hay corridas de sincronización registradas.",
                "cta_label": "Ir a Carga Almacén",
                "cta_url": "/inventario/carga/",
            }
        )
    elif latest_run.started_at and latest_run.started_at < stale_limit:
        alertas_operativas.append(
            {
                "nivel": "warning",
                "titulo": "Sync de almacén desactualizado",
                "detalle": f"Último sync: {latest_run.started_at:%Y-%m-%d %H:%M}.",
                "cta_label": "Ir a Carga Almacén",
                "cta_url": "/inventario/carga/",
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
        "logs": logs,
        "last_generated_api_key": last_generated_api_key,
        "top_clients_24h": top_clients_24h,
        "requests_24h": requests_24h,
        "errors_24h": errors_24h,
        "filter_client_id": filter_client_id,
        "filter_status": filter_status,
        "filter_q": filter_q,
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
    }
    if export_mode == "health_csv":
        return _export_health_csv(
            requests_24h=requests_24h,
            errors_24h=errors_24h,
            integracion_point=context["integracion_point"],
            alertas_operativas=alertas_operativas,
        )
    return render(request, "integraciones/panel.html", context)
