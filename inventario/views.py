import csv
import os
import subprocess
import sys
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from io import BytesIO, StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlencode

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.core.files.uploadedfile import UploadedFile
from django.core.exceptions import PermissionDenied
from django.db.models import Q
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils import timezone
from openpyxl import Workbook, load_workbook

from core.access import can_manage_inventario, can_view_inventario
from core.audit import log_event
from maestros.models import CostoInsumo, Insumo, InsumoAlias, PointPendingMatch
from recetas.models import LineaReceta
from recetas.utils.matching import clasificar_match, match_insumo
from recetas.utils.normalizacion import normalizar_nombre
from inventario.utils.almacen_import import (
    ENTRADAS_FILE,
    INVENTARIO_FILE,
    MERMA_FILE,
    SALIDAS_FILE,
    import_folder,
)
from inventario.utils.google_drive_sync import sync_almacen_from_drive
from inventario.utils.sync_logging import log_sync_run
from inventario.utils.reorder import FORMULA_EXCEL_LEGACY, FORMULA_LEADTIME_PLUS_SAFETY, calcular_punto_reorden

from .models import AjusteInventario, AlmacenSyncRun, ExistenciaInsumo, InventarioConfig, MovimientoInventario


SOURCE_TO_FILENAME = {
    "inventario": INVENTARIO_FILE,
    "entradas": ENTRADAS_FILE,
    "salidas": SALIDAS_FILE,
    "merma": MERMA_FILE,
}

FILENAME_TO_SOURCE = {v: k for k, v in SOURCE_TO_FILENAME.items()}


def _map_alias_import_header(raw: str) -> str:
    h = normalizar_nombre(raw)
    if h in {
        "alias",
        "nombre",
        "nombre origen",
        "nombre_origen",
        "origen",
        "insumo origen",
        "insumo_origen",
        "nombre almacen",
        "nombre_almacen",
        "point nombre",
        "point_nombre",
    }:
        return "alias"
    if h in {
        "insumo",
        "insumo oficial",
        "insumo_oficial",
        "insumo destino",
        "insumo_destino",
        "canonico",
        "oficial",
        "insumo erp",
        "insumo_erp",
    }:
        return "insumo"
    return h


def _read_alias_import_rows(uploaded: UploadedFile) -> list[dict]:
    ext = Path(uploaded.name or "").suffix.lower()
    rows: list[dict] = []

    if ext in {".xlsx", ".xlsm"}:
        uploaded.seek(0)
        wb = load_workbook(uploaded, read_only=True, data_only=True)
        ws = wb.active
        values = list(ws.values)
        if not values:
            return []
        headers = [_map_alias_import_header(str(h or "")) for h in values[0]]
        for raw in values[1:]:
            row = {}
            for idx, header in enumerate(headers):
                if not header:
                    continue
                row[header] = raw[idx] if idx < len(raw) else None
            rows.append(row)
        return rows

    if ext == ".csv":
        uploaded.seek(0)
        content = uploaded.read().decode("utf-8-sig", errors="ignore")
        reader = csv.DictReader(StringIO(content))
        for raw in reader:
            row = {}
            for k, v in raw.items():
                if not k:
                    continue
                row[_map_alias_import_header(k)] = v
            rows.append(row)
        return rows

    raise ValueError("Formato no soportado. Usa .xlsx, .xlsm o .csv.")


def _to_decimal(value: str, default: str = "0") -> Decimal:
    try:
        return Decimal(value or default)
    except (InvalidOperation, TypeError):
        return Decimal(default)


def _inventario_reorder_max_diff_pct() -> Decimal:
    default_pct = _to_decimal(str(getattr(settings, "INVENTARIO_REORDER_MAX_DIFF_PCT", 10)), "10")
    return InventarioConfig.get_solo(default_pct=default_pct).reorder_max_diff_pct


def _apply_movimiento(movimiento: MovimientoInventario) -> None:
    existencia, _ = ExistenciaInsumo.objects.get_or_create(insumo=movimiento.insumo)
    if movimiento.tipo == MovimientoInventario.TIPO_ENTRADA:
        existencia.stock_actual += movimiento.cantidad
    else:
        existencia.stock_actual -= movimiento.cantidad
    existencia.actualizado_en = timezone.now()
    existencia.save()


def _classify_upload(filename: str) -> tuple[str | None, str | None]:
    if not filename:
        return None, None
    original = filename.strip()
    normalized = normalizar_nombre(Path(original).name)

    for expected_name, source in FILENAME_TO_SOURCE.items():
        if normalized == normalizar_nombre(expected_name):
            return source, expected_name

    if "inventario" in normalized and "almacen" in normalized:
        return "inventario", INVENTARIO_FILE
    if "entradas" in normalized and "almacen" in normalized:
        return "entradas", ENTRADAS_FILE
    if "salidas" in normalized and "almacen" in normalized:
        return "salidas", SALIDAS_FILE
    if "merma" in normalized and "almacen" in normalized:
        return "merma", MERMA_FILE
    return None, None


def _save_uploaded_file(target_dir: str, target_name: str, uploaded: UploadedFile) -> None:
    filepath = Path(target_dir) / target_name
    with filepath.open("wb") as out:
        for chunk in uploaded.chunks():
            out.write(chunk)


def _upsert_alias(alias_name: str, insumo: Insumo) -> tuple[bool, str, str]:
    alias_norm = normalizar_nombre(alias_name)
    if not alias_norm:
        return False, "", "El nombre origen no es válido."

    obj, created = InsumoAlias.objects.get_or_create(
        nombre_normalizado=alias_norm,
        defaults={
            "nombre": alias_name[:250],
            "insumo": insumo,
        },
    )
    if not created and (obj.insumo_id != insumo.id or obj.nombre != alias_name[:250]):
        obj.insumo = insumo
        obj.nombre = alias_name[:250]
        obj.save(update_fields=["insumo", "nombre"])
    return True, alias_norm, "creado" if created else "actualizado"


def _remove_pending_name_from_session(request: HttpRequest, alias_norm: str) -> None:
    _remove_pending_names_from_session(request, {alias_norm})


def _remove_pending_name_from_recent_runs(alias_norm: str, max_runs: int = 20) -> None:
    _remove_pending_names_from_recent_runs({alias_norm}, max_runs=max_runs)


def _remove_pending_names_from_session(request: HttpRequest, alias_norms: set[str]) -> None:
    if not alias_norms:
        return
    pending = request.session.get("inventario_pending_preview")
    if not pending:
        return
    request.session["inventario_pending_preview"] = [
        row
        for row in pending
        if normalizar_nombre(str((row or {}).get("nombre_origen") or "")) not in alias_norms
    ]


def _remove_pending_names_from_recent_runs(alias_norms: set[str], max_runs: int = 20) -> None:
    if not alias_norms:
        return
    runs = AlmacenSyncRun.objects.only("id", "pending_preview").order_by("-started_at")[:max_runs]
    for run in runs:
        pending = list(getattr(run, "pending_preview", []) or [])
        filtered = [
            row
            for row in pending
            if normalizar_nombre(str((row or {}).get("nombre_origen") or "")) not in alias_norms
        ]
        if len(filtered) != len(pending):
            run.pending_preview = filtered
            run.save(update_fields=["pending_preview"])


def _build_pending_grouped(pending_preview: list[dict]) -> list[dict]:
    grouped = defaultdict(lambda: {"count": 0, "sources": set(), "name": "", "suggestion": "", "score_max": 0.0})
    for row in pending_preview:
        name = str((row or {}).get("nombre_origen") or "").strip()
        norm = str((row or {}).get("nombre_normalizado") or normalizar_nombre(name))
        if not norm:
            continue
        item = grouped[norm]
        item["count"] += 1
        item["name"] = item["name"] or name
        source = str((row or {}).get("source") or "").strip()
        if source:
            item["sources"].add(source)
        suggestion = str((row or {}).get("sugerencia") or "").strip()
        if suggestion and not item["suggestion"]:
            item["suggestion"] = suggestion
        try:
            score = float((row or {}).get("score") or 0.0)
        except (TypeError, ValueError):
            score = 0.0
        item["score_max"] = max(item["score_max"], score)

    return sorted(
        [
            {
                "nombre_origen": v["name"],
                "nombre_normalizado": k,
                "count": v["count"],
                "sources": ", ".join(sorted(v["sources"])) if v["sources"] else "-",
                "sugerencia": v["suggestion"],
                "score_max": v["score_max"],
            }
            for k, v in grouped.items()
        ],
        key=lambda x: (-x["count"], x["nombre_origen"]),
    )


def _export_cross_pending_csv(cross_unified_rows: list[dict]) -> HttpResponse:
    now_str = timezone.localtime().strftime("%Y%m%d_%H%M")
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="inventario_homologacion_pendientes_{now_str}.csv"'
    writer = csv.writer(response)
    writer.writerow(
        [
            "nombre_muestra",
            "nombre_normalizado",
            "point_count",
            "almacen_count",
            "receta_count",
            "fuentes_activas",
            "total_count",
            "sugerencia",
            "score_max",
        ]
    )
    for row in cross_unified_rows:
        writer.writerow(
            [
                row.get("nombre_muestra", ""),
                row.get("nombre_normalizado", ""),
                row.get("point_count", 0),
                row.get("almacen_count", 0),
                row.get("receta_count", 0),
                row.get("sources_active", 0),
                row.get("total_count", 0),
                row.get("suggestion", ""),
                row.get("score_max", 0.0),
            ]
        )
    return response


def _export_alias_template(export_format: str) -> HttpResponse:
    headers = ["alias", "insumo"]
    sample_rows = [
        ["Harina pastelera 25kg", "Harina Pastelera"],
        ["Mantequilla barra", "Mantequilla"],
        ["Fresa fresca premium", "Fresa Fresca"],
    ]

    if export_format == "alias_template_csv":
        response = HttpResponse(content_type="text/csv; charset=utf-8")
        response["Content-Disposition"] = 'attachment; filename="plantilla_aliases_inventario.csv"'
        writer = csv.writer(response)
        writer.writerow(headers)
        writer.writerows(sample_rows)
        return response

    wb = Workbook()
    ws = wb.active
    ws.title = "aliases_import"
    ws.append(headers)
    for row in sample_rows:
        ws.append(row)
    ws.column_dimensions["A"].width = 36
    ws.column_dimensions["B"].width = 36

    bytes_buffer = BytesIO()
    wb.save(bytes_buffer)
    bytes_buffer.seek(0)
    response = HttpResponse(
        bytes_buffer.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = 'attachment; filename="plantilla_aliases_inventario.xlsx"'
    return response


def _resolve_cross_source_with_alias(alias_name: str, insumo: Insumo) -> tuple[int, int]:
    alias_norm = normalizar_nombre(alias_name)
    if not alias_norm:
        return 0, 0

    point_resolved = 0
    for pending in PointPendingMatch.objects.filter(tipo=PointPendingMatch.TIPO_INSUMO).only("id", "point_nombre", "point_codigo"):
        if normalizar_nombre(pending.point_nombre or "") != alias_norm:
            continue
        point_code = (pending.point_codigo or "").strip()
        changed = []
        if point_code and (insumo.codigo_point or "").strip() != point_code:
            insumo.codigo_point = point_code[:80]
            changed.append("codigo_point")
        if (pending.point_nombre or "").strip() and insumo.nombre_point != pending.point_nombre:
            insumo.nombre_point = pending.point_nombre[:250]
            changed.append("nombre_point")
        if changed:
            insumo.save(update_fields=changed)
        pending.delete()
        point_resolved += 1

    latest_cost = (
        CostoInsumo.objects.filter(insumo=insumo).order_by("-fecha", "-id").values_list("costo_unitario", flat=True).first()
    )
    recetas_resolved = 0
    lineas_qs = LineaReceta.objects.filter(~Q(tipo_linea=LineaReceta.TIPO_SUBSECCION)).filter(
        Q(insumo__isnull=True) | Q(match_status=LineaReceta.STATUS_NEEDS_REVIEW) | Q(match_status=LineaReceta.STATUS_REJECTED)
    )
    for linea in lineas_qs.only("id", "insumo_texto", "unidad_texto", "unidad_id", "cantidad"):
        if normalizar_nombre(linea.insumo_texto or "") != alias_norm:
            continue
        linea.insumo = insumo
        if not linea.unidad_id and insumo.unidad_base_id:
            linea.unidad = insumo.unidad_base
        if (not linea.unidad_texto) and insumo.unidad_base_id and insumo.unidad_base:
            linea.unidad_texto = insumo.unidad_base.codigo
        if latest_cost is not None:
            linea.costo_unitario_snapshot = latest_cost
        linea.match_status = LineaReceta.STATUS_AUTO
        linea.match_method = "ALIAS"
        linea.match_score = 100.0
        linea.save(
            update_fields=[
                "insumo",
                "unidad",
                "unidad_texto",
                "costo_unitario_snapshot",
                "match_status",
                "match_method",
                "match_score",
            ]
        )
        recetas_resolved += 1

    return point_resolved, recetas_resolved


def _latest_cost_by_insumo_cached(insumo_id: int, cache: dict[int, Decimal | None]) -> Decimal | None:
    if insumo_id in cache:
        return cache[insumo_id]
    latest = (
        CostoInsumo.objects.filter(insumo_id=insumo_id).order_by("-fecha", "-id").values_list("costo_unitario", flat=True).first()
    )
    cache[insumo_id] = latest
    return latest


def _reprocess_recetas_pending_matching() -> dict[str, int]:
    qs = LineaReceta.objects.filter(~Q(tipo_linea=LineaReceta.TIPO_SUBSECCION)).filter(
        Q(insumo__isnull=True) | Q(match_status=LineaReceta.STATUS_NEEDS_REVIEW) | Q(match_status=LineaReceta.STATUS_REJECTED)
    )
    total = 0
    auto_ok = 0
    review = 0
    rejected = 0
    linked = 0
    cost_cache: dict[int, Decimal | None] = {}

    for linea in qs:
        total += 1
        insumo, score, method = match_insumo(linea.insumo_texto or "")
        status = clasificar_match(score)

        linea.match_score = score
        linea.match_method = method
        linea.match_status = status

        if status == LineaReceta.STATUS_REJECTED or not insumo:
            linea.insumo = None
        else:
            linea.insumo = insumo
            linked += 1
            if not linea.unidad_id and insumo.unidad_base_id:
                linea.unidad = insumo.unidad_base
            if (not linea.unidad_texto) and insumo.unidad_base_id and insumo.unidad_base:
                linea.unidad_texto = insumo.unidad_base.codigo
            latest_cost = _latest_cost_by_insumo_cached(insumo.id, cost_cache)
            if latest_cost is not None:
                linea.costo_unitario_snapshot = latest_cost

        linea.save(
            update_fields=[
                "insumo",
                "unidad",
                "unidad_texto",
                "costo_unitario_snapshot",
                "match_status",
                "match_method",
                "match_score",
            ]
        )

        if status == LineaReceta.STATUS_AUTO:
            auto_ok += 1
        elif status == LineaReceta.STATUS_NEEDS_REVIEW:
            review += 1
        else:
            rejected += 1

    return {
        "total": total,
        "auto_ok": auto_ok,
        "review": review,
        "rejected": rejected,
        "linked": linked,
    }


@login_required
def importar_archivos(request: HttpRequest) -> HttpResponse:
    if not can_view_inventario(request.user):
        raise PermissionDenied("No tienes permisos para ver Inventario.")

    summary = None
    pendientes_preview = list(request.session.get("inventario_pending_preview", []))[:80]
    warnings: list[str] = []
    drive_info = None

    if request.method == "POST":
        if not can_manage_inventario(request.user):
            raise PermissionDenied("No tienes permisos para importar archivos de almacén.")

        action = (request.POST.get("action") or "upload").strip().lower()
        if action == "create_alias":
            alias_name = (request.POST.get("alias_name") or "").strip()
            insumo_id = (request.POST.get("insumo_id") or "").strip()
            if not alias_name or not insumo_id:
                messages.error(request, "Debes indicar nombre origen e insumo para crear el alias.")
                return redirect("inventario:importar_archivos")

            insumo = Insumo.objects.filter(pk=insumo_id).first()
            if not insumo:
                messages.error(request, "Insumo inválido para crear alias.")
                return redirect("inventario:importar_archivos")

            ok, alias_norm, action_label = _upsert_alias(alias_name, insumo)
            if not ok:
                messages.error(request, "El nombre origen no es válido.")
                return redirect("inventario:importar_archivos")

            _remove_pending_name_from_session(request, alias_norm)
            _remove_pending_name_from_recent_runs(alias_norm)
            messages.success(request, f"Alias {action_label}: '{alias_name}' -> {insumo.nombre}. Ejecuta de nuevo la importación para aplicar el cambio.")
            return redirect("inventario:importar_archivos")

        selected_sources = set(request.POST.getlist("sources")) or set(SOURCE_TO_FILENAME.keys())
        selected_sources = selected_sources.intersection(set(SOURCE_TO_FILENAME.keys()))
        if not selected_sources:
            messages.error(request, "Selecciona al menos una fuente a importar.")
            return redirect("inventario:importar_archivos")

        fuzzy_threshold = int(_to_decimal(request.POST.get("fuzzy_threshold"), "96"))
        create_aliases = bool(request.POST.get("create_aliases"))
        create_missing_insumos = bool(request.POST.get("create_missing_insumos"))
        run_started_at = timezone.now()
        run_source = AlmacenSyncRun.SOURCE_DRIVE if action == "drive" else AlmacenSyncRun.SOURCE_MANUAL

        if action == "drive":
            month_override = (request.POST.get("month") or "").strip() or None
            try:
                drive_result = sync_almacen_from_drive(
                    include_sources=selected_sources,
                    month_override=month_override,
                    fallback_previous=True,
                    fuzzy_threshold=fuzzy_threshold,
                    create_aliases=create_aliases,
                    create_missing_insumos=create_missing_insumos,
                    dry_run=False,
                )
                summary = drive_result.summary
                drive_info = {
                    "folder_name": drive_result.folder_name,
                    "target_month": drive_result.target_month,
                    "used_fallback_month": drive_result.used_fallback_month,
                    "downloaded_sources": drive_result.downloaded_sources,
                }
                warnings.extend(drive_result.skipped_files)
            except Exception as exc:
                log_sync_run(
                    source=run_source,
                    status=AlmacenSyncRun.STATUS_ERROR,
                    triggered_by=request.user,
                    message=str(exc),
                    started_at=run_started_at,
                )
                messages.error(request, f"Falló la sincronización desde Google Drive: {exc}")
                return redirect("inventario:importar_archivos")
        else:
            uploaded_files = request.FILES.getlist("archivos")
            if not uploaded_files:
                messages.error(request, "Selecciona al menos un archivo .xlsx para importar.")
                return redirect("inventario:importar_archivos")

            saved_sources = set()
            with TemporaryDirectory(prefix="inv-upload-") as tmpdir:
                for f in uploaded_files:
                    source, target_name = _classify_upload(f.name)
                    if not source or not target_name:
                        warnings.append(f"Archivo no reconocido y omitido: {f.name}")
                        continue
                    if source not in selected_sources:
                        warnings.append(f"Archivo omitido por filtro de fuente ({source}): {f.name}")
                        continue
                    _save_uploaded_file(tmpdir, target_name, f)
                    saved_sources.add(source)

                run_sources = selected_sources.intersection(saved_sources)
                if not run_sources:
                    messages.error(request, "Ninguno de los archivos subidos coincide con las fuentes seleccionadas.")
                    return redirect("inventario:importar_archivos")

                try:
                    summary = import_folder(
                        folderpath=tmpdir,
                        include_sources=run_sources,
                        fuzzy_threshold=fuzzy_threshold,
                        create_aliases=create_aliases,
                        create_missing_insumos=create_missing_insumos,
                        dry_run=False,
                    )
                except Exception as exc:
                    log_sync_run(
                        source=run_source,
                        status=AlmacenSyncRun.STATUS_ERROR,
                        triggered_by=request.user,
                        message=str(exc),
                        started_at=run_started_at,
                    )
                    messages.error(request, f"Falló la importación de almacén: {exc}")
                    return redirect("inventario:importar_archivos")

        for w in warnings:
            messages.warning(request, w)

        if summary:
            if drive_info:
                suffix = (
                    f" (Drive: {drive_info['folder_name']} · objetivo {drive_info['target_month']}"
                    f"{' · fallback aplicado' if drive_info['used_fallback_month'] else ''})"
                )
            else:
                suffix = ""
            messages.success(
                request,
                (
                    f"Importación aplicada. Existencias actualizadas: {summary.existencias_updated}, "
                    f"Movimientos creados: {summary.movimientos_created}, "
                    f"Duplicados omitidos: {summary.movimientos_skipped_duplicate}.{suffix}"
                ),
            )
            if summary.unmatched:
                messages.warning(request, f"Quedaron {summary.unmatched} filas sin match.")
            pendientes_preview = summary.pendientes[:80]
            request.session["inventario_pending_preview"] = summary.pendientes[:200]
            log_sync_run(
                source=run_source,
                status=AlmacenSyncRun.STATUS_OK,
                summary=summary,
                triggered_by=request.user,
                folder_name=(drive_info or {}).get("folder_name", ""),
                target_month=(drive_info or {}).get("target_month", ""),
                fallback_used=bool((drive_info or {}).get("used_fallback_month")),
                downloaded_sources=(drive_info or {}).get("downloaded_sources", sorted(selected_sources)),
                message=" | ".join(warnings[:12]),
                started_at=run_started_at,
            )

    context = {
        "can_manage_inventario": can_manage_inventario(request.user),
        "sources": [
            ("inventario", "Inventario"),
            ("entradas", "Entradas"),
            ("salidas", "Salidas"),
            ("merma", "Merma"),
        ],
        "summary": summary,
        "pendientes_preview": pendientes_preview,
        "expected_names": [
            INVENTARIO_FILE,
            ENTRADAS_FILE,
            SALIDAS_FILE,
            MERMA_FILE,
        ],
        "defaults": {
            "fuzzy_threshold": 96,
            "create_missing_insumos": True,
            "create_aliases": False,
        },
        "current_month": timezone.localdate().strftime("%Y-%m"),
        "drive_info": drive_info,
        "latest_runs": AlmacenSyncRun.objects.select_related("triggered_by").all()[:10],
        "insumo_alias_targets": Insumo.objects.filter(activo=True).order_by("nombre")[:800],
    }
    return render(request, "inventario/importar_archivos.html", context)


@login_required
def aliases_catalog(request: HttpRequest) -> HttpResponse:
    if not can_view_inventario(request.user):
        raise PermissionDenied("No tienes permisos para ver Inventario.")

    if request.method == "POST":
        if not can_manage_inventario(request.user):
            raise PermissionDenied("No tienes permisos para administrar aliases.")

        action = (request.POST.get("action") or "").strip().lower()
        next_q = (request.POST.get("next_q") or "").strip()

        if action == "create":
            alias_name = (request.POST.get("alias_name") or "").strip()
            insumo_id = (request.POST.get("insumo_id") or "").strip()
            if not alias_name or not insumo_id:
                messages.error(request, "Debes indicar nombre origen e insumo.")
            else:
                insumo = Insumo.objects.filter(pk=insumo_id).first()
                if not insumo:
                    messages.error(request, "Insumo inválido.")
                else:
                    ok, alias_norm, action_label = _upsert_alias(alias_name, insumo)
                    if ok:
                        _remove_pending_name_from_session(request, alias_norm)
                        _remove_pending_name_from_recent_runs(alias_norm)
                        messages.success(request, f"Alias {action_label}: '{alias_name}' -> {insumo.nombre}.")
                        point_resolved, recetas_resolved = _resolve_cross_source_with_alias(alias_name, insumo)
                        if point_resolved or recetas_resolved:
                            messages.info(
                                request,
                                (
                                    "Homologación cruzada aplicada: "
                                    f"Point resueltos {point_resolved}, "
                                    f"líneas recetas resueltas {recetas_resolved}."
                                ),
                            )
                    else:
                        messages.error(request, "El nombre origen no es válido.")

        elif action == "apply_suggestion":
            alias_name = (request.POST.get("alias_name") or "").strip()
            suggestion = (request.POST.get("suggestion") or "").strip()
            min_score = float(_to_decimal(request.POST.get("score_min"), "90"))
            min_score = max(0.0, min(100.0, min_score))
            if not alias_name or not suggestion:
                messages.error(request, "Faltan datos para aplicar sugerencia.")
            else:
                insumo = (
                    Insumo.objects.filter(activo=True, nombre_normalizado=normalizar_nombre(suggestion))
                    .only("id", "nombre")
                    .first()
                )
                if not insumo:
                    candidate, candidate_score, _ = match_insumo(suggestion)
                    if candidate and float(candidate_score or 0.0) >= min_score:
                        insumo = candidate
                if not insumo:
                    messages.error(request, f"No se pudo resolver '{suggestion}' como insumo activo.")
                else:
                    ok, alias_norm, action_label = _upsert_alias(alias_name, insumo)
                    if ok:
                        _remove_pending_name_from_session(request, alias_norm)
                        _remove_pending_name_from_recent_runs(alias_norm)
                        point_resolved, recetas_resolved = _resolve_cross_source_with_alias(alias_name, insumo)
                        messages.success(
                            request,
                            (
                                f"Sugerencia aplicada ({action_label}): '{alias_name}' -> {insumo.nombre}. "
                                f"Point resueltos: {point_resolved}. Recetas resueltas: {recetas_resolved}."
                            ),
                        )
                    else:
                        messages.error(request, "No se pudo aplicar la sugerencia por normalización inválida.")

        elif action == "import_bulk":
            archivo = request.FILES.get("archivo_aliases")
            min_score = float(_to_decimal(request.POST.get("score_min"), "90"))
            min_score = max(0.0, min(100.0, min_score))

            if not archivo:
                messages.error(request, "Debes seleccionar un archivo .csv o .xlsx para importar aliases.")
            else:
                try:
                    rows = _read_alias_import_rows(archivo)
                except ValueError as exc:
                    messages.error(request, str(exc))
                    rows = []
                except Exception:
                    messages.error(request, "No se pudo leer el archivo de aliases. Verifica formato y columnas.")
                    rows = []

                if rows:
                    insumo_exact_map = {
                        i.nombre_normalizado: i
                        for i in Insumo.objects.filter(activo=True).only("id", "nombre", "nombre_normalizado")
                    }
                    created = 0
                    updated = 0
                    invalid = 0
                    unresolved = 0
                    point_resolved_total = 0
                    recetas_resolved_total = 0
                    cleaned_norms: set[str] = set()
                    unresolved_preview: list[dict] = []

                    for idx, row in enumerate(rows, start=2):
                        alias_name = str(row.get("alias") or "").strip()
                        insumo_raw = str(row.get("insumo") or "").strip()
                        reason = ""
                        resolved_insumo = None
                        score = 0.0
                        method = "-"
                        suggested_name = ""

                        if not alias_name:
                            reason = "Alias vacío."
                            invalid += 1
                        else:
                            if insumo_raw:
                                resolved_insumo = insumo_exact_map.get(normalizar_nombre(insumo_raw))
                                if resolved_insumo:
                                    score = 100.0
                                    method = "EXACT_NAME"
                                    suggested_name = resolved_insumo.nombre
                                else:
                                    candidate, candidate_score, candidate_method = match_insumo(insumo_raw)
                                    if candidate and candidate_score >= min_score:
                                        resolved_insumo = candidate
                                        score = float(candidate_score or 0.0)
                                        method = candidate_method or "FUZZY"
                                        suggested_name = candidate.nombre
                                    else:
                                        suggested_name = candidate.nombre if candidate else ""
                                        score = float(candidate_score or 0.0) if candidate else 0.0
                                        method = candidate_method or "NO_MATCH"
                                        reason = f"Insumo no resuelto (score<{min_score:.1f})."
                            else:
                                candidate, candidate_score, candidate_method = match_insumo(alias_name)
                                if candidate and candidate_score >= min_score:
                                    resolved_insumo = candidate
                                    score = float(candidate_score or 0.0)
                                    method = candidate_method or "FUZZY"
                                    suggested_name = candidate.nombre
                                else:
                                    suggested_name = candidate.nombre if candidate else ""
                                    score = float(candidate_score or 0.0) if candidate else 0.0
                                    method = candidate_method or "NO_MATCH"
                                    reason = "Sin columna 'insumo' resoluble para esta fila."

                        if not reason and resolved_insumo:
                            ok, alias_norm, action_label = _upsert_alias(alias_name, resolved_insumo)
                            if ok:
                                if action_label == "creado":
                                    created += 1
                                else:
                                    updated += 1
                                cleaned_norms.add(alias_norm)
                                point_resolved, recetas_resolved = _resolve_cross_source_with_alias(alias_name, resolved_insumo)
                                point_resolved_total += point_resolved
                                recetas_resolved_total += recetas_resolved
                            else:
                                reason = "Alias inválido tras normalización."
                                invalid += 1

                        if reason:
                            unresolved += 1
                            if len(unresolved_preview) < 200:
                                unresolved_preview.append(
                                    {
                                        "row": idx,
                                        "alias": alias_name,
                                        "insumo_archivo": insumo_raw,
                                        "sugerencia": suggested_name,
                                        "score": score,
                                        "method": method,
                                        "motivo": reason,
                                    }
                                )

                    if cleaned_norms:
                        _remove_pending_names_from_session(request, cleaned_norms)
                        _remove_pending_names_from_recent_runs(cleaned_norms)

                    request.session["inventario_alias_import_preview"] = unresolved_preview
                    request.session["inventario_alias_import_stats"] = {
                        "file_name": archivo.name,
                        "rows_total": len(rows),
                        "created": created,
                        "updated": updated,
                        "invalid": invalid,
                        "unresolved": unresolved,
                        "point_resolved": point_resolved_total,
                        "recetas_resolved": recetas_resolved_total,
                        "score_min": min_score,
                    }

                    messages.success(
                        request,
                        (
                            "Importación masiva de aliases completada. "
                            f"Filas: {len(rows)}. Creados: {created}. Actualizados: {updated}. "
                            f"Point resueltos: {point_resolved_total}. Recetas resueltas: {recetas_resolved_total}."
                        ),
                    )
                    if unresolved:
                        messages.warning(
                            request,
                            (
                                f"Quedaron {unresolved} filas sin resolver o inválidas. "
                                "Revísalas en el bloque 'Pendientes de importación'."
                            ),
                        )
                else:
                    messages.warning(request, "El archivo no contiene filas para importar.")

        elif action == "bulk_reassign":
            insumo_id = (request.POST.get("insumo_id") or "").strip()
            alias_ids = [a for a in request.POST.getlist("alias_ids") if a.isdigit()]
            if not insumo_id or not alias_ids:
                messages.error(request, "Selecciona aliases e insumo destino para la reasignación masiva.")
            else:
                insumo = Insumo.objects.filter(pk=insumo_id).first()
                if not insumo:
                    messages.error(request, "Insumo destino inválido.")
                else:
                    aliases_to_update = list(
                        InsumoAlias.objects.filter(id__in=alias_ids).exclude(insumo=insumo).only("id", "nombre")
                    )
                    updated = 0
                    point_resolved_total = 0
                    recetas_resolved_total = 0
                    cleaned_norms: set[str] = set()

                    for alias in aliases_to_update:
                        alias.insumo = insumo
                        alias.save(update_fields=["insumo"])
                        updated += 1
                        cleaned_norms.add(normalizar_nombre(alias.nombre))
                        point_resolved, recetas_resolved = _resolve_cross_source_with_alias(alias.nombre, insumo)
                        point_resolved_total += point_resolved
                        recetas_resolved_total += recetas_resolved

                    if cleaned_norms:
                        _remove_pending_names_from_session(request, cleaned_norms)
                        _remove_pending_names_from_recent_runs(cleaned_norms)

                    messages.success(
                        request,
                        (
                            f"Aliases reasignados a {insumo.nombre}: {updated}. "
                            f"Point resueltos: {point_resolved_total}. "
                            f"Recetas resueltas: {recetas_resolved_total}."
                        ),
                    )

        elif action == "delete":
            alias_id = (request.POST.get("alias_id") or "").strip()
            alias = InsumoAlias.objects.filter(pk=alias_id).first()
            if not alias:
                messages.error(request, "Alias no encontrado.")
            else:
                alias_display = alias.nombre
                alias.delete()
                messages.success(request, f"Alias eliminado: {alias_display}.")

        elif action == "clear_pending":
            request.session["inventario_pending_preview"] = []
            hide_run_id = (request.POST.get("hide_run_id") or "").strip()
            if hide_run_id.isdigit():
                request.session["inventario_hidden_pending_run_id"] = int(hide_run_id)
            messages.success(request, "Pendientes en pantalla limpiados.")
        elif action == "reset_hidden_pending":
            request.session.pop("inventario_hidden_pending_run_id", None)
            messages.success(request, "Se restauró la visibilidad de pendientes recientes.")
        elif action == "load_pending_run":
            run_id_raw = (request.POST.get("run_id") or "").strip()
            run = AlmacenSyncRun.objects.filter(pk=run_id_raw).first() if run_id_raw.isdigit() else None
            if not run:
                messages.error(request, "Run de sincronización no encontrado.")
            elif not isinstance(run.pending_preview, list) or not run.pending_preview:
                messages.error(request, "Ese run no tiene pendientes guardados para mostrar.")
            else:
                request.session["inventario_pending_preview"] = list(run.pending_preview)[:200]
                request.session.pop("inventario_hidden_pending_run_id", None)
                messages.success(
                    request,
                    f"Pendientes cargados desde run {run.id} ({run.started_at:%Y-%m-%d %H:%M}).",
                )
        elif action == "auto_apply_suggestions":
            min_score = float(_to_decimal(request.POST.get("auto_min_score"), "90"))
            min_score = max(0.0, min(100.0, min_score))
            max_rows = int(_to_decimal(request.POST.get("auto_max_rows"), "80"))
            max_rows = max(1, min(500, max_rows))

            session_pending = list(request.session.get("inventario_pending_preview", []))[:500]
            hidden_run_id = request.session.get("inventario_hidden_pending_run_id")
            latest_runs = list(
                AlmacenSyncRun.objects.only("id", "pending_preview").order_by("-started_at")[:20]
            )
            latest_pending_run = None
            for run in latest_runs:
                if hidden_run_id and run.id == hidden_run_id:
                    continue
                if isinstance(run.pending_preview, list) and run.pending_preview:
                    latest_pending_run = run
                    break
            persisted_pending = list((latest_pending_run.pending_preview if latest_pending_run else [])[:500])
            pending_preview = session_pending or persisted_pending
            pending_grouped = _build_pending_grouped(pending_preview)

            if not pending_grouped:
                messages.info(request, "No hay pendientes visibles para auto-aplicar sugerencias.")
            else:
                insumo_map = {
                    i.nombre_normalizado: i
                    for i in Insumo.objects.filter(activo=True).only("id", "nombre", "nombre_normalizado")
                }
                created = 0
                updated = 0
                skipped_no_suggestion = 0
                skipped_low_score = 0
                skipped_unresolved = 0
                skipped_invalid = 0
                point_resolved_total = 0
                recetas_resolved_total = 0
                processed = 0
                cleaned_norms: set[str] = set()

                for row in pending_grouped:
                    if processed >= max_rows:
                        break

                    suggestion = str(row.get("sugerencia") or "").strip()
                    if not suggestion:
                        skipped_no_suggestion += 1
                        continue

                    score_max = float(row.get("score_max") or 0.0)
                    if score_max < min_score:
                        skipped_low_score += 1
                        continue

                    insumo = insumo_map.get(normalizar_nombre(suggestion))
                    if not insumo:
                        skipped_unresolved += 1
                        continue

                    ok, alias_norm, action_label = _upsert_alias(row["nombre_origen"], insumo)
                    if not ok:
                        skipped_invalid += 1
                        continue

                    processed += 1
                    if action_label == "creado":
                        created += 1
                    else:
                        updated += 1
                    cleaned_norms.add(alias_norm)

                    point_resolved, recetas_resolved = _resolve_cross_source_with_alias(row["nombre_origen"], insumo)
                    point_resolved_total += point_resolved
                    recetas_resolved_total += recetas_resolved

                if cleaned_norms:
                    _remove_pending_names_from_session(request, cleaned_norms)
                    _remove_pending_names_from_recent_runs(cleaned_norms)

                messages.success(
                    request,
                    (
                        "Auto-aplicación completada. "
                        f"Creados: {created}, actualizados: {updated}, "
                        f"Point resueltos: {point_resolved_total}, "
                        f"Recetas resueltas: {recetas_resolved_total}."
                    ),
                )
                messages.info(
                    request,
                    (
                        "Omitidos → "
                        f"sin sugerencia: {skipped_no_suggestion}, "
                        f"score<{min_score:.1f}: {skipped_low_score}, "
                        f"sugerencia sin insumo exacto: {skipped_unresolved}, "
                        f"nombre inválido: {skipped_invalid}."
                    ),
                )
        elif action == "reprocess_recetas_pending":
            result = _reprocess_recetas_pending_matching()
            messages.success(
                request,
                (
                    "Reproceso recetas completado. "
                    f"Procesadas: {result['total']}, "
                    f"Auto/OK: {result['auto_ok']}, "
                    f"Por revisar: {result['review']}, "
                    f"Sin match: {result['rejected']}, "
                    f"Ligadas a insumo: {result['linked']}."
                ),
            )
        elif action == "clear_import_preview":
            request.session.pop("inventario_alias_import_preview", None)
            request.session.pop("inventario_alias_import_stats", None)
            messages.success(request, "Pendientes de importación limpiados.")

        base_url = reverse("inventario:aliases_catalog")
        if next_q:
            return redirect(f"{base_url}?{urlencode({'q': next_q})}")
        return redirect(base_url)

    q = (request.GET.get("q") or "").strip()
    aliases_qs = InsumoAlias.objects.select_related("insumo").order_by("nombre")
    if q:
        q_norm = normalizar_nombre(q)
        aliases_qs = aliases_qs.filter(
            Q(nombre__icontains=q)
            | Q(nombre_normalizado__icontains=q_norm)
            | Q(insumo__nombre__icontains=q)
        )
    paginator = Paginator(aliases_qs, 100)
    page = paginator.get_page(request.GET.get("page"))

    session_pending = list(request.session.get("inventario_pending_preview", []))[:120]
    hidden_run_id = request.session.get("inventario_hidden_pending_run_id")

    latest_runs = list(
        AlmacenSyncRun.objects.only(
            "id",
            "started_at",
            "source",
            "status",
            "matched",
            "unmatched",
            "rows_stock_read",
            "rows_mov_read",
            "aliases_created",
            "insumos_created",
            "pending_preview",
        ).order_by("-started_at")[:20]
    )
    latest_sync = latest_runs[0] if latest_runs else None
    latest_pending_run = None
    hidden_pending_run = None
    for run in latest_runs:
        if hidden_run_id and run.id == hidden_run_id:
            hidden_pending_run = run
            continue
        if isinstance(run.pending_preview, list) and run.pending_preview:
            latest_pending_run = run
            break

    persisted_pending = list((latest_pending_run.pending_preview if latest_pending_run else [])[:120])
    pending_preview = session_pending or persisted_pending

    recent_runs = latest_runs[:10]
    total_matched = sum(int(r.matched or 0) for r in recent_runs)
    total_unmatched = sum(int(r.unmatched or 0) for r in recent_runs)
    total_rows = total_matched + total_unmatched
    match_rate = round((total_matched * 100.0 / total_rows), 2) if total_rows else 100.0
    ok_runs = sum(1 for r in recent_runs if r.status == AlmacenSyncRun.STATUS_OK)
    pending_recent_runs = [
        {
            "id": r.id,
            "started_at": r.started_at,
            "source_label": r.get_source_display(),
            "status": r.status,
            "matched": int(r.matched or 0),
            "unmatched": int(r.unmatched or 0),
            "has_preview": bool(isinstance(r.pending_preview, list) and r.pending_preview),
            "is_hidden": bool(hidden_run_id and r.id == hidden_run_id),
        }
        for r in recent_runs
    ]

    pending_grouped = _build_pending_grouped(pending_preview)
    auto_default_score = 90.0
    insumo_norm_set = set(
        Insumo.objects.filter(activo=True).values_list("nombre_normalizado", flat=True)
    )
    auto_apply_candidates = sum(
        1
        for row in pending_grouped
        if row.get("sugerencia")
        and float(row.get("score_max") or 0.0) >= auto_default_score
        and normalizar_nombre(str(row.get("sugerencia") or "")) in insumo_norm_set
    )

    unified = defaultdict(
        lambda: {
            "nombre_muestra": "",
            "point_count": 0,
            "almacen_count": 0,
            "receta_count": 0,
            "suggestion": "",
            "score_max": 0.0,
        }
    )

    for row in pending_grouped:
        norm = row["nombre_normalizado"]
        if not norm:
            continue
        item = unified[norm]
        item["nombre_muestra"] = item["nombre_muestra"] or row["nombre_origen"]
        item["almacen_count"] += int(row["count"] or 0)
        if row.get("sugerencia") and not item["suggestion"]:
            item["suggestion"] = row["sugerencia"]
        item["score_max"] = max(item["score_max"], float(row.get("score_max") or 0.0))

    point_pending_insumos = list(
        PointPendingMatch.objects.filter(tipo=PointPendingMatch.TIPO_INSUMO).only("point_nombre", "fuzzy_sugerencia", "fuzzy_score")
    )
    for p in point_pending_insumos:
        norm = normalizar_nombre(p.point_nombre or "")
        if not norm:
            continue
        item = unified[norm]
        item["nombre_muestra"] = item["nombre_muestra"] or (p.point_nombre or "")
        item["point_count"] += 1
        if (p.fuzzy_sugerencia or "").strip() and not item["suggestion"]:
            item["suggestion"] = p.fuzzy_sugerencia.strip()
        item["score_max"] = max(item["score_max"], float(p.fuzzy_score or 0.0))

    receta_pending_lines_qs = LineaReceta.objects.filter(~Q(tipo_linea=LineaReceta.TIPO_SUBSECCION)).filter(
        Q(insumo__isnull=True) | Q(match_status=LineaReceta.STATUS_NEEDS_REVIEW) | Q(match_status=LineaReceta.STATUS_REJECTED)
    )
    receta_pending_lines = 0
    for linea in receta_pending_lines_qs.only("insumo_texto"):
        norm = normalizar_nombre(linea.insumo_texto or "")
        if not norm:
            continue
        item = unified[norm]
        item["nombre_muestra"] = item["nombre_muestra"] or (linea.insumo_texto or "")
        item["receta_count"] += 1
        receta_pending_lines += 1

    unified_rows = []
    overlaps = 0
    for norm, item in unified.items():
        sources_active = sum(
            1
            for v in (item["point_count"], item["almacen_count"], item["receta_count"])
            if v > 0
        )
        if sources_active >= 2:
            overlaps += 1
        unified_rows.append(
            {
                "nombre_normalizado": norm,
                "nombre_muestra": item["nombre_muestra"] or norm,
                "point_count": item["point_count"],
                "almacen_count": item["almacen_count"],
                "receta_count": item["receta_count"],
                "sources_active": sources_active,
                "total_count": item["point_count"] + item["almacen_count"] + item["receta_count"],
                "suggestion": item["suggestion"],
                "score_max": item["score_max"],
            }
        )
    unified_rows.sort(key=lambda x: (-x["sources_active"], -x["total_count"], x["nombre_muestra"]))

    export_format = (request.GET.get("export") or "").strip().lower()
    if export_format == "cross_pending_csv":
        return _export_cross_pending_csv(unified_rows)
    if export_format in {"alias_template_csv", "alias_template_xlsx"}:
        return _export_alias_template(export_format)

    import_preview = list(request.session.get("inventario_alias_import_preview", []))[:200]
    import_stats = request.session.get("inventario_alias_import_stats", {})

    context = {
        "q": q,
        "page": page,
        "pending_preview": pending_preview,
        "pending_grouped": pending_grouped[:80],
        "pending_source": "session" if session_pending else ("persisted" if persisted_pending else ""),
        "latest_pending_run": latest_pending_run,
        "hidden_pending_run": hidden_pending_run,
        "hidden_run_id": hidden_run_id,
        "latest_sync": latest_sync,
        "matching_summary": {
            "runs_count": len(recent_runs),
            "ok_runs": ok_runs,
            "total_matched": total_matched,
            "total_unmatched": total_unmatched,
            "match_rate": match_rate,
        },
        "pending_recent_runs": pending_recent_runs,
        "pending_visible_count": len(pending_preview),
        "pending_unique_count": len(pending_grouped),
        "auto_default_score": auto_default_score,
        "auto_default_limit": 80,
        "auto_apply_candidates": auto_apply_candidates,
        "insumo_alias_targets": Insumo.objects.filter(activo=True).order_by("nombre")[:1200],
        "can_manage_inventario": can_manage_inventario(request.user),
        "cross_summary": {
            "point_unmatched": len(point_pending_insumos),
            "almacen_unmatched": len(pending_preview),
            "recetas_unmatched": receta_pending_lines,
            "overlaps": overlaps,
        },
        "cross_unified_rows": unified_rows[:120],
        "alias_import_preview": import_preview,
        "alias_import_stats": import_stats,
    }
    return render(request, "inventario/aliases_catalog.html", context)


@login_required
def sync_drive_now(request: HttpRequest) -> HttpResponse:
    if request.method != "POST":
        return redirect("dashboard")
    if not can_manage_inventario(request.user):
        raise PermissionDenied("No tienes permisos para ejecutar sincronización manual.")

    next_url = (request.POST.get("next") or "").strip()
    if not next_url.startswith("/"):
        next_url = ""

    try:
        cmd = [
            sys.executable,
            "manage.py",
            "sync_almacen_drive",
            "--create-missing-insumos",
        ]
        subprocess.Popen(
            cmd,
            cwd=str(settings.BASE_DIR),
            env=os.environ.copy(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        messages.success(
            request,
            "Sincronización manual iniciada. Refresca el dashboard en 1-2 minutos para ver el resultado.",
        )
    except Exception as exc:
        messages.error(request, f"No se pudo iniciar la sincronización manual: {exc}")

    return redirect(next_url or "dashboard")


@login_required
def existencias(request: HttpRequest) -> HttpResponse:
    if not can_view_inventario(request.user):
        raise PermissionDenied("No tienes permisos para ver Inventario.")

    if request.method == "POST":
        if not can_manage_inventario(request.user):
            raise PermissionDenied("No tienes permisos para editar existencias.")
        action = (request.POST.get("action") or "").strip()
        if action == "update_reorder_config":
            max_diff_pct = _to_decimal(request.POST.get("reorder_max_diff_pct"), "10")
            if max_diff_pct < 0:
                messages.error(request, "El umbral máximo de desviación no puede ser negativo.")
                return redirect("inventario:existencias")
            config = InventarioConfig.get_solo()
            config.reorder_max_diff_pct = max_diff_pct
            config.save(update_fields=["reorder_max_diff_pct", "updated_at"])
            messages.success(
                request,
                f"Configuración guardada: umbral máximo manual en punto de reorden = {max_diff_pct}%.",
            )
            return redirect("inventario:existencias")

        insumo_id = request.POST.get("insumo_id")
        if insumo_id:
            existencia, _ = ExistenciaInsumo.objects.get_or_create(insumo_id=insumo_id)
            prev_stock = existencia.stock_actual
            prev_reorden = existencia.punto_reorden
            prev_minimo = existencia.stock_minimo
            prev_maximo = existencia.stock_maximo
            prev_inv_prom = existencia.inventario_promedio
            prev_dias = existencia.dias_llegada_pedido
            prev_consumo = existencia.consumo_diario_promedio
            new_stock = _to_decimal(request.POST.get("stock_actual"), "0")
            new_minimo = _to_decimal(request.POST.get("stock_minimo"), "0")
            new_maximo = _to_decimal(request.POST.get("stock_maximo"), "0")
            new_inv_prom = _to_decimal(request.POST.get("inventario_promedio"), "0")
            new_dias = int(_to_decimal(request.POST.get("dias_llegada_pedido"), "0"))
            new_consumo = _to_decimal(request.POST.get("consumo_diario_promedio"), "0")
            reorden_recomendado = calcular_punto_reorden(
                stock_minimo=new_minimo,
                dias_llegada_pedido=new_dias,
                consumo_diario_promedio=new_consumo,
                formula=getattr(settings, "INVENTARIO_REORDER_FORMULA", FORMULA_EXCEL_LEGACY),
            )
            reorden_raw = (request.POST.get("punto_reorden") or "").strip()
            if reorden_raw:
                new_reorden = _to_decimal(reorden_raw, "0")
                reorden_auto = False
                max_diff_pct = _inventario_reorder_max_diff_pct()
                if reorden_recomendado > 0 and max_diff_pct >= 0:
                    diff_pct = (abs(new_reorden - reorden_recomendado) / reorden_recomendado) * Decimal("100")
                    if diff_pct > max_diff_pct:
                        messages.error(
                            request,
                            (
                                "El punto de reorden manual difiere demasiado del recomendado "
                                f"({diff_pct:.2f}% > {max_diff_pct:.2f}%). "
                                f"Recomendado: {reorden_recomendado}."
                            ),
                        )
                        return redirect("inventario:existencias")
            else:
                new_reorden = reorden_recomendado
                reorden_auto = True
            if (
                new_stock < 0
                or new_reorden < 0
                or new_minimo < 0
                or new_maximo < 0
                or new_inv_prom < 0
                or new_dias < 0
                or new_consumo < 0
            ):
                messages.error(request, "Los indicadores de inventario no pueden ser negativos.")
                return redirect("inventario:existencias")

            existencia.stock_actual = new_stock
            existencia.punto_reorden = new_reorden
            existencia.stock_minimo = new_minimo
            existencia.stock_maximo = new_maximo
            existencia.inventario_promedio = new_inv_prom
            existencia.dias_llegada_pedido = new_dias
            existencia.consumo_diario_promedio = new_consumo
            existencia.actualizado_en = timezone.now()
            existencia.save()
            if reorden_auto:
                messages.info(
                    request,
                    f"Punto de reorden calculado automáticamente: {new_reorden} (según fórmula activa).",
                )
            log_event(
                request.user,
                "UPDATE",
                "inventario.ExistenciaInsumo",
                existencia.id,
                {
                    "insumo_id": existencia.insumo_id,
                    "from_stock": str(prev_stock),
                    "to_stock": str(existencia.stock_actual),
                    "from_reorden": str(prev_reorden),
                    "to_reorden": str(existencia.punto_reorden),
                    "from_stock_minimo": str(prev_minimo),
                    "to_stock_minimo": str(existencia.stock_minimo),
                    "from_stock_maximo": str(prev_maximo),
                    "to_stock_maximo": str(existencia.stock_maximo),
                    "from_inventario_promedio": str(prev_inv_prom),
                    "to_inventario_promedio": str(existencia.inventario_promedio),
                    "from_dias_llegada_pedido": prev_dias,
                    "to_dias_llegada_pedido": existencia.dias_llegada_pedido,
                    "from_consumo_diario_promedio": str(prev_consumo),
                    "to_consumo_diario_promedio": str(existencia.consumo_diario_promedio),
                },
            )
        return redirect("inventario:existencias")

    existencias_rows = list(ExistenciaInsumo.objects.select_related("insumo", "insumo__unidad_base")[:200])
    formula_mode = getattr(settings, "INVENTARIO_REORDER_FORMULA", FORMULA_EXCEL_LEGACY)
    for e in existencias_rows:
        recomendado = calcular_punto_reorden(
            stock_minimo=e.stock_minimo,
            dias_llegada_pedido=e.dias_llegada_pedido,
            consumo_diario_promedio=e.consumo_diario_promedio,
            formula=formula_mode,
        )
        e.punto_reorden_recomendado = recomendado
        e.punto_reorden_diferencia = e.punto_reorden - recomendado

    context = {
        "existencias": existencias_rows,
        "insumos": Insumo.objects.filter(activo=True).order_by("nombre")[:200],
        "can_manage_inventario": can_manage_inventario(request.user),
        "reorder_formula_mode": formula_mode,
        "reorder_max_diff_pct": _inventario_reorder_max_diff_pct(),
        "formula_excel_legacy": FORMULA_EXCEL_LEGACY,
        "formula_leadtime_plus_safety": FORMULA_LEADTIME_PLUS_SAFETY,
    }
    return render(request, "inventario/existencias.html", context)


@login_required
def movimientos(request: HttpRequest) -> HttpResponse:
    if not can_view_inventario(request.user):
        raise PermissionDenied("No tienes permisos para ver Inventario.")

    if request.method == "POST":
        if not can_manage_inventario(request.user):
            raise PermissionDenied("No tienes permisos para registrar movimientos.")
        insumo_id = request.POST.get("insumo_id")
        if insumo_id:
            tipo = request.POST.get("tipo") or MovimientoInventario.TIPO_ENTRADA
            if tipo == MovimientoInventario.TIPO_AJUSTE:
                messages.error(request, "El tipo AJUSTE se genera automáticamente desde la pantalla de ajustes.")
                return redirect("inventario:movimientos")

            cantidad = _to_decimal(request.POST.get("cantidad"), "0")
            if cantidad <= 0:
                messages.error(request, "La cantidad del movimiento debe ser mayor a cero.")
                return redirect("inventario:movimientos")

            existencia, _ = ExistenciaInsumo.objects.get_or_create(insumo_id=insumo_id)
            if tipo in {MovimientoInventario.TIPO_SALIDA, MovimientoInventario.TIPO_CONSUMO} and existencia.stock_actual < cantidad:
                messages.error(
                    request,
                    f"Stock insuficiente para {tipo.lower()}: disponible={existencia.stock_actual}, solicitado={cantidad}.",
                )
                return redirect("inventario:movimientos")

            movimiento = MovimientoInventario.objects.create(
                fecha=request.POST.get("fecha") or timezone.now(),
                tipo=tipo,
                insumo_id=insumo_id,
                cantidad=cantidad,
                referencia=request.POST.get("referencia", "").strip(),
            )
            _apply_movimiento(movimiento)
            log_event(
                request.user,
                "CREATE",
                "inventario.MovimientoInventario",
                movimiento.id,
                {
                    "tipo": movimiento.tipo,
                    "insumo_id": movimiento.insumo_id,
                    "cantidad": str(movimiento.cantidad),
                    "referencia": movimiento.referencia,
                },
            )
        return redirect("inventario:movimientos")

    existencias_by_insumo = {
        row["insumo_id"]: row["stock_actual"]
        for row in ExistenciaInsumo.objects.values("insumo_id", "stock_actual")
    }
    insumo_options = [
        {
            "id": i.id,
            "nombre": i.nombre,
            "stock": existencias_by_insumo.get(i.id, Decimal("0")),
        }
        for i in Insumo.objects.filter(activo=True).order_by("nombre")[:200]
    ]

    context = {
        "movimientos": MovimientoInventario.objects.select_related("insumo")[:100],
        "insumo_options": insumo_options,
        "tipo_choices": [
            (value, label)
            for value, label in MovimientoInventario.TIPO_CHOICES
            if value != MovimientoInventario.TIPO_AJUSTE
        ],
        "can_manage_inventario": can_manage_inventario(request.user),
    }
    return render(request, "inventario/movimientos.html", context)


@login_required
def ajustes(request: HttpRequest) -> HttpResponse:
    if not can_view_inventario(request.user):
        raise PermissionDenied("No tienes permisos para ver Inventario.")

    if request.method == "POST":
        if not can_manage_inventario(request.user):
            raise PermissionDenied("No tienes permisos para registrar ajustes.")
        insumo_id = request.POST.get("insumo_id")
        if insumo_id:
            cantidad_sistema = _to_decimal(request.POST.get("cantidad_sistema"), "0")
            cantidad_fisica = _to_decimal(request.POST.get("cantidad_fisica"), "0")
            if cantidad_sistema < 0 or cantidad_fisica < 0:
                messages.error(request, "Las cantidades del ajuste no pueden ser negativas.")
                return redirect("inventario:ajustes")

            ajuste = AjusteInventario.objects.create(
                insumo_id=insumo_id,
                cantidad_sistema=cantidad_sistema,
                cantidad_fisica=cantidad_fisica,
                motivo=request.POST.get("motivo", "").strip() or "Sin motivo",
                estatus=request.POST.get("estatus") or AjusteInventario.STATUS_PENDIENTE,
            )
            log_event(
                request.user,
                "CREATE",
                "inventario.AjusteInventario",
                ajuste.id,
                {
                    "folio": ajuste.folio,
                    "insumo_id": ajuste.insumo_id,
                    "cantidad_sistema": str(ajuste.cantidad_sistema),
                    "cantidad_fisica": str(ajuste.cantidad_fisica),
                    "estatus": ajuste.estatus,
                },
            )

            if ajuste.estatus == AjusteInventario.STATUS_APLICADO:
                existencia, _ = ExistenciaInsumo.objects.get_or_create(insumo_id=ajuste.insumo_id)
                prev_stock = existencia.stock_actual
                existencia.stock_actual = ajuste.cantidad_fisica
                existencia.actualizado_en = timezone.now()
                existencia.save()
                log_event(
                    request.user,
                    "APPLY",
                    "inventario.ExistenciaInsumo",
                    existencia.id,
                    {
                        "source": ajuste.folio,
                        "insumo_id": ajuste.insumo_id,
                        "from_stock": str(prev_stock),
                        "to_stock": str(existencia.stock_actual),
                    },
                )

                delta = ajuste.cantidad_fisica - ajuste.cantidad_sistema
                if delta != 0:
                    movimiento_ajuste = MovimientoInventario.objects.create(
                        tipo=(
                            MovimientoInventario.TIPO_ENTRADA
                            if delta > 0
                            else MovimientoInventario.TIPO_SALIDA
                        ),
                        insumo_id=ajuste.insumo_id,
                        cantidad=abs(delta),
                        referencia=ajuste.folio,
                    )
                    log_event(
                        request.user,
                        "CREATE",
                        "inventario.MovimientoInventario",
                        movimiento_ajuste.id,
                        {
                            "tipo": movimiento_ajuste.tipo,
                            "insumo_id": movimiento_ajuste.insumo_id,
                            "cantidad": str(movimiento_ajuste.cantidad),
                            "referencia": movimiento_ajuste.referencia,
                        },
                    )
        return redirect("inventario:ajustes")

    context = {
        "ajustes": AjusteInventario.objects.select_related("insumo")[:100],
        "insumos": Insumo.objects.filter(activo=True).order_by("nombre")[:200],
        "status_choices": AjusteInventario.STATUS_CHOICES,
        "can_manage_inventario": can_manage_inventario(request.user),
    }
    return render(request, "inventario/ajustes.html", context)


@login_required
def alertas(request: HttpRequest) -> HttpResponse:
    if not can_view_inventario(request.user):
        raise PermissionDenied("No tienes permisos para ver Inventario.")

    nivel = (request.GET.get("nivel") or "alerta").lower()
    valid_levels = {"alerta", "critico", "bajo", "ok", "all"}
    if nivel not in valid_levels:
        nivel = "alerta"

    existencias = list(
        ExistenciaInsumo.objects.select_related("insumo", "insumo__unidad_base").order_by("insumo__nombre")[:500]
    )

    rows = []
    criticos_count = 0
    bajo_reorden_count = 0
    ok_count = 0

    for e in existencias:
        stock = e.stock_actual
        reorden = e.punto_reorden
        diferencia = stock - reorden
        if stock <= 0:
            nivel_row = "critico"
            etiqueta = "Sin stock"
            criticos_count += 1
        elif stock < reorden:
            nivel_row = "bajo"
            etiqueta = "Bajo reorden"
            bajo_reorden_count += 1
        else:
            nivel_row = "ok"
            etiqueta = "Stock suficiente"
            ok_count += 1

        include = False
        if nivel == "all":
            include = True
        elif nivel == "alerta":
            include = nivel_row in {"critico", "bajo"}
        elif nivel == nivel_row:
            include = True

        if include:
            e.alerta_nivel = nivel_row
            e.alerta_etiqueta = etiqueta
            e.alerta_diferencia = diferencia
            rows.append(e)

    context = {
        "rows": rows,
        "nivel": nivel,
        "criticos_count": criticos_count,
        "bajo_reorden_count": bajo_reorden_count,
        "ok_count": ok_count,
        "total_count": len(existencias),
    }
    return render(request, "inventario/alertas.html", context)
