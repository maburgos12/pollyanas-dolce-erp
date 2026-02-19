import os
import subprocess
import sys
from collections import defaultdict
from decimal import Decimal, InvalidOperation
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

from core.access import can_manage_inventario, can_view_inventario
from core.audit import log_event
from maestros.models import Insumo, InsumoAlias
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

from .models import AjusteInventario, AlmacenSyncRun, ExistenciaInsumo, MovimientoInventario


SOURCE_TO_FILENAME = {
    "inventario": INVENTARIO_FILE,
    "entradas": ENTRADAS_FILE,
    "salidas": SALIDAS_FILE,
    "merma": MERMA_FILE,
}

FILENAME_TO_SOURCE = {v: k for k, v in SOURCE_TO_FILENAME.items()}


def _to_decimal(value: str, default: str = "0") -> Decimal:
    try:
        return Decimal(value or default)
    except (InvalidOperation, TypeError):
        return Decimal(default)


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
    pending = request.session.get("inventario_pending_preview")
    if not pending:
        return
    request.session["inventario_pending_preview"] = [
        row
        for row in pending
        if normalizar_nombre(str(row.get("nombre_origen") or "")) != alias_norm
    ]


def _remove_pending_name_from_recent_runs(alias_norm: str, max_runs: int = 20) -> None:
    runs = AlmacenSyncRun.objects.only("id", "pending_preview").order_by("-started_at")[:max_runs]
    for run in runs:
        pending = list(getattr(run, "pending_preview", []) or [])
        filtered = [
            row
            for row in pending
            if normalizar_nombre(str((row or {}).get("nombre_origen") or "")) != alias_norm
        ]
        if len(filtered) != len(pending):
            run.pending_preview = filtered
            run.save(update_fields=["pending_preview"])
            return


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
                    else:
                        messages.error(request, "El nombre origen no es válido.")

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
                    updated = InsumoAlias.objects.filter(id__in=alias_ids).exclude(insumo=insumo).update(insumo=insumo)
                    messages.success(request, f"Aliases reasignados a {insumo.nombre}: {updated}.")

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
    for run in latest_runs:
        if hidden_run_id and run.id == hidden_run_id:
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

    pending_grouped = sorted(
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

    context = {
        "q": q,
        "page": page,
        "pending_preview": pending_preview,
        "pending_grouped": pending_grouped[:80],
        "pending_source": "session" if session_pending else ("persisted" if persisted_pending else ""),
        "latest_pending_run": latest_pending_run,
        "latest_sync": latest_sync,
        "matching_summary": {
            "runs_count": len(recent_runs),
            "ok_runs": ok_runs,
            "total_matched": total_matched,
            "total_unmatched": total_unmatched,
            "match_rate": match_rate,
        },
        "insumo_alias_targets": Insumo.objects.filter(activo=True).order_by("nombre")[:1200],
        "can_manage_inventario": can_manage_inventario(request.user),
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
            new_reorden = _to_decimal(request.POST.get("punto_reorden"), "0")
            new_minimo = _to_decimal(request.POST.get("stock_minimo"), "0")
            new_maximo = _to_decimal(request.POST.get("stock_maximo"), "0")
            new_inv_prom = _to_decimal(request.POST.get("inventario_promedio"), "0")
            new_dias = int(_to_decimal(request.POST.get("dias_llegada_pedido"), "0"))
            new_consumo = _to_decimal(request.POST.get("consumo_diario_promedio"), "0")
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

    context = {
        "existencias": ExistenciaInsumo.objects.select_related("insumo", "insumo__unidad_base")[:200],
        "insumos": Insumo.objects.filter(activo=True).order_by("nombre")[:200],
        "can_manage_inventario": can_manage_inventario(request.user),
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
