import csv
from datetime import timedelta
from decimal import Decimal
from io import BytesIO

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Count, Q, Sum
from django.db.models.functions import Lower
from django.http import HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from openpyxl import Workbook

from core.access import can_manage_inventario, can_view_inventario
from core.audit import log_event
from maestros.models import Proveedor

from .models import Activo, BitacoraMantenimiento, OrdenMantenimiento, PlanMantenimiento
from .utils.bitacora_import import import_bitacora


def _safe_decimal(value) -> Decimal:
    try:
        return Decimal(str(value or 0))
    except Exception:
        return Decimal("0")


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(str(value or "").strip())
    except Exception:
        return default


def _parse_date(value):
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return timezone.datetime.fromisoformat(raw).date()
    except Exception:
        return None


def _module_tabs(active: str) -> list[dict]:
    return [
        {"label": "Dashboard", "url_name": "activos:dashboard", "active": active == "dashboard"},
        {"label": "Activos", "url_name": "activos:activos", "active": active == "activos"},
        {"label": "Planes", "url_name": "activos:planes", "active": active == "planes"},
        {"label": "Órdenes", "url_name": "activos:ordenes", "active": active == "ordenes"},
        {"label": "Reportes", "url_name": "activos:reportes", "active": active == "reportes"},
        {"label": "Calendario", "url_name": "activos:calendario", "active": active == "calendario"},
    ]


def _prioridad_por_criticidad(criticidad: str) -> str:
    if criticidad == Activo.CRITICIDAD_ALTA:
        return OrdenMantenimiento.PRIORIDAD_ALTA
    if criticidad == Activo.CRITICIDAD_BAJA:
        return OrdenMantenimiento.PRIORIDAD_BAJA
    return OrdenMantenimiento.PRIORIDAD_MEDIA


def _build_activos_depuracion_rows(activos_rows: list[Activo], *, all_name_counts: dict[str, int]) -> list[dict]:
    suspect_exact = {
        "matriz",
        "crucero",
        "colosio",
        "payan",
        "n i o",
        "nio",
        "tunel",
        "leyva",
        "logistica",
    }
    dep_rows = []
    for activo in activos_rows:
        nombre = (activo.nombre or "").strip()
        nombre_norm = nombre.lower()
        notas = (activo.notas or "").strip()

        motivos = []
        acciones = []
        if not notas:
            motivos.append("Sin detalle técnico (marca/modelo/serie)")
            acciones.append("Completar notas con marca, modelo y serie")
        if all_name_counts.get(nombre_norm, 0) > 1:
            motivos.append("Nombre duplicado entre activos")
            acciones.append("Estandarizar nombre con sufijo de ubicación o código interno")
        if nombre_norm in suspect_exact:
            motivos.append("Nombre parece ubicación/departamento, no equipo")
            acciones.append("Renombrar con nombre real del equipo")
        if (activo.categoria or "").strip().lower() == "equipos":
            motivos.append("Categoría genérica")
            acciones.append("Reclasificar categoría operativa")
        if len(nombre.split()) <= 1 and len(nombre) <= 8:
            motivos.append("Nombre muy corto o ambiguo")
            acciones.append("Usar nombre descriptivo del activo")

        if motivos:
            dep_rows.append(
                {
                    "codigo": activo.codigo or "",
                    "nombre": nombre,
                    "ubicacion": (activo.ubicacion or "").strip(),
                    "categoria": (activo.categoria or "").strip(),
                    "estado": activo.estado,
                    "notas": notas,
                    "motivos": " | ".join(dict.fromkeys(motivos)),
                    "acciones_sugeridas": " | ".join(dict.fromkeys(acciones)),
                }
            )
    return dep_rows


def _export_activos_depuracion_csv(rows: list[dict]) -> HttpResponse:
    timestamp = timezone.localtime().strftime("%Y%m%d_%H%M")
    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="activos_pendientes_depuracion_{timestamp}.csv"'
    writer = csv.writer(response)
    headers = ["codigo", "nombre", "ubicacion", "categoria", "estado", "notas", "motivos", "acciones_sugeridas"]
    writer.writerow(headers)
    for row in rows:
        writer.writerow([row.get(h, "") for h in headers])
    return response


def _export_activos_depuracion_xlsx(rows: list[dict]) -> HttpResponse:
    timestamp = timezone.localtime().strftime("%Y%m%d_%H%M")
    wb = Workbook()
    ws = wb.active
    ws.title = "pendientes_depuracion"
    headers = ["codigo", "nombre", "ubicacion", "categoria", "estado", "notas", "motivos", "acciones_sugeridas"]
    ws.append(headers)
    for row in rows:
        ws.append([row.get(h, "") for h in headers])
    ws.column_dimensions["A"].width = 16
    ws.column_dimensions["B"].width = 32
    ws.column_dimensions["C"].width = 24
    ws.column_dimensions["D"].width = 18
    ws.column_dimensions["E"].width = 16
    ws.column_dimensions["F"].width = 48
    ws.column_dimensions["G"].width = 48
    ws.column_dimensions["H"].width = 48

    output = BytesIO()
    wb.save(output)
    output.seek(0)
    response = HttpResponse(
        output.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="activos_pendientes_depuracion_{timestamp}.xlsx"'
    return response


@login_required
def dashboard(request):
    if not can_view_inventario(request.user):
        raise PermissionDenied("No tienes permisos para ver Activos.")

    today = timezone.localdate()
    week_limit = today + timedelta(days=7)
    month_limit = today + timedelta(days=30)

    activos_qs = Activo.objects.filter(activo=True)
    ordenes_abiertas_qs = OrdenMantenimiento.objects.filter(
        estatus__in=[OrdenMantenimiento.ESTATUS_PENDIENTE, OrdenMantenimiento.ESTATUS_EN_PROCESO]
    )
    planes_activos_qs = PlanMantenimiento.objects.filter(estatus=PlanMantenimiento.ESTATUS_ACTIVO, activo=True)

    proximos = list(
        planes_activos_qs.filter(proxima_ejecucion__isnull=False, proxima_ejecucion__lte=month_limit)
        .select_related("activo_ref")
        .order_by("proxima_ejecucion", "id")[:30]
    )
    ordenes_recientes = list(
        OrdenMantenimiento.objects.select_related("activo_ref", "plan_ref")
        .order_by("-fecha_programada", "-id")[:20]
    )

    costo_mes = (
        OrdenMantenimiento.objects.filter(
            fecha_cierre__year=today.year,
            fecha_cierre__month=today.month,
            estatus=OrdenMantenimiento.ESTATUS_CERRADA,
        ).aggregate(
            rep=Sum("costo_repuestos"),
            mo=Sum("costo_mano_obra"),
            otros=Sum("costo_otros"),
        )
    )
    costo_mes_total = _safe_decimal(costo_mes.get("rep")) + _safe_decimal(costo_mes.get("mo")) + _safe_decimal(
        costo_mes.get("otros")
    )

    criticidad_rows = list(
        activos_qs.values("criticidad")
        .annotate(total=Count("id"))
        .order_by()
    )
    criticidad = {
        "ALTA": 0,
        "MEDIA": 0,
        "BAJA": 0,
    }
    for row in criticidad_rows:
        criticidad[row["criticidad"]] = int(row["total"] or 0)

    context = {
        "module_tabs": _module_tabs("dashboard"),
        "activos_total": activos_qs.count(),
        "activos_operativos": activos_qs.filter(estado=Activo.ESTADO_OPERATIVO).count(),
        "activos_mantenimiento": activos_qs.filter(estado=Activo.ESTADO_MANTENIMIENTO).count(),
        "activos_fuera_servicio": activos_qs.filter(estado=Activo.ESTADO_FUERA_SERVICIO).count(),
        "ordenes_abiertas": ordenes_abiertas_qs.count(),
        "ordenes_en_proceso": ordenes_abiertas_qs.filter(estatus=OrdenMantenimiento.ESTATUS_EN_PROCESO).count(),
        "planes_vencidos": planes_activos_qs.filter(proxima_ejecucion__lt=today).count(),
        "planes_proxima_semana": planes_activos_qs.filter(
            proxima_ejecucion__gte=today,
            proxima_ejecucion__lte=week_limit,
        ).count(),
        "costo_mes_total": costo_mes_total,
        "criticidad": criticidad,
        "proximos": proximos,
        "ordenes_recientes": ordenes_recientes,
    }
    return render(request, "activos/dashboard.html", context)


@login_required
def activos_catalog(request):
    if not can_view_inventario(request.user):
        raise PermissionDenied("No tienes permisos para ver Activos.")

    if request.method == "POST":
        if not can_manage_inventario(request.user):
            raise PermissionDenied("No tienes permisos para gestionar Activos.")
        action = (request.POST.get("action") or "create_activo").strip().lower()
        if action == "create_activo":
            nombre = (request.POST.get("nombre") or "").strip()
            if not nombre:
                messages.error(request, "Nombre del activo es obligatorio.")
                return redirect("activos:activos")
            estado = (request.POST.get("estado") or Activo.ESTADO_OPERATIVO).strip().upper()
            criticidad = (request.POST.get("criticidad") or Activo.CRITICIDAD_MEDIA).strip().upper()
            proveedor_id = _safe_int(request.POST.get("proveedor_mantenimiento_id"))
            activo = Activo.objects.create(
                nombre=nombre,
                categoria=(request.POST.get("categoria") or "").strip(),
                ubicacion=(request.POST.get("ubicacion") or "").strip(),
                estado=estado if estado in {x[0] for x in Activo.ESTADO_CHOICES} else Activo.ESTADO_OPERATIVO,
                criticidad=(
                    criticidad if criticidad in {x[0] for x in Activo.CRITICIDAD_CHOICES} else Activo.CRITICIDAD_MEDIA
                ),
                proveedor_mantenimiento_id=proveedor_id if proveedor_id > 0 else None,
                fecha_alta=_parse_date(request.POST.get("fecha_alta")) or timezone.localdate(),
                valor_reposicion=_safe_decimal(request.POST.get("valor_reposicion")),
                vida_util_meses=max(1, _safe_int(request.POST.get("vida_util_meses"), default=60)),
                horas_uso_promedio_mes=_safe_decimal(request.POST.get("horas_uso_promedio_mes")),
                notas=(request.POST.get("notas") or "").strip(),
                activo=(request.POST.get("activo") or "").strip().lower() in {"1", "on", "true", "yes"},
            )
            log_event(
                request.user,
                "CREATE",
                "activos.Activo",
                activo.id,
                {"codigo": activo.codigo, "nombre": activo.nombre, "estado": activo.estado},
            )
            messages.success(request, f"Activo {activo.codigo} creado.")
            return redirect("activos:activos")

        if action == "set_estado":
            activo_id = _safe_int(request.POST.get("activo_id"))
            activo_obj = get_object_or_404(Activo, pk=activo_id)
            estado = (request.POST.get("estado") or "").strip().upper()
            if estado in {x[0] for x in Activo.ESTADO_CHOICES} and estado != activo_obj.estado:
                from_estado = activo_obj.estado
                activo_obj.estado = estado
                activo_obj.save(update_fields=["estado", "actualizado_en"])
                log_event(
                    request.user,
                    "UPDATE",
                    "activos.Activo",
                    activo_obj.id,
                    {"from_estado": from_estado, "to_estado": estado},
                )
                messages.success(request, f"Estado actualizado: {activo_obj.codigo}.")
            return redirect("activos:activos")

        if action == "toggle_activo":
            activo_id = _safe_int(request.POST.get("activo_id"))
            activo_obj = get_object_or_404(Activo, pk=activo_id)
            activo_obj.activo = not activo_obj.activo
            activo_obj.save(update_fields=["activo", "actualizado_en"])
            log_event(
                request.user,
                "UPDATE",
                "activos.Activo",
                activo_obj.id,
                {"activo": activo_obj.activo},
            )
            messages.success(request, f"{activo_obj.codigo}: {'Activo' if activo_obj.activo else 'Inactivo'}.")
            return redirect("activos:activos")

        if action == "import_bitacora":
            archivo = request.FILES.get("archivo_bitacora")
            if not archivo:
                messages.error(request, "Selecciona un archivo XLSX para importar.")
                return redirect("activos:activos")
            is_dry_run = (request.POST.get("dry_run") or "").strip().lower() in {"1", "on", "true", "yes"}
            skip_servicios = (request.POST.get("skip_servicios") or "").strip().lower() in {"1", "on", "true", "yes"}
            try:
                stats = import_bitacora(
                    archivo,
                    sheet_name=(request.POST.get("sheet_name") or "").strip(),
                    dry_run=is_dry_run,
                    skip_servicios=skip_servicios,
                )
            except ValueError as exc:
                messages.error(request, str(exc))
                return redirect("activos:activos")
            except Exception:
                messages.error(request, "No se pudo procesar el archivo. Verifica formato de hoja y columnas B-I.")
                return redirect("activos:activos")

            mode_label = "simulación (sin guardar)" if is_dry_run else "importación aplicada"
            messages.success(
                request,
                (
                    f"Bitácora procesada ({mode_label}): filas válidas {stats['filas_validas']}, "
                    f"activos creados {stats['activos_creados']}, actualizados {stats['activos_actualizados']}, "
                    f"servicios creados {stats['servicios_creados']}, omitidos {stats['servicios_omitidos']}."
                ),
            )
            return redirect("activos:activos")

        messages.error(request, "Acción no reconocida.")
        return redirect("activos:activos")

    q = (request.GET.get("q") or "").strip()
    estado = (request.GET.get("estado") or "").strip().upper()
    criticidad = (request.GET.get("criticidad") or "").strip().upper()
    solo_activos = (request.GET.get("solo_activos") or "1").strip()

    qs = Activo.objects.select_related("proveedor_mantenimiento").order_by("nombre", "id")
    if q:
        qs = qs.filter(Q(codigo__icontains=q) | Q(nombre__icontains=q) | Q(categoria__icontains=q) | Q(ubicacion__icontains=q))
    if estado in {x[0] for x in Activo.ESTADO_CHOICES}:
        qs = qs.filter(estado=estado)
    if criticidad in {x[0] for x in Activo.CRITICIDAD_CHOICES}:
        qs = qs.filter(criticidad=criticidad)
    if solo_activos == "1":
        qs = qs.filter(activo=True)

    export_format = (request.GET.get("export") or "").strip().lower()
    if export_format in {"depuracion_csv", "depuracion_xlsx"}:
        all_name_counts = {
            (row["nombre_lower"] or ""): int(row["total"] or 0)
            for row in Activo.objects.filter(activo=True)
            .annotate(nombre_lower=Lower("nombre"))
            .values("nombre_lower")
            .annotate(total=Count("id"))
        }
        dep_rows = _build_activos_depuracion_rows(list(qs), all_name_counts=all_name_counts)
        if export_format == "depuracion_csv":
            return _export_activos_depuracion_csv(dep_rows)
        return _export_activos_depuracion_xlsx(dep_rows)

    context = {
        "module_tabs": _module_tabs("activos"),
        "activos": list(qs[:300]),
        "proveedores": list(Proveedor.objects.filter(activo=True).order_by("nombre")[:800]),
        "estado_choices": Activo.ESTADO_CHOICES,
        "criticidad_choices": Activo.CRITICIDAD_CHOICES,
        "filters": {"q": q, "estado": estado, "criticidad": criticidad, "solo_activos": solo_activos},
        "can_manage_activos": can_manage_inventario(request.user),
    }
    return render(request, "activos/activos.html", context)


@login_required
def planes(request):
    if not can_view_inventario(request.user):
        raise PermissionDenied("No tienes permisos para ver Activos.")

    if request.method == "POST":
        if not can_manage_inventario(request.user):
            raise PermissionDenied("No tienes permisos para gestionar planes.")
        action = (request.POST.get("action") or "create_plan").strip().lower()
        if action == "create_plan":
            activo_id = _safe_int(request.POST.get("activo_id"))
            nombre = (request.POST.get("nombre") or "").strip()
            if not activo_id or not nombre:
                messages.error(request, "Activo y nombre del plan son obligatorios.")
                return redirect("activos:planes")
            activo_obj = get_object_or_404(Activo, pk=activo_id)
            tipo = (request.POST.get("tipo") or PlanMantenimiento.TIPO_PREVENTIVO).strip().upper()
            estatus = (request.POST.get("estatus") or PlanMantenimiento.ESTATUS_ACTIVO).strip().upper()
            plan = PlanMantenimiento.objects.create(
                activo_ref=activo_obj,
                nombre=nombre,
                tipo=tipo if tipo in {x[0] for x in PlanMantenimiento.TIPO_CHOICES} else PlanMantenimiento.TIPO_PREVENTIVO,
                frecuencia_dias=max(1, _safe_int(request.POST.get("frecuencia_dias"), default=30)),
                tolerancia_dias=max(0, _safe_int(request.POST.get("tolerancia_dias"), default=0)),
                ultima_ejecucion=_parse_date(request.POST.get("ultima_ejecucion")),
                proxima_ejecucion=_parse_date(request.POST.get("proxima_ejecucion")),
                responsable=(request.POST.get("responsable") or "").strip(),
                instrucciones=(request.POST.get("instrucciones") or "").strip(),
                estatus=(
                    estatus if estatus in {x[0] for x in PlanMantenimiento.ESTATUS_CHOICES} else PlanMantenimiento.ESTATUS_ACTIVO
                ),
                activo=(request.POST.get("activo") or "").strip().lower() in {"1", "on", "true", "yes"},
            )
            log_event(
                request.user,
                "CREATE",
                "activos.PlanMantenimiento",
                plan.id,
                {"activo_id": plan.activo_ref_id, "nombre": plan.nombre, "proxima_ejecucion": str(plan.proxima_ejecucion or "")},
            )
            messages.success(request, f"Plan creado para {activo_obj.nombre}.")
            return redirect("activos:planes")

        if action == "toggle_plan":
            plan_id = _safe_int(request.POST.get("plan_id"))
            plan = get_object_or_404(PlanMantenimiento, pk=plan_id)
            plan.estatus = (
                PlanMantenimiento.ESTATUS_PAUSADO
                if plan.estatus == PlanMantenimiento.ESTATUS_ACTIVO
                else PlanMantenimiento.ESTATUS_ACTIVO
            )
            plan.save(update_fields=["estatus", "actualizado_en"])
            log_event(request.user, "UPDATE", "activos.PlanMantenimiento", plan.id, {"estatus": plan.estatus})
            messages.success(request, f"Plan {plan.nombre} actualizado.")
            return redirect("activos:planes")

        if action == "registrar_ejecucion":
            plan_id = _safe_int(request.POST.get("plan_id"))
            plan = get_object_or_404(PlanMantenimiento, pk=plan_id)
            fecha = _parse_date(request.POST.get("fecha")) or timezone.localdate()
            plan.ultima_ejecucion = fecha
            plan.recompute_next_date()
            plan.save(update_fields=["ultima_ejecucion", "proxima_ejecucion", "actualizado_en"])
            log_event(
                request.user,
                "UPDATE",
                "activos.PlanMantenimiento",
                plan.id,
                {"ultima_ejecucion": str(fecha), "proxima_ejecucion": str(plan.proxima_ejecucion or "")},
            )
            messages.success(request, f"Ejecución registrada para {plan.nombre}.")
            return redirect("activos:planes")

        if action == "generar_ordenes_programadas":
            scope = (request.POST.get("scope") or "overdue").strip().lower()
            dry_run = (request.POST.get("dry_run") or "").strip().lower() in {"1", "on", "true", "yes"}
            today = timezone.localdate()
            if scope == "week":
                plan_qs = PlanMantenimiento.objects.filter(
                    estatus=PlanMantenimiento.ESTATUS_ACTIVO,
                    activo=True,
                    proxima_ejecucion__isnull=False,
                    proxima_ejecucion__gte=today,
                    proxima_ejecucion__lte=today + timedelta(days=7),
                )
            else:
                plan_qs = PlanMantenimiento.objects.filter(
                    estatus=PlanMantenimiento.ESTATUS_ACTIVO,
                    activo=True,
                    proxima_ejecucion__isnull=False,
                    proxima_ejecucion__lte=today,
                )

            created = 0
            skipped = 0
            plan_qs = plan_qs.select_related("activo_ref").order_by("proxima_ejecucion", "id")
            for plan in plan_qs:
                if not plan.activo_ref or not plan.activo_ref.activo:
                    skipped += 1
                    continue

                exists = OrdenMantenimiento.objects.filter(
                    plan_ref=plan,
                    fecha_programada=plan.proxima_ejecucion,
                ).exclude(estatus=OrdenMantenimiento.ESTATUS_CANCELADA).exists()
                if exists:
                    skipped += 1
                    continue

                if dry_run:
                    created += 1
                    continue

                orden = OrdenMantenimiento.objects.create(
                    activo_ref=plan.activo_ref,
                    plan_ref=plan,
                    tipo=OrdenMantenimiento.TIPO_PREVENTIVO,
                    prioridad=_prioridad_por_criticidad(plan.activo_ref.criticidad),
                    estatus=OrdenMantenimiento.ESTATUS_PENDIENTE,
                    fecha_programada=plan.proxima_ejecucion or today,
                    responsable=plan.responsable or "",
                    descripcion=f"Orden preventiva automática desde plan: {plan.nombre}",
                    creado_por=request.user if request.user.is_authenticated else None,
                )
                BitacoraMantenimiento.objects.create(
                    orden=orden,
                    accion="AUTO_PLAN",
                    comentario="Generada automáticamente desde plan activo",
                    usuario=request.user if request.user.is_authenticated else None,
                    costo_adicional=Decimal("0"),
                )
                created += 1
                log_event(
                    request.user,
                    "CREATE",
                    "activos.OrdenMantenimiento",
                    orden.id,
                    {
                        "origen": "plan_auto",
                        "plan_id": plan.id,
                        "fecha_programada": str(plan.proxima_ejecucion or ""),
                        "folio": orden.folio,
                    },
                )

            run_mode = "simulación" if dry_run else "aplicado"
            messages.success(
                request,
                f"Generación de órdenes ({run_mode}): creadas {created}, omitidas {skipped}.",
            )
            return redirect("activos:planes")

        messages.error(request, "Acción no reconocida.")
        return redirect("activos:planes")

    q = (request.GET.get("q") or "").strip()
    estatus = (request.GET.get("estatus") or "").strip().upper()
    scope = (request.GET.get("scope") or "all").strip().lower()
    today = timezone.localdate()

    qs = PlanMantenimiento.objects.select_related("activo_ref").order_by("proxima_ejecucion", "id")
    if q:
        qs = qs.filter(Q(nombre__icontains=q) | Q(activo_ref__nombre__icontains=q) | Q(activo_ref__codigo__icontains=q))
    if estatus in {x[0] for x in PlanMantenimiento.ESTATUS_CHOICES}:
        qs = qs.filter(estatus=estatus)
    if scope == "overdue":
        qs = qs.filter(proxima_ejecucion__isnull=False, proxima_ejecucion__lt=today, estatus=PlanMantenimiento.ESTATUS_ACTIVO)
    elif scope == "week":
        qs = qs.filter(
            proxima_ejecucion__isnull=False,
            proxima_ejecucion__gte=today,
            proxima_ejecucion__lte=today + timedelta(days=7),
            estatus=PlanMantenimiento.ESTATUS_ACTIVO,
        )

    context = {
        "module_tabs": _module_tabs("planes"),
        "planes": list(qs[:300]),
        "activos": list(Activo.objects.filter(activo=True).order_by("nombre")[:800]),
        "tipo_choices": PlanMantenimiento.TIPO_CHOICES,
        "estatus_choices": PlanMantenimiento.ESTATUS_CHOICES,
        "filters": {"q": q, "estatus": estatus, "scope": scope},
        "today": today,
        "can_manage_activos": can_manage_inventario(request.user),
    }
    return render(request, "activos/planes.html", context)


@login_required
def ordenes(request):
    if not can_view_inventario(request.user):
        raise PermissionDenied("No tienes permisos para ver Activos.")

    if request.method == "POST":
        if not can_manage_inventario(request.user):
            raise PermissionDenied("No tienes permisos para gestionar órdenes de mantenimiento.")
        activo_id = (request.POST.get("activo_id") or "").strip()
        plan_id = (request.POST.get("plan_id") or "").strip()
        tipo = (request.POST.get("tipo") or OrdenMantenimiento.TIPO_PREVENTIVO).strip().upper()
        prioridad = (request.POST.get("prioridad") or OrdenMantenimiento.PRIORIDAD_MEDIA).strip().upper()
        descripcion = (request.POST.get("descripcion") or "").strip()
        responsable = (request.POST.get("responsable") or "").strip()
        fecha_programada_raw = (request.POST.get("fecha_programada") or "").strip()
        try:
            fecha_programada = (
                timezone.datetime.fromisoformat(fecha_programada_raw).date()
                if fecha_programada_raw
                else timezone.localdate()
            )
        except ValueError:
            fecha_programada = timezone.localdate()
        if not activo_id.isdigit():
            messages.error(request, "Selecciona un activo válido.")
            return redirect("activos:ordenes")
        activo_obj = get_object_or_404(Activo, pk=int(activo_id))
        plan_obj = None
        if plan_id.isdigit():
            plan_obj = PlanMantenimiento.objects.filter(pk=int(plan_id), activo_ref=activo_obj).first()
        orden = OrdenMantenimiento.objects.create(
            activo_ref=activo_obj,
            plan_ref=plan_obj,
            tipo=tipo if tipo in {x[0] for x in OrdenMantenimiento.TIPO_CHOICES} else OrdenMantenimiento.TIPO_PREVENTIVO,
            prioridad=(
                prioridad
                if prioridad in {x[0] for x in OrdenMantenimiento.PRIORIDAD_CHOICES}
                else OrdenMantenimiento.PRIORIDAD_MEDIA
            ),
            descripcion=descripcion,
            responsable=responsable,
            fecha_programada=fecha_programada,
            creado_por=request.user,
        )
        BitacoraMantenimiento.objects.create(
            orden=orden,
            accion="CREADA",
            comentario="Orden creada desde UI",
            usuario=request.user,
        )
        log_event(
            request.user,
            "CREATE",
            "activos.OrdenMantenimiento",
            orden.id,
            {
                "folio": orden.folio,
                "activo_id": orden.activo_ref_id,
                "tipo": orden.tipo,
                "prioridad": orden.prioridad,
                "estatus": orden.estatus,
            },
        )
        messages.success(request, f"Orden {orden.folio} creada.")
        return redirect("activos:ordenes")

    estado = (request.GET.get("estatus") or "abiertas").strip().upper()
    qs = OrdenMantenimiento.objects.select_related("activo_ref", "plan_ref", "creado_por").order_by("-fecha_programada", "-id")
    if estado == "ABIERTAS":
        qs = qs.filter(estatus__in=[OrdenMantenimiento.ESTATUS_PENDIENTE, OrdenMantenimiento.ESTATUS_EN_PROCESO])
    elif estado in {x[0] for x in OrdenMantenimiento.ESTATUS_CHOICES}:
        qs = qs.filter(estatus=estado)

    context = {
        "module_tabs": _module_tabs("ordenes"),
        "ordenes": list(qs[:120]),
        "activos": list(Activo.objects.filter(activo=True).order_by("nombre")[:800]),
        "planes": list(
            PlanMantenimiento.objects.filter(
                estatus=PlanMantenimiento.ESTATUS_ACTIVO,
                activo=True,
                activo_ref__activo=True,
            )
            .select_related("activo_ref")
            .order_by("activo_ref__nombre", "nombre")[:1200]
        ),
        "estado": estado,
        "can_manage_activos": can_manage_inventario(request.user),
    }
    return render(request, "activos/ordenes.html", context)


@login_required
def actualizar_orden_estatus(request, pk: int, estatus: str):
    if request.method != "POST":
        return redirect("activos:ordenes")
    if not can_manage_inventario(request.user):
        raise PermissionDenied("No tienes permisos para gestionar órdenes de mantenimiento.")

    estatus = (estatus or "").strip().upper()
    if estatus not in {x[0] for x in OrdenMantenimiento.ESTATUS_CHOICES}:
        messages.error(request, "Estatus inválido.")
        return redirect("activos:ordenes")

    orden = get_object_or_404(OrdenMantenimiento, pk=pk)
    from_status = orden.estatus
    if from_status == estatus:
        return redirect("activos:ordenes")
    orden.estatus = estatus
    today = timezone.localdate()
    if estatus == OrdenMantenimiento.ESTATUS_EN_PROCESO and not orden.fecha_inicio:
        orden.fecha_inicio = today
    if estatus == OrdenMantenimiento.ESTATUS_CERRADA:
        orden.fecha_cierre = today
        if orden.plan_ref_id:
            plan = orden.plan_ref
            plan.ultima_ejecucion = today
            plan.recompute_next_date()
            plan.save(update_fields=["ultima_ejecucion", "proxima_ejecucion", "actualizado_en"])
    orden.save(update_fields=["estatus", "fecha_inicio", "fecha_cierre", "actualizado_en"])
    BitacoraMantenimiento.objects.create(
        orden=orden,
        accion="ESTATUS",
        comentario=f"{from_status} -> {estatus}",
        usuario=request.user,
    )
    log_event(
        request.user,
        "UPDATE",
        "activos.OrdenMantenimiento",
        orden.id,
        {"from": from_status, "to": estatus, "folio": orden.folio},
    )
    messages.success(request, f"Orden {orden.folio} actualizada a {estatus}.")
    return redirect("activos:ordenes")


@login_required
def reportes_servicio(request):
    if not can_view_inventario(request.user):
        raise PermissionDenied("No tienes permisos para ver Activos.")

    if request.method == "POST":
        activo_id = _safe_int(request.POST.get("activo_id"))
        descripcion = (request.POST.get("descripcion") or "").strip()
        if not activo_id or not descripcion:
            messages.error(request, "Activo y descripción del reporte son obligatorios.")
            return redirect("activos:reportes")
        activo_obj = get_object_or_404(Activo, pk=activo_id)
        prioridad = (request.POST.get("prioridad") or OrdenMantenimiento.PRIORIDAD_MEDIA).strip().upper()
        prioridad = (
            prioridad if prioridad in {x[0] for x in OrdenMantenimiento.PRIORIDAD_CHOICES} else OrdenMantenimiento.PRIORIDAD_MEDIA
        )
        perfil = getattr(request.user, "userprofile", None)
        area = perfil.departamento.nombre if perfil and perfil.departamento_id else ""
        sucursal = perfil.sucursal.nombre if perfil and perfil.sucursal_id else ""
        responsable = (request.POST.get("responsable") or "").strip() or request.user.get_full_name() or request.user.username
        fecha_programada = _parse_date(request.POST.get("fecha_programada")) or timezone.localdate()
        orden = OrdenMantenimiento.objects.create(
            activo_ref=activo_obj,
            tipo=OrdenMantenimiento.TIPO_CORRECTIVO,
            prioridad=prioridad,
            estatus=OrdenMantenimiento.ESTATUS_PENDIENTE,
            fecha_programada=fecha_programada,
            responsable=responsable,
            descripcion=descripcion,
            creado_por=request.user,
        )
        contexto = []
        if area:
            contexto.append(f"Área: {area}")
        if sucursal:
            contexto.append(f"Sucursal: {sucursal}")
        BitacoraMantenimiento.objects.create(
            orden=orden,
            accion="REPORTE_FALLA",
            comentario=" · ".join(contexto) if contexto else "Reporte desde módulo Activos",
            usuario=request.user,
        )
        log_event(
            request.user,
            "CREATE",
            "activos.OrdenMantenimiento",
            orden.id,
            {"folio": orden.folio, "tipo": "REPORTE_FALLA", "activo_id": activo_obj.id, "prioridad": prioridad},
        )
        messages.success(request, f"Reporte levantado. Orden generada: {orden.folio}.")
        return redirect("activos:reportes")

    estado = (request.GET.get("estatus") or "ABIERTAS").strip().upper()
    q = (request.GET.get("q") or "").strip()
    qs = OrdenMantenimiento.objects.select_related("activo_ref", "creado_por").filter(tipo=OrdenMantenimiento.TIPO_CORRECTIVO)
    if estado == "ABIERTAS":
        qs = qs.filter(estatus__in=[OrdenMantenimiento.ESTATUS_PENDIENTE, OrdenMantenimiento.ESTATUS_EN_PROCESO])
    elif estado in {x[0] for x in OrdenMantenimiento.ESTATUS_CHOICES}:
        qs = qs.filter(estatus=estado)
    if q:
        qs = qs.filter(Q(folio__icontains=q) | Q(activo_ref__nombre__icontains=q) | Q(descripcion__icontains=q))

    today = timezone.localdate()
    reportes = []
    for orden in qs.order_by("-fecha_programada", "-id")[:240]:
        dias = (today - orden.fecha_programada).days if orden.fecha_programada else 0
        if orden.estatus == OrdenMantenimiento.ESTATUS_CERRADA:
            semaforo = ("Verde", "badge-success")
        elif dias <= 2:
            semaforo = ("Verde", "badge-success")
        elif dias <= 5:
            semaforo = ("Amarillo", "badge-warning")
        else:
            semaforo = ("Rojo", "badge-danger")
        reportes.append({"orden": orden, "dias": dias, "semaforo_label": semaforo[0], "semaforo_class": semaforo[1]})

    context = {
        "module_tabs": _module_tabs("reportes"),
        "activos": list(Activo.objects.filter(activo=True).order_by("nombre")[:800]),
        "reportes": reportes,
        "estado": estado,
        "q": q,
        "today": today,
        "prioridad_choices": OrdenMantenimiento.PRIORIDAD_CHOICES,
        "can_manage_activos": can_manage_inventario(request.user),
    }
    return render(request, "activos/reportes.html", context)


@login_required
def calendario(request):
    if not can_view_inventario(request.user):
        raise PermissionDenied("No tienes permisos para ver Activos.")

    date_from_raw = (request.GET.get("from") or "").strip()
    date_to_raw = (request.GET.get("to") or "").strip()
    try:
        date_from = timezone.datetime.fromisoformat(date_from_raw).date() if date_from_raw else timezone.localdate()
    except ValueError:
        date_from = timezone.localdate()
    try:
        date_to = timezone.datetime.fromisoformat(date_to_raw).date() if date_to_raw else (date_from + timedelta(days=45))
    except ValueError:
        date_to = date_from + timedelta(days=45)
    if date_to < date_from:
        date_to = date_from + timedelta(days=45)

    planes = list(
        PlanMantenimiento.objects.select_related("activo_ref")
        .filter(
            activo=True,
            estatus=PlanMantenimiento.ESTATUS_ACTIVO,
            proxima_ejecucion__isnull=False,
            proxima_ejecucion__gte=date_from,
            proxima_ejecucion__lte=date_to,
        )
        .order_by("proxima_ejecucion", "id")
    )
    ordenes_qs = (
        OrdenMantenimiento.objects.select_related("activo_ref", "plan_ref")
        .filter(fecha_programada__gte=date_from, fecha_programada__lte=date_to)
        .order_by("fecha_programada", "id")
    )

    events = []
    for plan in planes:
        events.append(
            {
                "fecha": plan.proxima_ejecucion,
                "tipo": "Plan",
                "referencia": f"Plan #{plan.id}",
                "activo": plan.activo_ref.nombre,
                "detalle": plan.nombre,
                "estado": plan.estatus,
            }
        )
    for orden in ordenes_qs:
        events.append(
            {
                "fecha": orden.fecha_programada,
                "tipo": "Orden",
                "referencia": orden.folio,
                "activo": orden.activo_ref.nombre,
                "detalle": orden.descripcion or orden.get_tipo_display(),
                "estado": orden.estatus,
            }
        )
    events.sort(key=lambda r: (r["fecha"], r["tipo"], r["referencia"]))

    context = {
        "module_tabs": _module_tabs("calendario"),
        "date_from": date_from,
        "date_to": date_to,
        "events": events,
    }
    return render(request, "activos/calendario.html", context)
