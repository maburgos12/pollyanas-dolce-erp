from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.contrib.staticfiles import finders
from django.core.exceptions import PermissionDenied
from django.http import Http404, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.dateparse import parse_date
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import ensure_csrf_cookie

from core.access import can_manage_submodule, can_view_module, can_view_submodule
from core.models import Sucursal

from recetas.utils.normalizacion import normalizar_nombre

from .empleados import empleados_elegibles_bonos_ventas
from .models import BonoVentasEmpleado, CATEGORIAS_PRODUCTO, ConfigBonoVentasPeriodo, VentaCategoriaSucursal
from .services import sync_dias_repartidor, sync_ventas_categorias
from .services_checador import sincronizar_asistencia_desde_checador


CATEGORY_WEIGHT_FIELDS = {
    "GRANDE": "peso_grande",
    "MEDIANO": "peso_mediano",
    "CHICO": "peso_chico",
    "MINI": "peso_mini",
    "VELAS_ACCESORIOS": "peso_velas_accesorios",
    "VASOS": "peso_vasos",
}


def _parse_int(raw: str | None, default: int = 0) -> int:
    try:
        return int(raw or default)
    except (TypeError, ValueError):
        return default


def _parse_decimal(raw: str | None) -> Decimal:
    try:
        return Decimal(str(raw or "0"))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def _dashboard_redirect(mes: int, anio: int):
    return redirect(f"{reverse('bonos_ventas:bonos-ventas-dashboard')}?mes={mes}&anio={anio}")


def _recalcular_periodo(periodo: ConfigBonoVentasPeriodo) -> int:
    total = 0
    for bono in BonoVentasEmpleado.objects.filter(periodo=periodo).select_related("sucursal"):
        bono.recalcular()
        bono.save()
        total += 1
    return total


def _indice_sucursales_por_texto() -> dict[str, "Sucursal"]:
    """Índice de sucursales activas por nombre/código normalizado.

    Permite resolver `empleado.sucursal` (texto libre capturado en RRHH) sin
    depender de igualdad exacta. Es robusto al prefijo 'Sucursal ', a los acentos
    y a los separadores del código. NO emparejar por nombre exacto: el catálogo se
    renombra y rompe el vínculo (ver FASE 1: migrar Empleado.sucursal a FK).
    """
    indice: dict[str, Sucursal] = {}
    for sucursal in Sucursal.objects.filter(activa=True):
        nombre_norm = normalizar_nombre(sucursal.nombre)
        claves = {
            nombre_norm,
            normalizar_nombre(sucursal.codigo),
            normalizar_nombre((sucursal.codigo or "").replace("_", " ")),
        }
        if nombre_norm.startswith("sucursal "):
            claves.add(nombre_norm[len("sucursal "):])
        for clave in claves:
            if clave:
                indice.setdefault(clave, sucursal)
    return indice


def _resolver_sucursal(indice: dict[str, "Sucursal"], sucursal_nombre: str) -> "Sucursal | None":
    objetivo = normalizar_nombre(sucursal_nombre)
    if not objetivo:
        return None
    if objetivo in indice:
        return indice[objetivo]
    if objetivo.startswith("sucursal "):
        return indice.get(objetivo[len("sucursal "):])
    return None


def _inicializar_bonos(periodo: ConfigBonoVentasPeriodo) -> dict[str, object]:
    empleados = empleados_elegibles_bonos_ventas()
    indice = _indice_sucursales_por_texto()
    sucursal_matriz = indice.get("matriz")
    creados = 0
    sin_sucursal = []
    for empleado in empleados:
        sucursal_nombre = (empleado.sucursal or "").strip()
        sucursal = _resolver_sucursal(indice, sucursal_nombre)
        if sucursal is None:
            # Repartidores (y otros sin sucursal resoluble) van a Matriz por defecto
            if sucursal_matriz and (empleado.puesto_operativo or "").strip().upper() == "REPARTIDOR":
                sucursal = sucursal_matriz
            elif not sucursal_nombre:
                sin_sucursal.append(empleado.nombre)
                continue
            else:
                sin_sucursal.append(f"{empleado.nombre} (sucursal desconocida: {sucursal_nombre!r})")
                continue
        _, created = BonoVentasEmpleado.objects.get_or_create(
            periodo=periodo,
            empleado=empleado,
            defaults={"sucursal": sucursal},
        )
        if created:
            creados += 1
    return {"creados": creados, "total_ventas": empleados.count(), "sin_sucursal": sin_sucursal}


@login_required
@never_cache
def bonos_ventas_dashboard(request):
    if not can_view_submodule(request.user, "ventas", "bonos"):
        raise PermissionDenied

    today = timezone.localdate()
    mes = _parse_int(request.GET.get("mes"), today.month)
    anio = _parse_int(request.GET.get("anio"), today.year)
    can_manage = can_manage_submodule(request.user, "ventas", "bonos")

    if request.method == "POST":
        if not can_manage:
            raise PermissionDenied
        action = request.POST.get("action")
        mes = _parse_int(request.POST.get("mes"), mes)
        anio = _parse_int(request.POST.get("anio"), anio)

        if action == "config":
            periodo, _ = ConfigBonoVentasPeriodo.objects.get_or_create(mes=mes, anio=anio)
            periodo.dias_laborables = _parse_int(request.POST.get("dias_laborables"), periodo.dias_laborables)
            periodo.bono_base = _parse_decimal(request.POST.get("bono_base"))
            periodo.pct_uniforme = _parse_decimal(request.POST.get("pct_uniforme"))
            periodo.pct_asistencia = _parse_decimal(request.POST.get("pct_asistencia"))
            periodo.pct_puntualidad = _parse_decimal(request.POST.get("pct_puntualidad"))
            periodo.limite_uniforme = _parse_int(request.POST.get("limite_uniforme"), periodo.limite_uniforme)
            periodo.limite_asistencia = _parse_int(request.POST.get("limite_asistencia"), periodo.limite_asistencia)
            periodo.limite_puntualidad = _parse_int(request.POST.get("limite_puntualidad"), periodo.limite_puntualidad)
            periodo.cancela_por_asistencia = request.POST.get("cancela_por_asistencia") == "on"
            periodo.limite_asistencia_cancelacion = _parse_int(
                request.POST.get("limite_asistencia_cancelacion"), periodo.limite_asistencia_cancelacion
            )
            periodo.cancela_por_puntualidad = request.POST.get("cancela_por_puntualidad") == "on"
            periodo.limite_retardos_cancelacion = _parse_int(
                request.POST.get("limite_retardos_cancelacion"), periodo.limite_retardos_cancelacion
            )
            periodo.bono_ventas_adicional = _parse_decimal(request.POST.get("bono_ventas_adicional"))
            periodo.umbral_crecimiento_pct = _parse_decimal(request.POST.get("umbral_crecimiento_pct"))
            periodo.bono_repartidor_adicional = _parse_decimal(request.POST.get("bono_repartidor_adicional"))
            periodo.umbral_efectividad_pct = _parse_decimal(request.POST.get("umbral_efectividad_pct"))
            for category, field in CATEGORY_WEIGHT_FIELDS.items():
                setattr(periodo, field, _parse_decimal(request.POST.get(field)))
            fecha_inicio = parse_date(request.POST.get("fecha_inicio") or "")
            if fecha_inicio is not None:
                periodo.fecha_inicio = fecha_inicio
            fecha_fin = parse_date(request.POST.get("fecha_fin") or "")
            if fecha_fin is not None:
                periodo.fecha_fin = fecha_fin
            periodo.creado_por = periodo.creado_por or request.user
            periodo.save()
            _recalcular_periodo(periodo)
            messages.success(request, "Configuración de bonos ventas guardada.")
            return _dashboard_redirect(mes, anio)

        periodo = get_object_or_404(ConfigBonoVentasPeriodo, mes=mes, anio=anio)
        if action == "inicializar":
            result = _inicializar_bonos(periodo)
            message = f"Bonos inicializados: {result['creados']} creados."
            if result["sin_sucursal"]:
                message += f" Sin sucursal: {len(result['sin_sucursal'])}."
            messages.success(request, message)
            return _dashboard_redirect(mes, anio)

        if action == "sync_pos":
            updated = sync_ventas_categorias(periodo)
            _recalcular_periodo(periodo)
            messages.success(request, f"Ventas por categoría sincronizadas: {updated}.")
            return _dashboard_redirect(mes, anio)

        if action == "sync_repartidores":
            resultado = sync_dias_repartidor(periodo)
            messages.success(
                request,
                f"Repartidores sincronizados: {resultado['actualizados']}."
                + (f" Sin vínculo ERP: {resultado['sin_repartidor_vinculado']}." if resultado["sin_repartidor_vinculado"] else ""),
            )
            return _dashboard_redirect(mes, anio)

        if action == "recalcular":
            total = _recalcular_periodo(periodo)
            messages.success(request, f"Bonos recalculados: {total}.")
            return _dashboard_redirect(mes, anio)

        if action == "sync_checador":
            resultado = sincronizar_asistencia_desde_checador(periodo)
            messages.success(
                request,
                "Checador sincronizado: "
                f"{resultado['bonos_sincronizados']} bonos, "
                f"{resultado['registros_creados']} registros creados, "
                f"{resultado['registros_actualizados']} actualizados, "
                f"{resultado['bonos_omitidos']} omitidos.",
            )
            return _dashboard_redirect(mes, anio)

        if action == "ajuste_bono":
            bono = get_object_or_404(BonoVentasEmpleado, pk=request.POST.get("bono_id"), periodo=periodo)
            bono.dias_trabajados = _parse_int(request.POST.get("dias_trabajados"), bono.dias_trabajados)
            bono.dias_uniforme = _parse_int(request.POST.get("dias_uniforme"), bono.dias_uniforme)
            bono.dias_asistencia = _parse_int(request.POST.get("dias_asistencia"), bono.dias_asistencia)
            bono.dias_puntualidad = _parse_int(request.POST.get("dias_puntualidad"), bono.dias_puntualidad)
            if "dias_con_bitacora" in request.POST:
                bono.dias_con_bitacora = _parse_int(request.POST.get("dias_con_bitacora"), bono.dias_con_bitacora)
            bono.ajuste_positivo = _parse_decimal(request.POST.get("ajuste_positivo"))
            bono.ajuste_negativo = _parse_decimal(request.POST.get("ajuste_negativo"))
            bono.bono_extra = _parse_decimal(request.POST.get("bono_extra"))
            bono.desc_ajuste_positivo = (request.POST.get("desc_ajuste_positivo") or "").strip()[:200]
            bono.desc_ajuste_negativo = (request.POST.get("desc_ajuste_negativo") or "").strip()[:200]
            bono.desc_bono_extra = (request.POST.get("desc_bono_extra") or "").strip()[:200]
            bono.observaciones = (request.POST.get("observaciones") or "").strip()
            bono.recalcular()
            bono.save()
            messages.success(request, f"Ajuste guardado para {bono.empleado.nombre}.")
            return _dashboard_redirect(mes, anio)

    periodo = ConfigBonoVentasPeriodo.objects.filter(mes=mes, anio=anio).first()
    bonos_qs = list(
        BonoVentasEmpleado.objects.filter(periodo=periodo).select_related("empleado", "sucursal").order_by("sucursal__nombre", "empleado__nombre")
        if periodo
        else []
    )
    # Separar repartidores del resto para mostrarlos como grupo propio
    bonos_repartidores = [b for b in bonos_qs if b._es_repartidor()]
    bonos = [b for b in bonos_qs if not b._es_repartidor()] + bonos_repartidores

    ventas_categoria = list(
        VentaCategoriaSucursal.objects.filter(periodo=periodo).select_related("sucursal").order_by("sucursal__nombre", "categoria")
        if periodo
        else []
    )
    sucursales = list(Sucursal.objects.filter(activa=True).order_by("nombre"))
    sucursales_por_id = {sucursal.id: sucursal for sucursal in sucursales}
    for bono in bonos:
        sucursales_por_id.setdefault(bono.sucursal_id, bono.sucursal)
    for venta in ventas_categoria:
        sucursales_por_id.setdefault(venta.sucursal_id, venta.sucursal)

    # Ids de sucursal usados por repartidores (para excluirlos del conteo sucursal normal si todos son repartidores)
    ids_con_repartidores = {b.sucursal_id for b in bonos_repartidores}

    sucursal_rows = []
    for sucursal in sorted(sucursales_por_id.values(), key=lambda item: item.nombre):
        rows = [bono for bono in bonos if bono.sucursal_id == sucursal.id and not bono._es_repartidor()]
        cats = [venta for venta in ventas_categoria if venta.sucursal_id == sucursal.id]
        # NO agregar `if not rows: continue`: `rows` excluye repartidores, así que
        # una sucursal de solo-reparto (o activa sin bonos) quedaría oculta. Todas las
        # sucursales activas deben verse. Regresión cubierta por
        # test_dashboard_erp_muestra_sucursal_solo_con_repartidores.
        sucursal_rows.append(
            {
                "id": sucursal.id,
                "nombre": sucursal.nombre,
                "count": len(rows),
                "total": sum((bono.total_a_pagar for bono in rows), Decimal("0")),
                "bono_ventas": sum((bono.bono_ventas for bono in rows), Decimal("0")),
                "categorias_activas": sum(1 for venta in cats if venta.activo_bono),
            }
        )

    # Fila especial de repartidores
    repartidor_row = None
    if bonos_repartidores:
        repartidor_row = {
            "id": "REPARTIDORES",
            "nombre": "Repartidores",
            "count": len(bonos_repartidores),
            "total": sum((b.total_a_pagar for b in bonos_repartidores), Decimal("0")),
        }

    total_bonos = sum((bono.total_a_pagar for bono in bonos), Decimal("0"))
    passing_asistencia = sum(1 for bono in bonos if bono.pasa_asistencia)
    con_bono_ventas = sum(1 for bono in bonos if bono.pasa_bono_ventas and not bono._es_repartidor())
    repartidores_con_bono = sum(1 for bono in bonos if bono._es_repartidor() and bono.pasa_bono_ventas)
    defaults = {
        "dias_laborables": 23,
        "bono_base": Decimal("300.00"),
        "pct_uniforme": Decimal("20.00"),
        "pct_asistencia": Decimal("35.00"),
        "pct_puntualidad": Decimal("20.00"),
        "limite_uniforme": 1,
        "limite_asistencia": 2,
        "limite_puntualidad": 2,
        "bono_ventas_adicional": Decimal("300.00"),
        "umbral_crecimiento_pct": Decimal("5.00"),
        "bono_repartidor_adicional": Decimal("300.00"),
        "umbral_efectividad_pct": Decimal("85.00"),
        "limite_asistencia_cancelacion": 0,
        "limite_retardos_cancelacion": 0,
        "peso_grande": Decimal("15.00"),
        "peso_mediano": Decimal("35.00"),
        "peso_chico": Decimal("20.00"),
        "peso_mini": Decimal("15.00"),
        "peso_velas_accesorios": Decimal("5.00"),
        "peso_vasos": Decimal("10.00"),
    }
    category_rows = []
    for key, label in CATEGORIAS_PRODUCTO:
        field = CATEGORY_WEIGHT_FIELDS[key]
        category_rows.append(
            {
                "key": key,
                "label": label,
                "field": field,
                "value": getattr(periodo, field) if periodo else defaults[field],
            }
        )

    # Estado visual del período basado en fecha_fin vs hoy (sin cambio en BD)
    periodo_estado = None
    if periodo:
        if periodo.fecha_fin:
            dias_diff = (periodo.fecha_fin - today).days
            if dias_diff < 0:
                periodo_estado = "cerrado"
            elif dias_diff == 0:
                periodo_estado = "cierra_hoy"
            else:
                periodo_estado = "activo"
        else:
            periodo_estado = "activo"

    return render(
        request,
        "bonos_ventas/dashboard.html",
        {
            "periodo": periodo,
            "periodo_estado": periodo_estado,
            "mes": mes,
            "anio": anio,
            "bonos": bonos,
            "ventas_categoria": ventas_categoria,
            "sucursal_rows": sucursal_rows,
            "category_rows": category_rows,
            "total_bonos": total_bonos,
            "passing_asistencia": passing_asistencia,
            "con_bono_ventas": con_bono_ventas,
            "repartidores_con_bono": repartidores_con_bono,
            "repartidor_row": repartidor_row,
            "bonos_repartidores": bonos_repartidores,
            "can_manage": can_manage,
            "defaults": defaults,
        },
    )


@login_required
@ensure_csrf_cookie
def bonos_ventas_pwa(request):
    if not can_view_submodule(request.user, "ventas", "bonos"):
        return redirect("/seguimiento/" if can_view_module(request.user, "seguimiento") else "/dashboard/")
    force_capture = (request.GET.get("captura") or "").strip().lower() in {"1", "true", "si", "sí"}
    user_agent = (request.headers.get("User-Agent") or "").lower()
    is_mobile = any(token in user_agent for token in ("iphone", "ipad", "android", "mobile"))
    if not force_capture and not is_mobile:
        return redirect("bonos_ventas:bonos-ventas-dashboard")
    return render(request, "bonos_ventas/index.html")


def _static_file_path(relative_path: str) -> str:
    path = finders.find(relative_path)
    if not path:
        raise Http404(f"Static file not found: {relative_path}")
    return path


@never_cache
def bonos_ventas_manifest(request):
    with open(_static_file_path("bonos_ventas/manifest.json"), encoding="utf-8") as manifest:
        return JsonResponse(json.load(manifest), content_type="application/manifest+json")


@never_cache
def bonos_ventas_sw(request):
    with open(_static_file_path("bonos_ventas/sw.js"), encoding="utf-8") as service_worker:
        return HttpResponse(service_worker.read(), content_type="application/javascript")
