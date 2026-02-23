from decimal import Decimal

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.shortcuts import redirect, render
from django.utils import timezone
from django.utils.dateparse import parse_date

from core.access import can_capture_piso, can_view_reportes
from core.models import Sucursal
from recetas.models import Receta

from .models import MermaPOS, VentaPOS
from .services import build_discrepancias_report, resolve_period_range


def _module_tabs(active: str) -> list[dict]:
    return [
        {"label": "Discrepancias", "url_name": "control:discrepancias", "active": active == "discrepancias"},
        {"label": "Captura móvil", "url_name": "control:captura_movil", "active": active == "captura_movil"},
    ]


@login_required
def discrepancias(request):
    if not can_view_reportes(request.user):
        raise PermissionDenied("No tienes permisos para revisar discrepancias.")

    period_raw = (request.GET.get("periodo") or "").strip()
    date_from_raw = (request.GET.get("from") or "").strip()
    date_to_raw = (request.GET.get("to") or "").strip()
    sucursal_id_raw = (request.GET.get("sucursal_id") or "").strip()
    threshold_raw = (request.GET.get("threshold_pct") or "10").strip()

    date_from, date_to, period_resolved = resolve_period_range(
        period_raw=period_raw,
        date_from_raw=date_from_raw,
        date_to_raw=date_to_raw,
    )

    try:
        threshold_pct = Decimal(threshold_raw)
    except Exception:
        threshold_pct = Decimal("10")
    if threshold_pct < 0:
        threshold_pct = Decimal("0")

    sucursal_id = int(sucursal_id_raw) if sucursal_id_raw.isdigit() else None
    report = build_discrepancias_report(
        date_from=date_from,
        date_to=date_to,
        sucursal_id=sucursal_id,
        threshold_pct=threshold_pct,
    )

    context = {
        "module_tabs": _module_tabs("discrepancias"),
        "periodo": period_resolved,
        "date_from": date_from,
        "date_to": date_to,
        "threshold_pct": threshold_pct,
        "sucursal_id": sucursal_id,
        "sucursales": list(Sucursal.objects.filter(activa=True).order_by("codigo")),
        "report": report,
    }
    return render(request, "control/discrepancias.html", context)


def _parse_decimal(value: str, default: Decimal = Decimal("0")) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except Exception:
        return default
    return parsed


@login_required
def captura_movil(request):
    if not can_capture_piso(request.user):
        raise PermissionDenied("No tienes permisos para captura en piso.")

    if request.method == "POST":
        capture_type = (request.POST.get("capture_type") or "").strip().lower()
        receta_id = (request.POST.get("receta_id") or "").strip()
        sucursal_id = (request.POST.get("sucursal_id") or "").strip()
        fecha_raw = (request.POST.get("fecha") or "").strip()
        cantidad_raw = (request.POST.get("cantidad") or "").strip()
        producto_texto = (request.POST.get("producto_texto") or "").strip()
        codigo_point = (request.POST.get("codigo_point") or "").strip()

        receta = Receta.objects.filter(id=receta_id).first() if receta_id.isdigit() else None
        sucursal = Sucursal.objects.filter(id=sucursal_id, activa=True).first() if sucursal_id.isdigit() else None
        fecha = parse_date(fecha_raw) if fecha_raw else timezone.localdate()
        cantidad = _parse_decimal(cantidad_raw)

        if fecha is None:
            messages.error(request, "Fecha inválida. Usa formato YYYY-MM-DD.")
            return redirect("control:captura_movil")
        if cantidad <= 0:
            messages.error(request, "La cantidad debe ser mayor a cero.")
            return redirect("control:captura_movil")

        if receta and not codigo_point:
            codigo_point = (receta.codigo_point or "").strip()
        if receta and not producto_texto:
            producto_texto = receta.nombre

        if not receta and not producto_texto and not codigo_point:
            messages.error(request, "Selecciona receta o captura producto/código para guardar el registro.")
            return redirect("control:captura_movil")

        if capture_type == "venta":
            tickets_raw = (request.POST.get("tickets") or "0").strip()
            monto_raw = (request.POST.get("monto_total") or "0").strip()
            tickets = int(tickets_raw) if tickets_raw.isdigit() else 0
            monto_total = _parse_decimal(monto_raw)
            VentaPOS.objects.create(
                receta=receta,
                sucursal=sucursal,
                fecha=fecha,
                codigo_point=codigo_point,
                producto_texto=producto_texto,
                cantidad=cantidad,
                tickets=tickets,
                monto_total=monto_total,
                fuente="CAPTURA_MOVIL",
            )
            messages.success(request, "Venta capturada correctamente.")
            return redirect("control:captura_movil")

        if capture_type == "merma":
            motivo = (request.POST.get("motivo") or "").strip()
            MermaPOS.objects.create(
                receta=receta,
                sucursal=sucursal,
                fecha=fecha,
                codigo_point=codigo_point,
                producto_texto=producto_texto,
                cantidad=cantidad,
                motivo=motivo,
                fuente="CAPTURA_MOVIL",
            )
            messages.success(request, "Merma capturada correctamente.")
            return redirect("control:captura_movil")

        messages.error(request, "Tipo de captura no reconocido.")
        return redirect("control:captura_movil")

    ventas = list(
        VentaPOS.objects.select_related("receta", "sucursal")
        .order_by("-fecha", "-creado_en", "-id")[:30]
    )
    mermas = list(
        MermaPOS.objects.select_related("receta", "sucursal")
        .order_by("-fecha", "-creado_en", "-id")[:30]
    )
    recientes = sorted(
        [
            {
                "tipo": "VENTA",
                "fecha": row.fecha,
                "sucursal": row.sucursal,
                "producto": row.receta.nombre if row.receta_id else (row.producto_texto or row.codigo_point),
                "cantidad": row.cantidad,
                "extra": f"Tickets: {row.tickets} · Monto: ${row.monto_total or 0}",
                "fuente": row.fuente,
                "creado_en": row.creado_en,
            }
            for row in ventas
        ]
        + [
            {
                "tipo": "MERMA",
                "fecha": row.fecha,
                "sucursal": row.sucursal,
                "producto": row.receta.nombre if row.receta_id else (row.producto_texto or row.codigo_point),
                "cantidad": row.cantidad,
                "extra": row.motivo or "-",
                "fuente": row.fuente,
                "creado_en": row.creado_en,
            }
            for row in mermas
        ],
        key=lambda item: (item["fecha"], item["creado_en"]),
        reverse=True,
    )[:25]

    context = {
        "module_tabs": _module_tabs("captura_movil"),
        "sucursales": list(Sucursal.objects.filter(activa=True).order_by("codigo")),
        "recetas": list(Receta.objects.order_by("nombre").only("id", "nombre", "codigo_point")[:500]),
        "hoy": timezone.localdate().isoformat(),
        "recientes": recientes,
    }
    return render(request, "control/captura_movil.html", context)
