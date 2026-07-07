from __future__ import annotations

import logging
from datetime import date

from django.core.mail import send_mail

from core.tasks import _director_email, _from_email
from reportes.models import DgOperacionSnapshot
from reportes.services_dg_operacion_snapshot import hydrate_dg_operacion_payload

logger = logging.getLogger(__name__)


def _obtener_snapshot(fecha_operacion: str | None) -> DgOperacionSnapshot | None:
    if fecha_operacion:
        fecha = date.fromisoformat(fecha_operacion)
        return DgOperacionSnapshot.objects.filter(fecha_operacion=fecha).first()
    return DgOperacionSnapshot.objects.order_by("-fecha_operacion").first()


def _money(value) -> str:
    try:
        return f"${float(value):,.2f}"
    except (TypeError, ValueError):
        return "N/D"


def _qty(value) -> str:
    try:
        qty = float(value)
        if qty.is_integer():
            return f"{qty:,.0f}"
        return f"{qty:,.2f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return "N/D"


def _seccion_ventas(payload: dict) -> str:
    seccion = payload.get("point_exec_summary")
    if not seccion:
        return "Ventas del día: no disponible."
    return (
        "=== VENTAS DEL DÍA ===\n"
        f"Venta total: {_money(seccion.get('latest_sales_amount'))}\n"
        f"Tickets: {seccion.get('latest_tickets', 'N/D')}\n"
        f"Ticket promedio: {_money(seccion.get('latest_avg_ticket'))}\n"
        f"Sucursales activas: {seccion.get('active_branch_count', 'N/D')}"
    )


def _seccion_cierre(payload: dict) -> str:
    seccion = payload.get("resumen_cierre")
    if not seccion:
        return "Estado de cierre por sucursal: no disponible."
    pendientes = [
        fila for fila in (seccion.get("detalle") or []) if fila.get("semaforo") in {"rojo", "amarillo"}
    ]
    if not pendientes:
        return "=== CIERRE POR SUCURSAL ===\nTodas las sucursales cerraron sin alertas."
    lineas = ["=== CIERRE POR SUCURSAL — PENDIENTES/ALERTA ==="]
    for fila in pendientes:
        sucursal = (fila.get("sucursal") or {}).get("nombre", "N/D")
        lineas.append(f"- {sucursal}: {fila.get('estado_label', fila.get('estado', 'N/D'))}")
    return "\n".join(lineas)


def _seccion_merma(payload: dict) -> str:
    seccion = payload.get("point_waste_summary")
    if not seccion:
        return "Merma del día: no disponible."
    lineas = [
        "=== MERMA DEL DÍA ===",
        f"Cantidad total: {_qty(seccion.get('total_qty'))}",
        f"Costo total: {_money(seccion.get('total_cost'))}",
    ]
    top_branches = seccion.get("top_branches") or []
    if top_branches:
        lineas.append("Top sucursales con merma:")
        for branch in top_branches[:5]:
            nombre = branch.get("branch_name") or branch.get("branch_label") or branch.get("nombre") or "N/D"
            lineas.append(f"- {nombre}")
    return "\n".join(lineas)


def _armar_cuerpo(snapshot: DgOperacionSnapshot) -> str:
    payload = hydrate_dg_operacion_payload(snapshot.payload or {})
    partes = [
        _seccion_ventas(payload),
        _seccion_cierre(payload),
        _seccion_merma(payload),
        "\n-- ERP Pollyana's Dolce",
    ]
    return "\n\n".join(partes)


def construir_y_enviar_reporte_diario(*, fecha_operacion: str | None = None) -> dict:
    snapshot = _obtener_snapshot(fecha_operacion)
    if snapshot is None or snapshot.status != DgOperacionSnapshot.STATUS_READY:
        logger.warning(
            "No se envio reporte diario: snapshot no listo (fecha_operacion=%s, status=%s).",
            fecha_operacion,
            getattr(snapshot, "status", "sin_snapshot"),
        )
        return {"status": "omitido", "reason": "snapshot_no_listo"}

    recipient = _director_email()
    if not recipient:
        logger.warning("No se envio reporte diario: DIRECTOR_EMAIL/DEFAULT_FROM_EMAIL vacio.")
        return {"status": "omitido", "reason": "sin_destinatario"}

    subject = f"Reporte diario Pollyana's Dolce - {snapshot.fecha_operacion:%d/%m/%Y}"
    send_mail(
        subject=subject,
        message=_armar_cuerpo(snapshot),
        from_email=_from_email() or recipient,
        recipient_list=[recipient],
        fail_silently=False,
    )
    return {
        "status": "enviado",
        "fecha_operacion": snapshot.fecha_operacion.isoformat(),
        "recipient": recipient,
    }
