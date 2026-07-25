"""Conciliación mensual de combustible: facturas SAT vs bitácora de cargas.

Contexto operativo (dirección, 2026-07-19):
- La empresa compra vales de gasolina por adelantado, GENERALMENTE en montos
  redondos ($3,000, $6,000...) — es un patrón, NO una regla forzada.
- Los repartidores cargan con vale y suben el ticket a la bitácora.
- Una factura con centavos o monto no redondo SUGIERE consumo directo de una
  persona (ej. carga premium del DG) — se marca "revisar", nunca se clasifica
  en automático.
- Diesel: Ducato, Partner y Manager. Gasolina: Cheyenne + autos personales
  que paga la empresa. Emisores conocidos: Rosa Hildeliza Famania Ortega
  (diesel/gasolina), Petroservicios de Guasave (gasolina).
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

PALABRAS_COMBUSTIBLE = ("DIESEL", "DIÉSEL", "MAGNA", "PREMIUM", "GASOLINA")


def _es_monto_redondo(total: Decimal) -> bool:
    # ponytail: heurística suave — entero y múltiplo de 100 se considera "redondo".
    return total == total.to_integral_value() and int(total) % 100 == 0


def conciliar_combustible(periodo: date) -> dict:
    """Compara CFDIs de combustible del mes contra la bitácora de cargas."""
    from logistica.models import CargaCombustibleUnidad
    from sat_client.models import CfdiDescargado

    facturas = []
    total_facturado = {"DIESEL": Decimal("0"), "GASOLINA": Decimal("0")}
    cfdis = CfdiDescargado.objects.filter(
        fecha_emision__year=periodo.year,
        fecha_emision__month=periodo.month,
        tipo_comprobante="I",
    ).exclude(nombre_emisor__icontains="FONSMA")
    for c in cfdis:
        xml = (c.xml_raw or "").upper()
        if not any(k in xml for k in PALABRAS_COMBUSTIBLE):
            continue
        tipo = "DIESEL" if ("DIESEL" in xml or "DIÉSEL" in xml) else "GASOLINA"
        total = c.total or Decimal("0")
        total_facturado[tipo] += total
        facturas.append(
            {
                "fecha": c.fecha_emision.date().isoformat(),
                "emisor": c.nombre_emisor,
                "tipo": tipo,
                "total": total,
                "revisar": "" if _es_monto_redondo(total) else "posible consumo directo (monto no redondo)",
            }
        )

    bitacora = {}
    total_bitacora = Decimal("0")
    filas = (
        CargaCombustibleUnidad.objects.filter(
            fecha_registro__year=periodo.year, fecha_registro__month=periodo.month
        )
        .values("unidad__codigo")
    )
    from django.db.models import Count, Sum

    for f in (
        CargaCombustibleUnidad.objects.filter(
            fecha_registro__year=periodo.year, fecha_registro__month=periodo.month
        )
        .values("unidad__codigo")
        .annotate(t=Sum("importe_total"), n=Count("id"))
    ):
        bitacora[f["unidad__codigo"]] = {"cargas": f["n"], "total": f["t"] or Decimal("0")}
        total_bitacora += f["t"] or Decimal("0")

    return {
        "periodo": periodo.strftime("%Y-%m"),
        "facturas": facturas,
        "total_facturado": total_facturado,
        "bitacora": bitacora,
        "total_bitacora": total_bitacora,
        "diferencia": (total_facturado["DIESEL"] + total_facturado["GASOLINA"]) - total_bitacora,
    }


def render_conciliacion_texto(datos: dict) -> str:
    lineas = [f"Conciliación combustible {datos['periodo']} (facturas SAT vs bitácora)", ""]
    lineas.append("FACTURAS:")
    for f in datos["facturas"]:
        marca = f"  ⚠ {f['revisar']}" if f["revisar"] else ""
        lineas.append(f"  {f['fecha']} | {f['emisor'][:38]:38s} | {f['tipo']:8s} | ${f['total']:>10,.2f}{marca}")
    if not datos["facturas"]:
        lineas.append("  (sin facturas de combustible descargadas — revisar descarga SAT del mes)")
    lineas.append("")
    lineas.append(
        f"FACTURADO: diesel ${datos['total_facturado']['DIESEL']:,.2f} · "
        f"gasolina ${datos['total_facturado']['GASOLINA']:,.2f}"
    )
    lineas.append("BITÁCORA por unidad:")
    for unidad, b in sorted(datos["bitacora"].items()):
        lineas.append(f"  {unidad or '(sin unidad)'} | {b['cargas']} cargas | ${b['total']:,.2f}")
    lineas.append(f"BITÁCORA total: ${datos['total_bitacora']:,.2f}")
    lineas.append(f"DIFERENCIA facturado−bitácora: ${datos['diferencia']:,.2f}")
    lineas.append("")
    lineas.append("Recordatorios: vales generalmente en montos redondos (patrón, no regla);")
    lineas.append("montos con centavos sugieren consumo directo de una persona — confirmar con DG.")
    return "\n".join(lineas)
