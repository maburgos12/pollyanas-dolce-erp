from __future__ import annotations

import hashlib
import io
import re
import unicodedata
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import pandas as pd
from django.db.models import Count, Max, Min, Q, Sum
from django.db.models.functions import TruncDate
from django.db import transaction
from django.utils import timezone

from conciliacion.models import CfdiSucursalResolucion, ImportacionBancaria
from sat_client.models import CfdiDescargado, CfdiPagoRelacionado
from syncfy_client.models import CuentaBancaria, MovimientoBancario


MAX_PREVIEW_ROWS = 50
MAX_IMPORT_ROWS = 3000
MAX_UPLOAD_BYTES = 8 * 1024 * 1024
FORMATOS_SOPORTADOS = {"csv", "xlsx", "xlsm", "xls", "xml", "pdf"}
PDF_MONTHS = {
    "ENE": 1,
    "ENERO": 1,
    "FEB": 2,
    "FEBRERO": 2,
    "MAR": 3,
    "MARZO": 3,
    "ABR": 4,
    "ABRIL": 4,
    "MAY": 5,
    "MAYO": 5,
    "JUN": 6,
    "JUNIO": 6,
    "JUL": 7,
    "JULIO": 7,
    "AGO": 8,
    "AGOSTO": 8,
    "SEP": 9,
    "SEPT": 9,
    "SEPTIEMBRE": 9,
    "OCT": 10,
    "OCTUBRE": 10,
    "NOV": 11,
    "NOVIEMBRE": 11,
    "DIC": 12,
    "DICIEMBRE": 12,
}
PDF_MONTH_ABBR = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}
BBVA_ABONO_CODES = {"T20", "W02", "Y45"}
BBVA_CARGO_CODES = {"P14", "R01", "R15", "S39", "S40", "T17"}


class ImportacionBancariaError(ValueError):
    pass


@dataclass(frozen=True)
class MovimientoNormalizado:
    fecha: datetime
    descripcion: str
    monto: Decimal
    tipo: str
    moneda: str
    referencia: str
    saldo: Decimal | None
    fila: int
    raw: dict[str, Any]

    @property
    def monto_firmado(self) -> Decimal:
        return self.monto if self.tipo == MovimientoBancario.TIPO_ABONO else -self.monto

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["fecha"] = self.fecha.isoformat()
        payload["monto"] = str(self.monto)
        payload["saldo"] = str(self.saldo) if self.saldo is not None else ""
        payload["monto_firmado"] = str(self.monto_firmado)
        return payload

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "MovimientoNormalizado":
        fecha = datetime.fromisoformat(str(payload["fecha"]))
        if timezone.is_naive(fecha):
            fecha = timezone.make_aware(fecha, timezone=timezone.get_current_timezone())
        saldo_raw = payload.get("saldo")
        return cls(
            fecha=fecha,
            descripcion=str(payload.get("descripcion") or ""),
            monto=_decimal(payload.get("monto")),
            tipo=str(payload.get("tipo") or MovimientoBancario.TIPO_CARGO),
            moneda=str(payload.get("moneda") or "MXN"),
            referencia=str(payload.get("referencia") or ""),
            saldo=_decimal(saldo_raw) if saldo_raw not in (None, "") else None,
            fila=int(payload.get("fila") or 0),
            raw=payload.get("raw") if isinstance(payload.get("raw"), dict) else {},
        )


@dataclass(frozen=True)
class PreviewImportacion:
    cuenta_id: int
    archivo_nombre: str
    archivo_hash: str
    fuente: str
    movimientos: list[MovimientoNormalizado]
    errores: list[str]

    @property
    def total_filas(self) -> int:
        return len(self.movimientos) + len(self.errores)

    def to_session_payload(self) -> dict[str, Any]:
        return {
            "cuenta_id": self.cuenta_id,
            "archivo_nombre": self.archivo_nombre,
            "archivo_hash": self.archivo_hash,
            "fuente": self.fuente,
            "movimientos": [mov.to_payload() for mov in self.movimientos],
            "errores": self.errores,
        }

    @classmethod
    def from_session_payload(cls, payload: dict[str, Any]) -> "PreviewImportacion":
        return cls(
            cuenta_id=int(payload["cuenta_id"]),
            archivo_nombre=str(payload["archivo_nombre"]),
            archivo_hash=str(payload["archivo_hash"]),
            fuente=str(payload["fuente"]),
            movimientos=[MovimientoNormalizado.from_payload(item) for item in payload.get("movimientos", [])],
            errores=[str(item) for item in payload.get("errores", [])],
        )


def generar_preview(*, cuenta: CuentaBancaria, uploaded_file) -> PreviewImportacion:
    content = uploaded_file.read()
    if not content:
        raise ImportacionBancariaError("El archivo esta vacio.")
    if len(content) > MAX_UPLOAD_BYTES:
        raise ImportacionBancariaError("El archivo excede 8 MB.")

    nombre = str(uploaded_file.name or "estado_cuenta")
    suffix = nombre.rsplit(".", 1)[-1].lower() if "." in nombre else ""
    if suffix not in FORMATOS_SOPORTADOS:
        raise ImportacionBancariaError("Formato no soportado. Usa PDF, XML, CSV, XLSX, XLSM o XLS.")

    dataframe = _read_dataframe(content, suffix)
    if dataframe.empty:
        raise ImportacionBancariaError("No se encontraron filas para importar.")
    if len(dataframe) > MAX_IMPORT_ROWS:
        raise ImportacionBancariaError(f"El archivo tiene mas de {MAX_IMPORT_ROWS} filas.")

    movimientos: list[MovimientoNormalizado] = []
    errores: list[str] = []
    for index, raw_row in dataframe.iterrows():
        fila = int(index) + 2
        row = {_normalize_header(key): _clean_value(value) for key, value in raw_row.to_dict().items()}
        try:
            movimientos.append(_normalizar_movimiento(row=row, fila=fila))
        except ImportacionBancariaError as exc:
            errores.append(f"Fila {fila}: {exc}")

    if not movimientos:
        raise ImportacionBancariaError("No se pudo normalizar ningun movimiento del archivo.")

    return PreviewImportacion(
        cuenta_id=cuenta.pk,
        archivo_nombre=nombre,
        archivo_hash=hashlib.sha256(content).hexdigest(),
        fuente=_fuente_manual(suffix),
        movimientos=movimientos,
        errores=errores,
    )


@transaction.atomic
def confirmar_importacion(*, preview: PreviewImportacion, user) -> ImportacionBancaria:
    cuenta = CuentaBancaria.objects.select_for_update().get(pk=preview.cuenta_id)
    importacion = ImportacionBancaria.objects.create(
        cuenta=cuenta,
        fuente=preview.fuente,
        estado=ImportacionBancaria.ESTADO_IMPORTADA,
        archivo_nombre=preview.archivo_nombre,
        archivo_hash=preview.archivo_hash,
        total_filas=preview.total_filas,
        filas_con_error=len(preview.errores),
        preview=[mov.to_payload() for mov in preview.movimientos[:MAX_PREVIEW_ROWS]],
        errores=preview.errores[:MAX_PREVIEW_ROWS],
        creado_por=user if getattr(user, "is_authenticated", False) else None,
    )

    nuevos = 0
    duplicados = 0
    for movimiento in preview.movimientos:
        id_transaction = _manual_transaction_id(cuenta=cuenta, movimiento=movimiento)
        _, created = MovimientoBancario.objects.get_or_create(
            id_transaction=id_transaction,
            defaults={
                "cuenta": cuenta,
                "descripcion": movimiento.descripcion,
                "monto": movimiento.monto,
                "tipo": movimiento.tipo,
                "moneda": movimiento.moneda,
                "fecha_transaccion": movimiento.fecha,
                "fecha_refresh": timezone.now(),
                "extra_raw": {
                    "source": preview.fuente,
                    "archivo_hash": preview.archivo_hash,
                    "archivo_nombre": preview.archivo_nombre,
                    "referencia": movimiento.referencia,
                    "saldo": str(movimiento.saldo) if movimiento.saldo is not None else "",
                    "raw": movimiento.raw,
                },
            },
        )
        if created:
            nuevos += 1
        else:
            duplicados += 1

    importacion.movimientos_nuevos = nuevos
    importacion.movimientos_duplicados = duplicados
    importacion.save(update_fields=["movimientos_nuevos", "movimientos_duplicados", "actualizado_en"])
    return importacion


def resumen_conciliacion() -> dict[str, Any]:
    movimientos_qs = MovimientoBancario.objects.select_related("cuenta").order_by("-fecha_transaccion")
    pendientes = movimientos_qs.filter(conciliado=False)
    candidatos = sugerir_cfdis_para_movimientos(list(pendientes[:50]))
    return {
        "movimientos_total": movimientos_qs.count(),
        "movimientos_pendientes": pendientes.count(),
        "movimientos_conciliados": movimientos_qs.filter(conciliado=True).count(),
        "ultimos_movimientos": list(movimientos_qs[:20]),
        "candidatos": candidatos,
        "importaciones": list(ImportacionBancaria.objects.select_related("cuenta", "creado_por")[:10]),
    }


def periodo_default_conciliacion() -> tuple[int, int]:
    ultimo_movimiento = MovimientoBancario.objects.order_by("-fecha_transaccion").first()
    if ultimo_movimiento:
        fecha = timezone.localtime(ultimo_movimiento.fecha_transaccion).date()
        return fecha.year, fecha.month
    hoy = timezone.localdate()
    return hoy.year, hoy.month


def resumen_periodo_conciliacion(*, year: int, month: int) -> dict[str, Any]:
    inicio, fin = _periodo_bounds(year=year, month=month)
    movimientos_qs = MovimientoBancario.objects.select_related("cuenta").filter(
        fecha_transaccion__gte=inicio,
        fecha_transaccion__lt=fin,
    )
    cfdi_qs = CfdiDescargado.objects.filter(fecha_emision__gte=inicio, fecha_emision__lt=fin)

    movimientos_agregado = movimientos_qs.aggregate(
        total=Count("id"),
        fecha_min=Min("fecha_transaccion"),
        fecha_max=Max("fecha_transaccion"),
    )
    cargos = _aggregate_tipo_movimiento(movimientos_qs, MovimientoBancario.TIPO_CARGO)
    abonos = _aggregate_tipo_movimiento(movimientos_qs, MovimientoBancario.TIPO_ABONO)
    dias_banco = _resumen_diario_movimientos(movimientos_qs)
    fuentes = _resumen_fuentes_movimientos(movimientos_qs)

    cfdi_emitidos = _aggregate_tipo_cfdi(cfdi_qs, CfdiDescargado.TIPO_EMITIDO)
    cfdi_recibidos = _aggregate_tipo_cfdi(cfdi_qs, CfdiDescargado.TIPO_RECIBIDO)
    cfdi_sucursales = _resumen_cfdi_sucursales(inicio=inicio, fin=fin)
    alcance_fiscal = _resumen_alcance_fiscal(inicio=inicio, fin=fin)
    canales_comparativo = _resumen_comparativo_canales(movimientos_qs=movimientos_qs, cfdi_qs=cfdi_qs)

    movimientos_periodo = list(movimientos_qs.order_by("-fecha_transaccion")[:50])
    candidatos = sugerir_cfdis_para_movimientos([mov for mov in movimientos_periodo if not mov.conciliado])

    return {
        "periodo_value": f"{year:04d}-{month:02d}",
        "periodo_label": _periodo_label(year=year, month=month),
        "periodo_inicio": inicio.date(),
        "periodo_fin": (fin - timedelta(days=1)).date(),
        "movimientos_total": movimientos_agregado["total"] or 0,
        "movimientos_fecha_min": movimientos_agregado["fecha_min"],
        "movimientos_fecha_max": movimientos_agregado["fecha_max"],
        "movimientos_dias": len(dias_banco),
        "movimientos_cargos": cargos,
        "movimientos_abonos": abonos,
        "movimientos_fuentes": fuentes,
        "movimientos_diarios": dias_banco,
        "movimientos_rows": movimientos_periodo,
        "candidatos": candidatos,
        "cfdi_total": cfdi_qs.count(),
        "cfdi_emitidos": cfdi_emitidos,
        "cfdi_recibidos": cfdi_recibidos,
        "cfdi_sucursales": cfdi_sucursales,
        "alcance_fiscal": alcance_fiscal,
        "canales_comparativo": canales_comparativo,
        "cfdi_rows": list(cfdi_qs.order_by("-fecha_emision")[:20]),
    }


def sugerir_cfdis_para_movimientos(movimientos: list[MovimientoBancario]) -> dict[int, list[CfdiDescargado]]:
    sugerencias: dict[int, list[CfdiDescargado]] = {}
    for movimiento in movimientos:
        fecha = movimiento.fecha_transaccion.date()
        tipo_cfdi = CfdiDescargado.TIPO_EMITIDO if movimiento.tipo == MovimientoBancario.TIPO_ABONO else CfdiDescargado.TIPO_RECIBIDO
        qs = (
            CfdiDescargado.objects.filter(
                conciliado=False,
                tipo_cfdi=tipo_cfdi,
                total__gte=movimiento.monto - Decimal("1.00"),
                total__lte=movimiento.monto + Decimal("1.00"),
                fecha_emision__date__gte=fecha - timedelta(days=7),
                fecha_emision__date__lte=fecha + timedelta(days=7),
            )
            .order_by("-fecha_emision")[:5]
        )
        sugerencias[movimiento.pk] = list(qs)
    return sugerencias


def _periodo_bounds(*, year: int, month: int) -> tuple[datetime, datetime]:
    inicio = timezone.make_aware(datetime.combine(date(year, month, 1), time.min))
    if month == 12:
        fin = timezone.make_aware(datetime.combine(date(year + 1, 1, 1), time.min))
    else:
        fin = timezone.make_aware(datetime.combine(date(year, month + 1, 1), time.min))
    return inicio, fin


def _aggregate_tipo_movimiento(queryset, tipo: str) -> dict[str, Any]:
    data = queryset.filter(tipo=tipo).aggregate(conteo=Count("id"), total=Sum("monto"))
    return {"conteo": data["conteo"] or 0, "total": data["total"] or Decimal("0.00")}


def _aggregate_tipo_cfdi(queryset, tipo_cfdi: str) -> dict[str, Any]:
    data = queryset.filter(tipo_cfdi=tipo_cfdi).aggregate(conteo=Count("id"), total=Sum("total"))
    return {"conteo": data["conteo"] or 0, "total": data["total"] or Decimal("0.00")}


def _resumen_alcance_fiscal(*, inicio: datetime, fin: datetime) -> dict[str, Any]:
    complemento_fin_exclusivo = fin + timedelta(days=5)
    return {
        "periodo_inicio": inicio.date(),
        "periodo_fin": (fin - timedelta(days=1)).date(),
        "complemento_fin": (complemento_fin_exclusivo - timedelta(days=1)).date(),
        "pagos_emitidos": _aggregate_pagos_relacionados(
            tipo_cfdi=CfdiDescargado.TIPO_EMITIDO,
            inicio=inicio,
            fin=fin,
            complemento_fin_exclusivo=complemento_fin_exclusivo,
        ),
        "pagos_recibidos": _aggregate_pagos_relacionados(
            tipo_cfdi=CfdiDescargado.TIPO_RECIBIDO,
            inicio=inicio,
            fin=fin,
            complemento_fin_exclusivo=complemento_fin_exclusivo,
        ),
        "ppd_emitidos_abiertos": _aggregate_ppd_abiertos(
            tipo_cfdi=CfdiDescargado.TIPO_EMITIDO,
            inicio=inicio,
            fin=fin,
        ),
        "ppd_recibidos_abiertos": _aggregate_ppd_abiertos(
            tipo_cfdi=CfdiDescargado.TIPO_RECIBIDO,
            inicio=inicio,
            fin=fin,
        ),
    }


def _aggregate_pagos_relacionados(
    *,
    tipo_cfdi: str,
    inicio: datetime,
    fin: datetime,
    complemento_fin_exclusivo: datetime,
) -> dict[str, Any]:
    data = CfdiPagoRelacionado.objects.filter(
        cfdi_pago__tipo_cfdi=tipo_cfdi,
        fecha_pago__gte=inicio,
        fecha_pago__lt=fin,
        cfdi_pago__fecha_emision__lt=complemento_fin_exclusivo,
    ).aggregate(conteo=Count("id"), total=Sum("monto"))
    return {"conteo": data["conteo"] or 0, "total": data["total"] or Decimal("0.00")}


def _aggregate_ppd_abiertos(*, tipo_cfdi: str, inicio: datetime, fin: datetime) -> dict[str, Any]:
    cfdis = list(
        CfdiDescargado.objects.filter(
            tipo_cfdi=tipo_cfdi,
            tipo_comprobante="I",
            metodo_pago="PPD",
            fecha_emision__lt=inicio,
        ).values("uuid", "total")
    )
    if not cfdis:
        return {"conteo": 0, "total": Decimal("0.00"), "pagado": Decimal("0.00"), "saldo": Decimal("0.00")}

    pagos = {
        row["uuid_relacionado"]: row["pagado"] or Decimal("0.00")
        for row in CfdiPagoRelacionado.objects.filter(
            cfdi_pago__tipo_cfdi=tipo_cfdi,
            uuid_relacionado__in=[item["uuid"] for item in cfdis],
            fecha_pago__lt=fin,
        )
        .values("uuid_relacionado")
        .annotate(pagado=Sum("monto"))
    }
    conteo = 0
    total = Decimal("0.00")
    pagado_total = Decimal("0.00")
    saldo = Decimal("0.00")
    for cfdi in cfdis:
        cfdi_total = cfdi["total"] or Decimal("0.00")
        pagado = pagos.get(cfdi["uuid"], Decimal("0.00"))
        pendiente = cfdi_total - pagado
        if pendiente > Decimal("0.00"):
            conteo += 1
            total += cfdi_total
            pagado_total += pagado
            saldo += pendiente
    return {"conteo": conteo, "total": total, "pagado": pagado_total, "saldo": saldo}


def _resumen_cfdi_sucursales(*, inicio: datetime, fin: datetime) -> dict[str, Any]:
    resoluciones_qs = CfdiSucursalResolucion.objects.select_related("sucursal", "cfdi").filter(
        cfdi__fecha_emision__gte=inicio,
        cfdi__fecha_emision__lt=fin,
        cfdi__tipo_cfdi=CfdiDescargado.TIPO_EMITIDO,
    )
    rows = (
        resoluciones_qs.filter(sucursal__isnull=False)
        .values("sucursal__codigo", "sucursal__nombre")
        .annotate(
            cfdis=Count("id"),
            total=Sum("cfdi__total"),
            efectivo=Sum("cfdi__total", filter=Q(cfdi__forma_pago="01")),
            tarjeta=Sum("cfdi__total", filter=Q(cfdi__forma_pago__in=["04", "28", "29"])),
            transferencia=Sum("cfdi__total", filter=Q(cfdi__forma_pago="03")),
        )
        .order_by("sucursal__codigo")
    )
    sucursales = []
    for row in rows:
        sucursales.append(
            {
                "codigo": row["sucursal__codigo"],
                "nombre": row["sucursal__nombre"],
                "cfdis": row["cfdis"] or 0,
                "total": row["total"] or Decimal("0.00"),
                "efectivo": row["efectivo"] or Decimal("0.00"),
                "tarjeta": row["tarjeta"] or Decimal("0.00"),
                "transferencia": row["transferencia"] or Decimal("0.00"),
            }
        )

    pendientes = resoluciones_qs.filter(sucursal__isnull=True).aggregate(
        cfdis=Count("id"),
        total=Sum("cfdi__total"),
    )
    return {
        "sucursales": sucursales,
        "resueltos": sum(item["cfdis"] for item in sucursales),
        "pendientes": pendientes["cfdis"] or 0,
        "pendientes_total": pendientes["total"] or Decimal("0.00"),
    }


def _resumen_comparativo_canales(*, movimientos_qs, cfdi_qs) -> list[dict[str, Any]]:
    banco = {
        "efectivo": {"conteo": 0, "total": Decimal("0.00")},
        "tarjeta": {"conteo": 0, "total": Decimal("0.00")},
        "transferencia": {"conteo": 0, "total": Decimal("0.00")},
        "otros_abonos": {"conteo": 0, "total": Decimal("0.00")},
    }
    for descripcion, monto in movimientos_qs.filter(tipo=MovimientoBancario.TIPO_ABONO).values_list("descripcion", "monto"):
        canal = _clasificar_canal_banco(descripcion)
        banco[canal]["conteo"] += 1
        banco[canal]["total"] += monto or Decimal("0.00")

    cfdi_emitidos = cfdi_qs.filter(tipo_cfdi=CfdiDescargado.TIPO_EMITIDO)
    sat = {
        "efectivo": _aggregate_cfdi_formas(cfdi_emitidos, ["01"]),
        "tarjeta": _aggregate_cfdi_formas(cfdi_emitidos, ["04", "28", "29"]),
        "transferencia": _aggregate_cfdi_formas(cfdi_emitidos, ["03"]),
        "otros_abonos": _aggregate_cfdi_excluyendo_formas(cfdi_emitidos, ["01", "03", "04", "28", "29"]),
    }
    labels = {
        "efectivo": "Efectivo en ventanilla",
        "tarjeta": "Tarjetas y adquirentes",
        "transferencia": "Transferencias de clientes",
        "otros_abonos": "Otros abonos",
    }

    rows = []
    for canal in ["efectivo", "tarjeta", "transferencia", "otros_abonos"]:
        banco_total = banco[canal]["total"]
        sat_total = sat[canal]["total"]
        diferencia = banco_total - sat_total
        rows.append(
            {
                "canal": canal,
                "nombre": labels[canal],
                "banco_conteo": banco[canal]["conteo"],
                "banco_total": banco_total,
                "sat_conteo": sat[canal]["conteo"],
                "sat_total": sat_total,
                "diferencia": diferencia,
                "estado": _estado_diferencia(diferencia),
            }
        )
    return rows


def _aggregate_cfdi_formas(queryset, formas_pago: list[str]) -> dict[str, Any]:
    data = queryset.filter(forma_pago__in=formas_pago).aggregate(conteo=Count("id"), total=Sum("total"))
    return {"conteo": data["conteo"] or 0, "total": data["total"] or Decimal("0.00")}


def _aggregate_cfdi_excluyendo_formas(queryset, formas_pago: list[str]) -> dict[str, Any]:
    data = queryset.exclude(forma_pago__in=formas_pago).aggregate(conteo=Count("id"), total=Sum("total"))
    return {"conteo": data["conteo"] or 0, "total": data["total"] or Decimal("0.00")}


def _clasificar_canal_banco(descripcion: str) -> str:
    texto = _normalize_text(descripcion)
    if "DEPOSITO EN EFECTIVO" in texto:
        return "efectivo"
    if "NEGOCIOS AFILIADOS" in texto or "OPTBLUE" in texto or "AMEX" in texto:
        return "tarjeta"
    if "SPEI" in texto or "TRANSFER" in texto:
        return "transferencia"
    return "otros_abonos"


def _normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    without_accents = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    upper = without_accents.upper()
    upper = re.sub(r"[^A-Z0-9]+", " ", upper)
    return re.sub(r"\s+", " ", upper).strip()


def _estado_diferencia(diferencia: Decimal) -> str:
    abs_diff = abs(diferencia)
    if abs_diff <= Decimal("1.00"):
        return "ok"
    if abs_diff <= Decimal("100.00"):
        return "revision"
    return "pendiente"


def _resumen_diario_movimientos(queryset) -> list[dict[str, Any]]:
    rows = (
        queryset.annotate(dia=TruncDate("fecha_transaccion"))
        .values("dia", "tipo")
        .annotate(conteo=Count("id"), total=Sum("monto"))
        .order_by("dia", "tipo")
    )
    resumen: dict[date, dict[str, Any]] = {}
    for row in rows:
        dia = row["dia"]
        if not dia:
            continue
        item = resumen.setdefault(
            dia,
            {
                "fecha": dia,
                "cargos_conteo": 0,
                "cargos_total": Decimal("0.00"),
                "abonos_conteo": 0,
                "abonos_total": Decimal("0.00"),
                "total_conteo": 0,
            },
        )
        total = row["total"] or Decimal("0.00")
        conteo = row["conteo"] or 0
        if row["tipo"] == MovimientoBancario.TIPO_ABONO:
            item["abonos_conteo"] += conteo
            item["abonos_total"] += total
        else:
            item["cargos_conteo"] += conteo
            item["cargos_total"] += total
        item["total_conteo"] += conteo
    return list(resumen.values())


def _resumen_fuentes_movimientos(queryset) -> list[dict[str, Any]]:
    fuentes: dict[str, dict[str, Any]] = {}
    for extra_raw, tipo, monto in queryset.values_list("extra_raw", "tipo", "monto"):
        raw = extra_raw if isinstance(extra_raw, dict) else {}
        archivo = str(raw.get("archivo_nombre") or raw.get("source") or "Sin archivo registrado")
        item = fuentes.setdefault(
            archivo,
            {
                "archivo": archivo,
                "conteo": 0,
                "cargos": 0,
                "abonos": 0,
                "total_cargos": Decimal("0.00"),
                "total_abonos": Decimal("0.00"),
            },
        )
        item["conteo"] += 1
        if tipo == MovimientoBancario.TIPO_ABONO:
            item["abonos"] += 1
            item["total_abonos"] += monto
        else:
            item["cargos"] += 1
            item["total_cargos"] += monto
    return sorted(fuentes.values(), key=lambda item: item["conteo"], reverse=True)


def _periodo_label(*, year: int, month: int) -> str:
    meses = [
        "enero",
        "febrero",
        "marzo",
        "abril",
        "mayo",
        "junio",
        "julio",
        "agosto",
        "septiembre",
        "octubre",
        "noviembre",
        "diciembre",
    ]
    return f"{meses[month - 1]} {year}"


def _read_dataframe(content: bytes, suffix: str) -> pd.DataFrame:
    buffer = io.BytesIO(content)
    if suffix == "csv":
        return _read_csv_dataframe(content)
    if suffix == "xml":
        return _read_xml_dataframe(content)
    if suffix == "pdf":
        return _read_pdf_dataframe(content)
    return pd.read_excel(buffer)


def _read_csv_dataframe(content: bytes) -> pd.DataFrame:
    dataframe = _read_csv_with_header(content)
    if _dataframe_has_bank_columns(dataframe):
        return dataframe
    detailed = _read_bajio_detallado_csv(content)
    if not detailed.empty:
        return detailed
    return dataframe


def _read_csv_with_header(content: bytes) -> pd.DataFrame:
    try:
        return pd.read_csv(io.BytesIO(content))
    except UnicodeDecodeError:
        return pd.read_csv(io.BytesIO(content), encoding="latin-1")


def _dataframe_has_bank_columns(dataframe: pd.DataFrame) -> bool:
    normalized = {_normalize_header(column) for column in dataframe.columns}
    has_date = bool(normalized & {"fecha", "fecha_operacion", "fecha_de_operacion", "fecha_movimiento", "date"})
    has_description = bool(normalized & {"descripcion", "concepto", "detalle", "movimiento", "operacion", "description"})
    has_amount = bool(
        normalized
        & {
            "cargo",
            "cargos",
            "importe_cargo",
            "retiro",
            "retiros",
            "abono",
            "abonos",
            "importe_abono",
            "deposito",
            "depositos",
            "monto",
            "importe",
            "amount",
        }
    )
    return has_date and has_description and has_amount


def _read_bajio_detallado_csv(content: bytes) -> pd.DataFrame:
    try:
        raw = pd.read_csv(io.BytesIO(content), header=None, encoding="latin-1")
    except UnicodeDecodeError:
        raw = pd.read_csv(io.BytesIO(content), header=None)
    if raw.shape[1] < 10:
        return pd.DataFrame()
    raw = raw.iloc[:, :10].copy()
    raw.columns = [
        "cuenta_origen",
        "fecha",
        "cuenta_bancaria",
        "referencia",
        "descripcion",
        "secuencia",
        "cargo",
        "abono",
        "saldo",
        "folio",
    ]
    raw = raw[raw["fecha"].apply(_looks_like_fecha_bancaria)]
    raw = raw[raw["descripcion"].fillna("").astype(str).str.strip() != ""]
    raw = raw[raw.apply(_bajio_detallado_has_amount, axis=1)]
    return raw.reset_index(drop=True)


def _read_pdf_dataframe(content: bytes) -> pd.DataFrame:
    try:
        import pdfplumber
    except ImportError as exc:
        raise ImportacionBancariaError(
            "No se puede leer PDF porque falta la libreria pdfplumber en el servidor."
        ) from exc

    rows: list[dict[str, Any]] = []
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            pages_text = [page.extract_text(x_tolerance=1, y_tolerance=3) or "" for page in pdf.pages]
            rows.extend(_pdf_rows_from_bbva_maestra(pages_text))
            rows.extend(_pdf_rows_from_amex_business(pages_text))
            if rows:
                return pd.DataFrame(rows)
            for page_number, page in enumerate(pdf.pages, start=1):
                rows.extend(_pdf_rows_from_tables(page, page_number=page_number))
                rows.extend(_pdf_rows_from_text(pages_text[page_number - 1], page_number=page_number))
    except Exception as exc:  # noqa: BLE001
        raise ImportacionBancariaError("No se pudo leer el PDF del estado de cuenta.") from exc

    if not rows:
        raise ImportacionBancariaError(
            "No se encontraron movimientos legibles en el PDF. "
            "Si el archivo es escaneado, descarga una version con texto seleccionable o usa CSV/Excel."
        )
    return pd.DataFrame(rows)


def _pdf_rows_from_bbva_maestra(pages_text: list[str]) -> list[dict[str, Any]]:
    full_text = "\n".join(pages_text)
    if "MAESTRA PYME BBVA" not in full_text or "DETALLE DE MOVIMIENTOS" not in _normalize_text(full_text):
        return []

    year = _bbva_pdf_year(full_text)
    parsed_rows: list[dict[str, Any]] = []
    row_pattern = re.compile(
        r"^(?P<oper_day>\d{1,2})/(?P<oper_month>[A-ZÁÉÍÓÚÑ]{3})\s+"
        r"(?P<liq_day>\d{1,2})/(?P<liq_month>[A-ZÁÉÍÓÚÑ]{3})\s+"
        r"(?P<code>[A-Z0-9]{3})\s+"
        r"(?P<description>.+?)\s+"
        r"(?P<amount>\d{1,3}(?:,\d{3})*\.\d{2})"
        r"(?:\s+(?P<saldo_operacion>\d{1,3}(?:,\d{3})*\.\d{2})\s+"
        r"(?P<saldo_liquidacion>\d{1,3}(?:,\d{3})*\.\d{2}))?$"
    )

    for page_number, text in enumerate(pages_text, start=1):
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        for line_number, line in enumerate(lines, start=1):
            match = row_pattern.match(line)
            if not match:
                continue
            month = _pdf_month_number(match.group("oper_month"))
            if not month:
                continue
            row = {
                "fecha": f"{year}-{month:02d}-{int(match.group('oper_day')):02d}",
                "descripcion": f"{match.group('code')} {match.group('description')}",
                "referencia": _bbva_pdf_referencia(lines, start_index=line_number),
                "pagina_pdf": page_number,
                "linea_pdf": line_number,
                "formato_pdf": "bbva_maestra_pyme",
                "codigo_operacion": match.group("code"),
                "fecha_liquidacion": _bbva_pdf_fecha_liquidacion(match=match, year=year),
            }
            amount = match.group("amount")
            if _bbva_pdf_tipo(match.group("code"), match.group("description")) == MovimientoBancario.TIPO_ABONO:
                row["abono"] = amount
            else:
                row["cargo"] = amount
            saldo = match.group("saldo_operacion") or match.group("saldo_liquidacion")
            if saldo:
                row["saldo"] = saldo
            parsed_rows.append(row)
    return parsed_rows


def _pdf_rows_from_amex_business(pages_text: list[str]) -> list[dict[str, Any]]:
    full_text = "\n".join(pages_text)
    normalized_full_text = _normalize_text(full_text)
    if "AMERICAN EXPRESS" not in normalized_full_text or "FECHA Y DETALLE DE LAS OPERACIONES" not in normalized_full_text:
        return []

    cutoff_year, cutoff_month = _amex_pdf_cutoff(full_text)
    parsed_rows: list[dict[str, Any]] = []
    row_pattern = re.compile(
        r"^(?P<day>\d{1,2})\s+de\s*(?P<month>[A-Za-zÁÉÍÓÚÑáéíóúñ]+)\s+"
        r"(?P<description>.+?)\s+"
        r"(?P<amount>\d{1,3}(?:,\d{3})*\.\d{2})$"
    )

    skip_section = False
    for page_number, text in enumerate(pages_text, start=1):
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        for line_index, line in enumerate(lines):
            normalized = _normalize_text(line)
            if normalized.startswith("RESUMEN DE PLANES DE PAGOS DIFERIDOS"):
                skip_section = True
            if "INFORMACION AL TARJETAHABIENTE" in normalized:
                skip_section = False
            if skip_section:
                continue

            match = row_pattern.match(line)
            if not match:
                continue
            description = match.group("description").strip()
            if _normalize_text(description).startswith("TOTAL "):
                continue
            month = _pdf_month_number(match.group("month"))
            if not month:
                continue
            year = cutoff_year - 1 if cutoff_month == 1 and month == 12 else cutoff_year
            next_line = lines[line_index + 1].strip() if line_index + 1 < len(lines) else ""
            next_next_line = lines[line_index + 2].strip() if line_index + 2 < len(lines) else ""
            row = {
                "fecha": f"{year}-{month:02d}-{int(match.group('day')):02d}",
                "descripcion": description,
                "referencia": _amex_pdf_referencia(next_line, next_next_line),
                "pagina_pdf": page_number,
                "linea_pdf": line_index + 1,
                "formato_pdf": "american_express_business",
            }
            if next_line == "CR":
                row["abono"] = match.group("amount")
                row["raw_tipo"] = "CR"
            else:
                row["cargo"] = match.group("amount")
            parsed_rows.append(row)
    return parsed_rows


def _bbva_pdf_year(text: str) -> int:
    match = re.search(r"Periodo\s+DEL\s+\d{1,2}/\d{1,2}/(?P<year>20\d{2})", text, flags=re.IGNORECASE)
    if match:
        return int(match.group("year"))
    match = re.search(r"Fecha de Corte\s+\d{1,2}/\d{1,2}/(?P<year>20\d{2})", text, flags=re.IGNORECASE)
    return int(match.group("year")) if match else timezone.localdate().year


def _bbva_pdf_fecha_liquidacion(*, match: re.Match[str], year: int) -> str:
    month = _pdf_month_number(match.group("liq_month"))
    if not month:
        return ""
    return f"{year}-{month:02d}-{int(match.group('liq_day')):02d}"


def _bbva_pdf_tipo(code: str, description: str) -> str:
    if code in BBVA_ABONO_CODES:
        return MovimientoBancario.TIPO_ABONO
    if code in BBVA_CARGO_CODES:
        return MovimientoBancario.TIPO_CARGO
    return _pdf_tipo_from_line(body=f"{code} {description}", trailing="", amount="1.00")


def _bbva_pdf_referencia(lines: list[str], *, start_index: int) -> str:
    parts: list[str] = []
    for line in lines[start_index : start_index + 6]:
        normalized = _normalize_text(line)
        if re.match(r"^\d{1,2}/[A-ZÁÉÍÓÚÑ]{3}\b", line):
            break
        if normalized.startswith(
            (
                "BBVA MEXICO",
                "AV PASEO",
                "TOTAL DE MOVIMIENTOS",
                "ESTIMADO CLIENTE",
                "SU ESTADO DE CUENTA",
                "TAMBIEN LE INFORMAMOS",
                "CON BBVA",
                "LA GAT REAL",
            )
        ):
            break
        parts.append(line)
    return " ".join(parts).strip()


def _amex_pdf_cutoff(text: str) -> tuple[int, int]:
    match = re.search(r"(?P<day>\d{2})-(?P<month>[A-Za-z]{3})-(?P<year>20\d{2})", text)
    if not match:
        return timezone.localdate().year, timezone.localdate().month
    month = PDF_MONTH_ABBR.get(match.group("month").upper(), timezone.localdate().month)
    return int(match.group("year")), month


def _amex_pdf_referencia(*lines: str) -> str:
    for line in lines:
        match = re.search(r"/REF\s*([A-Za-z0-9-]+)", line)
        if match:
            return match.group(1)
    return ""


def _pdf_month_number(value: str) -> int | None:
    normalized = _normalize_header(value).upper().replace("_", "")
    return PDF_MONTHS.get(normalized)


def _pdf_rows_from_tables(page, *, page_number: int) -> list[dict[str, Any]]:
    parsed_rows: list[dict[str, Any]] = []
    for table in page.extract_tables() or []:
        if not table:
            continue
        header_idx = _pdf_header_index(table)
        if header_idx is None:
            continue
        headers = [_normalize_header(cell or "") for cell in table[header_idx]]
        for row_idx, values in enumerate(table[header_idx + 1 :], start=header_idx + 2):
            row = {
                headers[index]: _clean_value(value)
                for index, value in enumerate(values)
                if index < len(headers) and headers[index]
            }
            if _looks_like_bank_row(row):
                row["pagina_pdf"] = page_number
                row["fila_pdf"] = row_idx
                parsed_rows.append(row)
    return parsed_rows


def _pdf_header_index(table: list[list[Any]]) -> int | None:
    best_idx = None
    best_score = 0
    for index, row in enumerate(table[:12]):
        normalized = [_normalize_header(cell or "") for cell in row]
        score = _xml_header_score(normalized)
        if score > best_score:
            best_idx = index
            best_score = score
    return best_idx if best_score >= 3 else None


def _pdf_rows_from_text(text: str, *, page_number: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        row = _pdf_row_from_line(line)
        if row:
            row["pagina_pdf"] = page_number
            row["linea_pdf"] = line_number
            rows.append(row)
    return rows


def _pdf_row_from_line(line: str) -> dict[str, Any] | None:
    clean = re.sub(r"\s+", " ", str(line or "").strip())
    if not clean:
        return None

    date_match = re.match(r"^(?P<fecha>\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}-\d{2}-\d{2})\s+(?P<body>.+)$", clean)
    if not date_match:
        return None

    body = date_match.group("body")
    amounts = list(re.finditer(r"(?<![A-Z0-9])[-+]?\$?\s*\d{1,3}(?:,\d{3})*(?:\.\d{2})(?![A-Z0-9])", body))
    if not amounts:
        return None

    amount_match = amounts[-2] if len(amounts) >= 2 else amounts[-1]
    descripcion = body[: amount_match.start()].strip(" -")
    trailing = body[amount_match.end() :].strip()
    if not descripcion:
        return None

    amount_text = amount_match.group()
    tipo = _pdf_tipo_from_line(body=body, trailing=trailing, amount=amount_text)
    row = {
        "fecha": date_match.group("fecha"),
        "descripcion": descripcion,
        "referencia": _pdf_referencia_from_text(body),
    }
    if tipo == MovimientoBancario.TIPO_ABONO:
        row["abono"] = amount_text
    else:
        row["cargo"] = amount_text
    if trailing:
        row["saldo"] = trailing.split(" ", 1)[0]
    return row


def _pdf_tipo_from_line(*, body: str, trailing: str, amount: str) -> str:
    normalized = _normalize_text(f"{body} {trailing}")
    amount_value = _decimal_or_none(amount)
    if amount_value is not None and amount_value < 0:
        return MovimientoBancario.TIPO_CARGO
    if any(token in normalized for token in ["ABONO", "DEPOSITO", "PAGO RECIBIDO", "SPEI RECIBIDO", "CREDITO"]):
        return MovimientoBancario.TIPO_ABONO
    if any(token in normalized for token in ["CARGO", "RETIRO", "PAGO", "COMISION", "IVA", "COMPRA"]):
        return MovimientoBancario.TIPO_CARGO
    return MovimientoBancario.TIPO_CARGO


def _pdf_referencia_from_text(text: str) -> str:
    match = re.search(r"\b(?:REF|REFERENCIA|AUT|AUTORIZACION|FOLIO)[:\s-]*([A-Z0-9-]{4,})", text, flags=re.IGNORECASE)
    return match.group(1) if match else ""


def _looks_like_fecha_bancaria(value: Any) -> bool:
    return bool(re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", str(value or "").strip()))


def _bajio_detallado_has_amount(row) -> bool:
    try:
        cargo = _decimal(row.get("cargo"))
        abono = _decimal(row.get("abono"))
    except ImportacionBancariaError:
        return False
    return cargo != 0 or abono != 0


def _read_xml_dataframe(content: bytes) -> pd.DataFrame:
    try:
        root = ET.fromstring(content)
    except ET.ParseError as exc:
        raise ImportacionBancariaError("XML invalido.") from exc
    if _is_bajio_statement_cfdi(root):
        return _read_bajio_statement_cfdi_dataframe(root)
    if _is_cfdi_xml(root):
        raise ImportacionBancariaError(
            "Este XML es un CFDI/factura, no un estado de cuenta bancario. "
            "Para conciliar, descarga movimientos de la cuenta en CSV, XLS, XLSX o XML tabular."
        )

    table_dataframe = _read_xml_table_dataframe(root)
    if not table_dataframe.empty:
        return table_dataframe

    candidates: dict[str, list[dict[str, Any]]] = {}
    for element in root.iter():
        row = _flatten_xml_element(element)
        if _looks_like_bank_row(row):
            candidates.setdefault(_xml_tag(element.tag), []).append(row)

    if not candidates:
        return pd.DataFrame()

    rows = max(candidates.values(), key=lambda items: (len(items), _xml_rows_score(items)))
    return pd.DataFrame(rows)


def _read_xml_table_dataframe(root: ET.Element) -> pd.DataFrame:
    tables: list[list[list[str]]] = []
    for element in root.iter():
        row_elements = [child for child in list(element) if _xml_tag(child.tag).lower() == "row"]
        rows = [_xml_row_values(row) for row in row_elements]
        rows = [row for row in rows if any(value for value in row)]
        if rows:
            tables.append(rows)

    best_rows: list[dict[str, Any]] = []
    for table in tables:
        rows = _xml_table_to_dict_rows(table)
        if len(rows) > len(best_rows):
            best_rows = rows
    return pd.DataFrame(best_rows)


def _is_bajio_statement_cfdi(root: ET.Element) -> bool:
    if _normalize_header(_xml_tag(root.tag)) != "comprobante":
        return False
    has_bajio_addenda = any(_normalize_header(_xml_tag(element.tag)) == "estado_de_cuenta_bajio" for element in root.iter())
    emisor_rfc = ""
    for element in root.iter():
        if _normalize_header(_xml_tag(element.tag)) == "emisor":
            emisor_rfc = _xml_attr(element, "Rfc")
            break
    return has_bajio_addenda and emisor_rfc == "BBA940707IE1"


def _read_bajio_statement_cfdi_dataframe(root: ET.Element) -> pd.DataFrame:
    root_fecha = _xml_attr(root, "Fecha")
    rows: list[dict[str, Any]] = []
    for concepto in root.iter():
        if _normalize_header(_xml_tag(concepto.tag)) != "concepto":
            continue
        descripcion = _xml_attr(concepto, "Descripcion")
        importe = _xml_attr(concepto, "Importe")
        referencia = _xml_attr(concepto, "NoIdentificacion")
        if not descripcion or not importe:
            continue
        impuesto = Decimal("0")
        for child in concepto.iter():
            if _normalize_header(_xml_tag(child.tag)) == "traslado":
                impuesto += _decimal(_xml_attr(child, "Importe") or "0")
        rows.append(
            {
                "fecha": _fecha_bajio_no_identificacion(referencia) or root_fecha,
                "descripcion": descripcion,
                "cargo": str(_decimal(importe) + impuesto),
                "referencia": referencia,
                "moneda": _xml_attr(root, "Moneda") or "MXN",
                "raw_tipo": "cfdi_bajio_estado_cuenta",
                "importe_base": importe,
                "impuesto": str(impuesto),
            }
        )
    return pd.DataFrame(rows)


def _fecha_bajio_no_identificacion(value: str) -> str:
    match = re.search(r"(20\d{2})(\d{2})(\d{2})", str(value or ""))
    if not match:
        return ""
    year, month, day = match.groups()
    return f"{year}-{month}-{day}"


def _is_cfdi_xml(root: ET.Element) -> bool:
    root_tag = _normalize_header(_xml_tag(root.tag))
    if root_tag != "comprobante":
        return False
    namespace = str(root.tag).split("}", 1)[0].strip("{") if "}" in str(root.tag) else ""
    if "sat.gob.mx/cfd" in namespace:
        return True
    attrs = {_normalize_header(key) for key in root.attrib}
    return {"tipo_de_comprobante", "sello", "certificado"} & attrs != set()


def _xml_row_values(row: ET.Element) -> list[str]:
    values: list[str] = []
    for cell in list(row):
        if _xml_tag(cell.tag).lower() != "cell":
            continue
        index = _xml_attr(cell, "Index")
        if index and str(index).isdigit():
            while len(values) < int(index) - 1:
                values.append("")
        values.append(_xml_cell_text(cell))
    return values


def _xml_cell_text(cell: ET.Element) -> str:
    texts: list[str] = []
    for element in cell.iter():
        if element is cell:
            continue
        text = (element.text or "").strip()
        if text:
            texts.append(text)
    if texts:
        return " ".join(texts).strip()
    return (cell.text or "").strip()


def _xml_table_to_dict_rows(table: list[list[str]]) -> list[dict[str, Any]]:
    for header_index, header in enumerate(table):
        if _xml_header_score(header) < 3:
            continue
        normalized_header = [str(value or "").strip() for value in header]
        rows: list[dict[str, Any]] = []
        for raw_values in table[header_index + 1 :]:
            row = {
                normalized_header[index]: raw_values[index] if index < len(raw_values) else ""
                for index in range(len(normalized_header))
                if normalized_header[index]
            }
            if _looks_like_bank_row(row):
                rows.append(row)
        if rows:
            return rows
    return []


def _xml_header_score(header: list[str]) -> int:
    normalized = {_normalize_header(value) for value in header if value}
    score = 0
    if normalized & {"fecha", "fecha_operacion", "fecha_de_operacion", "fecha_movimiento", "fecha_valor"}:
        score += 1
    if normalized & {"descripcion", "concepto", "detalle", "movimiento", "operacion"}:
        score += 1
    if normalized & {"cargo", "cargos", "retiro", "retiros", "debe"}:
        score += 1
    if normalized & {"abono", "abonos", "deposito", "depositos", "credito", "haber"}:
        score += 1
    if normalized & {"monto", "importe", "importe_movimiento", "valor", "importe_total"}:
        score += 1
    return score


def _flatten_xml_element(element: ET.Element) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for key, value in element.attrib.items():
        row[_xml_tag(key)] = value
    _flatten_xml_children(element, row=row, prefix="")
    return row


def _flatten_xml_children(element: ET.Element, *, row: dict[str, Any], prefix: str) -> None:
    for child in list(element):
        tag = _xml_tag(child.tag)
        key = f"{prefix}_{tag}" if prefix else tag
        text = (child.text or "").strip()
        if child.attrib:
            for attr_key, attr_value in child.attrib.items():
                row[f"{key}_{_xml_tag(attr_key)}"] = attr_value
        if list(child):
            _flatten_xml_children(child, row=row, prefix=key)
        elif text:
            row[key] = text


def _looks_like_bank_row(row: dict[str, Any]) -> bool:
    normalized = {_normalize_header(key): value for key, value in row.items()}
    has_date = _first(
        normalized,
        "fecha",
        "fecha_operacion",
        "fecha_de_operacion",
        "fecha_movimiento",
        "fecha_de_movimiento",
        "fecha_valor",
        "fecha_aplicacion",
        "fecha_contable",
        "date",
    ) not in (None, "")
    has_description = _first(
        normalized,
        "descripcion",
        "concepto",
        "concepto_pago",
        "detalle",
        "movimiento",
        "descripcion_movimiento",
        "operacion",
        "description",
        "referencia",
        "folio",
        "rastreo",
    ) not in (None, "")
    has_amount = _first(
        normalized,
        "cargo",
        "cargos",
        "importe_cargo",
        "retiro",
        "retiros",
        "debe",
        "abono",
        "abonos",
        "importe_abono",
        "deposito",
        "depositos",
        "credito",
        "creditos",
        "haber",
        "monto",
        "importe",
        "importe_movimiento",
        "amount",
        "valor",
        "importe_total",
    ) not in (None, "")
    return has_date and has_description and has_amount


def _xml_rows_score(rows: list[dict[str, Any]]) -> int:
    return sum(len(row) for row in rows)


def _xml_tag(value: str) -> str:
    return str(value).rsplit("}", 1)[-1]


def _xml_attr(element: ET.Element, name: str) -> str:
    normalized_name = _normalize_header(name)
    for key, value in element.attrib.items():
        if _normalize_header(_xml_tag(key)) == normalized_name:
            return str(value)
    return ""


def _fuente_manual(suffix: str) -> str:
    if suffix == "csv":
        return ImportacionBancaria.FUENTE_MANUAL_CSV
    if suffix == "pdf":
        return ImportacionBancaria.FUENTE_MANUAL_PDF
    return ImportacionBancaria.FUENTE_MANUAL_EXCEL


def _normalizar_movimiento(*, row: dict[str, Any], fila: int) -> MovimientoNormalizado:
    fecha = _parse_fecha(
        _first(
            row,
            "fecha",
            "fecha_operacion",
            "fecha_de_operacion",
            "fecha_movimiento",
            "fecha_de_movimiento",
            "fecha_valor",
            "fecha_aplicacion",
            "fecha_contable",
            "date",
        )
    )
    descripcion = str(
        _first(
            row,
            "descripcion",
            "concepto",
            "concepto_pago",
            "detalle",
            "movimiento",
            "descripcion_movimiento",
            "operacion",
            "description",
        )
        or ""
    ).strip()
    referencia = str(_first(row, "referencia", "folio", "autorizacion", "rastreo", "reference") or "").strip()
    if not descripcion and referencia:
        descripcion = referencia
    if not descripcion:
        raise ImportacionBancariaError("falta descripcion/concepto.")

    cargo = _decimal_or_none(_first_decimal_value(row, "cargo", "cargos", "importe_cargo", "retiro", "retiros", "debe"))
    abono = _decimal_or_none(
        _first_decimal_value(
            row,
            "abono",
            "abonos",
            "importe_abono",
            "deposito",
            "depositos",
            "credito",
            "creditos",
            "haber",
        )
    )
    monto_directo = _decimal_or_none(
        _first_decimal_value(row, "monto", "importe", "importe_movimiento", "amount", "valor", "importe_total")
    )
    if cargo is not None or abono is not None:
        firmado = (abono or Decimal("0")) - (cargo or Decimal("0"))
    elif monto_directo is not None:
        firmado = _aplicar_tipo_movimiento(monto_directo, row)
    else:
        raise ImportacionBancariaError("falta monto, cargo o abono.")
    if firmado == 0:
        raise ImportacionBancariaError("monto en cero.")

    tipo = MovimientoBancario.TIPO_ABONO if firmado > 0 else MovimientoBancario.TIPO_CARGO
    return MovimientoNormalizado(
        fecha=fecha,
        descripcion=descripcion[:500],
        monto=abs(firmado),
        tipo=tipo,
        moneda=str(_first(row, "moneda", "currency") or "MXN").strip()[:10] or "MXN",
        referencia=referencia[:120],
        saldo=_decimal_or_none(_first_decimal_value(row, "saldo", "balance")),
        fila=fila,
        raw={str(key): _stringify(value) for key, value in row.items()},
    )


def _manual_transaction_id(*, cuenta: CuentaBancaria, movimiento: MovimientoNormalizado) -> str:
    fingerprint = "|".join(
        [
            str(cuenta.pk),
            movimiento.fecha.date().isoformat(),
            str(movimiento.monto_firmado),
            _compact(movimiento.descripcion),
            _compact(movimiento.referencia),
            str(movimiento.saldo or ""),
        ]
    )
    digest = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:32]
    return f"manual:{cuenta.pk}:{digest}"


def _parse_fecha(value: Any) -> datetime:
    if value in (None, ""):
        raise ImportacionBancariaError("falta fecha.")
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    else:
        text = str(value).strip()
        dayfirst = not bool(re.match(r"^\d{4}-\d{2}-\d{2}(?:[T\\s]|$)", text))
        parsed_ts = pd.to_datetime(text, dayfirst=dayfirst, errors="coerce")
        if pd.isna(parsed_ts):
            raise ImportacionBancariaError("fecha invalida.")
        parsed = parsed_ts.to_pydatetime()
    if timezone.is_naive(parsed):
        parsed = timezone.make_aware(parsed, timezone=timezone.get_current_timezone())
    return parsed


def _normalize_header(value: Any) -> str:
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(value or "").strip()).lower()
    replacements = str.maketrans("áéíóúüñ", "aeiouun")
    text = text.translate(replacements)
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text


def _aplicar_tipo_movimiento(monto: Decimal, row: dict[str, Any]) -> Decimal:
    tipo_raw = str(
        _first(row, "tipo", "tipo_movimiento", "tipo_operacion", "naturaleza", "cargo_abono", "signo") or ""
    ).strip()
    tipo = _normalize_header(tipo_raw)
    if tipo in {"cargo", "cargos", "retiro", "retiros", "debito", "debe", "egreso", "salida", "minus", "negativo"}:
        return -abs(monto)
    if tipo in {"abono", "abonos", "deposito", "depositos", "credito", "haber", "ingreso", "entrada", "plus", "positivo"}:
        return abs(monto)
    return monto


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        normalized = _normalize_header(key)
        if normalized in row and row[normalized] not in (None, ""):
            return row[normalized]
        suffix = f"_{normalized}"
        for row_key, value in row.items():
            if str(row_key).endswith(suffix) and value not in (None, ""):
                return value
    return None


def _first_decimal_value(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        normalized = _normalize_header(key)
        candidates: list[Any] = []
        if normalized in row and row[normalized] not in (None, ""):
            candidates.append(row[normalized])
        suffix = f"_{normalized}"
        for row_key, value in row.items():
            if str(row_key).endswith(suffix) and value not in (None, ""):
                candidates.append(value)
        for value in candidates:
            try:
                _decimal(value)
            except ImportacionBancariaError:
                continue
            return value
    return None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return _decimal(value)


def _decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    text = str(value or "0").strip()
    if not text:
        return Decimal("0")
    negative = text.startswith("(") and text.endswith(")")
    text = text.replace("$", "").replace(",", "").replace("MXN", "").replace("USD", "").strip("() ")
    try:
        amount = Decimal(text)
    except (InvalidOperation, TypeError) as exc:
        raise ImportacionBancariaError("monto invalido.") from exc
    return -amount if negative else amount


def _clean_value(value: Any) -> Any:
    if pd.isna(value):
        return ""
    return value


def _stringify(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value or "")


def _compact(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())
