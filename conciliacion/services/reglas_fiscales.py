from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class ReglaConciliacionFiscal:
    codigo: str
    flujo: str
    sat: str
    banco: str
    metodo: str
    formas_pago: tuple[str, ...]
    movimiento: str
    permite_match_directo: bool
    requiere_cfdi: str
    requiere_evidencia: str
    siguiente_revision: str
    mesa_label: str
    mesa_detalle: str
    tono: str = "is-info"


MATRIZ_REGLAS_CONCILIACION: tuple[ReglaConciliacionFiscal, ...] = (
    ReglaConciliacionFiscal(
        codigo="EFECTIVO_SUCURSAL_DIA",
        flujo="Efectivo por sucursal y dia",
        sat="CFDI global o publico en general con forma 01 Efectivo.",
        banco="Deposito en efectivo de ventanilla o concentracion bancaria.",
        metodo="Bolsa diaria por sucursal: no se concilia CFDI por CFDI.",
        formas_pago=("01",),
        movimiento="abono",
        permite_match_directo=False,
        requiere_cfdi="emitido",
        requiere_evidencia="Corte de caja, sucursal, fecha de venta y ficha de deposito.",
        siguiente_revision="Comparar venta en efectivo por sucursal contra depositos; explicar faltantes, sobrantes o depositos diferidos.",
        mesa_label="Efectivo",
        mesa_detalle="No CFDI individual; revisar bolsa por sucursal y dia.",
        tono="is-warn",
    ),
    ReglaConciliacionFiscal(
        codigo="TARJETA_TPV_NETO",
        flujo="Tarjetas: deposito neto",
        sat="CFDI emitidos con forma 04 Credito, 28 Debito o 29 Servicios.",
        banco="Depositos de adquirente, BanBajio negocios afiliados, OptBlue o American Express.",
        metodo="Venta bruta contra deposito neto, comisiones, IVA de comisiones y liquidacion del adquirente.",
        formas_pago=("04", "28", "29"),
        movimiento="abono",
        permite_match_directo=False,
        requiere_cfdi="emitido y recibido",
        requiere_evidencia="Liquidacion del adquirente y CFDI recibido por comisiones.",
        siguiente_revision="Cruzar ventas brutas de tarjeta contra depositos netos y cargos de comision.",
        mesa_label="Tarjeta",
        mesa_detalle="No empatar por monto exacto; revisar deposito neto mas comisiones.",
        tono="is-warn",
    ),
    ReglaConciliacionFiscal(
        codigo="TRANSFERENCIA_CLIENTE",
        flujo="Transferencia de cliente",
        sat="CFDI emitido PUE forma 03 o complemento de pago si la factura fue PPD.",
        banco="SPEI o transferencia recibida identificable por cliente.",
        metodo="Match por monto, fecha, RFC/cliente y referencia cuando exista.",
        formas_pago=("03",),
        movimiento="abono",
        permite_match_directo=True,
        requiere_cfdi="emitido o pago",
        requiere_evidencia="Referencia SPEI, cliente y factura relacionada.",
        siguiente_revision="Buscar CFDI o complemento por monto y fecha; validar cliente si hay varias opciones.",
        mesa_label="Transferencia",
        mesa_detalle="Puede tener CFDI candidato por monto y fecha.",
        tono="is-info",
    ),
    ReglaConciliacionFiscal(
        codigo="CREDITO_COMPLEMENTO_PAGO",
        flujo="Facturas a credito",
        sat="CFDI PPD forma 99 y complemento de pago cuando se cobre o pague.",
        banco="Movimiento real de cobro o pago en el mes, aunque la factura venga de meses anteriores.",
        metodo="Conciliar contra complemento de pago y saldo insoluto, no solo contra la factura original.",
        formas_pago=("99",),
        movimiento="ambos",
        permite_match_directo=False,
        requiere_cfdi="pago",
        requiere_evidencia="Complemento de pago, parcialidad, saldo anterior e insoluto.",
        siguiente_revision="Traer facturas PPD abiertas anteriores y pagos timbrados dentro del mes.",
        mesa_label="Credito",
        mesa_detalle="Revisar complemento de pago y saldo, no solo fecha de factura.",
        tono="is-info",
    ),
    ReglaConciliacionFiscal(
        codigo="GASTO_PROVEEDOR",
        flujo="Proveedores y gastos",
        sat="CFDI recibido vigente, o soporte cuando el movimiento no lleva CFDI.",
        banco="Cargo de proveedor, servicio, compra, tarjeta corporativa o pago operativo.",
        metodo="Match por cargo, proveedor, RFC, fecha y monto con tolerancia.",
        formas_pago=("03", "04", "28", "29"),
        movimiento="cargo",
        permite_match_directo=True,
        requiere_cfdi="recibido",
        requiere_evidencia="Factura recibida, comprobante bancario o autorizacion si no hay CFDI.",
        siguiente_revision="Buscar CFDI recibido por monto y proveedor; clasificar no deducibles aparte.",
        mesa_label="Gasto",
        mesa_detalle="Puede tener CFDI recibido candidato por monto y fecha.",
        tono="is-info",
    ),
    ReglaConciliacionFiscal(
        codigo="COMISION_TPV",
        flujo="Comisiones de tarjeta",
        sat="CFDI recibido del banco o adquirente por comisiones e IVA.",
        banco="Cargo por tasa de descuento, OptBlue, negocios afiliados o comision TPV.",
        metodo="Relacionar comision con liquidacion de tarjeta y CFDI recibido.",
        formas_pago=(),
        movimiento="cargo",
        permite_match_directo=False,
        requiere_cfdi="recibido",
        requiere_evidencia="Liquidacion del adquirente y factura de comision.",
        siguiente_revision="Agrupar comisiones por dia/adquirente y validar IVA acreditable.",
        mesa_label="Comision TPV",
        mesa_detalle="Revisar liquidacion y CFDI de comision.",
        tono="is-warn",
    ),
    ReglaConciliacionFiscal(
        codigo="NOMINA_TIMBRADA",
        flujo="Nomina y dispersion",
        sat="CFDI de nomina timbrado por empleado y periodo.",
        banco="Dispersion de nomina, PTU, finiquito o transferencia a personal.",
        metodo="Cruzar dispersion contra nomina timbrada y recibos autorizados.",
        formas_pago=(),
        movimiento="cargo",
        permite_match_directo=False,
        requiere_cfdi="nomina",
        requiere_evidencia="Nomina timbrada, layout de dispersion y autorizacion.",
        siguiente_revision="Validar que la dispersion corresponde al periodo y empleados pagados.",
        mesa_label="Nomina",
        mesa_detalle="Revisar contra timbrado de nomina y dispersion.",
        tono="is-info",
    ),
    ReglaConciliacionFiscal(
        codigo="IMPUESTOS_DEVOLUCIONES",
        flujo="Impuestos y devoluciones SAT",
        sat="Linea de captura, declaracion, opinion o resolucion; normalmente no hay CFDI.",
        banco="Pago de impuestos, devolucion SAT, recargos o actualizaciones.",
        metodo="Conciliacion documental fiscal, separada de ventas y proveedores.",
        formas_pago=(),
        movimiento="ambos",
        permite_match_directo=False,
        requiere_cfdi="ninguno",
        requiere_evidencia="Declaracion, acuse, linea de captura, resolucion o devolucion.",
        siguiente_revision="Clasificar como fiscal y no forzar CFDI de venta o gasto.",
        mesa_label="Fiscal",
        mesa_detalle="Movimiento fiscal; requiere soporte, no CFDI directo.",
        tono="is-muted",
    ),
    ReglaConciliacionFiscal(
        codigo="TRASPASO_BALANCE",
        flujo="Traspasos, prestamos y balance",
        sat="No siempre genera CFDI; depende de naturaleza legal y fiscal.",
        banco="Traspaso entre cuentas, prestamo recibido, pago de prestamo o aportacion.",
        metodo="Conciliacion de balance con cuenta contraparte y soporte.",
        formas_pago=(),
        movimiento="ambos",
        permite_match_directo=False,
        requiere_cfdi="opcional",
        requiere_evidencia="Cuenta origen/destino, contrato, autorizacion o poliza contable.",
        siguiente_revision="Validar contraparte y no mezclar con ingresos por ventas.",
        mesa_label="Balance",
        mesa_detalle="No es venta; revisar contraparte y soporte.",
        tono="is-muted",
    ),
    ReglaConciliacionFiscal(
        codigo="PENDIENTE_CLASIFICAR",
        flujo="Pendiente de clasificar",
        sat="Requiere revisar forma/metodo de pago o naturaleza del movimiento.",
        banco="Descripcion insuficiente para clasificar automaticamente.",
        metodo="Revision manual antes de marcar conciliado.",
        formas_pago=(),
        movimiento="ambos",
        permite_match_directo=False,
        requiere_cfdi="por definir",
        requiere_evidencia="Descripcion bancaria, cuenta, comprobante y criterio contable.",
        siguiente_revision="Asignar regla fiscal antes de conciliar.",
        mesa_label="Pendiente",
        mesa_detalle="Clasificar antes de conciliar.",
        tono="is-muted",
    ),
)

_REGLAS_POR_CODIGO = {regla.codigo: regla for regla in MATRIZ_REGLAS_CONCILIACION}


def matriz_reglas_conciliacion() -> tuple[ReglaConciliacionFiscal, ...]:
    return MATRIZ_REGLAS_CONCILIACION


def resumen_matriz_reglas() -> dict[str, int]:
    directas = sum(1 for regla in MATRIZ_REGLAS_CONCILIACION if regla.permite_match_directo)
    return {
        "total": len(MATRIZ_REGLAS_CONCILIACION),
        "directas": directas,
        "requieren_bolsa": len(MATRIZ_REGLAS_CONCILIACION) - directas,
    }


def regla_por_codigo(codigo: str) -> ReglaConciliacionFiscal:
    return _REGLAS_POR_CODIGO.get(codigo, _REGLAS_POR_CODIGO["PENDIENTE_CLASIFICAR"])


def regla_para_forma_pago(
    forma_pago: str | None,
    *,
    metodo_pago: str | None = None,
    tipo_comprobante: str | None = None,
) -> ReglaConciliacionFiscal:
    forma = (forma_pago or "").strip()
    metodo = (metodo_pago or "").strip().upper()
    tipo = (tipo_comprobante or "").strip().upper()
    if tipo == "P" or metodo == "PPD" or forma == "99":
        return regla_por_codigo("CREDITO_COMPLEMENTO_PAGO")
    if forma == "01":
        return regla_por_codigo("EFECTIVO_SUCURSAL_DIA")
    if forma in {"04", "28", "29"}:
        return regla_por_codigo("TARJETA_TPV_NETO")
    if forma == "03":
        return regla_por_codigo("TRANSFERENCIA_CLIENTE")
    return regla_por_codigo("PENDIENTE_CLASIFICAR")


def regla_para_movimiento(movimiento) -> ReglaConciliacionFiscal:
    texto = _normalize_text(getattr(movimiento, "descripcion", ""))
    tipo = getattr(movimiento, "tipo", "")
    cuenta = getattr(movimiento, "cuenta", None)
    banco = getattr(cuenta, "banco", "")

    if any(term in texto for term in ("DEVOLUCION SAT", "DEVOLUCION IVA", "DEVOLUCION ISR")):
        return regla_por_codigo("IMPUESTOS_DEVOLUCIONES")
    if any(term in texto for term in ("IMPUESTO", "TESORERIA", "LINEA DE CAPTURA", "SAT ")):
        return regla_por_codigo("IMPUESTOS_DEVOLUCIONES")
    if any(term in texto for term in ("TRASPASO", "PRESTAMO", "APORTACION")):
        return regla_por_codigo("TRASPASO_BALANCE")
    if any(term in texto for term in ("NOMINA", "DISPERSION", "PTU", "FINIQUITO", "FONACOT")):
        return regla_por_codigo("NOMINA_TIMBRADA")
    if any(term in texto for term in ("COMISION", "TASA DE DESCUENTO", "OPTBLUE")):
        return regla_por_codigo("COMISION_TPV")
    if tipo == "abono" and any(term in texto for term in ("DEPOSITO EN EFECTIVO", "VENTANILLA")):
        return regla_por_codigo("EFECTIVO_SUCURSAL_DIA")
    if tipo == "abono" and any(term in texto for term in ("NEGOCIOS AFILIADOS", "VENTAS AL DETALLE", "AMEX")):
        return regla_por_codigo("TARJETA_TPV_NETO")
    if tipo == "abono" and any(term in texto for term in ("SPEI", "TRANSFER")):
        return regla_por_codigo("TRANSFERENCIA_CLIENTE")
    if tipo == "cargo" and banco == "amex":
        return regla_por_codigo("GASTO_PROVEEDOR")
    if tipo == "cargo":
        return regla_por_codigo("GASTO_PROVEEDOR")
    return regla_por_codigo("PENDIENTE_CLASIFICAR")


def reglas_para_formas_pago(formas_pago: Iterable[str]) -> list[ReglaConciliacionFiscal]:
    return [regla_para_forma_pago(forma) for forma in formas_pago]


def _normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    without_accents = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    upper = without_accents.upper()
    upper = re.sub(r"[^A-Z0-9]+", " ", upper)
    return re.sub(r"\s+", " ", upper).strip()
