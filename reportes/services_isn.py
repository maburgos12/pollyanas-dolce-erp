from collections import defaultdict
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
import re
from xml.etree import ElementTree as ET

from rrhh.models import NominaConceptoLinea, NominaPeriodo


CENT = Decimal("0.01")
ZERO = Decimal("0")
CFDI_NS = "{http://www.sat.gob.mx/cfd/4}"
RFC_SINALOA = "GES8101015I7"
RFC_EMPRESA = "GEF211230KR2"
CODIGOS_EXENTOS_COMPLETOS = {"20", "22", "26", "32"}
UMA_DIARIA = {2026: Decimal("117.31")}


def money(value: Decimal) -> Decimal:
    return Decimal(value).quantize(CENT, rounding=ROUND_HALF_UP)


def calcular_isn_sinaloa(base: Decimal) -> Decimal:
    base = max(ZERO, Decimal(base))
    if base <= Decimal("500000.00"):
        return money(base * Decimal("0.024"))
    if base <= Decimal("700000.00"):
        return money(
            Decimal("12000")
            + (base - Decimal("500000.01")) * Decimal("0.026")
        )
    if base <= Decimal("900000.00"):
        return money(
            Decimal("17200")
            + (base - Decimal("700000.01")) * Decimal("0.028")
        )
    return money(
        Decimal("22800") + (base - Decimal("900000.01")) * Decimal("0.03")
    )


def prorratear_isn(
    bases: dict[int, Decimal], total: Decimal
) -> dict[int, Decimal]:
    bases = {key: max(ZERO, Decimal(value)) for key, value in bases.items()}
    denominator = sum(bases.values(), ZERO)
    if denominator <= ZERO:
        raise ValueError("La base gravada total debe ser positiva.")

    exact = {
        key: Decimal(total) * value / denominator for key, value in bases.items()
    }
    rounded = {
        key: value.quantize(CENT, rounding=ROUND_HALF_UP)
        for key, value in exact.items()
    }
    difference = money(Decimal(total) - sum(rounded.values(), ZERO))
    cents = int(abs(difference / CENT))
    direction = CENT if difference > ZERO else -CENT
    order = sorted(
        exact,
        key=lambda key: (
            -(exact[key] - rounded[key]) * (1 if direction > ZERO else -1),
            key,
        ),
    )
    for key in order[:cents]:
        rounded[key] += direction
    return rounded


def extraer_isn_cfdi(cfdi) -> tuple[date, Decimal]:
    if cfdi.rfc_emisor != RFC_SINALOA or cfdi.rfc_receptor != RFC_EMPRESA:
        raise ValueError("El CFDI no corresponde al ISN de la empresa.")
    if (
        (cfdi.estatus or "").lower() != "vigente"
        or cfdi.tipo_cfdi != "recibido"
        or cfdi.tipo_comprobante != "I"
    ):
        raise ValueError("El CFDI no esta vigente como ingreso recibido.")

    try:
        root = ET.fromstring((cfdi.xml_raw or "").lstrip("\ufeff"))
    except ET.ParseError as exc:
        raise ValueError("El XML del CFDI de ISN no es valido.") from exc

    matches = []
    for concepto in root.findall(f".//{CFDI_NS}Concepto"):
        if "nomina" not in concepto.attrib.get("Descripcion", "").lower():
            continue
        match = re.match(
            r"(\d{4})(\d{2})\b",
            concepto.attrib.get("NoIdentificacion", ""),
        )
        if not match:
            continue
        try:
            periodo = date(int(match[1]), int(match[2]), 1)
            importe = Decimal(concepto.attrib["Importe"])
        except (KeyError, ValueError, ArithmeticError) as exc:
            raise ValueError("El concepto de ISN tiene periodo o importe invalido.") from exc
        matches.append((periodo, importe))

    periodos = {periodo for periodo, _ in matches}
    if not matches or len(periodos) != 1:
        raise ValueError("Periodo fiscal de ISN no inequivoco.")
    return periodos.pop(), money(sum((importe for _, importe in matches), ZERO))


def bases_gravadas_empleados(periodo: date) -> dict[int, Decimal]:
    try:
        uma_diaria = UMA_DIARIA[periodo.year]
    except KeyError as exc:
        raise ValueError(f"No hay UMA configurada para {periodo.year}.") from exc

    conceptos = (
        NominaConceptoLinea.objects.filter(
            tipo=NominaConceptoLinea.TIPO_PERCEPCION,
            linea__periodo__estatus__in=(
                NominaPeriodo.ESTATUS_CERRADA,
                NominaPeriodo.ESTATUS_PAGADA,
            ),
            linea__periodo__fecha_fin__year=periodo.year,
            linea__periodo__fecha_fin__month=periodo.month,
        )
        .select_related("linea__empleado", "linea__empleado__sucursal_ref")
        .order_by("linea__empleado_id", "id")
    )

    bases = defaultdict(lambda: ZERO)
    aguinaldos = defaultdict(lambda: ZERO)
    empleados_vistos = set()
    for concepto in conceptos:
        empleado = concepto.linea.empleado
        if not empleado.sucursal_ref_id or not empleado.departamento:
            raise ValueError(
                f"El empleado {empleado.codigo or empleado.pk} requiere sucursal y departamento."
            )

        empleado_id = empleado.pk
        empleados_vistos.add(empleado_id)
        codigo = (concepto.codigo_concepto or "").strip()
        importe = Decimal(concepto.importe or ZERO)
        if codigo in CODIGOS_EXENTOS_COMPLETOS:
            continue
        if codigo == "24":
            aguinaldos[empleado_id] += importe
            continue
        bases[empleado_id] += importe

    exencion_aguinaldo = Decimal("30") * uma_diaria
    for empleado_id in empleados_vistos:
        bases[empleado_id] += max(
            ZERO,
            aguinaldos[empleado_id] - exencion_aguinaldo,
        )

    return {
        empleado_id: money(base)
        for empleado_id, base in sorted(bases.items())
    }
