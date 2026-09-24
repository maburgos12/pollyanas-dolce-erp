from collections import Counter, defaultdict
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
import re
import unicodedata
from xml.etree import ElementTree as ET

from rrhh.models import NominaConceptoLinea, NominaLinea, NominaPeriodo


CENT = Decimal("0.01")
ZERO = Decimal("0")
CFDI_NS = "{http://www.sat.gob.mx/cfd/4}"
RFC_SINALOA = "GES8101015I7"
RFC_EMPRESA = "GEF211230KR2"
CODIGOS_EXENTOS_COMPLETOS = {"20", "22", "26", "32"}
# Dos MiB cubren holgadamente un CFDI individual y limitan uso de memoria abusivo.
MAX_CFDI_XML_BYTES = 2 * 1024 * 1024
UMA_DIARIA_VIGENCIAS = (
    (date(2026, 1, 1), date(2026, 1, 31), Decimal("113.14")),
    (date(2026, 2, 1), date(2026, 12, 31), Decimal("117.31")),
)


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
    if (
        (cfdi.rfc_emisor or "").strip().upper() != RFC_SINALOA
        or (cfdi.rfc_receptor or "").strip().upper() != RFC_EMPRESA
    ):
        raise ValueError("El CFDI no corresponde al ISN de la empresa.")
    if (
        (cfdi.estatus or "").strip().upper() != "VIGENTE"
        or cfdi.tipo_cfdi != "recibido"
        or cfdi.tipo_comprobante != "I"
    ):
        raise ValueError("El CFDI no esta vigente como ingreso recibido.")

    xml = cfdi.xml_raw or ""
    if not xml.strip():
        raise ValueError("El XML del CFDI de ISN esta vacio.")
    if len(xml.encode("utf-8")) > MAX_CFDI_XML_BYTES:
        raise ValueError("El XML del CFDI de ISN excede el limite permitido.")
    xml_upper = xml.upper()
    if "<!DOCTYPE" in xml_upper or "<!ENTITY" in xml_upper:
        raise ValueError("El XML del CFDI de ISN contiene declaraciones no permitidas.")
    try:
        root = ET.fromstring(xml.lstrip("\ufeff"))
    except ET.ParseError as exc:
        raise ValueError("El XML del CFDI de ISN no es valido.") from exc

    matches = []
    for concepto in root.findall(f".//{CFDI_NS}Concepto"):
        identificador = " ".join(
            concepto.attrib.get("NoIdentificacion", "").split()
        ).upper()
        match_canonico = re.fullmatch(
            r"(\d{4})(0[1-9]|1[0-2]) 2-003",
            identificador,
        )
        match_legacy = re.fullmatch(
            r"(\d{4})(0[1-9]|1[0-2])",
            identificador,
        )
        if match_canonico:
            match = match_canonico
        elif match_legacy:
            descripcion = "".join(
                caracter
                for caracter in unicodedata.normalize(
                    "NFKD",
                    concepto.attrib.get("Descripcion", ""),
                )
                if not unicodedata.combining(caracter)
            ).casefold()
            if not re.search(r"\bnomina\b", descripcion):
                raise ValueError("El concepto fiscal legado de ISN no es clasificable.")
            match = match_legacy
        elif re.match(r"^\d{4}(0[1-9]|1[0-2])", identificador):
            raise ValueError("El concepto fiscal de ISN usa un codigo no permitido.")
        else:
            continue
        try:
            periodo = date(int(match[1]), int(match[2]), 1)
            importe = Decimal(concepto.attrib["Importe"])
        except (KeyError, ValueError, ArithmeticError) as exc:
            raise ValueError("El concepto de ISN tiene periodo o importe invalido.") from exc
        if not importe.is_finite() or importe <= ZERO:
            raise ValueError("El importe del concepto de ISN debe ser positivo y finito.")
        matches.append((periodo, importe))

    periodos = {periodo for periodo, _ in matches}
    if not matches or len(periodos) != 1:
        raise ValueError("El periodo fiscal de ISN no es inequivoco.")
    return periodos.pop(), money(sum((importe for _, importe in matches), ZERO))


def _uma_diaria_vigente(fecha: date) -> Decimal:
    for inicio, fin, valor in UMA_DIARIA_VIGENCIAS:
        if inicio <= fecha <= fin:
            return valor
    raise ValueError(f"No hay UMA configurada para {fecha.isoformat()}.")


def _validar_periodos_nomina_mes(periodos) -> None:
    if not periodos:
        raise ValueError("El mes no tiene periodos de nomina cerrados o pagados.")
    tipos = {periodo.tipo_periodo for periodo in periodos}
    if len(tipos) != 1:
        raise ValueError("El mes mezcla mas de un tipo de periodo de nomina.")

    rangos = [(periodo.fecha_inicio, periodo.fecha_fin) for periodo in periodos]
    if len(set(rangos)) != len(rangos):
        raise ValueError("El mes contiene rangos de nomina duplicados.")
    rangos_ordenados = sorted(rangos)
    for (inicio_anterior, fin_anterior), (inicio, fin) in zip(
        rangos_ordenados,
        rangos_ordenados[1:],
    ):
        if inicio <= fin_anterior:
            raise ValueError(
                "El mes contiene rangos de nomina solapados: "
                f"{inicio_anterior} a {fin_anterior} y {inicio} a {fin}."
            )


def _validar_lineas_nomina_mes(lineas) -> None:
    conteos = Counter((linea.periodo_id, linea.empleado_id) for linea in lineas)
    if any(total > 1 for total in conteos.values()):
        raise ValueError("Existe una linea de nomina duplicada para periodo y empleado.")


def bases_gravadas_empleados(periodo: date) -> dict[int, Decimal]:
    fecha_mes = date(periodo.year, periodo.month, 1)
    uma_diaria = _uma_diaria_vigente(fecha_mes)
    estados_validos = (
        NominaPeriodo.ESTATUS_CERRADA,
        NominaPeriodo.ESTATUS_PAGADA,
    )
    periodos_mes = list(
        NominaPeriodo.objects.filter(
            estatus__in=estados_validos,
            fecha_fin__year=periodo.year,
            fecha_fin__month=periodo.month,
        ).order_by("fecha_inicio", "fecha_fin", "id")
    )
    _validar_periodos_nomina_mes(periodos_mes)

    lineas = list(
        NominaLinea.objects.filter(periodo_id__in=[item.pk for item in periodos_mes])
        .select_related("empleado", "empleado__sucursal_ref", "periodo")
        .order_by("periodo_id", "empleado_id", "id")
    )
    _validar_lineas_nomina_mes(lineas)

    bases = {}
    empleado_ids = set()
    for linea in lineas:
        empleado = linea.empleado
        if not empleado.sucursal_ref_id or not empleado.departamento:
            raise ValueError(
                f"El empleado {empleado.codigo or empleado.pk} requiere sucursal y departamento."
            )
        empleado_ids.add(empleado.pk)
        bases[empleado.pk] = ZERO

    conceptos_mes = list(
        NominaConceptoLinea.objects.filter(
            linea_id__in=[linea.pk for linea in lineas],
            tipo=NominaConceptoLinea.TIPO_PERCEPCION,
        ).order_by("linea__empleado_id", "id")
    )
    aguinaldos_previos = defaultdict(lambda: ZERO)
    aguinaldos_mes = defaultdict(lambda: ZERO)
    for concepto in conceptos_mes:
        empleado_id = concepto.linea.empleado_id
        codigo = (concepto.codigo_concepto or "").strip()
        importe = Decimal(concepto.importe or ZERO)
        if importe < ZERO:
            raise ValueError("La nomina contiene una percepcion negativa.")
        if codigo in CODIGOS_EXENTOS_COMPLETOS:
            continue
        if codigo == "24":
            aguinaldos_mes[empleado_id] += importe
            continue
        bases[empleado_id] += importe

    conceptos_aguinaldo_previos = NominaConceptoLinea.objects.filter(
        linea__empleado_id__in=empleado_ids,
        linea__periodo__estatus__in=estados_validos,
        linea__periodo__fecha_fin__year=periodo.year,
        linea__periodo__fecha_fin__month__lt=periodo.month,
        tipo=NominaConceptoLinea.TIPO_PERCEPCION,
        codigo_concepto="24",
    ).order_by("linea__empleado_id", "id")
    for concepto in conceptos_aguinaldo_previos:
        importe = Decimal(concepto.importe or ZERO)
        if importe < ZERO:
            raise ValueError("La nomina contiene una percepcion negativa.")
        aguinaldos_previos[concepto.linea.empleado_id] += importe

    exencion_aguinaldo = Decimal("30") * uma_diaria
    for empleado_id in empleado_ids:
        gravado_previo = max(
            ZERO,
            aguinaldos_previos[empleado_id] - exencion_aguinaldo,
        )
        gravado_acumulado = max(
            ZERO,
            aguinaldos_previos[empleado_id]
            + aguinaldos_mes[empleado_id]
            - exencion_aguinaldo,
        )
        bases[empleado_id] += gravado_acumulado - gravado_previo

    return {
        empleado_id: money(base)
        for empleado_id, base in sorted(bases.items())
    }
