from collections import Counter, defaultdict
from calendar import monthrange
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
import re
import unicodedata
from xml.etree import ElementTree as ET

from django.db import IntegrityError, transaction
from django.db.models import Count, Sum
from django.utils import timezone

from reportes.models import DistribucionISNEmpleado, ExpedienteISN
from rrhh.models import Empleado, NominaConceptoLinea, NominaLinea, NominaPeriodo
from sat_client.models import CfdiDescargado


CENT = Decimal("0.01")
ZERO = Decimal("0")
TOLERANCIA_BASE_DECLARADA = CENT
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


@dataclass(frozen=True)
class FilaPreviewISN:
    empleado_id: int
    base_gravada: Decimal
    monto_isn: Decimal
    area_codigo: str
    sucursal_id: int


@dataclass(frozen=True)
class PreviewExpedienteISN:
    cfdi: CfdiDescargado
    cfdi_uuid: str
    periodo: date
    importe_pagado: Decimal
    base_gravada_total: Decimal
    base_declarada: Decimal | None
    diferencia_base: Decimal | None
    estado_previsto: str
    filas: tuple[FilaPreviewISN, ...]

    def render(self) -> str:
        base_declarada = (
            f"{self.base_declarada:.2f}"
            if self.base_declarada is not None
            else "N/D"
        )
        diferencia = (
            f"{self.diferencia_base:.2f}"
            if self.diferencia_base is not None
            else "N/D"
        )
        encabezado = (
            f"ISN periodo={self.periodo:%Y-%m} uuid={self.cfdi_uuid} "
            f"importe={self.importe_pagado:.2f} "
            f"base_calculada={self.base_gravada_total:.2f} "
            f"base_declarada={base_declarada} diferencia={diferencia} "
            f"estado_previsto={self.estado_previsto} empleados={len(self.filas)}"
        )
        detalle = (
            f"empleado={fila.empleado_id} base={fila.base_gravada:.2f} "
            f"isn={fila.monto_isn:.2f} area={fila.area_codigo} "
            f"sucursal={fila.sucursal_id}"
            for fila in self.filas
        )
        return "\n".join((encabezado, *detalle))


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


def _validar_periodos_nomina_mes(periodos, fecha_mes: date) -> None:
    if not periodos:
        raise ValueError("El mes no tiene periodos de nomina cerrados o pagados.")
    if len(periodos) != 2:
        raise ValueError("El mes fiscal debe tener exactamente dos periodos de nomina.")
    if any(
        periodo.tipo_periodo != NominaPeriodo.TIPO_QUINCENAL
        for periodo in periodos
    ):
        raise ValueError("El mes fiscal solo admite periodos de tipo quincenal.")
    if any(periodo.fecha_inicio > periodo.fecha_fin for periodo in periodos):
        raise ValueError("El mes contiene un rango de nomina invertido.")
    if any(getattr(periodo, "lineas_count", 0) <= 0 for periodo in periodos):
        raise ValueError("El mes contiene una quincena vacia.")

    rangos = [(periodo.fecha_inicio, periodo.fecha_fin) for periodo in periodos]
    if len(set(rangos)) != len(rangos):
        raise ValueError("El mes contiene rangos de nomina duplicados.")
    rangos_ordenados = sorted(rangos)
    (inicio_primera, fin_primera), (inicio_segunda, fin_segunda) = rangos_ordenados
    ultimo_dia = date(
        fecha_mes.year,
        fecha_mes.month,
        monthrange(fecha_mes.year, fecha_mes.month)[1],
    )
    if inicio_primera != fecha_mes or fin_segunda != ultimo_dia:
        raise ValueError("Los periodos no dan cobertura completa al mes fiscal.")
    if inicio_segunda <= fin_primera:
        raise ValueError("El mes contiene rangos de nomina solapados.")
    if inicio_segunda != fin_primera + timedelta(days=1):
        raise ValueError("El mes contiene un hueco entre quincenas.")


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
        )
        .annotate(lineas_count=Count("lineas"))
        .order_by("fecha_inicio", "fecha_fin", "id")
    )
    _validar_periodos_nomina_mes(periodos_mes, fecha_mes)

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
        )
        .order_by("linea__empleado_id", "id")
        .values("linea__empleado_id", "codigo_concepto", "importe")
    )
    aguinaldos_previos = defaultdict(lambda: ZERO)
    aguinaldos_mes = defaultdict(lambda: ZERO)
    for concepto in conceptos_mes:
        empleado_id = concepto["linea__empleado_id"]
        codigo = (concepto["codigo_concepto"] or "").strip()
        importe = Decimal(concepto["importe"] or ZERO)
        if importe < ZERO:
            raise ValueError("La nomina contiene una percepcion negativa.")
        if codigo in CODIGOS_EXENTOS_COMPLETOS:
            continue
        if codigo == "24":
            aguinaldos_mes[empleado_id] += importe
            continue
        bases[empleado_id] += importe

    conceptos_aguinaldo_previos = list(
        NominaConceptoLinea.objects.filter(
            linea__empleado_id__in=empleado_ids,
            linea__periodo__estatus__in=estados_validos,
            linea__periodo__fecha_fin__year=periodo.year,
            linea__periodo__fecha_fin__month__lt=periodo.month,
            tipo=NominaConceptoLinea.TIPO_PERCEPCION,
            codigo_concepto__regex=r"^[[:space:]]*24[[:space:]]*$",
        )
        .order_by("linea__periodo__fecha_fin", "linea__empleado_id", "id")
        .values(
            "linea__periodo__fecha_fin",
            "linea__empleado_id",
            "codigo_concepto",
            "importe",
        )
    )
    meses_historicos = {
        fila["linea__periodo__fecha_fin"].month
        for fila in conceptos_aguinaldo_previos
    }
    periodos_historicos = list(
        NominaPeriodo.objects.filter(
            estatus__in=estados_validos,
            fecha_fin__year=periodo.year,
            fecha_fin__month__in=meses_historicos,
        )
        .annotate(lineas_count=Count("lineas"))
        .order_by("fecha_fin", "fecha_inicio", "id")
    )
    periodos_por_mes = defaultdict(list)
    for periodo_historico in periodos_historicos:
        periodos_por_mes[periodo_historico.fecha_fin.month].append(periodo_historico)
    for mes_historico in meses_historicos:
        _validar_periodos_nomina_mes(
            periodos_por_mes[mes_historico],
            date(periodo.year, mes_historico, 1),
        )

    for concepto in conceptos_aguinaldo_previos:
        importe = Decimal(concepto["importe"] or ZERO)
        if importe < ZERO:
            raise ValueError("La nomina contiene una percepcion negativa.")
        aguinaldos_previos[concepto["linea__empleado_id"]] += importe

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


def _resolver_cfdi_isn(periodo: date, uuid: str | None) -> CfdiDescargado:
    if uuid:
        try:
            cfdi = CfdiDescargado.objects.get(uuid=uuid)
        except CfdiDescargado.DoesNotExist as exc:
            raise ValueError(f"No existe el CFDI con UUID {uuid}.") from exc
        periodo_cfdi, _ = extraer_isn_cfdi(cfdi)
        if periodo_cfdi != periodo:
            raise ValueError("El periodo del CFDI no coincide con el periodo solicitado.")
        return cfdi

    candidatos = []
    queryset = CfdiDescargado.objects.filter(
        tipo_cfdi=CfdiDescargado.TIPO_RECIBIDO,
        tipo_comprobante="I",
    ).order_by("pk")
    for cfdi in queryset.iterator():
        try:
            periodo_cfdi, _ = extraer_isn_cfdi(cfdi)
        except ValueError:
            continue
        if periodo_cfdi == periodo:
            candidatos.append(cfdi)
    if len(candidatos) != 1:
        raise ValueError(
            "Debe existir un unico CFDI candidato de ISN para el periodo "
            f"{periodo:%Y-%m}; encontrados={len(candidatos)}."
        )
    return candidatos[0]


def preparar_expediente_isn(
    periodo: date,
    *,
    uuid: str | None = None,
    base_declarada: Decimal | None = None,
) -> PreviewExpedienteISN:
    if not isinstance(periodo, date) or periodo.day != 1:
        raise ValueError("El periodo de ISN debe ser el primer dia del mes.")

    cfdi = _resolver_cfdi_isn(periodo, uuid)
    periodo_cfdi, importe_pagado = extraer_isn_cfdi(cfdi)
    if periodo_cfdi != periodo:
        raise ValueError("El periodo del CFDI no coincide con el periodo solicitado.")

    bases = bases_gravadas_empleados(periodo)
    montos = prorratear_isn(bases, importe_pagado)
    empleados = Empleado.objects.filter(pk__in=bases).in_bulk()
    if len(empleados) != len(bases):
        raise ValueError("El universo de nomina contiene empleados inexistentes.")

    base_gravada_total = money(sum(bases.values(), ZERO))
    base_declarada, diferencia_base, estado_previsto = _conciliar_base_declarada(
        base_gravada_total,
        base_declarada,
    )

    filas = tuple(
        FilaPreviewISN(
            empleado_id=empleado_id,
            base_gravada=money(base),
            monto_isn=money(montos[empleado_id]),
            area_codigo=empleados[empleado_id].departamento,
            sucursal_id=empleados[empleado_id].sucursal_ref_id,
        )
        for empleado_id, base in sorted(bases.items())
    )
    return PreviewExpedienteISN(
        cfdi=cfdi,
        cfdi_uuid=cfdi.uuid,
        periodo=periodo,
        importe_pagado=money(importe_pagado),
        base_gravada_total=base_gravada_total,
        base_declarada=base_declarada,
        diferencia_base=diferencia_base,
        estado_previsto=estado_previsto,
        filas=filas,
    )


def _validar_preview(preview: PreviewExpedienteISN) -> None:
    if preview.periodo.day != 1:
        raise ValueError("El periodo de ISN debe ser el primer dia del mes.")
    if not preview.filas:
        raise ValueError("El preview de ISN no contiene empleados.")
    empleado_ids = [fila.empleado_id for fila in preview.filas]
    if len(empleado_ids) != len(set(empleado_ids)):
        raise ValueError("El preview de ISN contiene empleados duplicados.")
    if any(
        fila.base_gravada < ZERO or fila.monto_isn < ZERO
        for fila in preview.filas
    ):
        raise ValueError("El preview de ISN contiene importes negativos.")
    if money(sum((fila.base_gravada for fila in preview.filas), ZERO)) != money(
        preview.base_gravada_total
    ):
        raise ValueError("Las bases del preview de ISN no cuadran.")
    if money(sum((fila.monto_isn for fila in preview.filas), ZERO)) != money(
        preview.importe_pagado
    ):
        raise ValueError("Los montos del preview de ISN no cuadran con el CFDI.")

    if preview.estado_previsto not in (
        ExpedienteISN.ESTADO_APLICADO,
        ExpedienteISN.ESTADO_DISCREPANCIA,
    ):
        raise ValueError("El estado previsto del preview de ISN no es valido.")
    base_declarada, diferencia, estado = _conciliar_base_declarada(
        preview.base_gravada_total,
        preview.base_declarada,
    )
    if (
        base_declarada != preview.base_declarada
        or diferencia != preview.diferencia_base
        or estado != preview.estado_previsto
    ):
        raise ValueError("La conciliacion del preview de ISN no es coherente.")


def _normalizar_base_declarada(base_declarada: Decimal | None) -> Decimal | None:
    if base_declarada is None:
        return None
    valor = Decimal(base_declarada)
    if not valor.is_finite() or valor < ZERO:
        raise ValueError("La base declarada debe ser finita y no negativa.")
    return money(valor)


def _conciliar_base_declarada(
    base_gravada_total: Decimal,
    base_declarada: Decimal | None,
) -> tuple[Decimal | None, Decimal | None, str]:
    base_declarada = _normalizar_base_declarada(base_declarada)
    if base_declarada is None:
        return None, None, ExpedienteISN.ESTADO_APLICADO
    diferencia = money(base_declarada - money(base_gravada_total))
    estado = (
        ExpedienteISN.ESTADO_DISCREPANCIA
        if abs(diferencia) > TOLERANCIA_BASE_DECLARADA
        else ExpedienteISN.ESTADO_APLICADO
    )
    return base_declarada, diferencia, estado


def _aplicar_expediente_isn_una_vez(
    preview: PreviewExpedienteISN,
    *,
    base_declarada: Decimal | None,
    diferencia_base: Decimal | None,
    estado_previsto: str,
    aplicado_por=None,
) -> ExpedienteISN:
    with transaction.atomic():
        try:
            cfdi_actual = CfdiDescargado.objects.select_for_update().get(
                pk=preview.cfdi.pk
            )
        except CfdiDescargado.DoesNotExist as exc:
            raise ValueError("El CFDI del preview ya no existe.") from exc
        periodo_cfdi, importe_cfdi = extraer_isn_cfdi(cfdi_actual)
        if (
            cfdi_actual.uuid != preview.cfdi_uuid
            or periodo_cfdi != preview.periodo
            or importe_cfdi != money(preview.importe_pagado)
        ):
            raise ValueError("El CFDI cambio despues de preparar el preview de ISN.")

        expedientes = list(
            ExpedienteISN.objects.select_for_update()
            .filter(periodo=preview.periodo)
            .order_by("revision", "pk")
        )
        existente = next(
            (item for item in expedientes if item.uuid == preview.cfdi_uuid),
            None,
        )
        if existente is not None:
            return existente

        if estado_previsto == ExpedienteISN.ESTADO_APLICADO:
            for anterior in expedientes:
                if anterior.estado == ExpedienteISN.ESTADO_APLICADO:
                    anterior.estado = ExpedienteISN.ESTADO_REEMPLAZADO
                    anterior.save(update_fields={"estado"})

        expediente = ExpedienteISN.objects.create(
            periodo=preview.periodo,
            revision=max((item.revision for item in expedientes), default=0) + 1,
            uuid=preview.cfdi_uuid,
            cfdi=cfdi_actual,
            importe_pagado=preview.importe_pagado,
            base_gravada_calculada=preview.base_gravada_total,
            base_declarada=base_declarada,
            estado=ExpedienteISN.ESTADO_VALIDO,
            aplicado_por=(
                aplicado_por
                if estado_previsto == ExpedienteISN.ESTADO_APLICADO
                else None
            ),
            aplicado_en=None,
            metadata={
                "empleados": len(preview.filas),
                "diferencia_base": (
                    str(diferencia_base) if diferencia_base is not None else None
                ),
                "estado_previsto": estado_previsto,
            },
        )
        DistribucionISNEmpleado.objects.bulk_create(
            [
                DistribucionISNEmpleado(
                    expediente=expediente,
                    empleado_id=fila.empleado_id,
                    base_gravada=fila.base_gravada,
                    monto_isn=fila.monto_isn,
                    area_codigo=fila.area_codigo,
                    sucursal_id=fila.sucursal_id,
                )
                for fila in preview.filas
            ]
        )

        totales = expediente.distribuciones.aggregate(
            base=Sum("base_gravada"),
            importe=Sum("monto_isn"),
        )
        if money(totales["base"] or ZERO) != money(preview.base_gravada_total):
            raise ValueError("Las bases materializadas de ISN no cuadran.")
        if money(totales["importe"] or ZERO) != money(preview.importe_pagado):
            raise ValueError("Los montos materializados de ISN no cuadran.")

        expediente.estado = estado_previsto
        expediente.aplicado_en = (
            timezone.now()
            if estado_previsto == ExpedienteISN.ESTADO_APLICADO
            else None
        )
        expediente.save(update_fields={"estado", "aplicado_en"})
        return expediente


def aplicar_expediente_isn(
    preview: PreviewExpedienteISN,
    *,
    base_declarada: Decimal | None = None,
    aplicado_por=None,
) -> ExpedienteISN:
    _validar_preview(preview)
    if base_declarada is not None and preview.base_declarada is not None:
        if _normalizar_base_declarada(base_declarada) != preview.base_declarada:
            raise ValueError("La base declarada no coincide con el preview de ISN.")
    base_a_conciliar = (
        base_declarada if base_declarada is not None else preview.base_declarada
    )
    base_declarada, diferencia_base, estado_previsto = _conciliar_base_declarada(
        preview.base_gravada_total,
        base_a_conciliar,
    )

    ultimo_error = None
    for _ in range(3):
        try:
            return _aplicar_expediente_isn_una_vez(
                preview,
                base_declarada=base_declarada,
                diferencia_base=diferencia_base,
                estado_previsto=estado_previsto,
                aplicado_por=aplicado_por,
            )
        except IntegrityError as exc:
            ultimo_error = exc
    raise ultimo_error
