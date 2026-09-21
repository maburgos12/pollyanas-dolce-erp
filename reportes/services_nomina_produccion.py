from __future__ import annotations

import calendar
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

from django.db import transaction
from django.db.models import Q
from unidecode import unidecode

from reportes.models import CategoriaGasto, CentroCosto, GastoOperativoMensual
from reportes.services_operating_finance import OperatingFinanceBootstrapService
from rrhh.models import Empleado, NominaConceptoLinea, NominaLinea, NominaPeriodo

EXTERNAL_KEY_PREFIX = "NOMINA_PROD_AUTO|"
ZERO = Decimal("0")

SIN_AREA = "(sin área asignada)"

# A dónde pertenece el costo de cada persona que la nómina marca como Producción.
# Reglas dadas por dirección el 2026-09-21:
#   · El jefe de producción reporta a administración, no al costo de fabricar.
#   · Encargada y supervisora de producción sí son mando de planta.
#   · Cuartos fríos resguarda inventarios de insumos preparados y producto
#     terminado: es almacén, no línea de producción.
#   · Envío a sucursales es logística, aunque hoy dependa de producción.
#   · «Crucero» distinguía a quienes producían fuera de CEDIS. Esa división por
#     locación ya no existe: hoy están en la planta, así que son producción.
DESTINO_PRODUCCION = "PRODUCCION"
DESTINO_CEDIS = "CEDIS"
DESTINO_LOGISTICA = "LOGISTICA"
DESTINO_ADMINISTRACION = "ADMINISTRACION"

# El puesto de RRHH manda sobre el área operativa: dentro de una misma área
# conviven destinos distintos (cuartos fríos y envío a sucursales, por ejemplo).
DESTINO_POR_PUESTO = {
    "jefe de produccion": DESTINO_ADMINISTRACION,
    "cuartos frios": DESTINO_CEDIS,
    "envio a sucursales": DESTINO_LOGISTICA,
}
DESTINO_POR_AREA = {
    "ENVIO_SUCURSAL": DESTINO_LOGISTICA,
}


def _normalizar(valor: str) -> str:
    return unidecode(str(valor or "")).strip().lower()


def area_de(empleado) -> str:
    return (empleado.puesto_operativo or "").strip().upper() or SIN_AREA


def destino_de(empleado) -> str:
    """Dónde cae el costo de esta persona. Producción es el caso normal."""
    puesto = _normalizar(empleado.puesto)
    if puesto in DESTINO_POR_PUESTO:
        return DESTINO_POR_PUESTO[puesto]
    return DESTINO_POR_AREA.get(area_de(empleado), DESTINO_PRODUCCION)


def _month_bounds(periodo: date) -> tuple[date, date]:
    start = periodo.replace(day=1)
    last_day = calendar.monthrange(start.year, start.month)[1]
    return start, start.replace(day=last_day)


def _periodos_cerrados(periodo: date) -> list[int]:
    start, end = _month_bounds(periodo)
    return list(
        NominaPeriodo.objects.filter(
            fecha_fin__gte=start,
            fecha_fin__lte=end,
            estatus__in=(NominaPeriodo.ESTATUS_CERRADA, NominaPeriodo.ESTATUS_PAGADA),
        ).values_list("id", flat=True)
    )


def _lineas_produccion(periodos_ids: list[int]):
    # Mismo criterio que bonos_produccion para clasificar quién es Producción:
    # departamento o su fallback departamento_origen.
    es_produccion = Q(empleado__departamento=Empleado.DEP_PRODUCCION) | Q(
        empleado__departamento_origen=Empleado.DEP_PRODUCCION
    )
    return (
        NominaLinea.objects.filter(es_produccion, periodo_id__in=periodos_ids)
        .select_related("empleado")
        .prefetch_related("conceptos")
    )


def percepciones_de_linea(linea: NominaLinea) -> Decimal:
    """Lo que realmente se le pagó a la persona en el periodo.

    `total_percepciones` es `salario_base + bonos`, así que deja fuera
    prestaciones que sí se pagan —la despensa, sin ir más lejos—. Cuando la
    línea trae su desglose de conceptos, ése es el importe bueno; el campo
    agregado solo se usa como respaldo para nóminas capturadas sin detalle.
    """
    importes = [
        concepto.importe
        for concepto in linea.conceptos.all()
        if concepto.tipo == NominaConceptoLinea.TIPO_PERCEPCION
    ]
    if importes:
        return sum(importes, ZERO)
    return linea.total_percepciones or ZERO


def calcular_mano_obra_produccion(periodo: date) -> Decimal | None:
    """Costo laboral de Producción del mes, con prestaciones incluidas.

    Devuelve None si no hay ningún periodo cerrado o pagado ese mes, para no
    confundir «sin datos todavía» con «$0 de mano de obra».

    Solo cuenta a quien pertenece a producción: el jefe de producción, cuartos
    fríos y envío a sucursales se excluyen porque su costo es de otra área
    (ver DESTINO_POR_PUESTO).

    No incluye la parte patronal de IMSS e Infonavit: la nómina registra lo que
    percibe el trabajador, y las cargas del patrón llegan por su propio canal.
    """
    periodos_ids = _periodos_cerrados(periodo)
    if not periodos_ids:
        return None
    total = ZERO
    for linea in _lineas_produccion(periodos_ids):
        if destino_de(linea.empleado) == DESTINO_PRODUCCION:
            total += percepciones_de_linea(linea)
    return total


def desglose_mano_obra_produccion(periodo: date) -> dict[str, Decimal]:
    """Mismo importe, abierto por área operativa, para poder auditarlo.

    El área sale de `puesto_operativo`. Quien no la tenga cae en SIN_AREA: son
    los mandos de planta —que no pertenecen a una línea— y los expedientes de
    RRHH a los que todavía no se les captura el puesto.
    """
    periodos_ids = _periodos_cerrados(periodo)
    if not periodos_ids:
        return {}
    desglose: dict[str, Decimal] = {}
    for linea in _lineas_produccion(periodos_ids):
        if destino_de(linea.empleado) != DESTINO_PRODUCCION:
            continue
        area = area_de(linea.empleado)
        desglose[area] = desglose.get(area, ZERO) + percepciones_de_linea(linea)
    return dict(sorted(desglose.items(), key=lambda kv: -kv[1]))


def desglose_por_destino(periodo: date) -> dict[str, Decimal]:
    """Todo lo que la nómina marca como Producción, repartido por destino real.

    Sirve para ver qué se quedó fuera del costo de fabricación y a dónde se fue,
    en vez de que desaparezca sin dejar rastro.
    """
    periodos_ids = _periodos_cerrados(periodo)
    if not periodos_ids:
        return {}
    por_destino: dict[str, Decimal] = {}
    for linea in _lineas_produccion(periodos_ids):
        destino = destino_de(linea.empleado)
        por_destino[destino] = por_destino.get(destino, ZERO) + percepciones_de_linea(linea)
    return dict(sorted(por_destino.items(), key=lambda kv: -kv[1]))


@dataclass
class SincronizacionResumen:
    periodo: date
    escrito: bool
    monto: Decimal = Decimal("0")
    filas_previas: int = 0
    external_key: str = ""
    motivo: str = ""
    periodos_nomina: list[str] = field(default_factory=list)
    desglose: dict[str, Decimal] = field(default_factory=dict)
    destinos: dict[str, Decimal] = field(default_factory=dict)


def sincronizar_mano_obra_produccion(periodo: date, *, dry_run: bool = False) -> SincronizacionResumen:
    """Escribe la mano de obra del mes en GastoOperativoMensual.

    Solo administra su propia fila. Si el mes ya tiene capturas de mano de obra
    hechas por otra vía, se detiene y lo reporta en vez de reemplazarlas: esas
    capturas traen el desglose por concepto y la parte patronal, que este motor
    no sabe reconstruir.
    """
    start, _ = _month_bounds(periodo)
    monto = calcular_mano_obra_produccion(start)
    if monto is None:
        return SincronizacionResumen(
            periodo=start, escrito=False,
            motivo="Sin periodos de nómina CERRADA/PAGADA en el mes",
        )

    OperatingFinanceBootstrapService().bootstrap()
    categoria = CategoriaGasto.objects.get(codigo="MANO_OBRA_PROD")
    centro = CentroCosto.objects.get(codigo="PROD")
    external_key = f"{EXTERNAL_KEY_PREFIX}{start:%Y-%m}"
    desglose = desglose_mano_obra_produccion(start)
    destinos = desglose_por_destino(start)

    ajenas = GastoOperativoMensual.objects.filter(
        periodo=start, categoria_gasto=categoria, centro_costo=centro,
    ).exclude(external_key=external_key)
    previas = ajenas.count()
    if previas:
        return SincronizacionResumen(
            periodo=start, escrito=False, monto=monto, filas_previas=previas,
            external_key=external_key, desglose=desglose, destinos=destinos,
            motivo=(
                f"El mes ya tiene {previas} captura(s) de mano de obra por otra vía; "
                "no se sobrescriben. Revísalas y dales de baja antes de sincronizar."
            ),
        )

    if dry_run:
        return SincronizacionResumen(
            periodo=start, escrito=False, monto=monto, external_key=external_key,
            desglose=desglose, destinos=destinos, motivo="dry-run: no se persistió nada",
        )

    with transaction.atomic():
        GastoOperativoMensual.objects.update_or_create(
            external_key=external_key,
            defaults={
                "periodo": start,
                "categoria_gasto": categoria,
                "centro_costo": centro,
                "monto": monto,
                "tipo_dato": GastoOperativoMensual.TIPO_DATO_REAL,
                "fuente": GastoOperativoMensual.FUENTE_IMPORTADA,
                "comentario": (
                    "Percepciones de nómina, depto PRODUCCION (automático). "
                    "Sin parte patronal de IMSS/Infonavit."
                ),
                "es_estimado": False,
            },
        )

    return SincronizacionResumen(
        periodo=start, escrito=True, monto=monto,
        external_key=external_key, desglose=desglose, destinos=destinos,
    )
