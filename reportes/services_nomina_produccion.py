from __future__ import annotations

import calendar
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

from django.db import transaction
from django.db.models import Q, Sum

from reportes.models import CategoriaGasto, CentroCosto, GastoOperativoMensual
from reportes.services_operating_finance import OperatingFinanceBootstrapService
from rrhh.models import Empleado, NominaLinea, NominaPeriodo

EXTERNAL_KEY_PREFIX = "NOMINA_PROD_AUTO|"


def _month_bounds(periodo: date) -> tuple[date, date]:
    start = periodo.replace(day=1)
    last_day = calendar.monthrange(start.year, start.month)[1]
    return start, start.replace(day=last_day)


def calcular_mano_obra_produccion(periodo: date) -> Decimal | None:
    """Σ NominaLinea.total_percepciones de Producción para los NominaPeriodo
    (CERRADA/PAGADA) cuyo fecha_fin cae en el mes de `periodo`.

    Devuelve None si no hay ningún periodo cerrado/pagado ese mes (para no
    confundir "sin datos todavía" con "$0 de mano de obra")."""
    start, end = _month_bounds(periodo)
    periodos_ids = list(
        NominaPeriodo.objects.filter(
            fecha_fin__gte=start,
            fecha_fin__lte=end,
            estatus__in=(NominaPeriodo.ESTATUS_CERRADA, NominaPeriodo.ESTATUS_PAGADA),
        ).values_list("id", flat=True)
    )
    if not periodos_ids:
        return None
    # Mismo criterio que bonos_produccion (bonos_produccion/views.py) para
    # clasificar quién es Producción: departamento o su fallback departamento_origen.
    es_produccion = Q(empleado__departamento=Empleado.DEP_PRODUCCION) | Q(
        empleado__departamento_origen=Empleado.DEP_PRODUCCION
    )
    total = NominaLinea.objects.filter(es_produccion, periodo_id__in=periodos_ids).aggregate(
        total=Sum("total_percepciones")
    )["total"]
    return Decimal(str(total or 0))


@dataclass
class SincronizacionResumen:
    periodo: date
    escrito: bool
    monto: Decimal = Decimal("0")
    filas_legacy_borradas: int = 0
    external_key: str = ""
    motivo: str = ""
    periodos_nomina: list[str] = field(default_factory=list)


def sincronizar_mano_obra_produccion(periodo: date, *, dry_run: bool = False) -> SincronizacionResumen:
    start, _ = _month_bounds(periodo)
    monto = calcular_mano_obra_produccion(start)
    if monto is None:
        return SincronizacionResumen(periodo=start, escrito=False, motivo="Sin periodos de nómina CERRADA/PAGADA en el mes")

    OperatingFinanceBootstrapService().bootstrap()
    categoria = CategoriaGasto.objects.get(codigo="MANO_OBRA_PROD")
    centro = CentroCosto.objects.get(codigo="PROD")
    external_key = f"{EXTERNAL_KEY_PREFIX}{start:%Y-%m}"

    legacy_qs = GastoOperativoMensual.objects.filter(
        periodo=start,
        categoria_gasto=categoria,
        centro_costo=centro,
    ).exclude(external_key=external_key)
    legacy_count = legacy_qs.count()

    if dry_run:
        return SincronizacionResumen(
            periodo=start,
            escrito=False,
            monto=monto,
            filas_legacy_borradas=legacy_count,
            external_key=external_key,
            motivo="dry-run: no se persistió nada",
        )

    with transaction.atomic():
        legacy_qs.delete()
        GastoOperativoMensual.objects.update_or_create(
            external_key=external_key,
            defaults={
                "periodo": start,
                "categoria_gasto": categoria,
                "centro_costo": centro,
                "monto": monto,
                "tipo_dato": GastoOperativoMensual.TIPO_DATO_REAL,
                "fuente": GastoOperativoMensual.FUENTE_IMPORTADA,
                "comentario": "Σ NominaLinea.total_percepciones, depto PRODUCCION (automático)",
                "es_estimado": False,
            },
        )

    return SincronizacionResumen(
        periodo=start,
        escrito=True,
        monto=monto,
        filas_legacy_borradas=legacy_count,
        external_key=external_key,
    )
