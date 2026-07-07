from __future__ import annotations

from datetime import date
from decimal import Decimal

from django.db.models import Q, Sum

from pos_bridge.models import PointProductionLine
from reportes.models import CostoManoObraDiarioArea, RecetaAreaProduccion
from rrhh.models import Empleado, NominaLinea, NominaPeriodo

_PERIODOS_VALIDOS = (NominaPeriodo.ESTATUS_CERRADA, NominaPeriodo.ESTATUS_PAGADA)
MINUTOS_TURNO_ESTANDAR = Decimal("480")  # 8 horas — misma aproximación que el prorrateo 6/7


def area_produccion_empleado(empleado: Empleado) -> str | None:
    """Clasifica un empleado a una de las 3 áreas de costeo por su
    puesto_operativo directo. A diferencia de
    bonos_produccion.area_bono_produccion_empleado(), NO colapsa
    EMBETUNADO dentro de un bucket genérico de producción — para costeo sí
    necesitamos ese bucket separado, y el dato existe a nivel de empleado."""
    puesto = (empleado.puesto_operativo or "").strip().upper()
    if puesto in {
        RecetaAreaProduccion.AREA_HORNOS,
        RecetaAreaProduccion.AREA_ARMADO,
        RecetaAreaProduccion.AREA_EMBETUNADO,
    }:
        return puesto
    return None


def _dias_laborables_periodo(periodo: NominaPeriodo) -> int:
    """Aproximación: 1 de cada 7 días es descanso (por ley), aplicado al
    rango de fechas del período. No usa asistencia real día por día."""
    dias_calendario = (periodo.fecha_fin - periodo.fecha_inicio).days + 1
    return max(1, round(dias_calendario * 6 / 7))


def _periodos_vigentes(fecha: date):
    return NominaPeriodo.objects.filter(
        fecha_inicio__lte=fecha,
        fecha_fin__gte=fecha,
        estatus__in=_PERIODOS_VALIDOS,
    )


def nomina_diaria_area(fecha: date, area: str) -> Decimal | None:
    periodos = list(_periodos_vigentes(fecha))
    if not periodos:
        return None

    total = Decimal("0")
    for periodo in periodos:
        nomina_periodo = (
            NominaLinea.objects.filter(periodo=periodo, empleado__departamento=Empleado.DEP_PRODUCCION)
            .select_related("empleado")
        )
        nomina_area = sum(
            (linea.total_percepciones for linea in nomina_periodo if area_produccion_empleado(linea.empleado) == area),
            Decimal("0"),
        )
        dias = _dias_laborables_periodo(periodo)
        total += nomina_area / dias
    return total


def empleados_area_periodo(fecha: date, area: str) -> int:
    """Headcount de empleados del área vigentes ese día (para
    minutos_disponibles) — no prorrateado, es un conteo de personas, no de
    nómina. Asume que todos trabajan el turno completo (mismo nivel de
    aproximación que `_dias_laborables_periodo`, no descuenta incapacidades
    ni permisos individuales)."""
    periodos = list(_periodos_vigentes(fecha))
    if not periodos:
        return 0
    empleados_ids: set[int] = set()
    for periodo in periodos:
        nomina_periodo = (
            NominaLinea.objects.filter(periodo=periodo, empleado__departamento=Empleado.DEP_PRODUCCION)
            .select_related("empleado")
        )
        empleados_ids.update(
            linea.empleado_id for linea in nomina_periodo if area_produccion_empleado(linea.empleado) == area
        )
    return len(empleados_ids)


def _ids_por_grupo(area: str, es_grupo_insumo: bool, queryset) -> set[int]:
    """ids de `queryset` (Receta.objects.all() o Insumo filtrado a
    tipo interno) clasificados a esa área, vía su propio `grupo_mano_obra`
    (o `nombre` si no ha sido fusionado) — calibrados o no. Para el dato
    informativo `unidades_area_dia`."""
    ids: set[int] = set()
    for fila in RecetaAreaProduccion.objects.filter(area=area, familia__gt="", es_grupo_insumo=es_grupo_insumo):
        ids.update(
            queryset.filter(
                Q(grupo_mano_obra=fila.familia) | Q(grupo_mano_obra="", nombre=fila.familia)
            ).values_list("id", flat=True)
        )
    return ids


def _minutos_por_grupo(area: str, es_grupo_insumo: bool, queryset) -> dict[int, Decimal]:
    """id de `queryset` (Receta.objects.all() o Insumo filtrado a tipo
    interno) -> minutos_estandar_pieza para esa área. Cada grupo
    (autocontenido o fusionado) aporta un solo minuto, compartido por
    todos sus miembros — un miembro sin calibrar en el grupo no aporta ni
    distorsiona el minuto de los demás grupos."""
    resultado: dict[int, Decimal] = {}
    for fila in RecetaAreaProduccion.objects.filter(area=area, familia__gt="", es_grupo_insumo=es_grupo_insumo):
        minutos = fila.minutos_estandar_pieza
        if minutos is None:
            continue
        ids = queryset.filter(
            Q(grupo_mano_obra=fila.familia) | Q(grupo_mano_obra="", nombre=fila.familia)
        ).values_list("id", flat=True)
        for obj_id in ids:
            resultado[obj_id] = minutos
    return resultado


def unidades_area_dia(fecha: date, area: str) -> Decimal:
    """Total físico de piezas del área ese día, calibradas o no — dato
    informativo de actividad, ya no es el divisor del costo (ver
    minutos_area_dia)."""
    from maestros.models import Insumo
    from recetas.models import Receta

    receta_ids = _ids_por_grupo(area, False, Receta.objects.all())
    insumo_ids = _ids_por_grupo(area, True, Insumo.objects.filter(tipo_item=Insumo.TIPO_INTERNO))
    if not receta_ids and not insumo_ids:
        return Decimal("0")
    filtro = Q(receta_id__in=receta_ids) | Q(insumo_id__in=insumo_ids)
    total = PointProductionLine.objects.filter(filtro, production_date=fecha).aggregate(
        total=Sum("produced_quantity")
    )["total"]
    return Decimal(str(total or 0))


def minutos_area_dia(fecha: date, area: str) -> Decimal:
    """Minutos-persona demandados ese día en el área, según lo producido y
    los minutos estándar calibrados. Recetas/insumos clasificados pero sin
    calibrar NO aportan — no distorsionan el minuto de las que sí están
    calibradas."""
    from maestros.models import Insumo
    from recetas.models import Receta

    receta_minutos = _minutos_por_grupo(area, False, Receta.objects.all())
    insumo_minutos = _minutos_por_grupo(area, True, Insumo.objects.filter(tipo_item=Insumo.TIPO_INTERNO))
    if not receta_minutos and not insumo_minutos:
        return Decimal("0")

    filtro = Q(receta_id__in=receta_minutos.keys()) | Q(insumo_id__in=insumo_minutos.keys())
    lineas = PointProductionLine.objects.filter(filtro, production_date=fecha).values(
        "receta_id", "insumo_id", "produced_quantity"
    )
    total = Decimal("0")
    for linea in lineas:
        minutos = receta_minutos.get(linea["receta_id"])
        if minutos is None:
            minutos = insumo_minutos.get(linea["insumo_id"])
        if minutos is None:
            continue
        total += Decimal(str(linea["produced_quantity"])) * minutos
    return total


def calcular_costo_diario_area(fecha: date, area: str) -> CostoManoObraDiarioArea:
    nomina = nomina_diaria_area(fecha, area)
    unidades = unidades_area_dia(fecha, area)
    minutos = minutos_area_dia(fecha, area)
    costo_minuto = (nomina / minutos) if (nomina is not None and minutos > 0) else None
    empleados = empleados_area_periodo(fecha, area)
    minutos_disponibles = Decimal(empleados) * MINUTOS_TURNO_ESTANDAR if empleados else None

    snapshot, _ = CostoManoObraDiarioArea.objects.update_or_create(
        fecha=fecha,
        area=area,
        defaults={
            "nomina_dia_area": nomina or Decimal("0"),
            "unidades_producidas": unidades,
            "minutos_demandados": minutos,
            "minutos_disponibles": minutos_disponibles,
            "costo_minuto": costo_minuto,
            "es_dia_laborable_esperado": nomina is not None,
        },
    )
    return snapshot


def costo_mano_obra_diario_receta(fecha: date, receta) -> dict:
    grupo = receta.grupo_mano_obra or receta.nombre
    filas_por_area = {
        fila.area: fila
        for fila in RecetaAreaProduccion.objects.filter(familia=grupo, es_grupo_insumo=False)
    }

    if not filas_por_area:
        return {"completo": False, "costo_total": None, "areas_faltantes": [], "sin_clasificar": True}

    costo_total = Decimal("0")
    areas_faltantes = []
    for area, fila in filas_por_area.items():
        minutos_receta = fila.minutos_estandar_pieza
        snapshot = calcular_costo_diario_area(fecha, area)
        if minutos_receta is None or snapshot.costo_minuto is None:
            areas_faltantes.append(area)
        else:
            costo_total += minutos_receta * snapshot.costo_minuto

    return {
        "completo": not areas_faltantes,
        "costo_total": costo_total if not areas_faltantes else None,
        "areas_faltantes": areas_faltantes,
        "sin_clasificar": False,
    }
