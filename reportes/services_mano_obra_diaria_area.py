from __future__ import annotations

from datetime import date
from decimal import Decimal

from django.db.models import Q, Sum

from pos_bridge.models import PointProductionLine
from reportes.mano_obra_grupos_familia import familias_del_grupo, grupo_de_familia
from reportes.models import CostoManoObraDiarioArea, RecetaAreaProduccion
from rrhh.models import Empleado, NominaLinea, NominaPeriodo

_PERIODOS_VALIDOS = (NominaPeriodo.ESTATUS_CERRADA, NominaPeriodo.ESTATUS_PAGADA)


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


def _familias_reales_clasificadas(area: str) -> list[str]:
    """RecetaAreaProduccion.familia guarda el GRUPO canónico (ej. "Pastel"),
    que puede representar varias familias/categorías reales de Point (ej.
    "Pastel Chico", "Pastel Grande" — ver reportes/mano_obra_grupos_familia.py).
    Expande cada grupo clasificado a sus familias reales antes de consultar
    Receta.familia/Insumo.categoria, que siguen guardando el texto tal cual
    viene de Point, sin normalizar."""
    grupos = RecetaAreaProduccion.objects.filter(area=area, familia__gt="").values_list("familia", flat=True)
    familias_reales: list[str] = []
    for grupo in grupos:
        familias_reales.extend(familias_del_grupo(grupo))
    return familias_reales


def _recetas_ids_por_area(area: str) -> set[int]:
    """Excepciones por receta tienen prioridad sobre la fila de familia."""
    excepciones = set(
        RecetaAreaProduccion.objects.filter(area=area, receta__isnull=False).values_list("receta_id", flat=True)
    )
    familias = _familias_reales_clasificadas(area)
    recetas_excepcion_otra_area = set(
        RecetaAreaProduccion.objects.exclude(area=area)
        .filter(receta__isnull=False)
        .values_list("receta_id", flat=True)
    )
    if familias:
        from recetas.models import Receta

        recetas_por_familia = set(
            Receta.objects.filter(familia__in=familias).exclude(id__in=recetas_excepcion_otra_area).values_list(
                "id", flat=True
            )
        )
    else:
        recetas_por_familia = set()
    return excepciones | recetas_por_familia


def _insumo_ids_por_area(area: str) -> set[int]:
    """Point registra producción tanto contra Receta (productos terminados)
    como contra Insumo (preparaciones internas: masas, betunes, rellenos —
    ~51% de la producción real). Insumo.categoria usa las mismas etiquetas
    que Receta.familia (PAN, GALLETAS, MASAS, etc., verificado en
    producción), así que la misma clasificación por familia en
    RecetaAreaProduccion aplica a ambos catálogos. Sin excepciones puntuales
    por insumo en este MVP — no hay evidencia de que haga falta."""
    familias = _familias_reales_clasificadas(area)
    if not familias:
        return set()
    from maestros.models import Insumo

    return set(
        Insumo.objects.filter(tipo_item=Insumo.TIPO_INTERNO, categoria__in=familias).values_list("id", flat=True)
    )


def unidades_area_dia(fecha: date, area: str) -> Decimal:
    receta_ids = _recetas_ids_por_area(area)
    insumo_ids = _insumo_ids_por_area(area)
    if not receta_ids and not insumo_ids:
        return Decimal("0")
    filtro = Q(receta_id__in=receta_ids) | Q(insumo_id__in=insumo_ids)
    total = PointProductionLine.objects.filter(filtro, production_date=fecha).aggregate(
        total=Sum("produced_quantity")
    )["total"]
    return Decimal(str(total or 0))


def calcular_costo_diario_area(fecha: date, area: str) -> CostoManoObraDiarioArea:
    nomina = nomina_diaria_area(fecha, area)
    unidades = unidades_area_dia(fecha, area)
    costo_unidad = (nomina / unidades) if (nomina is not None and unidades > 0) else None

    snapshot, _ = CostoManoObraDiarioArea.objects.update_or_create(
        fecha=fecha,
        area=area,
        defaults={
            "nomina_dia_area": nomina or Decimal("0"),
            "unidades_producidas": unidades,
            "costo_unidad": costo_unidad,
            "es_dia_laborable_esperado": nomina is not None,
        },
    )
    return snapshot


def costo_mano_obra_diario_receta(fecha: date, receta) -> dict:
    excepcion_areas = list(
        RecetaAreaProduccion.objects.filter(receta=receta).values_list("area", flat=True)
    )
    if excepcion_areas:
        areas = excepcion_areas
    else:
        grupo = grupo_de_familia(receta.familia)
        areas = list(
            RecetaAreaProduccion.objects.filter(familia=grupo).values_list("area", flat=True)
        )

    if not areas:
        return {"completo": False, "costo_total": None, "areas_faltantes": [], "sin_clasificar": True}

    costo_total = Decimal("0")
    areas_faltantes = []
    for area in areas:
        snapshot = calcular_costo_diario_area(fecha, area)
        if snapshot.costo_unidad is None:
            areas_faltantes.append(area)
        else:
            costo_total += snapshot.costo_unidad

    return {
        "completo": not areas_faltantes,
        "costo_total": costo_total if not areas_faltantes else None,
        "areas_faltantes": areas_faltantes,
        "sin_clasificar": False,
    }
