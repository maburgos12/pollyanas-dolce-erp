from __future__ import annotations

from dataclasses import dataclass

from .models import ParadaRuta, RutaEntrega


@dataclass(frozen=True)
class ParadaTiempo:
    parada: ParadaRuta
    permanencia_real_minutos: int | None
    promedio_surtido_minutos: int | None


@dataclass(frozen=True)
class ResumenTiemposRuta:
    paradas: list[ParadaTiempo]
    transito_programado_minutos: int
    surtido_estimado_minutos: int
    total_operativo_estimado_minutos: int


def _minutos_redondeados(segundos: float) -> int:
    return max(int(round(segundos / 60)), 0)


def permanencia_real_minutos(parada: ParadaRuta) -> int | None:
    if not parada.hora_llegada_real or not parada.hora_salida_real:
        return None
    segundos = (parada.hora_salida_real - parada.hora_llegada_real).total_seconds()
    if segundos <= 0:
        return None
    return _minutos_redondeados(segundos)


def promedio_surtido_punto_minutos(punto_id: int, *, exclude_parada_id: int | None = None) -> int | None:
    qs = ParadaRuta.objects.filter(
        punto_id=punto_id,
        hora_llegada_real__isnull=False,
        hora_salida_real__isnull=False,
    )
    if exclude_parada_id:
        qs = qs.exclude(pk=exclude_parada_id)

    duraciones = []
    for llegada, salida in qs.values_list("hora_llegada_real", "hora_salida_real"):
        segundos = (salida - llegada).total_seconds()
        if segundos > 0:
            duraciones.append(segundos)
    if not duraciones:
        return None
    return _minutos_redondeados(sum(duraciones) / len(duraciones))


def resumen_tiempos_ruta(ruta: RutaEntrega) -> ResumenTiemposRuta:
    rows = []
    surtido_estimado = 0
    for parada in ruta.paradas.select_related("punto").order_by("orden", "id"):
        real = permanencia_real_minutos(parada)
        promedio = promedio_surtido_punto_minutos(parada.punto_id, exclude_parada_id=parada.id)
        rows.append(ParadaTiempo(parada=parada, permanencia_real_minutos=real, promedio_surtido_minutos=promedio))
        if promedio is not None:
            surtido_estimado += promedio

    transito = _minutos_redondeados(ruta.ruta_programada_duracion_segundos or 0)
    return ResumenTiemposRuta(
        paradas=rows,
        transito_programado_minutos=transito,
        surtido_estimado_minutos=surtido_estimado,
        total_operativo_estimado_minutos=transito + surtido_estimado,
    )
