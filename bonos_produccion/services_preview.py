"""Simulación de contexto RRHH para bonos sin persistir cambios."""

from __future__ import annotations

import hashlib
import json
from copy import copy
from decimal import Decimal

from django.db import transaction

from rrhh.services_asistencia_contexto import (
    CODIGO_ASISTENCIA,
    CODIGO_RETARDO,
    cargar_contexto_asistencia,
)

from .models import BonoProduccionEmpleado, ConfigBonoPeriodo, RegistroDiarioProduccion
from .services_checador import _fecha_registro_legacy, _fechas, _fecha_sincronizable


CAMPOS_MANUALES = (
    "bono_extra",
    "ajuste_positivo",
    "ajuste_negativo",
    "desc_bono_extra",
    "desc_ajuste_positivo",
    "desc_ajuste_negativo",
    "observaciones",
)


def _serializar(valor):
    return str(valor) if isinstance(valor, Decimal) else valor


def _huella_manual(periodo_id: int) -> str:
    filas = list(
        BonoProduccionEmpleado.objects.filter(periodo_id=periodo_id)
        .order_by("id")
        .values("id", *CAMPOS_MANUALES)
    )
    contenido = json.dumps(filas, sort_keys=True, default=_serializar, ensure_ascii=False)
    return hashlib.sha256(contenido.encode("utf-8")).hexdigest()


def generar_preview_contexto_rrhh(periodo: ConfigBonoPeriodo) -> dict:
    """Calcula el resultado propuesto y fuerza rollback de cualquier efecto ORM."""
    hash_antes = _huella_manual(periodo.id)
    filas = []
    with transaction.atomic():
        bonos = list(periodo.bonos.select_related("empleado").order_by("empleado__nombre"))
        inicio, fin = periodo.rango_fechas()
        fechas = [fecha for fecha in _fechas(inicio, fin) if _fecha_sincronizable(periodo, fecha)]
        contexto = cargar_contexto_asistencia(
            empleados=[bono.empleado for bono in bonos],
            fecha_inicio=inicio,
            fecha_fin=fin,
        )
        registros = list(
            RegistroDiarioProduccion.objects.filter(bono__in=bonos).select_related("bono__periodo")
        )
        existentes = {}
        for registro in registros:
            fecha_registro = registro.fecha or _fecha_registro_legacy(registro.bono.periodo, registro.dia)
            if fecha_registro:
                existentes[(registro.bono_id, fecha_registro)] = registro

        for bono in bonos:
            dias_asistencia = 0
            dias_uniforme = 0
            dias_puntualidad = 0
            dias_produccion = 0
            total_embetunados = 0
            faltas_rrhh = 0
            estados = {}
            for fecha in fechas:
                if bono.empleado.fecha_ingreso and fecha < bono.empleado.fecha_ingreso:
                    continue
                dia = contexto.clasificar(bono.empleado, fecha)
                estados[dia.codigo] = estados.get(dia.codigo, 0) + 1
                registro = existentes.get((bono.id, fecha))
                if registro is not None and registro.capturado_por_id:
                    asistencia = registro.tiene_asistencia
                    puntualidad = registro.tiene_puntualidad
                    uniforme = registro.tiene_uniforme
                    produccion = registro.tiene_produccion
                    embetunados = registro.cantidad_embetunados
                    falta = bool(dia.es_exigible and not asistencia)
                else:
                    asistencia = dia.codigo in {CODIGO_ASISTENCIA, CODIGO_RETARDO}
                    puntualidad = dia.codigo == CODIGO_ASISTENCIA
                    uniforme = asistencia
                    produccion = asistencia
                    embetunados = 0
                    falta = dia.falta_penalizable
                faltas_rrhh += int(falta)
                if asistencia:
                    dias_asistencia += 1
                    dias_uniforme += int(uniforme)
                    dias_puntualidad += int(puntualidad)
                    dias_produccion += int(produccion)
                    total_embetunados += embetunados

            simulado = copy(bono)
            simulado.dias_trabajados = dias_asistencia
            simulado.dias_asistencia = dias_asistencia
            simulado.dias_uniforme = dias_uniforme
            simulado.dias_puntualidad = dias_puntualidad
            simulado.dias_produccion = dias_produccion
            simulado.total_embetunados = total_embetunados
            simulado.faltas_rrhh = faltas_rrhh
            simulado.recalcular()
            faltas_actuales = (
                bono.faltas_rrhh
                if bono.faltas_rrhh is not None
                else max(periodo.dias_laborables_exigibles() - int(bono.dias_asistencia or 0), 0)
            )
            filas.append({
                "bono_id": bono.id,
                "empleado_id": bono.empleado_id,
                "empleado": bono.empleado.nombre,
                "area": bono.area,
                "faltas_actuales": int(faltas_actuales),
                "faltas_rrhh_propuestas": faltas_rrhh,
                "total_actual": str(bono.total_a_pagar),
                "total_propuesto": str(simulado.total_a_pagar),
                "cancela_actual": bono.cancela_bono,
                "cancela_propuesto": simulado.cancela_bono,
                "estados_rrhh": estados,
            })

        hash_despues = _huella_manual(periodo.id)
        transaction.set_rollback(True)

    return {
        "periodo_id": periodo.id,
        "mes": periodo.mes,
        "anio": periodo.anio,
        "fecha_inicio": str(inicio),
        "fecha_fin": str(fin),
        "hash_manual_antes": hash_antes,
        "hash_manual_despues": hash_despues,
        "manuales_intactos": hash_antes == hash_despues,
        "filas": filas,
    }
