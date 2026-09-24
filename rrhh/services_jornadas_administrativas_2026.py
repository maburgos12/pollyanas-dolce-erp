"""Carga cerrada, previsualizable y auditada de seis jornadas administrativas de 2026."""

from __future__ import annotations

from datetime import date, time
from decimal import Decimal
import hashlib
import json
from types import SimpleNamespace

from django.db import connection, transaction
from django.db.models import Q
from django.utils import timezone

from core.access import can_manage_rrhh
from core.audit import log_event
from core.models import AuditLog
from recetas.utils.normalizacion import normalizar_nombre

from .models import (
    AsignacionJornadaEmpleado, AsignacionTurnoEmpleado, AsistenciaEmpleado, Empleado,
    HoraExtra, IncidenciaAsistencia, JornadaSemanal, JornadaSemanalDia, Turno,
)
from .services_extra_conciliacion import (
    NOTA_EXTRA_AUTOMATICA, NOTA_SALDO_CUBIERTO, diagnosticar_horas_extra, horas_a_minutos,
    saldo_automatico_esperado,
)
from .services_asistencia_reglas import _evaluar_hora_extra
from .services_turnos import asignar_jornada_empleado
from .signals_extra import _conciliando


class ConfiguracionJornadasError(ValueError):
    """El plan no puede aplicarse sin revisar el conflicto y generar nueva huella."""


INICIO = date(2026, 9, 1)
FIN = date(2026, 12, 31)
MOTIVO = "Carga administrativa 2026 aprobada; vigencia septiembre a diciembre"
LOCK_KEY = 2026090106
MANIFIESTO = (
    (3, "SOTO INZUNZA YESENIA", "Administrativa 2026"),
    (4, "NORZAGARAY CONTRERAS JULIETA GUADALUPE", "Administrativa 2026"),
    (8, "LOPEZ PALOS JOHANA ADELIN", "Johana 2026"),
    (33, "LUGO ESPINOZA PAULA ELIZABETH", "Administrativa 2026"),
    (53, "EGUINO REYES LUIS OCTAVIO", "Administrativa 2026"),
    (99, "FIGUEROA SOTO JOHAN", "Administrativa 2026"),
)
TURNOS = (
    ("Administrativa 2026 08:00-13:30", time(8), time(13, 30)),
    ("Administrativa 2026 08:00-16:30", time(8), time(16, 30)),
    ("Administrativa 2026 09:00-17:30", time(9), time(17, 30)),
)
PERFILES = {
    "Administrativa 2026": ("Administrativa 2026 08:00-16:30",) * 5
    + ("Administrativa 2026 08:00-13:30", None),
    "Johana 2026": ("Administrativa 2026 09:00-17:30",) * 5
    + ("Administrativa 2026 08:00-13:30", None),
}
TIPOS_EXTRA = (
    IncidenciaAsistencia.TIPO_HORA_EXTRA_NO_CALCULABLE,
    IncidenciaAsistencia.TIPO_HORA_EXTRA_PENDIENTE,
)


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _fingerprint(plan):
    contenido = {k: v for k, v in plan.items() if k not in {"modo", "fingerprint"}}
    return hashlib.sha256(_canonical(contenido).encode("utf-8")).hexdigest()


def _turno_compatible(turno, entrada, salida):
    return bool(
        turno.hora_entrada == entrada and turno.hora_salida == salida
        and turno.tolerancia_minutos == 10 and turno.activo
        and not turno.deteccion_por_checada
    )


def _jornada_compatible(jornada, dias):
    if not jornada.activo or jornada.vigencia_desde != INICIO or jornada.vigencia_hasta != FIN:
        return False
    if len(dias) != 7 or {dia.dia_semana for dia in dias} != set(range(7)):
        return False
    requisitos = {nombre: (entrada, salida) for nombre, entrada, salida in TURNOS}
    for dia in dias:
        esperado = PERFILES[jornada.nombre][dia.dia_semana]
        if esperado is None:
            if dia.turno_id is not None:
                return False
        elif not dia.turno_id or not _turno_compatible(dia.turno, *requisitos[esperado]):
            return False
    return True


def _asistencia_proyectada(asistencia, turno):
    # El diagnóstico existente solo lee atributos. Este objeto evita guardar un
    # turno provisional y permite calcular antes de que exista el catálogo.
    return SimpleNamespace(
        empleado_id=asistencia.empleado_id, empleado=asistencia.empleado,
        fecha=asistencia.fecha, entrada=asistencia.entrada,
        salida_comida=asistencia.salida_comida, regreso_comida=asistencia.regreso_comida,
        salida=asistencia.salida, fuente=asistencia.fuente,
        turno_id=turno.pk if turno and turno.pk else (-1 if turno else None),
        turno=turno,
    )


def _plan(hoy: date, *, bloquear=False):
    limite = min(hoy, FIN)
    ids = [pk for pk, _, _ in MANIFIESTO]
    def filas(qs):
        if bloquear:
            qs = qs.select_for_update(of=("self",))
        return list(qs.order_by("pk"))

    personas_db = {e.pk: e for e in filas(Empleado.objects.filter(pk__in=ids))}
    turnos_db = filas(Turno.objects.filter(
        Q(nombre__in=[t[0] for t in TURNOS])
        | Q(hora_entrada__in=[t[1] for t in TURNOS], hora_salida__in=[t[2] for t in TURNOS])
    ))
    jornadas_db = filas(JornadaSemanal.objects.filter(nombre__in=PERFILES))
    dias_db = filas(JornadaSemanalDia.objects.filter(jornada_id__in=[j.pk for j in jornadas_db]))
    if bloquear:
        filas(Turno.objects.filter(pk__in=[d.turno_id for d in dias_db if d.turno_id]))
    asignaciones_db = filas(AsignacionJornadaEmpleado.objects.filter(
        empleado_id__in=ids, fecha_inicio__lte=FIN,
    ).filter(Q(fecha_fin__isnull=True) | Q(fecha_fin__gte=INICIO)))
    legacy_db = filas(AsignacionTurnoEmpleado.objects.filter(
        empleado_id__in=ids, fecha_inicio__lte=FIN,
    ).filter(Q(fecha_fin__isnull=True) | Q(fecha_fin__gte=INICIO)))
    asistencias_db = filas(AsistenciaEmpleado.objects.filter(
        empleado_id__in=ids, fecha__range=(INICIO, limite),
    )) if limite >= INICIO else []
    incidencias_db = filas(IncidenciaAsistencia.objects.filter(
        empleado_id__in=ids, fecha__range=(INICIO, limite), tipo__in=TIPOS_EXTRA,
    )) if limite >= INICIO else []
    extras_db = filas(HoraExtra.objects.filter(
        empleado_id__in=ids, fecha__range=(INICIO, limite),
    )) if limite >= INICIO else []

    conflictos = []
    personas = []
    for pk, esperado, perfil in MANIFIESTO:
        empleado = personas_db.get(pk)
        observado = normalizar_nombre(empleado.nombre).upper() if empleado else None
        if not empleado or observado != normalizar_nombre(esperado).upper() or not empleado.activo:
            conflictos.append({"tipo": "identidad", "empleado_id": pk,
                               "esperado": esperado, "observado": observado,
                               "activo": empleado.activo if empleado else None})
        personas.append({"id": pk, "esperado": esperado, "observado": observado,
                         "activo": empleado.activo if empleado else None,
                         "perfil": perfil, "fecha_inicio": INICIO.isoformat(),
                         "fecha_fin": FIN.isoformat(),
                         "asistencias": sum(a.empleado_id == pk for a in asistencias_db),
                         "asignaciones_existentes": sum(a.empleado_id == pk for a in asignaciones_db),
                         "extras_automaticas_pendientes": sum(
                             h.empleado_id == pk and h.asistencia_id is not None
                             and h.estado == HoraExtra.ESTADO_PENDIENTE for h in extras_db
                         )})

    turnos = []
    turnos_obj = {}
    for nombre, entrada, salida in TURNOS:
        por_nombre = [t for t in turnos_db if t.nombre == nombre]
        if len(por_nombre) > 1 or (por_nombre and not _turno_compatible(por_nombre[0], entrada, salida)):
            conflictos.append({"tipo": "turno_incompatible", "nombre": nombre,
                               "ids": [t.pk for t in por_nombre]})
            elegido = None
            estado = "conflicto"
        else:
            exactos = [t for t in turnos_db if _turno_compatible(t, entrada, salida)]
            elegido = por_nombre[0] if por_nombre else min(exactos, key=lambda t: t.pk, default=None)
            estado = "reutilizar" if elegido else "crear"
        turnos_obj[nombre] = elegido or Turno(
            nombre=nombre, hora_entrada=entrada, hora_salida=salida,
            tolerancia_minutos=10, activo=True, deteccion_por_checada=False,
        )
        turnos.append({"nombre": nombre, "hora_entrada": entrada.isoformat(timespec="minutes"),
                       "hora_salida": salida.isoformat(timespec="minutes"),
                       "tolerancia_minutos": 10, "deteccion_por_checada": False,
                       "id": elegido.pk if elegido else None, "accion": estado})

    jornadas = []
    jornadas_obj = {}
    for nombre in sorted(PERFILES):
        existentes = [j for j in jornadas_db if j.nombre == nombre]
        existente = existentes[0] if len(existentes) == 1 else None
        dias = [d for d in dias_db if existente and d.jornada_id == existente.pk]
        if existentes and (not existente or not _jornada_compatible(existente, dias)):
            conflictos.append({"tipo": "jornada_incompatible", "nombre": nombre,
                               "ids": [j.pk for j in existentes]})
            accion = "conflicto"
        else:
            accion = "reutilizar" if existente else "crear"
        jornadas_obj[nombre] = existente
        jornadas.append({"nombre": nombre, "id": existente.pk if existente else None,
                         "fecha_inicio": INICIO.isoformat(), "fecha_fin": FIN.isoformat(),
                         "dias": list(PERFILES[nombre]), "horas_semanales": 48,
                         "accion": accion})

    asignaciones = []
    for pk, _, perfil in MANIFIESTO:
        coincidencias = [a for a in asignaciones_db if a.empleado_id == pk]
        exacta = next((a for a in coincidencias if a.fecha_inicio == INICIO
                       and a.fecha_fin == FIN and a.jornada.nombre == perfil), None)
        otras = [a for a in coincidencias if a != exacta]
        legacy = [a.pk for a in legacy_db if a.empleado_id == pk]
        if otras or legacy:
            conflictos.append({"tipo": "asignacion_traslapada", "empleado_id": pk,
                               "ids": [a.pk for a in otras], "legacy_ids": legacy})
        asignaciones.append({"empleado_id": pk, "perfil": perfil,
                             "fecha_inicio": INICIO.isoformat(), "fecha_fin": FIN.isoformat(),
                             "id": exacta.pk if exacta else None,
                             "accion": "existente" if exacta else "crear",
                             "traslapes": [a.pk for a in otras], "traslapes_legacy": legacy})

    perfil_por_id = {pk: perfil for pk, _, perfil in MANIFIESTO}
    asistencias_a_actualizar = []
    for a in asistencias_db:
        nombre_turno = PERFILES[perfil_por_id[a.empleado_id]][a.fecha.weekday()]
        turno = turnos_obj[nombre_turno] if nombre_turno else None
        if a.turno_id != (turno.pk if turno else None) or (turno and not turno.pk):
            asistencias_a_actualizar.append({
                "id": a.pk, "empleado_id": a.empleado_id, "fecha": a.fecha.isoformat(),
                "turno_anterior_id": a.turno_id, "turno_nuevo": nombre_turno,
                "turno_nuevo_id": turno.pk if turno else None,
            })

    extras_por_fecha = {}
    for he in extras_db:
        extras_por_fecha.setdefault((he.empleado_id, he.fecha), []).append(he)
    incidencias_por_fecha = {(i.empleado_id, i.fecha, i.tipo): i for i in incidencias_db}
    fechas_turno_cambiado = {(a["empleado_id"], date.fromisoformat(a["fecha"])) for a in asistencias_a_actualizar}
    fechas_reconciliar = set(fechas_turno_cambiado)
    fechas_reconciliar.update((he.empleado_id, he.fecha) for he in extras_db
                              if he.asistencia_id and he.estado == HoraExtra.ESTADO_PENDIENTE)
    fechas_examinar = set(fechas_reconciliar)
    fechas_examinar.update((he.empleado_id, he.fecha) for he in extras_db
                           if he.asistencia_id and he.estado in {
                                HoraExtra.ESTADO_AUTORIZADO, HoraExtra.ESTADO_PAGADO,
                                HoraExtra.ESTADO_RECHAZADO, HoraExtra.ESTADO_CANCELADO,
                            })
    pendientes = []
    resueltas = []
    incidencias = []
    for a in asistencias_db:
        key = (a.empleado_id, a.fecha)
        if key not in fechas_examinar:
            continue
        nombre_turno = PERFILES[perfil_por_id[a.empleado_id]][a.fecha.weekday()]
        turno = turnos_obj[nombre_turno] if nombre_turno else None
        diagnostico = diagnosticar_horas_extra(_asistencia_proyectada(a, turno))
        registros = extras_por_fecha.get(key, [])
        vinculado = next((he for he in registros if he.asistencia_id == a.pk), None)
        pendientes_antes = len(pendientes)
        if key in fechas_reconciliar and vinculado and vinculado.estado == HoraExtra.ESTADO_PENDIENTE and not vinculado.ajuste_autorizacion:
            saldo = saldo_automatico_esperado(diagnostico, registros, vinculado)
            if saldo is not None:
                accion = "cancelar" if saldo <= 0 else "cambiar" if vinculado.horas != saldo else "sin_cambio"
                pendientes.append({"id": vinculado.pk, "asistencia_id": a.pk,
                                   "empleado_id": a.empleado_id, "fecha": a.fecha.isoformat(),
                                   "anterior": str(vinculado.horas), "nuevo": str(saldo),
                                   "accion": accion})
        elif key in fechas_reconciliar and not vinculado:
            saldo = saldo_automatico_esperado(diagnostico, registros)
            if saldo and saldo > 0:
                pendientes.append({"id": None, "asistencia_id": a.pk,
                                   "empleado_id": a.empleado_id, "fecha": a.fecha.isoformat(),
                                   "anterior": None, "nuevo": str(saldo), "accion": "crear"})
        reevaluar = key in fechas_turno_cambiado or any(
            item["accion"] != "sin_cambio" for item in pendientes[pendientes_antes:]
        )
        if reevaluar and diagnostico.minutos is not None and diagnostico.minutos > 0:
            tipo = IncidenciaAsistencia.TIPO_HORA_EXTRA_PENDIENTE
            existente = incidencias_por_fecha.get((a.empleado_id, a.fecha, tipo))
            if existente is None or (
                existente.estado != IncidenciaAsistencia.ESTADO_RESUELTO
                and not existente.editado_manual
            ):
                incidencias.append({"id": existente.pk if existente else None,
                                    "asistencia_id": a.pk, "empleado_id": a.empleado_id,
                                    "fecha": a.fecha.isoformat(), "tipo": tipo,
                                    "accion": "actualizar" if existente else "crear"})
        for he in registros:
            if he.asistencia_id != a.pk or he.estado == HoraExtra.ESTADO_PENDIENTE:
                continue
            if he.estado not in {HoraExtra.ESTADO_AUTORIZADO, HoraExtra.ESTADO_PAGADO,
                                 HoraExtra.ESTADO_RECHAZADO, HoraExtra.ESTADO_CANCELADO}:
                continue
            if diagnostico.minutos is None or diagnostico.minutos != horas_a_minutos(he.horas):
                resueltas.append({"id": he.pk, "empleado_id": a.empleado_id,
                                  "fecha": a.fecha.isoformat(), "estado": he.estado,
                                  "horas": str(he.horas),
                                  "detectado_minutos": diagnostico.minutos,
                                  "monto_calculado": str(he.monto_calculado) if he.monto_calculado is not None else None})
        for tipo in TIPOS_EXTRA:
            if key not in fechas_reconciliar:
                continue
            incidencia = incidencias_por_fecha.get((a.empleado_id, a.fecha, tipo))
            if incidencia and incidencia.estado == IncidenciaAsistencia.ESTADO_PENDIENTE and not incidencia.editado_manual:
                # La carga solo cierra alertas de extra obsoletas. Los demás
                # tipos de asistencia requieren una reevaluación ordinaria.
                obsoleta = ((tipo == IncidenciaAsistencia.TIPO_HORA_EXTRA_NO_CALCULABLE
                             and diagnostico.codigo != "sin_turno")
                            or (tipo == IncidenciaAsistencia.TIPO_HORA_EXTRA_PENDIENTE
                                and (diagnostico.minutos is None or diagnostico.minutos == 0)))
                if obsoleta:
                    incidencias.append({"id": incidencia.pk, "empleado_id": a.empleado_id,
                                        "fecha": a.fecha.isoformat(), "tipo": tipo,
                                        "accion": "resolver"})

    plan = {
        "modo": "preview", "personas_objetivo": 6, "personas": personas,
        "turnos": turnos, "jornadas": jornadas, "asignaciones": asignaciones,
        "asistencias_a_actualizar": asistencias_a_actualizar,
        "incidencias_a_reconciliar": incidencias,
        "pendientes_a_reconciliar": pendientes,
        "extras_resueltas_con_diferencia": resueltas,
        "conflictos": sorted(conflictos, key=_canonical),
    }
    plan["fingerprint"] = _fingerprint(plan)
    return plan


def _auditar(actor, action, model, object_id, antes, despues, *, fecha=None):
    log_event(actor, action, model, str(object_id), {
        "motivo": MOTIVO, "fecha": fecha.isoformat() if fecha else None,
        "antes": antes, "despues": despues,
    })


def _aplicar_plan(plan, actor):
    aplicadas = []
    turnos = {}
    for fila in plan["turnos"]:
        if fila["accion"] == "crear":
            turno = Turno(nombre=fila["nombre"],
                          hora_entrada=time.fromisoformat(fila["hora_entrada"]),
                          hora_salida=time.fromisoformat(fila["hora_salida"]),
                          tolerancia_minutos=10, activo=True, deteccion_por_checada=False)
            turno.full_clean()
            turno.save()
            _auditar(actor, "CREATE", "rrhh.Turno", turno.pk, None, fila)
            aplicadas.append({"modelo": "Turno", "id": turno.pk, "accion": "crear"})
        else:
            turno = Turno.objects.get(pk=fila["id"])
        turnos[fila["nombre"]] = turno

    jornadas = {}
    for fila in plan["jornadas"]:
        if fila["accion"] == "crear":
            jornada = JornadaSemanal(nombre=fila["nombre"], activo=True,
                                      vigencia_desde=INICIO, vigencia_hasta=FIN,
                                      descripcion="48 horas semanales; domingo descanso")
            jornada.full_clean()
            jornada.save()
            for indice, nombre_turno in enumerate(PERFILES[fila["nombre"]]):
                dia = JornadaSemanalDia(jornada=jornada, dia_semana=indice,
                                        turno=turnos[nombre_turno] if nombre_turno else None)
                dia.full_clean()
                dia.save()
                _auditar(actor, "CREATE", "rrhh.JornadaSemanalDia", dia.pk, None,
                         {"jornada": jornada.nombre, "dia_semana": indice,
                          "turno": nombre_turno})
            _auditar(actor, "CREATE", "rrhh.JornadaSemanal", jornada.pk, None, fila)
            aplicadas.append({"modelo": "JornadaSemanal", "id": jornada.pk, "accion": "crear"})
        else:
            jornada = JornadaSemanal.objects.get(pk=fila["id"])
        jornadas[fila["nombre"]] = jornada

    for fila in plan["asignaciones"]:
        if fila["accion"] != "crear":
            continue
        asignacion = asignar_jornada_empleado(
            empleado=Empleado.objects.get(pk=fila["empleado_id"]),
            jornada=jornadas[fila["perfil"]], fecha_inicio=INICIO, fecha_fin=FIN,
            motivo=MOTIVO, actor=actor,
        )
        _auditar(actor, "CREATE", "rrhh.AsignacionJornadaEmpleado", asignacion.pk,
                 None, fila, fecha=INICIO)
        aplicadas.append({"modelo": "AsignacionJornadaEmpleado", "id": asignacion.pk,
                           "accion": "crear"})

    for fila in plan["asistencias_a_actualizar"]:
        asistencia = AsistenciaEmpleado.objects.get(pk=fila["id"])
        asistencia.turno = turnos.get(fila["turno_nuevo"])
        asistencia.save(update_fields=["turno"])
        _auditar(actor, "UPDATE", "rrhh.AsistenciaEmpleado", asistencia.pk,
                 {"turno_id": fila["turno_anterior_id"]},
                 {"turno_id": asistencia.turno_id}, fecha=asistencia.fecha)
        aplicadas.append({"modelo": "AsistenciaEmpleado", "id": asistencia.pk,
                           "accion": "actualizar_turno"})

    for fila in plan["pendientes_a_reconciliar"]:
        if fila["accion"] == "sin_cambio":
            continue
        asistencia = AsistenciaEmpleado.objects.get(pk=fila["asistencia_id"])
        # El post_save normal reevalúa y puede reabrir incidencias resueltas.
        # Esta carga tiene su propio plan cerrado y conserva dichas decisiones.
        token = _conciliando.set(True)
        try:
            if fila["accion"] == "crear":
                he = HoraExtra(empleado_id=asistencia.empleado_id, asistencia=asistencia,
                               fecha=asistencia.fecha, horas=Decimal(fila["nuevo"]),
                               notas=f"{NOTA_EXTRA_AUTOMATICA} Saldo por salida programada.")
                he.full_clean()
                he.save()
                antes = None
                accion = "CREATE"
            else:
                he = HoraExtra.objects.get(pk=fila["id"])
                antes = {"horas": str(he.horas), "estado": he.estado, "notas": he.notas}
                if he.estado != HoraExtra.ESTADO_PENDIENTE or he.asistencia_id != asistencia.pk:
                    raise ConfiguracionJornadasError("La propuesta cambió durante la aplicación.")
                if fila["accion"] == "cancelar":
                    he.estado = HoraExtra.ESTADO_CANCELADO
                    he.notas += "\n" + NOTA_SALDO_CUBIERTO
                    he.save(update_fields=["estado", "notas"])
                else:
                    he.horas = Decimal(fila["nuevo"])
                    he.save(update_fields=["horas"])
                accion = "UPDATE"
        finally:
            _conciliando.reset(token)
        _auditar(actor, accion, "rrhh.HoraExtra", he.pk, antes,
                 {"horas": str(he.horas), "estado": he.estado, "notas": he.notas},
                 fecha=he.fecha)
        aplicadas.append({"modelo": "HoraExtra", "id": he.pk, "accion": fila["accion"]})

    for fila in plan["incidencias_a_reconciliar"]:
        if fila["accion"] in {"crear", "actualizar"}:
            asistencia = AsistenciaEmpleado.objects.select_related("empleado", "turno").get(
                pk=fila["asistencia_id"]
            )
            anterior = None
            if fila["id"]:
                previa = IncidenciaAsistencia.objects.get(pk=fila["id"])
                anterior = {"estado": previa.estado, "severidad": previa.severidad,
                            "minutos": previa.minutos, "detalle": previa.detalle,
                            "hora_extra_id": previa.hora_extra_id, "metadata": previa.metadata}
            _evaluar_hora_extra(asistencia, set(), generar=False)
            incidencia = IncidenciaAsistencia.objects.get(
                empleado_id=fila["empleado_id"], fecha=asistencia.fecha, tipo=fila["tipo"],
            )
            despues = {"estado": incidencia.estado, "severidad": incidencia.severidad,
                       "minutos": incidencia.minutos, "detalle": incidencia.detalle,
                       "hora_extra_id": incidencia.hora_extra_id, "metadata": incidencia.metadata}
            if anterior != despues:
                _auditar(actor, "CREATE" if anterior is None else "UPDATE",
                         "rrhh.IncidenciaAsistencia", incidencia.pk,
                         anterior, despues, fecha=incidencia.fecha)
                aplicadas.append({"modelo": "IncidenciaAsistencia", "id": incidencia.pk,
                                   "accion": fila["accion"]})
            continue
        incidencia = IncidenciaAsistencia.objects.get(pk=fila["id"])
        if incidencia.estado != IncidenciaAsistencia.ESTADO_PENDIENTE or incidencia.editado_manual:
            raise ConfiguracionJornadasError("La incidencia cambió durante la aplicación.")
        anterior = incidencia.estado
        incidencia.estado = IncidenciaAsistencia.ESTADO_RESUELTO
        incidencia.save(update_fields=["estado", "actualizado_en"])
        _auditar(actor, "UPDATE", "rrhh.IncidenciaAsistencia", incidencia.pk,
                 {"estado": anterior}, {"estado": incidencia.estado}, fecha=incidencia.fecha)
        aplicadas.append({"modelo": "IncidenciaAsistencia", "id": incidencia.pk,
                           "accion": "resolver"})

    for fila in plan["extras_resueltas_con_diferencia"]:
        clave = hashlib.sha256(_canonical(fila).encode()).hexdigest()
        if not AuditLog.objects.filter(
            action="REVIEW", model="rrhh.JornadasAdministrativas2026",
            payload__revision_clave=clave,
        ).exists():
            log_event(actor, "REVIEW", "rrhh.JornadasAdministrativas2026", str(fila["id"]), {
                "motivo": MOTIVO, "fecha": fila["fecha"], "revision_clave": clave,
                "extra_resuelta_sin_modificar": fila,
            })
            aplicadas.append({"modelo": "HoraExtra", "id": fila["id"], "accion": "revision"})
    return aplicadas


def configurar_jornadas_administrativas_2026(
    *, aplicar=False, hoy=None, actor=None, expected_fingerprint=None,
) -> dict:
    """Previsualiza sin escribir; aplica solo con actor y huella de un plan fresco."""
    hoy = hoy or timezone.localdate()
    if type(hoy) is not date:
        raise ConfiguracionJornadasError("hoy debe ser una fecha válida.")
    if not aplicar:
        return _plan(hoy)
    if not actor or not getattr(actor, "is_authenticated", False) or not actor.is_active or not can_manage_rrhh(actor):
        raise ConfiguracionJornadasError("Se requiere un actor activo con permiso para gestionar RRHH.")
    if not expected_fingerprint:
        raise ConfiguracionJornadasError("--expected-fingerprint es obligatorio al aplicar.")
    with transaction.atomic():
        if connection.vendor != "postgresql":
            raise ConfiguracionJornadasError("La aplicación requiere PostgreSQL.")
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", [LOCK_KEY])
        plan = _plan(hoy, bloquear=True)
        if plan["conflictos"]:
            raise ConfiguracionJornadasError(f"La carga tiene conflictos: {_canonical(plan['conflictos'])}")
        if plan["fingerprint"] != expected_fingerprint:
            raise ConfiguracionJornadasError("La huella del plan cambió; genere una previsualización nueva.")
        aplicadas = _aplicar_plan(plan, actor)
        resultado = _plan(hoy, bloquear=True)
        resultado["modo"] = "apply"
        resultado["aplicadas"] = aplicadas
        return resultado
