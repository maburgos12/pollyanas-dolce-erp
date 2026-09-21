"""Aplica el horario normal confirmado, sin recalcular incidencias ni autorizar extra."""

import json
from datetime import date, time
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import models, transaction
from django.utils import timezone

from core.models import AuditLog
from rrhh.models import AsignacionTurnoEmpleado, AsistenciaEmpleado, Empleado, Turno
from rrhh.services_extra_conciliacion import diagnosticar_horas_extra


DEFAULT_MANIFEST = Path(__file__).resolve().parents[2] / "data" / "horarios_produccion_2026.json"


class Command(BaseCommand):
    help = "Previsualiza o asigna los horarios normales 2026 del equipo activo de Producción."

    def add_arguments(self, parser):
        parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
        parser.add_argument("--hasta", help="Fecha de corte YYYY-MM-DD; por defecto hoy local.")
        parser.add_argument("--apply", action="store_true", help="Escribe turnos, vigencias y jornadas sin turno.")

    def handle(self, *args, **options):
        try:
            manifest = json.loads(options["manifest"].read_text(encoding="utf-8"))
            inicio = date.fromisoformat(manifest["fecha_inicio"])
            hasta = date.fromisoformat(options["hasta"]) if options["hasta"] else timezone.localdate()
            turnos_config = manifest["turnos"]
            empleados_config = manifest["empleados"]
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise CommandError(f"Manifiesto o fecha de corte inválidos: {exc}") from exc
        if inicio.year != 2026 or hasta < inicio or hasta > timezone.localdate():
            raise CommandError("El corte debe estar entre 2026-01-01 y el día actual.")
        if set(empleados_config.values()) != set(turnos_config):
            raise CommandError("Cada turno del manifiesto debe tener personas asignadas.")

        if options["apply"]:
            with transaction.atomic():
                result = self._execute(manifest, inicio, hasta, apply=True)
        else:
            result = self._execute(manifest, inicio, hasta, apply=False)
        self.stdout.write(json.dumps(result, ensure_ascii=False, sort_keys=True))

    def _execute(self, manifest, inicio, hasta, *, apply):
        empleados_config = manifest["empleados"]
        qs = Empleado.objects.filter(activo=True, departamento=manifest["departamento"])
        if apply:
            qs = qs.select_for_update()
        empleados = {e.codigo: e for e in qs}
        if set(empleados) != set(empleados_config):
            raise CommandError(
                "La plantilla activa de Producción cambió; revisa el manifiesto antes de aplicar. "
                f"Sin manifestar: {sorted(set(empleados)-set(empleados_config))}; "
                f"No activos en Producción: {sorted(set(empleados_config)-set(empleados))}."
            )

        turnos = {}
        turnos_nuevos = 0
        for index, (clave, config) in enumerate(manifest["turnos"].items(), start=1):
            esperado = (time.fromisoformat(config["entrada"]), time.fromisoformat(config["salida"]))
            turno = Turno.objects.filter(nombre=config["nombre"]).first()
            if turno:
                actual = (turno.hora_entrada, turno.hora_salida)
                if actual != esperado or turno.tolerancia_minutos != config["tolerancia_minutos"] or not turno.activo or turno.deteccion_por_checada:
                    raise CommandError(f"El turno {config['nombre']} ya existe con una configuración distinta.")
            elif apply:
                turno = Turno.objects.create(
                    nombre=config["nombre"], hora_entrada=esperado[0], hora_salida=esperado[1],
                    tolerancia_minutos=config["tolerancia_minutos"], activo=True,
                    deteccion_por_checada=False,
                )
                turnos_nuevos += 1
                AuditLog.objects.create(action="CREATE", model="rrhh.Turno", object_id=str(turno.pk),
                    payload={"motivo": manifest["motivo"], "deteccion_por_checada": False})
            else:
                turno = Turno(pk=-index, nombre=config["nombre"], hora_entrada=esperado[0],
                    hora_salida=esperado[1], tolerancia_minutos=config["tolerancia_minutos"],
                    activo=True, deteccion_por_checada=False)
                turnos_nuevos += 1
            turnos[clave] = turno

        stats = {
            "modo": "APLICADO" if apply else "DRY-RUN", "desde": str(inicio), "hasta": str(hasta),
            "personas": len(empleados), "turnos_nuevos": turnos_nuevos,
            "vigencias_nuevas": 0, "jornadas_sin_turno": 0,
            "jornadas_con_turno_coincidente": 0, "jornadas_con_turno_conflictivo": [],
            "extra_calculable_positivo": 0, "extra_detectado_minutos": 0,
            "extra_no_calculable": 0, "comida_no_observable": 0,
        }
        for codigo in sorted(empleados, key=int):
            empleado = empleados[codigo]
            turno = turnos[empleados_config[codigo]]
            fecha_inicio = max(inicio, empleado.fecha_ingreso)
            existente = AsignacionTurnoEmpleado.objects.select_related("turno").filter(
                empleado=empleado, fecha_inicio=fecha_inicio,
            ).first()
            if existente and (
                existente.turno.hora_entrada != turno.hora_entrada
                or existente.turno.hora_salida != turno.hora_salida
                or existente.fecha_fin is not None
                or not existente.proteger_reingesta_historica
            ):
                raise CommandError(f"La vigencia de {codigo} ya existe con horario diferente.")
            if not existente:
                solapada = AsignacionTurnoEmpleado.objects.filter(empleado=empleado).filter(
                    fecha_inicio__lte=hasta,
                ).filter(models.Q(fecha_fin__isnull=True) | models.Q(fecha_fin__gte=fecha_inicio)).exists()
                if solapada:
                    raise CommandError(f"La vigencia de {codigo} se traslapa con otra asignación.")
                stats["vigencias_nuevas"] += 1
                if apply:
                    existente = AsignacionTurnoEmpleado.objects.create(
                        empleado=empleado, turno=turno, fecha_inicio=fecha_inicio,
                        motivo=manifest["motivo"], proteger_reingesta_historica=True,
                    )
                    AuditLog.objects.create(action="CREATE", model="rrhh.AsignacionTurnoEmpleado",
                        object_id=str(existente.pk), payload={"codigo": codigo, "turno_id": turno.pk,
                        "fecha_inicio": str(fecha_inicio), "motivo": manifest["motivo"]})

            asistencias = AsistenciaEmpleado.objects.select_related("turno", "empleado").filter(
                empleado=empleado, fecha__gte=fecha_inicio, fecha__lte=hasta,
            ).order_by("fecha")
            if apply:
                asistencias = asistencias.select_for_update(of=("self",))
            sin_turno_ids = []
            for asistencia in asistencias:
                if asistencia.turno_id:
                    if (asistencia.turno.hora_entrada, asistencia.turno.hora_salida) != (
                        turno.hora_entrada, turno.hora_salida,
                    ):
                        stats["jornadas_con_turno_conflictivo"].append({
                            "codigo": codigo, "fecha": str(asistencia.fecha),
                            "turno_actual": asistencia.turno.nombre,
                        })
                        continue
                    stats["jornadas_con_turno_coincidente"] += 1
                else:
                    sin_turno_ids.append(asistencia.pk)
                    stats["jornadas_sin_turno"] += 1
                    asistencia.turno = turno
                diagnostico = diagnosticar_horas_extra(asistencia)
                if diagnostico.minutos is None:
                    stats["extra_no_calculable"] += 1
                elif diagnostico.minutos > 0:
                    stats["extra_calculable_positivo"] += 1
                    stats["extra_detectado_minutos"] += diagnostico.minutos
                if not diagnostico.comida_observable:
                    stats["comida_no_observable"] += 1
            if apply and sin_turno_ids:
                actualizadas = AsistenciaEmpleado.objects.filter(pk__in=sin_turno_ids, turno__isnull=True).update(turno=turno)
                if actualizadas != len(sin_turno_ids):
                    raise CommandError(f"Las jornadas de {codigo} cambiaron durante la asignación; se revirtió la transacción.")
                AuditLog.objects.create(action="UPDATE", model="rrhh.AsistenciaEmpleado",
                    object_id=str(empleado.pk), payload={"codigo": codigo, "turno_id": turno.pk,
                    "asistencias_ids": sin_turno_ids, "fecha_inicio": str(fecha_inicio),
                    "hasta": str(hasta), "motivo": manifest["motivo"],
                    "sin_recalculo_incidencias_ni_autorizaciones": True})
        if stats["jornadas_con_turno_conflictivo"] and apply:
            raise CommandError("Hay jornadas con otro turno; la transacción no se aplicó.")
        return stats
