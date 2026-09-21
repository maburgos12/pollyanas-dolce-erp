"""Reconoce correcciones humanas históricas de propuestas automáticas pendientes."""
import json
import re

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from core.models import AuditLog
from rrhh.models import HoraExtra
from rrhh.services_extra_bloqueos import bloquear_hora_extra
from rrhh.services_extra_conciliacion import (
    NOTA_EXTRA_AUTOMATICA, diagnosticar_horas_extra,
    evidencia_ajuste_extra, saldo_automatico_esperado,
)


CORRECCION = re.compile(r"^Correccion registrada por ([\w.]+) el .*?: (.+)$", re.MULTILINE)


class Command(BaseCommand):
    help = "Previsualiza o registra la evidencia de ajustes de extra ya capturados por un supervisor."

    def add_arguments(self, parser):
        parser.add_argument("--ids", nargs="+", type=int, required=True)
        parser.add_argument("--apply", action="store_true")

    @transaction.atomic
    def handle(self, *args, **options):
        resultados = []
        for pk in sorted(set(options["ids"])):
            with transaction.atomic():
                if options["apply"]:
                    hora, registros = bloquear_hora_extra(pk)
                else:
                    hora = HoraExtra.objects.select_related("asistencia__turno", "asistencia__empleado").filter(pk=pk).first()
                    registros = list(HoraExtra.objects.filter(empleado_id=hora.empleado_id, fecha=hora.fecha)) if hora else []
                if not hora:
                    raise CommandError(f"No existe HoraExtra {pk}.")
                correcciones = CORRECCION.findall(hora.notas or "")
                if (hora.estado != HoraExtra.ESTADO_PENDIENTE or not hora.asistencia_id
                    or not (hora.notas or "").startswith(NOTA_EXTRA_AUTOMATICA)
                    or len(correcciones) != 1):
                    raise CommandError(f"HoraExtra {pk} no es una propuesta automática pendiente corregida con motivo.")
                if (hora.empleado_id, hora.fecha) != (hora.asistencia.empleado_id, hora.asistencia.fecha):
                    raise CommandError(f"HoraExtra {pk} tiene vínculo inconsistente.")
                diagnostico = diagnosticar_horas_extra(hora.asistencia)
                saldo = saldo_automatico_esperado(diagnostico, registros, hora)
                if saldo is None or saldo <= 0:
                    raise CommandError(f"HoraExtra {pk} no tiene saldo positivo calculable.")
                usuario, motivo = correcciones[-1]
                evidencia = evidencia_ajuste_extra(hora, saldo, motivo, usuario)
                estado = "ya_registrado" if hora.ajuste_autorizacion == evidencia else "por_registrar"
                # La marca de tiempo varía entre ejecuciones: comparar solo insumos estables.
                if hora.ajuste_autorizacion and all(
                    hora.ajuste_autorizacion.get(k) == evidencia[k]
                    for k in ("horas", "saldo", "huella", "motivo", "usuario")
                ):
                    estado = "ya_registrado"
                elif hora.horas == saldo:
                    estado = "saldo_exacto"
                elif options["apply"]:
                    hora.ajuste_autorizacion = evidencia
                    hora.save(update_fields=["ajuste_autorizacion"])
                    AuditLog.objects.create(action="UPDATE", model="rrhh.HoraExtra", object_id=str(pk),
                        payload={"motivo": "Reconocer corrección humana previa sin alterar horas ni autorización",
                                 "horas": str(hora.horas), "saldo": str(saldo), "usuario": usuario,
                                 "correccion": motivo})
                    estado = "registrado"
                resultados.append({"id": pk, "codigo": hora.empleado.codigo, "fecha": str(hora.fecha),
                    "horas": str(hora.horas), "saldo": str(saldo), "motivo": motivo, "estado": estado})
        self.stdout.write(json.dumps(resultados, ensure_ascii=False))
