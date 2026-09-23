"""Cancela propuestas automáticas pendientes cuyo saldo no alcanza 50 minutos."""
import json

from django.core.management.base import BaseCommand
from django.db import transaction

from core.audit import log_event
from rrhh.models import HoraExtra
from rrhh.services_extra_bloqueos import bloquear_hora_extra
from rrhh.services_extra_conciliacion import (
    UMBRAL_SOLICITUD_EXTRA_MINUTOS,
    diagnosticar_horas_extra,
    formatear_duracion_minutos,
    saldo_automatico_minutos,
)


NOTA_REGULARIZACION_UMBRAL = (
    "[Umbral mínimo de 50 minutos] Solicitud automática cancelada; "
    "el saldo no cubierto no alcanza el mínimo para generar tiempo extra."
)


class Command(BaseCommand):
    help = "Previsualiza o cancela propuestas automáticas pendientes menores a 50 minutos."

    def add_arguments(self, parser):
        parser.add_argument("--ids", nargs="+", type=int)
        parser.add_argument("--apply", action="store_true")

    def _evaluar(self, hora_extra, registros):
        if (
            hora_extra.estado != HoraExtra.ESTADO_PENDIENTE
            or not hora_extra.asistencia_id
            or (hora_extra.empleado_id, hora_extra.fecha)
            != (hora_extra.asistencia.empleado_id, hora_extra.asistencia.fecha)
        ):
            return None
        diagnostico = diagnosticar_horas_extra(hora_extra.asistencia)
        saldo_minutos = saldo_automatico_minutos(diagnostico, registros, hora_extra)
        if saldo_minutos is None or not 0 < saldo_minutos < UMBRAL_SOLICITUD_EXTRA_MINUTOS:
            return None
        return {
            "id": hora_extra.pk,
            "empleado": hora_extra.empleado.nombre,
            "fecha": hora_extra.fecha.isoformat(),
            "tiempo_propuesto": str(hora_extra.horas),
            "saldo_minutos": saldo_minutos,
            "saldo_legible": formatear_duracion_minutos(saldo_minutos),
            "estado": "por_cancelar",
        }

    def handle(self, *args, **options):
        queryset = HoraExtra.objects.filter(
            estado=HoraExtra.ESTADO_PENDIENTE,
            asistencia__isnull=False,
        ).select_related("empleado", "asistencia__empleado", "asistencia__turno").order_by("pk")
        if options.get("ids"):
            queryset = queryset.filter(pk__in=sorted(set(options["ids"])))

        resultados = []
        for pk in queryset.values_list("pk", flat=True):
            with transaction.atomic():
                if options["apply"]:
                    hora_extra, registros = bloquear_hora_extra(pk)
                else:
                    hora_extra = queryset.filter(pk=pk).first()
                    registros = list(HoraExtra.objects.filter(
                        empleado_id=hora_extra.empleado_id,
                        fecha=hora_extra.fecha,
                    ))
                resultado = self._evaluar(hora_extra, registros)
                if not resultado:
                    continue
                if options["apply"]:
                    hora_extra.estado = HoraExtra.ESTADO_CANCELADO
                    hora_extra.ajuste_autorizacion = {}
                    hora_extra.notas = "\n".join(filter(None, [
                        (hora_extra.notas or "").strip(),
                        NOTA_REGULARIZACION_UMBRAL,
                    ]))
                    hora_extra.save(update_fields=["estado", "ajuste_autorizacion", "notas"])
                    log_event(
                        None,
                        "RRHH_EXTRA_UMBRAL_CANCELADA",
                        "rrhh.HoraExtra",
                        str(hora_extra.pk),
                        {
                            "empleado_id": hora_extra.empleado_id,
                            "fecha": hora_extra.fecha.isoformat(),
                            "saldo_minutos": resultado["saldo_minutos"],
                            "umbral_minutos": UMBRAL_SOLICITUD_EXTRA_MINUTOS,
                            "horas_previas": resultado["tiempo_propuesto"],
                        },
                    )
                    resultado["estado"] = "cancelada"
                resultados.append(resultado)

        self.stdout.write(json.dumps(resultados, ensure_ascii=False))
