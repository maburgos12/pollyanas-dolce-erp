from datetime import date, datetime, timedelta

from django.core.exceptions import ValidationError
from django.utils import timezone
from django.utils.crypto import salted_hmac

from .models import AsignacionJornadaEmpleado
from .services_turnos import ESTADO_DESCANSO, ESTADO_SIN_ASIGNACION, horario_programado_para_fecha
from .services_vacaciones import es_descanso_oficial


def calcular_permiso_repetido(empleado, inicio, fin):
    zona = timezone.get_default_timezone()
    if not fin:
        raise ValidationError("Selecciona la última fecha y la hora de fin.")
    inicio, fin = timezone.localtime(inicio, zona), timezone.localtime(fin, zona)
    dias = (fin.date() - inicio.date()).days
    if not 0 <= dias < 366 or fin.time() <= inicio.time():
        raise ValidationError("Selecciona hasta 366 fechas y una hora final posterior a la inicial.")
    fechas, descansos = [], []
    for offset in range(dias + 1):
        fecha = inicio.date() + timedelta(days=offset)
        if empleado.fecha_ingreso and fecha < empleado.fecha_ingreso:
            raise ValidationError(f"{fecha:%d/%m/%Y} es anterior al ingreso del colaborador.")
        if es_descanso_oficial(fecha):
            descansos.append(fecha.isoformat())
            continue
        try:
            horario = horario_programado_para_fecha(empleado, fecha)
        except ValidationError as exc:
            raise ValidationError(f"{fecha:%d/%m/%Y}: {exc.messages[0]}") from exc
        if horario.estado == ESTADO_SIN_ASIGNACION:
            raise ValidationError(f"Revisa la jornada del colaborador para el {fecha:%d/%m/%Y}.")
        if horario.estado == ESTADO_DESCANSO or (fecha.weekday() == 6 and not isinstance(horario.asignacion, AsignacionJornadaEmpleado)):
            descansos.append(fecha.isoformat())
        else:
            fechas.append(fecha.isoformat())
    if not fechas:
        raise ValidationError("El rango no contiene días laborables.")
    minutos = int((datetime.combine(inicio.date(), fin.time(), zona) - inicio).total_seconds() // 60)
    firma = salted_hmac("rrhh.permiso.repetido", repr((empleado.pk, inicio.isoformat(), fin.isoformat(), fechas))).hexdigest()
    return {
        "fechas_laborables": fechas, "descansos": descansos, "confirmacion": firma,
        "minutos_por_dia": minutos, "minutos_total": minutos * len(fechas),
    }


def ventana_permiso_repetido(permiso, fecha: date):
    if fecha.isoformat() not in permiso.fechas_laborables:
        return None
    zona = timezone.get_default_timezone()
    inicio = timezone.localtime(permiso.fecha_inicio, zona).time()
    fin = timezone.localtime(permiso.fecha_fin, zona).time()
    return datetime.combine(fecha, inicio, zona), datetime.combine(fecha, fin, zona)
