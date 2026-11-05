"""Mutaciones transaccionales y alcance propio de Empleado del mes CEDIS."""
import json
from datetime import date
from decimal import Decimal, InvalidOperation

from django.core.exceptions import PermissionDenied, ValidationError
from django.core.serializers.json import DjangoJSONEncoder
from django.db import IntegrityError, transaction
from django.db.models import Q
from django.utils import timezone

from core.access import ROLE_DG, can_manage_rrhh, has_any_role
from core.models import AuditLog
from .models import Empleado, EvaluacionEmpleadoMes, PeriodoEmpleadoMes
from .models_empleado_mes import CRITERIOS
from .services_catalogos import etiquetas_puestos, puesto_presentacion
from .services_identidad import empleado_vinculado_usuario

DEPARTAMENTOS = ("PRODUCCION", "ADMINISTRACION")


def _activo(user):
    return bool(user and user.is_authenticated and getattr(user, "is_active", False))


def puede_gestionar(user):
    return bool(_activo(user) and not user.is_superuser and not has_any_role(user, ROLE_DG) and can_manage_rrhh(user) and Empleado.objects.filter(usuario_erp=user, activo=True, departamento="RRHH", tipo_personal="POLLYANA").exists())


def puede_ver_todo(user):
    return bool(_activo(user) and (puede_gestionar(user) or user.is_superuser or has_any_role(user, ROLE_DG)))


def _jefatura(user):
    return Empleado.objects.filter(usuario_erp=user, activo=True, tipo_personal="POLLYANA", nivel_organizacional="JEFATURA", departamento__in=DEPARTAMENTOS).first() if _activo(user) else None


def puede_acceder(user):
    if not _activo(user):
        return False
    if puede_ver_todo(user):
        return True
    jefe = empleado_vinculado_usuario(user)
    es_jefe = jefe and jefe.activo and jefe.tipo_personal == "POLLYANA" and jefe.nivel_organizacional == "JEFATURA" and jefe.departamento in DEPARTAMENTOS
    tiene_equipo = bool(es_jefe and Empleado.objects.filter(jefe_directo=jefe, activo=True, tipo_personal="POLLYANA", departamento=jefe.departamento).exclude(nivel_organizacional__in=["JEFATURA", "DIRECCION"]).exists())
    return tiene_equipo or PeriodoEmpleadoMes.objects.filter(Q(evaluador_produccion=user) | Q(evaluador_administracion=user)).exists()


def periodos_visibles(user):
    qs = PeriodoEmpleadoMes.objects.all()
    if puede_ver_todo(user):
        return qs
    if not _activo(user):
        return qs.none()
    return qs.filter(Q(evaluador_produccion=user) | Q(evaluador_administracion=user))


def evaluaciones_visibles(user, periodo):
    qs = EvaluacionEmpleadoMes.objects.filter(periodo=periodo)
    if puede_ver_todo(user):
        return qs
    return qs.filter(evaluador=user) if _activo(user) and periodos_visibles(user).filter(pk=periodo.pk).exists() else qs.none()


def _equipo(user, periodo):
    jefe = _jefatura(user)
    if not jefe or puede_ver_todo(user) or getattr(periodo, 'evaluador_' + jefe.departamento.lower() + '_id') != user.pk:
        return Empleado.objects.none()
    return Empleado.objects.filter(activo=True, tipo_personal="POLLYANA", departamento=jefe.departamento, jefe_directo=jefe).exclude(pk=jefe.pk).exclude(usuario_erp=user).exclude(nivel_organizacional__in=["JEFATURA", "DIRECCION"])


def candidatos(user, periodo):
    equipo = _equipo(user, periodo)
    if periodo.evaluaciones.filter(evaluador_id=getattr(user, "pk", None)).count() >= 2:
        return equipo.none()
    return equipo.exclude(pk__in=periodo.evaluaciones.values("empleado_id"))


def _gestor(user):
    if not puede_gestionar(user):
        raise PermissionDenied("Solo Capital Humano puede realizar esta operación.")


def _snapshot(obj):
    return json.loads(json.dumps({f.attname: getattr(obj, f.attname) for f in obj._meta.concrete_fields}, cls=DjangoJSONEncoder))


def _guardar(user, obj, accion, antes=None, motivo=""):
    obj.full_clean()
    obj.save()
    AuditLog.objects.create(user=user, action=accion, model=obj._meta.label, object_id=str(obj.pk), payload={"antes": antes, "despues": _snapshot(obj), "motivo": motivo})
    return obj


def _version(obj, version):
    if type(version) is not int or obj.version != version:
        raise ValidationError("La versión cambió. Recarga antes de continuar.")


def _abierto(periodo):
    if periodo.ganador_id:
        raise ValidationError("El periodo ya tiene una decisión definitiva.")
    if timezone.localdate() > periodo.fecha_limite:
        raise ValidationError("El plazo terminó; Capital Humano debe ampliarlo.")


def _fecha(value):
    if type(value) is not date:
        raise ValidationError("La fecha no es válida.")


def _texto(value, nombre):
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{nombre} es obligatorio.")
    return value.strip()


@transaction.atomic
def abrir_periodo(user, mes, fecha_limite):
    _gestor(user)
    _fecha(mes)
    _fecha(fecha_limite)
    if mes.day != 1 or fecha_limite < mes or fecha_limite < timezone.localdate():
        raise ValidationError("Indica el primer día del mes y un plazo vigente posterior al inicio.")
    responsables = {}
    for dep in DEPARTAMENTOS:
        jefes = list(Empleado.objects.filter(activo=True, tipo_personal="POLLYANA", nivel_organizacional="JEFATURA", departamento=dep))
        if len(jefes) != 1 or not jefes[0].usuario_erp_id or not jefes[0].usuario_erp.is_active:
            raise ValidationError(f"{dep}: debe existir una única jefatura activa con cuenta activa vinculada.")
        responsables['evaluador_' + dep.lower()] = jefes[0].usuario_erp
    try:
        with transaction.atomic():
            return _guardar(user, PeriodoEmpleadoMes(mes=mes, fecha_limite=fecha_limite, creado_por=user, **responsables), "EM_MES_ABRIR")
    except IntegrityError as exc:
        if PeriodoEmpleadoMes.objects.filter(mes=mes).exists():
            raise ValidationError("Ya existe un periodo para este mes.") from exc
        raise


@transaction.atomic
def postular(user, periodo_id, empleado_id):
    periodo = PeriodoEmpleadoMes.objects.select_for_update().get(pk=periodo_id)
    _abierto(periodo)
    empleado = _equipo(user, periodo).filter(pk=empleado_id).first()
    if empleado is None:
        raise PermissionDenied("Solo puedes postular colaboradores activos de tu equipo autorizado.")
    if periodo.evaluaciones.filter(departamento_snapshot=empleado.departamento).count() >= 2:
        raise ValidationError("Ya existen dos postulaciones para este departamento.")
    return _guardar(user, EvaluacionEmpleadoMes(periodo=periodo, empleado=empleado, evaluador=user, nombre_snapshot=empleado.nombre, codigo_snapshot=empleado.codigo, puesto_snapshot=puesto_presentacion(empleado, etiquetas_puestos()), departamento_snapshot=empleado.departamento), "EM_MES_POSTULAR")


def puede_editar_evaluacion(user, evaluacion):
    return bool(_activo(user) and not puede_ver_todo(user) and evaluacion.evaluador_id == user.pk and evaluacion.empleado.usuario_erp_id != user.pk and evaluacion.estado == "borrador" and not evaluacion.periodo.ganador_id and evaluacion.periodo.fecha_limite >= timezone.localdate() and Empleado.objects.filter(usuario_erp=user, activo=True, tipo_personal="POLLYANA", departamento=evaluacion.departamento_snapshot).exists())


def _bloquear_evaluacion(evaluacion_id):
    periodo_id = EvaluacionEmpleadoMes.objects.values_list("periodo_id", flat=True).get(pk=evaluacion_id)
    periodo = PeriodoEmpleadoMes.objects.select_for_update().get(pk=periodo_id)
    evaluacion = EvaluacionEmpleadoMes.objects.select_for_update().get(pk=evaluacion_id)
    return periodo, evaluacion


@transaction.atomic
def guardar_evaluacion(user, evaluacion_id, *, calificaciones, comentarios, reconocimiento, reconocimiento_detalle, recomienda, justificacion, version, enviar=False):
    periodo, evaluacion = _bloquear_evaluacion(evaluacion_id)
    if not _activo(user) or puede_ver_todo(user) or evaluacion.evaluador_id != user.pk or evaluacion.empleado.usuario_erp_id == user.pk:
        raise PermissionDenied("Solo el evaluador asignado puede editar esta evaluación.")
    if not Empleado.objects.filter(usuario_erp=user, activo=True, tipo_personal="POLLYANA", departamento=evaluacion.departamento_snapshot).exists():
        raise PermissionDenied("El evaluador debe seguir activo en el departamento asignado.")
    _abierto(periodo)
    _version(evaluacion, version)
    if evaluacion.estado != "borrador":
        raise ValidationError("La evaluación enviada está cerrada.")
    if not isinstance(calificaciones, dict) or set(calificaciones) - set(CRITERIOS) or any(type(v) is not int or not 1 <= v <= 5 for v in calificaciones.values()):
        raise ValidationError("Las calificaciones deben ser enteros entre 1 y 5 de los criterios establecidos.")
    if not isinstance(comentarios, dict) or set(comentarios) - set(CRITERIOS) or any(not isinstance(v, str) for v in comentarios.values()):
        raise ValidationError("Los comentarios deben corresponder a los criterios establecidos.")
    if type(reconocimiento) is not bool or recomienda is not None and type(recomienda) is not bool:
        raise ValidationError("Reconocimiento y recomendación deben ser opciones válidas.")
    if not isinstance(reconocimiento_detalle, str) or not isinstance(justificacion, str):
        raise ValidationError("El detalle y la justificación deben ser texto.")
    antes = _snapshot(evaluacion)
    for key, value in dict(calificaciones=calificaciones, comentarios=comentarios, reconocimiento=reconocimiento, reconocimiento_detalle=reconocimiento_detalle.strip(), recomienda=recomienda, justificacion=justificacion.strip()).items():
        setattr(evaluacion, key, value)
    if enviar:
        if not evaluacion.completa or recomienda is None or not evaluacion.justificacion or reconocimiento and not evaluacion.reconocimiento_detalle:
            raise ValidationError("Completa los 14 criterios, recomendación, justificación y detalle del reconocimiento cuando corresponda.")
        evaluacion.estado = "enviada"
        evaluacion.enviada_en = timezone.now()
    evaluacion.version += 1
    return _guardar(user, evaluacion, "EM_MES_ENVIAR" if enviar else "EM_MES_GUARDAR", antes)


@transaction.atomic
def devolver(user, evaluacion_id, motivo, version):
    _gestor(user)
    motivo = _texto(motivo, "El motivo")
    periodo, evaluacion = _bloquear_evaluacion(evaluacion_id)
    _abierto(periodo)
    _version(evaluacion, version)
    if evaluacion.estado != "enviada":
        raise ValidationError("Solo se pueden devolver evaluaciones enviadas.")
    antes = _snapshot(evaluacion)
    evaluacion.estado = "borrador"
    evaluacion.enviada_en = None
    evaluacion.version += 1
    return _guardar(user, evaluacion, "EM_MES_DEVOLVER", antes, motivo)


@transaction.atomic
def ampliar_plazo(user, periodo_id, fecha_limite, motivo, version):
    _gestor(user)
    motivo = _texto(motivo, "El motivo")
    _fecha(fecha_limite)
    periodo = PeriodoEmpleadoMes.objects.select_for_update().get(pk=periodo_id)
    _version(periodo, version)
    if periodo.ganador_id or fecha_limite <= periodo.fecha_limite or fecha_limite < timezone.localdate():
        raise ValidationError("El nuevo plazo debe ampliar el anterior y estar vigente; no puede cambiarse una decisión cerrada.")
    antes = _snapshot(periodo)
    periodo.fecha_limite = fecha_limite
    periodo.version += 1
    return _guardar(user, periodo, "EM_MES_AMPLIAR", antes, motivo)


@transaction.atomic
def elegir(user, periodo_id, evaluacion_id, motivo, version):
    _gestor(user)
    motivo = _texto(motivo, "El fundamento de la decisión")
    periodo = PeriodoEmpleadoMes.objects.select_for_update().get(pk=periodo_id)
    _version(periodo, version)
    if periodo.ganador_id:
        raise ValidationError("La decisión ya está cerrada.")
    evaluaciones = list(periodo.evaluaciones.select_for_update())
    if len(evaluaciones) != 4 or any(sum(e.departamento_snapshot == dep for e in evaluaciones) != 2 for dep in DEPARTAMENTOS) or any(e.estado != "enviada" or not e.completa for e in evaluaciones):
        raise ValidationError("Se requieren dos evaluaciones completas y enviadas por cada departamento.")
    ganador = next((e for e in evaluaciones if e.pk == evaluacion_id), None)
    if ganador is None or ganador.recomienda is not True:
        raise ValidationError("Selecciona un candidato recomendado de este periodo.")
    antes = _snapshot(periodo)
    periodo.ganador = ganador
    periodo.motivo_decision = motivo
    periodo.elegido_por = user
    periodo.elegido_en = timezone.now()
    periodo.version += 1
    return _guardar(user, periodo, "EM_MES_ELEGIR", antes, motivo)


@transaction.atomic
def aprobar_premio(user, periodo_id, tipo, detalle, importe, version):
    _gestor(user)
    detalle = _texto(detalle, "El detalle del premio")
    periodo = PeriodoEmpleadoMes.objects.select_for_update().get(pk=periodo_id)
    _version(periodo, version)
    if not periodo.ganador_id or periodo.entregado_en:
        raise ValidationError("Requiere ganador y entrega pendiente.")
    if tipo not in ("efectivo", "vales", "descanso"):
        raise ValidationError("Tipo de premio no válido.")
    if importe in (None, ""):
        importe = None
    else:
        try:
            importe = Decimal(str(importe))
        except (InvalidOperation, ValueError):
            raise ValidationError("Importe no válido.")
        if not importe.is_finite() or importe <= 0 or importe.as_tuple().exponent < -2:
            raise ValidationError("El importe debe ser positivo con un máximo de dos decimales.")
    if tipo in ("efectivo", "vales") and importe is None:
        raise ValidationError("El premio requiere un importe positivo.")
    antes = _snapshot(periodo)
    periodo.premio_tipo, periodo.premio_detalle, periodo.premio_importe = tipo, detalle, importe
    periodo.premio_aprobado_por, periodo.premio_aprobado_en = user, timezone.now()
    periodo.version += 1
    return _guardar(user, periodo, "EM_MES_PREMIO", antes)


@transaction.atomic
def entregar(user, periodo_id, fecha, responsable, comprobante, version):
    _gestor(user)
    _fecha(fecha)
    responsable = _texto(responsable, "El responsable")
    if not isinstance(comprobante, str):
        raise ValidationError("El comprobante debe ser una referencia o acuse.")
    periodo = PeriodoEmpleadoMes.objects.select_for_update().get(pk=periodo_id)
    _version(periodo, version)
    if not periodo.premio_aprobado_en or periodo.entregado_en or fecha > timezone.localdate() or fecha < timezone.localdate(periodo.premio_aprobado_en):
        raise ValidationError("La entrega requiere premio aprobado, fecha válida no anterior a la aprobación y no puede repetirse.")
    antes = _snapshot(periodo)
    periodo.entrega_fecha, periodo.entrega_responsable, periodo.entrega_comprobante = fecha, responsable, comprobante.strip()
    periodo.entregado_por, periodo.entregado_en = user, timezone.now()
    periodo.version += 1
    return _guardar(user, periodo, "EM_MES_ENTREGAR", antes)
