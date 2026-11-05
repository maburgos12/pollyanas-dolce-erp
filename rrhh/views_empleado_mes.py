"""Captura y revisión del empleado del mes, sin movimientos de nómina."""
from calendar import monthrange
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied, ValidationError
from django.db.models import Count, Q
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_http_methods
from core.models import AuditLog
from . import forms_empleado_mes as forms
from . import services_empleado_mes as service
from .models import AsistenciaEmpleado, IncidenciaAsistencia
from .models_empleado_mes import SECCIONES


def _acceso(user):
    if not service.puede_acceder(user):
        raise PermissionDenied


def _historial(modelo, pk):
    titulos = {"EM_MES_ABRIR": "Periodo abierto", "EM_MES_POSTULAR": "Candidato postulado", "EM_MES_GUARDAR": "Borrador guardado", "EM_MES_ENVIAR": "Evaluación enviada", "EM_MES_DEVOLVER": "Devuelta para corrección", "EM_MES_AMPLIAR": "Plazo ampliado", "EM_MES_ELEGIR": "Ganador seleccionado", "EM_MES_PREMIO": "Premio aprobado", "EM_MES_ENTREGAR": "Entrega registrada"}
    eventos = list(AuditLog.objects.filter(model=modelo, object_id=str(pk)).select_related("user"))
    for evento in eventos:
        evento.titulo = titulos.get(evento.action, evento.action)
        datos = evento.payload.get("despues") or {}
        notas = datos.get("calificaciones", {})
        evento.notas = [(label, notas[key], datos.get("comentarios", {}).get(key, "")) for _, criterios in SECCIONES for key, label in criterios if key in notas]
    return eventos


def _error(form, error):
    form.add_error(None, " ".join(error.messages))


@login_required
@never_cache
@require_http_methods(["GET", "POST"])
def periodos(request):
    _acceso(request.user)
    gestiona = service.puede_gestionar(request.user)
    form = forms.PeriodoForm(request.POST if request.method == "POST" else None)
    if request.method == "POST":
        if not gestiona:
            raise PermissionDenied
        if form.is_valid():
            try:
                p = service.abrir_periodo(request.user, **form.cleaned_data)
            except ValidationError as exc:
                _error(form, exc)
            else:
                messages.success(request, "Periodo abierto. Las jefaturas ya pueden postular a sus equipos.")
                return redirect("rrhh:empleado_mes_detalle", pk=p.pk)
    return render(request, "rrhh/empleado_mes/periodos.html", {"periodos": service.periodos_visibles(request.user).order_by("-mes"), "form": form, "gestiona": gestiona})


@login_required
@never_cache
@require_http_methods(["GET", "POST"])
def detalle(request, pk):
    _acceso(request.user)
    periodo = get_object_or_404(service.periodos_visibles(request.user), pk=pk)
    qs = service.evaluaciones_visibles(request.user, periodo).select_related("evaluador", "empleado").order_by("departamento_snapshot", "nombre_snapshot")
    gestiona = service.puede_gestionar(request.user)
    opciones = service.candidatos(request.user, periodo)
    initial = {"version": periodo.version}
    forms_by_action = {
        "postular": forms.PostulacionForm(candidatos=opciones),
        "elegir": forms.EleccionForm(evaluaciones=qs, initial=initial),
        "premio": forms.PremioForm(initial={**initial, "tipo": periodo.premio_tipo, "detalle": periodo.premio_detalle, "importe": periodo.premio_importe}),
        "entregar": forms.EntregaForm(initial={**initial, "fecha": timezone.localdate()}),
        "plazo": forms.PlazoForm(initial={**initial, "fecha_limite": periodo.fecha_limite}),
    }
    for action_name, action_form in forms_by_action.items():
        action_form.auto_id = f"id_{action_name}_%s"
    if request.method == "POST":
        action = request.POST.get("accion")
        if action not in forms_by_action:
            raise PermissionDenied
        if action != "postular" and not gestiona:
            raise PermissionDenied
        constructors = {
            "postular": lambda: forms.PostulacionForm(request.POST, candidatos=opciones),
            "elegir": lambda: forms.EleccionForm(request.POST, evaluaciones=qs),
            "premio": lambda: forms.PremioForm(request.POST),
            "entregar": lambda: forms.EntregaForm(request.POST),
            "plazo": lambda: forms.PlazoForm(request.POST),
        }
        form = forms_by_action[action] = constructors[action]()
        form.auto_id = f"id_{action}_%s"
        if form.is_valid():
            d = form.cleaned_data
            try:
                if action == "postular":
                    e = service.postular(request.user, periodo.pk, d["empleado"].pk)
                    return redirect("rrhh:empleado_mes_evaluacion", pk=e.pk)
                if action == "elegir":
                    service.elegir(request.user, periodo.pk, d["evaluacion"].pk, d["motivo"], d["version"])
                elif action == "premio":
                    service.aprobar_premio(request.user, periodo.pk, **d)
                elif action == "entregar":
                    service.entregar(request.user, periodo.pk, **d)
                elif action == "plazo":
                    service.ampliar_plazo(request.user, periodo.pk, **d)
            except ValidationError as exc:
                _error(form, exc)
            else:
                messages.success(request, "Cambio registrado en el historial.")
                return redirect("rrhh:empleado_mes_detalle", pk=periodo.pk)
    evaluaciones = list(qs)
    grupos = []
    for dep, nombre in [("PRODUCCION", "Producción"), ("ADMINISTRACION", "Administración")]:
        filas = [e for e in evaluaciones if e.departamento_snapshot == dep]
        if service.puede_ver_todo(request.user) or filas or request.user.pk == getattr(periodo, "evaluador_produccion_id" if dep == "PRODUCCION" else "evaluador_administracion_id"):
            grupos.append({"nombre": nombre, "filas": filas, "enviadas": sum(e.estado == "enviada" for e in filas)})
    ganador = periodo.ganador if service.puede_ver_todo(request.user) or any(e.pk == periodo.ganador_id for e in evaluaciones) else None
    historial = _historial("rrhh.PeriodoEmpleadoMes", periodo.pk) if service.puede_ver_todo(request.user) else []
    return render(request, "rrhh/empleado_mes/detalle.html", {"periodo": periodo, "grupos": grupos, "forms": forms_by_action, "gestiona": gestiona, "ganador": ganador, "ver_todo": service.puede_ver_todo(request.user), "puede_postular": opciones.exists() and not periodo.ganador_id and not periodo.plazo_vencido, "historial": historial, "listo": len(evaluaciones) == 4 and all(e.estado == "enviada" for e in evaluaciones), "vencido": periodo.plazo_vencido, "fin_mes": periodo.mes.replace(day=monthrange(periodo.mes.year, periodo.mes.month)[1])})


@login_required
@never_cache
@require_http_methods(["GET", "POST"])
def evaluacion(request, pk):
    _acceso(request.user)
    from .models import EvaluacionEmpleadoMes
    candidato = get_object_or_404(EvaluacionEmpleadoMes.objects.select_related("periodo"), pk=pk)
    e = get_object_or_404(service.evaluaciones_visibles(request.user, candidato.periodo).select_related("periodo", "evaluador"), pk=pk)
    editable = service.puede_editar_evaluacion(request.user, e)
    form = forms.EvaluacionForm(evaluacion=e)
    devolver_form = forms.MotivoForm(initial={"version": e.version})
    if request.method == "POST":
        action = request.POST.get("accion")
        if action == "devolver":
            if not service.puede_gestionar(request.user):
                raise PermissionDenied
            devolver_form = forms.MotivoForm(request.POST)
            if devolver_form.is_valid():
                try:
                    service.devolver(request.user, e.pk, **devolver_form.cleaned_data)
                except ValidationError as exc:
                    _error(devolver_form, exc)
                else:
                    messages.success(request, "Evaluación devuelta. Si venció el plazo, amplíalo para permitir la corrección.")
                    return redirect("rrhh:empleado_mes_evaluacion", pk=e.pk)
        elif action in {"guardar", "enviar"}:
            if e.evaluador_id != request.user.pk or service.puede_ver_todo(request.user):
                raise PermissionDenied
            form = forms.EvaluacionForm(request.POST, evaluacion=e)
            if form.is_valid():
                try:
                    service.guardar_evaluacion(request.user, e.pk, **form.datos(), enviar=action == "enviar")
                except ValidationError as exc:
                    _error(form, exc)
                else:
                    messages.success(request, "Evaluación enviada a RRHH." if action == "enviar" else "Borrador guardado.")
                    return redirect("rrhh:empleado_mes_evaluacion", pk=e.pk)
        else:
            raise PermissionDenied
    if form.is_bound and not editable:
        for field in form.fields.values():
            field.widget.attrs["disabled"] = True
    inicio = e.periodo.mes
    fin = inicio.replace(day=monthrange(inicio.year, inicio.month)[1])
    asistencias = AsistenciaEmpleado.objects.filter(empleado_id=e.empleado_id, fecha__range=(inicio, fin))
    evidencia = asistencias.aggregate(registros=Count("id"), sin_salida=Count("id", filter=Q(salida=None)))
    evidencia["incidencias"] = list(IncidenciaAsistencia.objects.filter(empleado_id=e.empleado_id, fecha__range=(inicio, fin)).values("estado").annotate(total=Count("id")).order_by("estado"))
    secciones = [(nombre, [{"label": label, "nota": e.calificaciones.get(key), "comentario": e.comentarios.get(key, "")} for key, label in criterios]) for nombre, criterios in SECCIONES]
    historial = _historial("rrhh.EvaluacionEmpleadoMes", e.pk)
    return render(request, "rrhh/empleado_mes/evaluacion.html", {"evaluacion": e, "periodo": e.periodo, "form": form, "editable": editable, "mostrar_formulario": editable or form.is_bound, "devolver_form": devolver_form, "puede_devolver": service.puede_gestionar(request.user) and e.estado == "enviada" and not e.periodo.ganador_id, "secciones": secciones, "evidencia": evidencia, "historial": historial})
