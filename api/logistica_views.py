from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from django.core.exceptions import PermissionDenied, ValidationError
from django.db import IntegrityError, transaction
from django.db.models import Count, Max, Q, Sum
from django.shortcuts import get_object_or_404
from django.utils import timezone
from rest_framework import generics, status
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from rest_framework.parsers import FormParser, JSONParser, MultiPartParser
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.views import TokenObtainPairView

from core.access import can_manage_logistica, can_manage_submodule, can_view_logistica, can_view_module, can_view_submodule
from core.audit import log_event
from crm.models import PedidoCliente
from logistica.models import (
    BitacoraRepartidor,
    BitacoraSalidaLlegada,
    CargaCombustibleUnidad,
    EntregaRuta,
    EventoRuta,
    InspeccionDiaria,
    InspeccionVehiculo,
    LavadoUnidad,
    ParadaRuta,
    Repartidor,
    ReporteUnidad,
    ReporteUnidadReafirmacion,
    RutaEntrega,
    Unidad,
)
from logistica.services_rutas_control import registrar_ubicacion_ruta, resumen_control_rutas
from rrhh.services_identidad import nombre_operativo_usuario

from .logistica_serializers import (
    LogisticaBitacoraSerializer,
    LogisticaBitacoraLlegadaSerializer,
    LogisticaBitacoraSalidaCreateSerializer,
    LogisticaBitacoraSalidaLlegadaSerializer,
    LogisticaCargaCombustibleCreateSerializer,
    LogisticaCargaCombustibleSerializer,
    LogisticaEntregaCreateSerializer,
    LogisticaEntregaSerializer,
    EventoRutaCreateSerializer,
    EventoRutaSerializer,
    LogisticaInspeccionVehiculoCreateSerializer,
    LogisticaInspeccionVehiculoSerializer,
    LogisticaInspeccionDiariaSerializer,
    LogisticaLavadoUnidadCreateSerializer,
    LogisticaLavadoUnidadSerializer,
    LogisticaRepartidorSerializer,
    LogisticaReporteCreateSerializer,
    LogisticaReportePatchSerializer,
    LogisticaReporteReafirmacionSerializer,
    LogisticaReporteSerializer,
    LogisticaRutaSerializer,
    LogisticaUnidadSerializer,
    ParadaRutaSerializer,
    UbicacionRutaCreateSerializer,
    UbicacionRutaSerializer,
)


LOGISTICA_ROLE_REPARTIDOR = "repartidor"
LOGISTICA_ROLE_COMPRAS = "compras_logistica"
LOGISTICA_ROLE_SUPERVISOR = "supervisor_logistica"


class _LogisticaBaseView(APIView):
    authentication_classes = [JWTAuthentication, TokenAuthentication, SessionAuthentication]
    permission_classes = [IsAuthenticated]

    @staticmethod
    def _bounded_int(value, *, default: int, min_value: int, max_value: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = default
        return max(min_value, min(parsed, max_value))


def _has_group(user, group_name: str) -> bool:
    if not user or not user.is_authenticated:
        return False
    target = group_name.lower()
    return user.is_superuser or user.groups.filter(name__iexact=target).exists()


def _is_repartidor(user) -> bool:
    return _has_group(user, LOGISTICA_ROLE_REPARTIDOR)


def _can_operate_pwa(user) -> bool:
    return bool(_get_repartidor_for_user(user)) or _is_repartidor(user) or can_view_module(user, "mantenimiento")


def _is_compras_logistica(user) -> bool:
    return _has_group(user, LOGISTICA_ROLE_COMPRAS)


def _is_supervisor_logistica(user) -> bool:
    return _has_group(user, LOGISTICA_ROLE_SUPERVISOR)


def _can_view_all_reportes(user) -> bool:
    return _is_compras_logistica(user) or _is_supervisor_logistica(user)


def _get_repartidor_for_user(user) -> Repartidor | None:
    try:
        return user.repartidor_logistica
    except Repartidor.DoesNotExist:
        return None


def _serializer_error_message(errors) -> str:
    for field, messages in errors.items():
        if isinstance(messages, (list, tuple)) and messages:
            return f"{field}: {messages[0]}"
        if isinstance(messages, dict):
            nested = _serializer_error_message(messages)
            if nested:
                return f"{field}: {nested}"
        return f"{field}: {messages}"
    return "Datos inválidos."


def _licencia_turno_bloqueo(repartidor: Repartidor) -> dict | None:
    if not repartidor.licencia_expiracion:
        return {
            "error": "licencia_no_vigente",
            "estado": "sin_datos",
            "mensaje": "No puedes iniciar turno porque tu licencia no está registrada.",
        }
    dias = (repartidor.licencia_expiracion - timezone.localdate()).days
    if dias < 0:
        return {
            "error": "licencia_no_vigente",
            "estado": "vencida",
            "mensaje": "No puedes iniciar turno porque tu licencia está vencida.",
            "licencia_expiracion": repartidor.licencia_expiracion.isoformat(),
        }
    return None


def _gas_rank(value: str | None) -> int | None:
    return {"vacio": 0, "1/4": 1, "1/2": 2, "3/4": 3, "lleno": 4}.get(value or "")


def _reportes_with_reafirmaciones(qs):
    return qs.annotate(
        reafirmaciones_count=Count("reafirmaciones", distinct=True),
        ultima_reafirmacion=Max("reafirmaciones__creado_en"),
    )


class LogisticaTokenView(TokenObtainPairView):
    parser_classes = [JSONParser, FormParser]


class LogisticaSessionTokenView(_LogisticaBaseView):
    authentication_classes = [SessionAuthentication]

    def get(self, request):
        user = request.user
        if not (
            user.is_superuser
            or _is_repartidor(user)
            or _get_repartidor_for_user(user)
            or _is_compras_logistica(user)
            or _is_supervisor_logistica(user)
            or can_view_logistica(user)
        ):
            return Response({"detail": "No tienes acceso a logística."}, status=status.HTTP_403_FORBIDDEN)

        refresh = RefreshToken.for_user(user)
        return Response({"access": str(refresh.access_token), "refresh": str(refresh)}, status=status.HTTP_200_OK)


class LogisticaMiPerfilView(_LogisticaBaseView):
    def get(self, request):
        repartidor = _get_repartidor_for_user(request.user)
        data = {
            "user": {
                "id": request.user.id,
                "username": request.user.username,
                "nombre": nombre_operativo_usuario(request.user),
                "email": request.user.email,
            },
            "roles": list(request.user.groups.values_list("name", flat=True)),
            "repartidor": LogisticaRepartidorSerializer(repartidor).data if repartidor else None,
            "ultimos_servicios": [],
        }
        if repartidor and repartidor.unidad_asignada_id:
            servicios = _reportes_with_reafirmaciones(ReporteUnidad.objects.filter(unidad=repartidor.unidad_asignada)).exclude(
                estatus=ReporteUnidad.ESTATUS_ABIERTO
            )[:10]
            data["ultimos_servicios"] = LogisticaReporteSerializer(servicios, many=True).data
        return Response(data, status=status.HTTP_200_OK)


class LogisticaResumenSemanalView(_LogisticaBaseView):
    def get(self, request):
        repartidor = _get_repartidor_for_user(request.user)
        if not repartidor:
            return Response({"detail": "No tienes perfil de repartidor registrado."}, status=status.HTTP_404_NOT_FOUND)

        today = timezone.localdate()
        current_week_start = today - timedelta(days=today.weekday())
        start = current_week_start - timedelta(days=7)
        end = current_week_start
        bitacoras = BitacoraSalidaLlegada.objects.filter(repartidor=repartidor, hora_salida__date__gte=start, hora_salida__date__lt=end)
        inspecciones = InspeccionDiaria.objects.filter(repartidor=repartidor, fecha__gte=start, fecha__lt=end)
        reportes = ReporteUnidad.objects.filter(repartidor=repartidor, fecha_reporte__date__gte=start, fecha_reporte__date__lt=end)
        reafirmaciones = ReporteUnidadReafirmacion.objects.filter(repartidor=repartidor, creado_en__date__gte=start, creado_en__date__lt=end)

        turnos_total = bitacoras.count()
        turnos_cerrados = bitacoras.filter(cerrada=True).count()
        turnos_abiertos = bitacoras.filter(cerrada=False).count()
        fotos_salida = bitacoras.exclude(foto_tablero_salida="").count()
        fotos_llegada = bitacoras.exclude(foto_tablero_llegada="").count()
        inspecciones_total = inspecciones.count()
        inspecciones_fallas = inspecciones.filter(tiene_fallas=True).count()
        reportes_total = reportes.count()
        reafirmaciones_total = reafirmaciones.count()

        if turnos_abiertos:
            semaforo = "rojo"
            mensaje = "Tienes turnos sin cerrar de la semana pasada. Regulariza el cierre antes de iniciar nuevos turnos."
        elif turnos_total and turnos_cerrados == turnos_total and inspecciones_total >= turnos_total:
            semaforo = "verde"
            mensaje = "Buen cierre semanal: turnos cerrados e inspecciones diarias registradas."
        else:
            semaforo = "amarillo"
            mensaje = "Semana con datos incompletos. Revisa que cada turno tenga inspección diaria y cierre completo."

        return Response(
            {
                "periodo_inicio": start.isoformat(),
                "periodo_fin": (end - timedelta(days=1)).isoformat(),
                "semaforo": semaforo,
                "mensaje": mensaje,
                "metricas": {
                    "turnos_total": turnos_total,
                    "turnos_cerrados": turnos_cerrados,
                    "turnos_abiertos": turnos_abiertos,
                    "fotos_salida": fotos_salida,
                    "fotos_llegada": fotos_llegada,
                    "inspecciones_diarias": inspecciones_total,
                    "inspecciones_con_fallas": inspecciones_fallas,
                    "reportes_creados": reportes_total,
                    "reportes_reafirmados": reafirmaciones_total,
                },
            },
            status=status.HTTP_200_OK,
        )


class LogisticaCombustibleAlertaView(_LogisticaBaseView):
    def get(self, request):
        repartidor = _get_repartidor_for_user(request.user)
        if not repartidor:
            return Response({"detail": "No tienes perfil de repartidor registrado."}, status=status.HTTP_404_NOT_FOUND)

        pendientes = []
        bitacoras = (
            BitacoraSalidaLlegada.objects.select_related("unidad")
            .prefetch_related("cargas_combustible")
            .filter(repartidor=repartidor, cerrada=True)
            .exclude(nivel_gas_llegada="")
            .order_by("-hora_llegada", "-id")
        )
        for bitacora in bitacoras:
            gas_salida = _gas_rank(bitacora.nivel_gas_salida)
            gas_llegada = _gas_rank(bitacora.nivel_gas_llegada)
            if gas_salida is None or gas_llegada is None or gas_llegada <= gas_salida:
                continue
            tiene_combustible = any(
                [
                    bitacora.litros_cargados is not None,
                    bitacora.costo_combustible is not None,
                    bool(bitacora.foto_ticket_combustible),
                    bitacora.cargas_combustible.exists(),
                ]
            )
            if tiene_combustible:
                continue
            pendientes.append(
                {
                    "id": bitacora.id,
                    "folio": bitacora.folio,
                    "unidad_codigo": bitacora.unidad.codigo,
                    "fecha": timezone.localtime(bitacora.hora_llegada or bitacora.hora_salida).date().isoformat()
                    if bitacora.hora_llegada or bitacora.hora_salida
                    else bitacora.fecha.isoformat(),
                    "nivel_gas_salida": bitacora.nivel_gas_salida,
                    "nivel_gas_llegada": bitacora.nivel_gas_llegada,
                }
            )

        if pendientes:
            return Response(
                {
                    "tipo": "pendiente",
                    "pendiente": True,
                    "pendientes_count": len(pendientes),
                    "pendientes": pendientes[:5],
                    "mensaje": "No registraste carga de combustible.",
                },
                status=status.HTTP_200_OK,
            )

        return Response(
            {
                "tipo": "recordatorio",
                "pendiente": False,
                "pendientes_count": 0,
                "pendientes": [],
                "mensaje": "Recuerda registrar la gasolina si cargas combustible durante tu turno.",
            },
            status=status.HTTP_200_OK,
        )


class LogisticaReporteCreateView(_LogisticaBaseView):
    parser_classes = [MultiPartParser, FormParser, JSONParser]

    def post(self, request):
        repartidor = _get_repartidor_for_user(request.user)
        if not repartidor or not _can_operate_pwa(request.user):
            return Response({"detail": "Solo repartidores registrados pueden levantar reportes."}, status=status.HTTP_403_FORBIDDEN)

        serializer = LogisticaReporteCreateSerializer(data=request.data, context={"repartidor": repartidor})
        serializer.is_valid(raise_exception=True)
        reporte = serializer.save(ip_reporte=request.META.get("REMOTE_ADDR"))
        log_event(
            request.user,
            "CREATE",
            "logistica.ReporteUnidad",
            str(reporte.id),
            {"unidad": reporte.unidad.codigo, "tipo": reporte.tipo, "severidad": reporte.severidad},
        )
        return Response(LogisticaReporteSerializer(reporte).data, status=status.HTTP_201_CREATED)


class LogisticaReportesUnidadAbiertosView(_LogisticaBaseView):
    def get(self, request):
        repartidor = _get_repartidor_for_user(request.user)
        if not repartidor:
            return Response({"detail": "No tienes perfil de repartidor registrado."}, status=status.HTTP_404_NOT_FOUND)
        unidad_id = request.query_params.get("unidad_id")
        if not unidad_id:
            return Response({"detail": "unidad_id es obligatorio."}, status=status.HTTP_400_BAD_REQUEST)
        reportes = _reportes_with_reafirmaciones(
            ReporteUnidad.objects.filter(unidad_id=unidad_id)
            .exclude(estatus=ReporteUnidad.ESTATUS_CERRADO)
            .select_related("unidad", "repartidor__user", "repartidor__user__empleado_rrhh", "asignado_a")
        )
        tipo = (request.query_params.get("tipo") or "").strip()
        if tipo:
            reportes = reportes.filter(tipo=tipo)
        return Response(LogisticaReporteSerializer(reportes[:20], many=True).data, status=status.HTTP_200_OK)


class LogisticaReporteReafirmarView(_LogisticaBaseView):
    parser_classes = [MultiPartParser, FormParser, JSONParser]

    def post(self, request, reporte_id: int):
        repartidor = _get_repartidor_for_user(request.user)
        if not repartidor or not _can_operate_pwa(request.user):
            return Response({"detail": "Solo repartidores registrados pueden reafirmar reportes."}, status=status.HTTP_403_FORBIDDEN)
        reporte = get_object_or_404(
            ReporteUnidad.objects.select_related(
                "unidad",
                "repartidor__user",
                "repartidor__user__empleado_rrhh",
                "asignado_a",
            ).exclude(estatus=ReporteUnidad.ESTATUS_CERRADO),
            pk=reporte_id,
        )
        reafirmacion = ReporteUnidadReafirmacion.objects.create(
            reporte=reporte,
            repartidor=repartidor,
            comentario=(request.data.get("comentario") or "").strip(),
            latitud=request.data.get("latitud") or None,
            longitud=request.data.get("longitud") or None,
            ip_registro=request.META.get("REMOTE_ADDR"),
        )
        log_event(
            request.user,
            "CREATE",
            "logistica.ReporteUnidadReafirmacion",
            str(reafirmacion.id),
            {"reporte": reporte.id, "unidad": reporte.unidad.codigo},
        )
        reporte = _reportes_with_reafirmaciones(
            ReporteUnidad.objects.filter(pk=reporte.pk).select_related(
                "unidad",
                "repartidor__user",
                "repartidor__user__empleado_rrhh",
                "asignado_a",
            )
        ).first()
        return Response(
            {
                "reafirmacion": LogisticaReporteReafirmacionSerializer(reafirmacion).data,
                "reporte": LogisticaReporteSerializer(reporte).data,
                "mensaje": f"Reporte reafirmado. Ya van {reporte.reafirmaciones_count} avisos sobre este ticket.",
            },
            status=status.HTTP_201_CREATED,
        )


class LogisticaMisReportesView(_LogisticaBaseView):
    def get(self, request):
        repartidor = _get_repartidor_for_user(request.user)
        if not repartidor:
            return Response({"detail": "No tienes perfil de repartidor registrado."}, status=status.HTTP_404_NOT_FOUND)
        reportes = _reportes_with_reafirmaciones(
            ReporteUnidad.objects.filter(repartidor=repartidor).select_related(
                "unidad",
                "repartidor__user",
                "repartidor__user__empleado_rrhh",
                "asignado_a",
            )
        )
        return Response(LogisticaReporteSerializer(reportes, many=True).data, status=status.HTTP_200_OK)


class LogisticaTodosReportesView(_LogisticaBaseView):
    def get(self, request):
        if not _can_view_all_reportes(request.user):
            return Response({"detail": "No tienes permisos para consultar todos los reportes."}, status=status.HTTP_403_FORBIDDEN)

        qs = _reportes_with_reafirmaciones(
            ReporteUnidad.objects.select_related("unidad", "repartidor__user", "repartidor__user__empleado_rrhh", "asignado_a")
        )
        estatus_filtro = (request.query_params.get("estatus") or "").strip()
        severidad = (request.query_params.get("severidad") or "").strip()
        if estatus_filtro:
            qs = qs.filter(estatus=estatus_filtro)
        if severidad:
            qs = qs.filter(severidad=severidad)
        return Response(LogisticaReporteSerializer(qs, many=True).data, status=status.HTTP_200_OK)


class LogisticaReporteDetailView(_LogisticaBaseView):
    parser_classes = [MultiPartParser, FormParser, JSONParser]

    def patch(self, request, reporte_id: int):
        reporte = get_object_or_404(
            ReporteUnidad.objects.select_related("repartidor__user", "repartidor__user__empleado_rrhh", "unidad", "asignado_a"),
            pk=reporte_id,
        )
        repartidor = _get_repartidor_for_user(request.user)
        is_owner = repartidor and reporte.repartidor_id == repartidor.id
        is_compras = _is_compras_logistica(request.user)
        is_supervisor = _is_supervisor_logistica(request.user)
        if not (is_owner or is_compras or is_supervisor):
            return Response({"detail": "No tienes permisos para actualizar este reporte."}, status=status.HTTP_403_FORBIDDEN)
        if request.data.get("estatus") == ReporteUnidad.ESTATUS_CERRADO and not is_supervisor:
            return Response({"detail": "Solo supervisor_logistica puede cerrar reportes."}, status=status.HTTP_403_FORBIDDEN)

        mutable_for_owner = {"descripcion", "foto", "kilometraje", "latitud", "longitud"}
        if is_owner and not (is_compras or is_supervisor):
            forbidden = set(request.data.keys()) - mutable_for_owner
            if forbidden:
                return Response({"detail": "El repartidor solo puede actualizar evidencia y datos del reporte."}, status=status.HTTP_403_FORBIDDEN)

        serializer = LogisticaReportePatchSerializer(reporte, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        reporte = serializer.save()
        log_event(
            request.user,
            "UPDATE",
            "logistica.ReporteUnidad",
            str(reporte.id),
            {"estatus": reporte.estatus, "severidad": reporte.severidad},
        )
        return Response(LogisticaReporteSerializer(reporte).data, status=status.HTTP_200_OK)


class LogisticaBitacoraView(_LogisticaBaseView):
    def get(self, request):
        repartidor = _get_repartidor_for_user(request.user)
        if _can_view_all_reportes(request.user):
            qs = BitacoraRepartidor.objects.select_related("repartidor__user", "repartidor__user__empleado_rrhh").all()
            repartidor_id = request.query_params.get("repartidor")
            fecha = request.query_params.get("fecha")
            if repartidor_id:
                qs = qs.filter(repartidor_id=repartidor_id)
            if fecha:
                qs = qs.filter(fecha=fecha)
        elif repartidor:
            qs = BitacoraRepartidor.objects.select_related("repartidor__user", "repartidor__user__empleado_rrhh").filter(repartidor=repartidor)
        else:
            return Response({"detail": "No tienes perfil de repartidor registrado."}, status=status.HTTP_404_NOT_FOUND)
        return Response(LogisticaBitacoraSerializer(qs, many=True).data, status=status.HTTP_200_OK)

    def post(self, request):
        repartidor = _get_repartidor_for_user(request.user)
        if not repartidor or not _can_operate_pwa(request.user):
            return Response({"detail": "Solo repartidores registrados pueden capturar bitácora."}, status=status.HTTP_403_FORBIDDEN)

        serializer = LogisticaBitacoraSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        payload = serializer.validated_data
        bitacora, _ = BitacoraRepartidor.objects.update_or_create(
            repartidor=repartidor,
            fecha=payload.get("fecha"),
            defaults={
                "km_inicio": payload["km_inicio"],
                "km_fin": payload.get("km_fin"),
                "novedades": payload.get("novedades") or "",
            },
        )
        return Response(LogisticaBitacoraSerializer(bitacora).data, status=status.HTTP_201_CREATED)


class LogisticaUnidadesView(_LogisticaBaseView):
    def get(self, request):
        unidades = Unidad.objects.filter(activa=True).select_related("sucursal").order_by("codigo")
        return Response(LogisticaUnidadSerializer(unidades, many=True).data, status=status.HTTP_200_OK)


class LogisticaBitacoraSalidaView(_LogisticaBaseView):
    parser_classes = [MultiPartParser, FormParser, JSONParser]

    def post(self, request):
        repartidor = _get_repartidor_for_user(request.user)
        if not repartidor or not _can_operate_pwa(request.user):
            return Response({"detail": "Solo repartidores registrados pueden iniciar bitácora."}, status=status.HTTP_403_FORBIDDEN)

        licencia_bloqueo = _licencia_turno_bloqueo(repartidor)
        if licencia_bloqueo:
            return Response(licencia_bloqueo, status=status.HTTP_403_FORBIDDEN)

        abierta = BitacoraSalidaLlegada.objects.select_related("unidad").filter(repartidor=repartidor, cerrada=False).first()
        if abierta:
            return Response(
                {
                    "error": "turno_abierto",
                    "mensaje": f"Tienes un turno abierto en la unidad {abierta.unidad.codigo}. Debes cerrarlo antes de iniciar uno nuevo.",
                    "bitacora_id": abierta.id,
                    "bitacora": LogisticaBitacoraSalidaLlegadaSerializer(abierta).data,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )

        serializer = LogisticaBitacoraSalidaCreateSerializer(data=request.data, context={"repartidor": repartidor})
        if not serializer.is_valid():
            return Response(
                {
                    "error": "validacion",
                    "mensaje": _serializer_error_message(serializer.errors),
                    "detalles": serializer.errors,
                },
                status=status.HTTP_400_BAD_REQUEST,
            )
        bitacora = serializer.save(ip_registro=request.META.get("REMOTE_ADDR"))
        return Response(LogisticaBitacoraSalidaLlegadaSerializer(bitacora).data, status=status.HTTP_201_CREATED)


class LogisticaCargaCombustibleView(_LogisticaBaseView):
    parser_classes = [MultiPartParser, FormParser, JSONParser]

    def post(self, request):
        repartidor = _get_repartidor_for_user(request.user)
        if not repartidor or not _can_operate_pwa(request.user):
            return Response({"detail": "Solo repartidores registrados pueden capturar combustible."}, status=status.HTTP_403_FORBIDDEN)

        bitacora = (
            BitacoraSalidaLlegada.objects.select_related("unidad")
            .filter(repartidor=repartidor, cerrada=False)
            .first()
        )
        if not bitacora:
            return Response({"detail": "Necesitas un turno abierto para registrar gasolina."}, status=status.HTTP_400_BAD_REQUEST)

        serializer = LogisticaCargaCombustibleCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        carga = CargaCombustibleUnidad.objects.create(
            bitacora=bitacora,
            unidad=bitacora.unidad,
            repartidor=repartidor,
            ip_registro=request.META.get("REMOTE_ADDR"),
            **serializer.validated_data,
        )
        log_event(
            request.user,
            "CREATE",
            "logistica.CargaCombustibleUnidad",
            str(carga.id),
            {
                "bitacora": bitacora.folio,
                "unidad": bitacora.unidad.codigo,
                "litros": str(carga.litros),
                "importe_total": str(carga.importe_total),
            },
        )
        return Response(LogisticaCargaCombustibleSerializer(carga).data, status=status.HTTP_201_CREATED)


class LogisticaLavadoEstadoView(_LogisticaBaseView):
    def get(self, request):
        repartidor = _get_repartidor_for_user(request.user)
        if not repartidor:
            return Response({"detail": "No tienes perfil de repartidor registrado."}, status=status.HTTP_404_NOT_FOUND)

        unidad_id = request.query_params.get("unidad_id")
        if not unidad_id:
            return Response({"detail": "unidad_id es obligatorio."}, status=status.HTTP_400_BAD_REQUEST)

        unidad = get_object_or_404(Unidad.objects.filter(activa=True), pk=unidad_id)
        hoy = timezone.localdate()
        ultimo_lavado = (
            LavadoUnidad.objects.select_related("unidad", "registrado_por", "registrado_por__empleado_rrhh")
            .filter(unidad=unidad)
            .order_by("-fecha", "-fecha_registro", "-id")
            .first()
        )
        lavado_hoy = LavadoUnidad.objects.filter(unidad=unidad, fecha=hoy).exists()
        dias_sin_lavar = (hoy - ultimo_lavado.fecha).days if ultimo_lavado else None

        return Response(
            {
                "unidad": unidad.id,
                "unidad_codigo": unidad.codigo,
                "unidad_descripcion": unidad.descripcion,
                "ultimo_lavado": LogisticaLavadoUnidadSerializer(ultimo_lavado).data if ultimo_lavado else None,
                "lavado_hoy": lavado_hoy,
                "dias_sin_lavar": dias_sin_lavar,
            },
            status=status.HTTP_200_OK,
        )


class LogisticaLavadoUnidadView(_LogisticaBaseView):
    parser_classes = [MultiPartParser, FormParser, JSONParser]

    def post(self, request):
        repartidor = _get_repartidor_for_user(request.user)
        if not repartidor or not _can_operate_pwa(request.user):
            return Response({"detail": "Solo repartidores registrados pueden capturar lavados."}, status=status.HTTP_403_FORBIDDEN)

        serializer = LogisticaLavadoUnidadCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        lavado = LavadoUnidad.objects.create(
            registrado_por=request.user,
            fecha=timezone.localdate(),
            ip_registro=request.META.get("REMOTE_ADDR"),
            **serializer.validated_data,
        )
        log_event(
            request.user,
            "CREATE",
            "logistica.LavadoUnidad",
            str(lavado.id),
            {
                "unidad": lavado.unidad.codigo,
                "partes_lavadas": lavado.partes_lavadas,
                "costo": str(lavado.costo) if lavado.costo is not None else "",
            },
        )
        return Response(LogisticaLavadoUnidadSerializer(lavado).data, status=status.HTTP_201_CREATED)


class LogisticaBitacoraSalidaDetailView(_LogisticaBaseView):
    parser_classes = [MultiPartParser, FormParser, JSONParser]

    def patch(self, request, bitacora_id: int):
        repartidor = _get_repartidor_for_user(request.user)
        bitacora = get_object_or_404(
            BitacoraSalidaLlegada.objects.select_related("repartidor__user", "repartidor__user__empleado_rrhh", "unidad"),
            pk=bitacora_id,
        )
        if not repartidor or bitacora.repartidor_id != repartidor.id:
            return Response({"detail": "No tienes permisos para actualizar esta bitácora."}, status=status.HTTP_403_FORBIDDEN)
        if bitacora.cerrada:
            return Response({"error": "ya_cerrada"}, status=status.HTTP_400_BAD_REQUEST)

        serializer = LogisticaBitacoraLlegadaSerializer(bitacora, data=request.data, partial=True)
        serializer.is_valid(raise_exception=True)
        bitacora = serializer.save(hora_llegada=timezone.now(), cerrada=True)
        return Response(LogisticaBitacoraSalidaLlegadaSerializer(bitacora).data, status=status.HTTP_200_OK)


class LogisticaBitacoraSalidaActivaView(_LogisticaBaseView):
    def get(self, request):
        repartidor = _get_repartidor_for_user(request.user)
        if not repartidor:
            return Response({"detail": "No tienes perfil de repartidor registrado."}, status=status.HTTP_404_NOT_FOUND)
        bitacora = (
            BitacoraSalidaLlegada.objects.filter(repartidor=repartidor, cerrada=False)
            .select_related("unidad", "repartidor__user", "repartidor__user__empleado_rrhh")
            .prefetch_related("cargas_combustible")
            .first()
        )
        if not bitacora:
            return Response(status=status.HTTP_204_NO_CONTENT)
        return Response(LogisticaBitacoraSalidaLlegadaSerializer(bitacora).data, status=status.HTTP_200_OK)


class LogisticaBitacoraSalidaHoyView(LogisticaBitacoraSalidaActivaView):
    pass


class LogisticaInspeccionView(_LogisticaBaseView):
    parser_classes = [MultiPartParser, FormParser, JSONParser]

    def post(self, request):
        repartidor = _get_repartidor_for_user(request.user)
        if not repartidor or not _can_operate_pwa(request.user):
            return Response({"detail": "Solo repartidores registrados pueden capturar inspección."}, status=status.HTTP_403_FORBIDDEN)

        serializer = LogisticaInspeccionVehiculoCreateSerializer(data=request.data, context={"repartidor": repartidor})
        serializer.is_valid(raise_exception=True)
        inspeccion = serializer.save(ip_registro=request.META.get("REMOTE_ADDR"))
        return Response(LogisticaInspeccionVehiculoSerializer(inspeccion).data, status=status.HTTP_201_CREATED)


class LogisticaInspeccionUltimaView(_LogisticaBaseView):
    def get(self, request):
        repartidor = _get_repartidor_for_user(request.user)
        if not repartidor or not repartidor.unidad_asignada_id:
            return Response({"detail": "No tienes unidad asignada."}, status=status.HTTP_404_NOT_FOUND)
        inspeccion = (
            InspeccionVehiculo.objects.filter(unidad=repartidor.unidad_asignada)
            .select_related("repartidor__user", "repartidor__user__empleado_rrhh", "unidad")
            .first()
        )
        if not inspeccion:
            return Response({}, status=status.HTTP_200_OK)
        return Response(LogisticaInspeccionVehiculoSerializer(inspeccion).data, status=status.HTTP_200_OK)


INSPECCION_DIARIA_BOOL_FIELDS = [
    "aceite_ok",
    "refrigerante_ok",
    "liquido_frenos_ok",
    "limpiaparabrisas_ok",
    "presion_llantas_ok",
    "desgaste_llantas_ok",
    "luces_ok",
    "escobillas_ok",
    "bateria_ok",
    "tablero_ok",
    "documentos_ok",
    "licencia_ok",
    "kit_emergencia_ok",
]


class InspeccionDiariaCheckView(_LogisticaBaseView):
    def get(self, request):
        unidad_id = request.query_params.get("unidad_id")
        if not unidad_id:
            return Response({"detail": "unidad_id es obligatorio."}, status=status.HTTP_400_BAD_REQUEST)
        inspeccion = (
            InspeccionDiaria.objects.select_related("repartidor__user", "repartidor__user__empleado_rrhh", "unidad")
            .filter(unidad_id=unidad_id, fecha=timezone.localdate())
            .first()
        )
        if not inspeccion:
            return Response({"ya_inspeccionada": False}, status=status.HTTP_200_OK)
        hora_local = timezone.localtime(inspeccion.hora)
        return Response(
            {
                "ya_inspeccionada": True,
                "repartidor_nombre": nombre_operativo_usuario(inspeccion.repartidor.user),
                "hora": hora_local.strftime("%H:%M"),
                "fecha": inspeccion.fecha.isoformat(),
                "inspeccion_id": inspeccion.id,
            },
            status=status.HTTP_200_OK,
        )


class InspeccionDiariaCreateView(generics.CreateAPIView):
    authentication_classes = _LogisticaBaseView.authentication_classes
    permission_classes = _LogisticaBaseView.permission_classes
    parser_classes = [MultiPartParser, FormParser, JSONParser]
    serializer_class = LogisticaInspeccionDiariaSerializer

    def create(self, request, *args, **kwargs):
        repartidor = _get_repartidor_for_user(request.user)
        if not repartidor or not _can_operate_pwa(request.user):
            return Response({"detail": "Solo repartidores registrados pueden capturar inspección diaria."}, status=status.HTTP_403_FORBIDDEN)

        serializer = self.get_serializer(data=request.data, context={"repartidor": repartidor})
        serializer.is_valid(raise_exception=True)
        unidad = serializer.validated_data.get("unidad")
        if InspeccionDiaria.objects.filter(unidad=unidad, fecha=timezone.localdate()).exists():
            return Response(
                {"error": "ya_existe", "mensaje": "Esta unidad ya fue inspeccionada hoy"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        observaciones = (serializer.validated_data.get("observaciones") or "").strip()
        tiene_fallas = bool(observaciones) and any(
            serializer.validated_data.get(field) is False for field in INSPECCION_DIARIA_BOOL_FIELDS
        )
        try:
            with transaction.atomic():
                inspeccion = serializer.save(ip_registro=request.META.get("REMOTE_ADDR"), tiene_fallas=tiene_fallas)
                if tiene_fallas:
                    reporte = ReporteUnidad.objects.create(
                        repartidor=repartidor,
                        unidad=inspeccion.unidad,
                        tipo=ReporteUnidad.TIPO_OTRO,
                        severidad=ReporteUnidad.SEVERIDAD_URGENTE,
                        descripcion=f"Falla detectada en inspección diaria: {observaciones}",
                        latitud=inspeccion.latitud,
                        longitud=inspeccion.longitud,
                        ip_reporte=request.META.get("REMOTE_ADDR"),
                    )
                    inspeccion.reporte_generado = reporte
                    inspeccion.save(update_fields=["reporte_generado"])
        except IntegrityError:
            return Response(
                {"error": "ya_existe", "mensaje": "Esta unidad ya fue inspeccionada hoy"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        return Response(LogisticaInspeccionDiariaSerializer(inspeccion).data, status=status.HTTP_201_CREATED)


class LogisticaRutasView(_LogisticaBaseView):
    def get(self, request):
        if not can_view_submodule(request.user, "logistica", "rutas"):
            return Response({"detail": "No tienes permisos para consultar Logística."}, status=status.HTTP_403_FORBIDDEN)

        q = (request.query_params.get("q") or "").strip()
        estatus = (request.query_params.get("estatus") or "").strip().upper()
        limit = self._bounded_int(request.query_params.get("limit"), default=50, min_value=1, max_value=500)
        offset = self._bounded_int(request.query_params.get("offset"), default=0, min_value=0, max_value=100000)

        qs = RutaEntrega.objects.all()
        if q:
            qs = qs.filter(
                Q(folio__icontains=q)
                | Q(nombre__icontains=q)
                | Q(chofer__icontains=q)
                | Q(unidad__icontains=q)
            )
        if estatus:
            qs = qs.filter(estatus=estatus)

        total = qs.count()
        rows = list(qs.order_by("-fecha_ruta", "-id")[offset : offset + limit])
        return Response(
            {
                "count": total,
                "limit": limit,
                "offset": offset,
                "results": LogisticaRutaSerializer(rows, many=True).data,
            },
            status=status.HTTP_200_OK,
        )

    def post(self, request):
        if not can_manage_submodule(request.user, "logistica", "rutas"):
            return Response({"detail": "No tienes permisos para crear rutas."}, status=status.HTTP_403_FORBIDDEN)

        serializer = LogisticaRutaSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        if serializer.validated_data.get("estatus", RutaEntrega.ESTATUS_PLANEADA) != RutaEntrega.ESTATUS_PLANEADA:
            return Response({"detail": "Crea la ruta como planeada y libérala desde el flujo de planeación."}, status=status.HTTP_400_BAD_REQUEST)
        ruta = serializer.save(created_by=request.user)
        log_event(
            request.user,
            "CREATE",
            "logistica.RutaEntrega",
            str(ruta.id),
            {"folio": ruta.folio, "nombre": ruta.nombre},
        )
        return Response(LogisticaRutaSerializer(ruta).data, status=status.HTTP_201_CREATED)


class LogisticaRutaEntregasView(_LogisticaBaseView):
    def get(self, request, ruta_id: int):
        if not can_view_submodule(request.user, "logistica", "rutas"):
            return Response({"detail": "No tienes permisos para consultar Logística."}, status=status.HTTP_403_FORBIDDEN)

        ruta = get_object_or_404(RutaEntrega, pk=ruta_id)
        rows = list(ruta.entregas.select_related("pedido").order_by("secuencia", "id"))
        return Response(
            {
                "ruta": LogisticaRutaSerializer(ruta).data,
                "entregas": LogisticaEntregaSerializer(rows, many=True).data,
            },
            status=status.HTTP_200_OK,
        )

    def post(self, request, ruta_id: int):
        if not can_manage_submodule(request.user, "logistica", "rutas"):
            return Response({"detail": "No tienes permisos para crear entregas."}, status=status.HTTP_403_FORBIDDEN)

        ruta = get_object_or_404(RutaEntrega, pk=ruta_id)
        if ruta.estatus != RutaEntrega.ESTATUS_PLANEADA:
            return Response({"detail": "Solo puedes agregar entregas a rutas planeadas."}, status=status.HTTP_400_BAD_REQUEST)
        serializer = LogisticaEntregaCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        payload = serializer.validated_data

        pedido = None
        pedido_id = payload.get("pedido_id")
        if pedido_id is not None:
            pedido = get_object_or_404(PedidoCliente, pk=pedido_id)

        with transaction.atomic():
            entrega = EntregaRuta.objects.create(
                ruta=ruta,
                secuencia=payload["secuencia"],
                pedido=pedido,
                cliente_nombre=payload.get("cliente_nombre") or "",
                direccion=payload.get("direccion") or "",
                contacto=payload.get("contacto") or "",
                telefono=payload.get("telefono") or "",
                ventana_inicio=payload.get("ventana_inicio"),
                ventana_fin=payload.get("ventana_fin"),
                estatus=payload.get("estatus") or EntregaRuta.ESTATUS_PENDIENTE,
                monto_estimado=payload.get("monto_estimado") or Decimal("0"),
                comentario=payload.get("comentario") or "",
            )
            ruta.recompute_totals()
            ruta.save(update_fields=["total_entregas", "entregas_completadas", "entregas_incidencia", "monto_estimado_total", "updated_at"])

        log_event(
            request.user,
            "CREATE",
            "logistica.EntregaRuta",
            str(entrega.id),
            {"ruta": ruta.folio, "secuencia": entrega.secuencia, "estatus": entrega.estatus},
        )
        return Response(
            {
                "ruta": LogisticaRutaSerializer(ruta).data,
                "entrega": LogisticaEntregaSerializer(entrega).data,
            },
            status=status.HTTP_201_CREATED,
        )


class LogisticaRutaStatusView(_LogisticaBaseView):
    def post(self, request, ruta_id: int):
        if not can_manage_submodule(request.user, "logistica", "rutas"):
            return Response({"detail": "No tienes permisos para cambiar estatus de ruta."}, status=status.HTTP_403_FORBIDDEN)

        ruta = get_object_or_404(RutaEntrega, pk=ruta_id)
        estatus_nuevo = (request.data.get("estatus") or "").strip().upper()
        valid = {choice[0] for choice in RutaEntrega.ESTATUS_CHOICES}
        if estatus_nuevo not in valid:
            return Response({"detail": "Estatus inválido."}, status=status.HTTP_400_BAD_REQUEST)
        if ruta.estatus in {RutaEntrega.ESTATUS_COMPLETADA, RutaEntrega.ESTATUS_CANCELADA} and estatus_nuevo != ruta.estatus:
            return Response({"detail": "La ruta ya está cerrada o cancelada y no puede reabrirse."}, status=status.HTTP_400_BAD_REQUEST)
        if ruta.estatus == RutaEntrega.ESTATUS_EN_RUTA and estatus_nuevo == RutaEntrega.ESTATUS_PLANEADA:
            return Response({"detail": "La ruta ya inició seguimiento y no puede regresar a planeada."}, status=status.HTTP_400_BAD_REQUEST)
        if estatus_nuevo == RutaEntrega.ESTATUS_EN_RUTA:
            blockers = []
            if not ruta.repartidor_id:
                blockers.append("asigna repartidor")
            if not ruta.unidad_operativa_id:
                blockers.append("asigna unidad operativa")
            if not ruta.paradas.exists():
                blockers.append("agrega al menos una parada")
            if (
                ruta.repartidor_id
                and RutaEntrega.objects.filter(
                    repartidor_id=ruta.repartidor_id,
                    estatus=RutaEntrega.ESTATUS_EN_RUTA,
                )
                .exclude(pk=ruta.pk)
                .exists()
            ):
                blockers.append("el repartidor ya tiene otra ruta en curso")
            if (
                ruta.unidad_operativa_id
                and RutaEntrega.objects.filter(
                    unidad_operativa_id=ruta.unidad_operativa_id,
                    estatus=RutaEntrega.ESTATUS_EN_RUTA,
                )
                .exclude(pk=ruta.pk)
                .exists()
            ):
                blockers.append("la unidad ya tiene otra ruta en curso")
            if blockers:
                return Response({"detail": "No se puede liberar la ruta: " + ", ".join(blockers) + "."}, status=status.HTTP_400_BAD_REQUEST)
        if estatus_nuevo == RutaEntrega.ESTATUS_COMPLETADA:
            if ruta.estatus != RutaEntrega.ESTATUS_EN_RUTA:
                return Response({"detail": "Solo puedes completar una ruta que ya está en seguimiento."}, status=status.HTTP_400_BAD_REQUEST)
            if not ruta.repartidor_id or not ruta.unidad_operativa_id or not ruta.paradas.exists():
                return Response({"detail": "No se puede completar la ruta: falta repartidor, unidad o paradas."}, status=status.HTTP_400_BAD_REQUEST)
            if ruta.paradas.filter(estado=ParadaRuta.ESTADO_PENDIENTE).exists():
                return Response({"detail": "No se puede completar la ruta: hay paradas pendientes por visitar u omitir."}, status=status.HTTP_400_BAD_REQUEST)

        from_status = ruta.estatus
        ruta.estatus = estatus_nuevo
        if estatus_nuevo == RutaEntrega.ESTATUS_EN_RUTA and not ruta.hora_inicio_real:
            ruta.hora_inicio_real = timezone.now()
        if estatus_nuevo == RutaEntrega.ESTATUS_COMPLETADA and not ruta.hora_cierre_real:
            ruta.hora_cierre_real = timezone.now()
        try:
            ruta.save(update_fields=["estatus", "hora_inicio_real", "hora_cierre_real", "updated_at"])
        except IntegrityError:
            return Response(
                {"detail": "No se puede liberar la ruta: el repartidor o la unidad ya tiene otra ruta en curso."},
                status=status.HTTP_409_CONFLICT,
            )
        if estatus_nuevo == RutaEntrega.ESTATUS_EN_RUTA and not EventoRuta.objects.filter(ruta=ruta, tipo=EventoRuta.TIPO_SALIDA).exists():
            EventoRuta.objects.create(
                ruta=ruta,
                tipo=EventoRuta.TIPO_SALIDA,
                severidad=EventoRuta.SEVERIDAD_INFO,
                descripcion="Ruta liberada para seguimiento operativo.",
                creado_por=request.user,
            )
        if estatus_nuevo == RutaEntrega.ESTATUS_COMPLETADA and not EventoRuta.objects.filter(ruta=ruta, tipo=EventoRuta.TIPO_CIERRE).exists():
            EventoRuta.objects.create(
                ruta=ruta,
                tipo=EventoRuta.TIPO_CIERRE,
                severidad=EventoRuta.SEVERIDAD_INFO,
                descripcion="Ruta completada y cerrada operativamente.",
                creado_por=request.user,
            )
        log_event(
            request.user,
            "UPDATE",
            "logistica.RutaEntrega",
            str(ruta.id),
            {"folio": ruta.folio, "from": from_status, "to": estatus_nuevo},
        )
        return Response(LogisticaRutaSerializer(ruta).data, status=status.HTTP_200_OK)


class LogisticaRutaActivaView(_LogisticaBaseView):
    def get(self, request):
        if not _can_operate_pwa(request.user):
            return Response({"detail": "No tienes permisos para consultar ruta activa."}, status=status.HTTP_403_FORBIDDEN)

        repartidor = _get_repartidor_for_user(request.user)
        if not repartidor:
            return Response({"detail": "No tienes perfil de repartidor registrado."}, status=status.HTTP_404_NOT_FOUND)

        ruta = (
            RutaEntrega.objects.select_related("repartidor__user", "unidad_operativa", "bitacora_salida")
            .filter(repartidor=repartidor, estatus=RutaEntrega.ESTATUS_EN_RUTA)
            .order_by("-fecha_ruta", "-id")
            .first()
        )
        if not ruta:
            return Response(status=status.HTTP_204_NO_CONTENT)

        return Response(
            {
                "ruta": LogisticaRutaSerializer(ruta).data,
                "paradas": ParadaRutaSerializer(ruta.paradas.select_related("punto").order_by("orden", "id"), many=True).data,
                "ultima_ubicacion": UbicacionRutaSerializer(ruta.ubicaciones.select_related("repartidor__user", "unidad").first()).data
                if ruta.ubicaciones.exists()
                else None,
                "eventos": EventoRutaSerializer(ruta.eventos.select_related("parada__punto", "ubicacion", "creado_por")[:20], many=True).data,
            },
            status=status.HTTP_200_OK,
        )


class LogisticaRutasControlView(_LogisticaBaseView):
    def get(self, request):
        if not can_view_submodule(request.user, "logistica", "rutas"):
            return Response({"detail": "No tienes permisos para consultar control de rutas."}, status=status.HTTP_403_FORBIDDEN)

        limit = self._bounded_int(request.query_params.get("limit"), default=50, min_value=1, max_value=200)
        fecha = timezone.localdate()
        fecha_param = (request.query_params.get("fecha") or "").strip()
        if fecha_param:
            try:
                fecha = timezone.datetime.fromisoformat(fecha_param).date()
            except ValueError:
                return Response({"detail": "Fecha inválida. Usa YYYY-MM-DD."}, status=status.HTTP_400_BAD_REQUEST)

        data = resumen_control_rutas(fecha=fecha, limit=limit)
        return Response(
            {
                "fecha": data["fecha"].isoformat(),
                "metricas": {
                    "eventos_criticos": data["eventos_criticos"],
                    "desvios": data["desvios"],
                    "gps_perdido": data["gps_perdido"],
                },
                "rutas": [
                    {
                        "ruta": LogisticaRutaSerializer(row["ruta"]).data,
                        "ultima_ubicacion": UbicacionRutaSerializer(row["ultima_ubicacion"]).data if row["ultima_ubicacion"] else None,
                        "paradas_total": row["paradas_total"],
                        "paradas_visitadas": row["paradas_visitadas"],
                        "eventos_alerta": row["eventos_alerta"],
                        "gps_minutos": row["gps_minutos"],
                    }
                    for row in data["rutas"]
                ],
            },
            status=status.HTTP_200_OK,
        )


class LogisticaRutaTrackingView(_LogisticaBaseView):
    def get(self, request, ruta_id: int):
        ruta = get_object_or_404(RutaEntrega, pk=ruta_id)
        repartidor = _get_repartidor_for_user(request.user)
        can_view_tracking = can_manage_submodule(request.user, "logistica", "rutas") or (
            repartidor is not None and ruta.repartidor_id == repartidor.id
        )
        if not can_view_tracking:
            return Response({"detail": "No tienes permisos para consultar seguimiento GPS de esta ruta."}, status=status.HTTP_403_FORBIDDEN)
        return Response(
            {
                "ruta": LogisticaRutaSerializer(ruta).data,
                "paradas": ParadaRutaSerializer(ruta.paradas.select_related("punto"), many=True).data,
                "ubicaciones": UbicacionRutaSerializer(ruta.ubicaciones.select_related("repartidor__user", "unidad")[:200], many=True).data,
                "eventos": EventoRutaSerializer(ruta.eventos.select_related("parada__punto", "ubicacion", "creado_por")[:200], many=True).data,
            },
            status=status.HTTP_200_OK,
        )

    def post(self, request, ruta_id: int):
        if not _can_operate_pwa(request.user):
            return Response({"detail": "No tienes permisos para registrar seguimiento de ruta."}, status=status.HTTP_403_FORBIDDEN)

        ruta = get_object_or_404(RutaEntrega.objects.select_related("repartidor", "unidad_operativa", "bitacora_salida"), pk=ruta_id)
        serializer = UbicacionRutaCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        try:
            ubicacion = registrar_ubicacion_ruta(
                user=request.user,
                ruta=ruta,
                payload=serializer.validated_data,
                ip_registro=request.META.get("REMOTE_ADDR"),
            )
        except PermissionDenied as exc:
            return Response({"detail": str(exc)}, status=status.HTTP_403_FORBIDDEN)
        except ValidationError as exc:
            return Response({"detail": exc.message if hasattr(exc, "message") else exc.messages}, status=status.HTTP_400_BAD_REQUEST)

        return Response(UbicacionRutaSerializer(ubicacion).data, status=status.HTTP_201_CREATED)


class LogisticaRutaEventosView(_LogisticaBaseView):
    def post(self, request, ruta_id: int):
        if not can_manage_submodule(request.user, "logistica", "rutas"):
            return Response({"detail": "No tienes permisos para registrar eventos de ruta."}, status=status.HTTP_403_FORBIDDEN)

        ruta = get_object_or_404(RutaEntrega, pk=ruta_id)
        if ruta.estatus in {RutaEntrega.ESTATUS_COMPLETADA, RutaEntrega.ESTATUS_CANCELADA}:
            return Response({"detail": "La ruta ya está cerrada o cancelada; no se pueden agregar eventos manuales."}, status=status.HTTP_400_BAD_REQUEST)
        serializer = EventoRutaCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        payload = serializer.validated_data
        if payload["tipo"] != EventoRuta.TIPO_INCIDENCIA_MANUAL:
            return Response({"detail": "Los eventos automáticos no se registran manualmente desde este endpoint."}, status=status.HTTP_400_BAD_REQUEST)
        parada = None
        if payload.get("parada_id"):
            parada = get_object_or_404(ParadaRuta, pk=payload["parada_id"], ruta=ruta)

        evento = EventoRuta.objects.create(
            ruta=ruta,
            parada=parada,
            tipo=payload.get("tipo") or EventoRuta.TIPO_INCIDENCIA_MANUAL,
            severidad=payload.get("severidad") or EventoRuta.SEVERIDAD_ALERTA,
            descripcion=payload["descripcion"],
            latitud=payload.get("latitud"),
            longitud=payload.get("longitud"),
            metadata=payload.get("metadata") or {},
            creado_por=request.user,
        )
        log_event(
            request.user,
            "CREATE",
            "logistica.EventoRuta",
            str(evento.id),
            {"ruta": ruta.folio, "tipo": evento.tipo, "severidad": evento.severidad},
        )
        return Response(EventoRutaSerializer(evento).data, status=status.HTTP_201_CREATED)


class LogisticaDashboardView(_LogisticaBaseView):
    def get(self, request):
        if not can_view_logistica(request.user):
            return Response({"detail": "No tienes permisos para consultar Logística."}, status=status.HTTP_403_FORBIDDEN)

        return Response(
            {
                "rutas": {
                    "total": RutaEntrega.objects.count(),
                    "planeadas": RutaEntrega.objects.filter(estatus=RutaEntrega.ESTATUS_PLANEADA).count(),
                    "en_ruta": RutaEntrega.objects.filter(estatus=RutaEntrega.ESTATUS_EN_RUTA).count(),
                    "completadas": RutaEntrega.objects.filter(estatus=RutaEntrega.ESTATUS_COMPLETADA).count(),
                },
                "entregas": {
                    "total": EntregaRuta.objects.count(),
                    "pendientes": EntregaRuta.objects.filter(estatus=EntregaRuta.ESTATUS_PENDIENTE).count(),
                    "en_camino": EntregaRuta.objects.filter(estatus=EntregaRuta.ESTATUS_EN_CAMINO).count(),
                    "entregadas": EntregaRuta.objects.filter(estatus=EntregaRuta.ESTATUS_ENTREGADA).count(),
                    "incidencia": EntregaRuta.objects.filter(estatus=EntregaRuta.ESTATUS_INCIDENCIA).count(),
                },
                "monto_estimado_total": str(
                    EntregaRuta.objects.aggregate(total=Sum("monto_estimado")).get("total") or Decimal("0")
                ),
            },
            status=status.HTTP_200_OK,
        )
