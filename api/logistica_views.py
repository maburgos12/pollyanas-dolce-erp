from __future__ import annotations

from decimal import Decimal

from django.db import transaction
from django.db.models import Q, Sum
from django.shortcuts import get_object_or_404
from django.utils import timezone
from rest_framework import status
from rest_framework.authentication import SessionAuthentication, TokenAuthentication
from rest_framework.parsers import FormParser, JSONParser, MultiPartParser
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework_simplejwt.views import TokenObtainPairView

from core.access import can_manage_logistica, can_view_logistica
from core.audit import log_event
from crm.models import PedidoCliente
from logistica.models import BitacoraRepartidor, BitacoraSalidaLlegada, EntregaRuta, InspeccionVehiculo, Repartidor, ReporteUnidad, RutaEntrega, Unidad

from .logistica_serializers import (
    LogisticaBitacoraSerializer,
    LogisticaBitacoraLlegadaSerializer,
    LogisticaBitacoraSalidaCreateSerializer,
    LogisticaBitacoraSalidaLlegadaSerializer,
    LogisticaEntregaCreateSerializer,
    LogisticaEntregaSerializer,
    LogisticaInspeccionVehiculoCreateSerializer,
    LogisticaInspeccionVehiculoSerializer,
    LogisticaRepartidorSerializer,
    LogisticaReporteCreateSerializer,
    LogisticaReportePatchSerializer,
    LogisticaReporteSerializer,
    LogisticaRutaSerializer,
    LogisticaUnidadSerializer,
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


class LogisticaTokenView(TokenObtainPairView):
    parser_classes = [JSONParser, FormParser]


class LogisticaMiPerfilView(_LogisticaBaseView):
    def get(self, request):
        repartidor = _get_repartidor_for_user(request.user)
        data = {
            "user": {
                "id": request.user.id,
                "username": request.user.username,
                "nombre": request.user.get_full_name() or request.user.username,
                "email": request.user.email,
            },
            "roles": list(request.user.groups.values_list("name", flat=True)),
            "repartidor": LogisticaRepartidorSerializer(repartidor).data if repartidor else None,
            "ultimos_servicios": [],
        }
        if repartidor and repartidor.unidad_asignada_id:
            servicios = ReporteUnidad.objects.filter(unidad=repartidor.unidad_asignada).exclude(
                estatus=ReporteUnidad.ESTATUS_ABIERTO
            )[:10]
            data["ultimos_servicios"] = LogisticaReporteSerializer(servicios, many=True).data
        return Response(data, status=status.HTTP_200_OK)


class LogisticaReporteCreateView(_LogisticaBaseView):
    parser_classes = [MultiPartParser, FormParser, JSONParser]

    def post(self, request):
        repartidor = _get_repartidor_for_user(request.user)
        if not repartidor or not _is_repartidor(request.user):
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


class LogisticaMisReportesView(_LogisticaBaseView):
    def get(self, request):
        repartidor = _get_repartidor_for_user(request.user)
        if not repartidor:
            return Response({"detail": "No tienes perfil de repartidor registrado."}, status=status.HTTP_404_NOT_FOUND)
        reportes = ReporteUnidad.objects.filter(repartidor=repartidor).select_related("unidad", "repartidor__user", "asignado_a")
        return Response(LogisticaReporteSerializer(reportes, many=True).data, status=status.HTTP_200_OK)


class LogisticaTodosReportesView(_LogisticaBaseView):
    def get(self, request):
        if not _can_view_all_reportes(request.user):
            return Response({"detail": "No tienes permisos para consultar todos los reportes."}, status=status.HTTP_403_FORBIDDEN)

        qs = ReporteUnidad.objects.select_related("unidad", "repartidor__user", "asignado_a")
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
            ReporteUnidad.objects.select_related("repartidor__user", "unidad", "asignado_a"),
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
            qs = BitacoraRepartidor.objects.select_related("repartidor__user").all()
            repartidor_id = request.query_params.get("repartidor")
            fecha = request.query_params.get("fecha")
            if repartidor_id:
                qs = qs.filter(repartidor_id=repartidor_id)
            if fecha:
                qs = qs.filter(fecha=fecha)
        elif repartidor:
            qs = BitacoraRepartidor.objects.select_related("repartidor__user").filter(repartidor=repartidor)
        else:
            return Response({"detail": "No tienes perfil de repartidor registrado."}, status=status.HTTP_404_NOT_FOUND)
        return Response(LogisticaBitacoraSerializer(qs, many=True).data, status=status.HTTP_200_OK)

    def post(self, request):
        repartidor = _get_repartidor_for_user(request.user)
        if not repartidor or not _is_repartidor(request.user):
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
        if not repartidor or not _is_repartidor(request.user):
            return Response({"detail": "Solo repartidores registrados pueden iniciar bitácora."}, status=status.HTTP_403_FORBIDDEN)

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


class LogisticaBitacoraSalidaDetailView(_LogisticaBaseView):
    parser_classes = [MultiPartParser, FormParser, JSONParser]

    def patch(self, request, bitacora_id: int):
        repartidor = _get_repartidor_for_user(request.user)
        bitacora = get_object_or_404(BitacoraSalidaLlegada.objects.select_related("repartidor__user", "unidad"), pk=bitacora_id)
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
        bitacora = BitacoraSalidaLlegada.objects.filter(repartidor=repartidor, cerrada=False).select_related("unidad", "repartidor__user").first()
        if not bitacora:
            return Response({"detail": "No hay turno abierto."}, status=status.HTTP_404_NOT_FOUND)
        return Response(LogisticaBitacoraSalidaLlegadaSerializer(bitacora).data, status=status.HTTP_200_OK)


class LogisticaBitacoraSalidaHoyView(LogisticaBitacoraSalidaActivaView):
    pass


class LogisticaInspeccionView(_LogisticaBaseView):
    parser_classes = [MultiPartParser, FormParser, JSONParser]

    def post(self, request):
        repartidor = _get_repartidor_for_user(request.user)
        if not repartidor or not _is_repartidor(request.user):
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
        inspeccion = InspeccionVehiculo.objects.filter(unidad=repartidor.unidad_asignada).select_related("repartidor__user", "unidad").first()
        if not inspeccion:
            return Response({}, status=status.HTTP_200_OK)
        return Response(LogisticaInspeccionVehiculoSerializer(inspeccion).data, status=status.HTTP_200_OK)


class LogisticaRutasView(_LogisticaBaseView):
    def get(self, request):
        if not can_view_logistica(request.user):
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
        if not can_manage_logistica(request.user):
            return Response({"detail": "No tienes permisos para crear rutas."}, status=status.HTTP_403_FORBIDDEN)

        serializer = LogisticaRutaSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
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
        if not can_view_logistica(request.user):
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
        if not can_manage_logistica(request.user):
            return Response({"detail": "No tienes permisos para crear entregas."}, status=status.HTTP_403_FORBIDDEN)

        ruta = get_object_or_404(RutaEntrega, pk=ruta_id)
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
        if not can_manage_logistica(request.user):
            return Response({"detail": "No tienes permisos para cambiar estatus de ruta."}, status=status.HTTP_403_FORBIDDEN)

        ruta = get_object_or_404(RutaEntrega, pk=ruta_id)
        estatus_nuevo = (request.data.get("estatus") or "").strip().upper()
        valid = {choice[0] for choice in RutaEntrega.ESTATUS_CHOICES}
        if estatus_nuevo not in valid:
            return Response({"detail": "Estatus inválido."}, status=status.HTTP_400_BAD_REQUEST)

        from_status = ruta.estatus
        ruta.estatus = estatus_nuevo
        ruta.save(update_fields=["estatus", "updated_at"])
        log_event(
            request.user,
            "UPDATE",
            "logistica.RutaEntrega",
            str(ruta.id),
            {"folio": ruta.folio, "from": from_status, "to": estatus_nuevo},
        )
        return Response(LogisticaRutaSerializer(ruta).data, status=status.HTTP_200_OK)


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
