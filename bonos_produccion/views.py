from decimal import Decimal

from django.db import transaction
from django.db.models import Q
from django.shortcuts import get_object_or_404
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.permissions import BasePermission, IsAuthenticated
from rest_framework.response import Response

from rrhh.models import Empleado, NominaPeriodo
from rrhh.bonos_permisos import BasePermisosEquipoViewSet, _empleado_payload, _permiso_payload
from core.access import can_view_submodule, is_bonos_produccion_capture_only
from recetas.utils.normalizacion import normalizar_nombre

from .models import (
    AREA_HORNOS,
    AREA_PRODUCCION,
    AREAS_PRODUCCION,
    BonoProduccionEmpleado,
    ConfigBonoPeriodo,
    RegistroDiarioProduccion,
    area_bono_produccion_empleado,
    normalizar_area_produccion,
)
from .serializers import (
    BonoProduccionResumenSerializer,
    BonoProduccionSerializer,
    ConfigBonoPeriodoSerializer,
    RegistroDiarioSerializer,
)


class CanAccessBonosProduccion(BasePermission):
    def has_permission(self, request, view):
        user = request.user
        return bool(
            user
            and user.is_authenticated
            and (is_bonos_produccion_capture_only(user) or can_view_submodule(user, "produccion", "bonos"))
        )


def _empleado_de_usuario(user) -> Empleado | None:
    if not user or not user.is_authenticated:
        return None
    empleado = Empleado.objects.filter(usuario_erp=user, activo=True).first()
    if empleado:
        return empleado
    if user.email:
        empleado = Empleado.objects.filter(email__iexact=user.email, activo=True).first()
        if empleado:
            return empleado

    user_name = normalizar_nombre(user.get_full_name() or user.username or "")
    user_tokens = set(user_name.split())
    if not user_tokens:
        return None
    for empleado in Empleado.objects.filter(activo=True).only("id", "nombre", "nombre_normalizado"):
        empleado_name = empleado.nombre_normalizado or normalizar_nombre(empleado.nombre)
        if user_tokens.issubset(set(empleado_name.split())):
            return empleado
    return None


def _recalcular_desde_registros(bono: BonoProduccionEmpleado) -> None:
    registros = bono.registros.all()
    bono.dias_trabajados = registros.count()
    bono.dias_uniforme = registros.filter(tiene_uniforme=True).count()
    bono.dias_puntualidad = registros.filter(tiene_puntualidad=True).count()
    bono.dias_asistencia = registros.filter(tiene_asistencia=True).count()
    bono.dias_produccion = registros.filter(tiene_produccion=True).count()
    bono.total_embetunados = sum(r.cantidad_embetunados for r in registros)
    bono.recalcular()
    bono.save()


class ConfigBonoPeriodoViewSet(viewsets.ModelViewSet):
    queryset = ConfigBonoPeriodo.objects.all()
    serializer_class = ConfigBonoPeriodoSerializer
    permission_classes = [IsAuthenticated, CanAccessBonosProduccion]

    def get_queryset(self):
        qs = super().get_queryset()
        mes = self.request.query_params.get("mes")
        anio = self.request.query_params.get("anio")
        if mes:
            qs = qs.filter(mes=mes)
        if anio:
            qs = qs.filter(anio=anio)
        return qs

    def perform_create(self, serializer):
        serializer.save(creado_por=self.request.user if self.request.user.is_authenticated else None)

    @action(detail=True, methods=["post"], url_path="inicializar-bonos")
    def inicializar_bonos(self, request, pk=None):
        periodo = self.get_object()
        areas_validas = {code for code, _ in AREAS_PRODUCCION}
        empleados = Empleado.objects.filter(
            Q(participa_bonos_produccion=True) | Q(area__in=[*areas_validas, "PRODUCCION"]),
            activo=True,
        )
        creados = 0
        considerados = 0
        for empleado in empleados:
            area = area_bono_produccion_empleado(empleado)
            if area not in areas_validas:
                area = AREA_PRODUCCION
            considerados += 1
            _, created = BonoProduccionEmpleado.objects.get_or_create(
                periodo=periodo,
                empleado=empleado,
                defaults={"area": area},
            )
            if created:
                creados += 1
        return Response({"creados": creados, "total": considerados}, status=status.HTTP_200_OK)

    @action(detail=True, methods=["post"], url_path="aplicar-a-nomina")
    def aplicar_a_nomina(self, request, pk=None):
        periodo = self.get_object()
        nomina_id = request.data.get("nomina_periodo_id")
        if not nomina_id:
            return Response({"detail": "Se requiere nomina_periodo_id."}, status=status.HTTP_400_BAD_REQUEST)
        nomina = get_object_or_404(NominaPeriodo, pk=nomina_id)
        updated = periodo.aplicar_a_nomina(nomina)
        return Response({"actualizados": updated, "nomina_periodo_id": nomina.id}, status=status.HTTP_200_OK)


class BonoProduccionViewSet(viewsets.ModelViewSet):
    queryset = BonoProduccionEmpleado.objects.select_related("empleado", "periodo").prefetch_related("registros")
    serializer_class = BonoProduccionSerializer
    permission_classes = [IsAuthenticated, CanAccessBonosProduccion]

    def get_queryset(self):
        qs = super().get_queryset()
        mes = self.request.query_params.get("mes")
        anio = self.request.query_params.get("anio")
        area = self.request.query_params.get("area")
        if mes:
            qs = qs.filter(periodo__mes=mes)
        if anio:
            qs = qs.filter(periodo__anio=anio)
        if area:
            qs = qs.filter(area=normalizar_area_produccion(area))
        return qs

    def perform_create(self, serializer):
        bono = serializer.save()
        bono.recalcular()
        bono.save()

    def perform_update(self, serializer):
        bono = serializer.save()
        bono.recalcular()
        bono.save()

    @action(detail=False, methods=["get"], url_path="resumen")
    def resumen(self, request):
        mes = request.query_params.get("mes")
        anio = request.query_params.get("anio")
        if not mes or not anio:
            return Response({"detail": "Se requieren mes y anio."}, status=status.HTTP_400_BAD_REQUEST)

        bonos = list(
            BonoProduccionEmpleado.objects.filter(periodo__mes=mes, periodo__anio=anio).select_related("empleado", "periodo")
        )
        with transaction.atomic():
            periodo = ConfigBonoPeriodo.objects.filter(mes=mes, anio=anio).first()
            if periodo:
                periodo.recalcular_todos()
                bonos = list(
                    BonoProduccionEmpleado.objects.filter(periodo=periodo).select_related("empleado", "periodo")
                )

        data = BonoProduccionResumenSerializer(bonos, many=True).data
        total = sum(Decimal(row["total_a_pagar"]) for row in data)
        return Response({"mes": int(mes), "anio": int(anio), "total_a_pagar": str(total), "bonos": data})

    @action(detail=True, methods=["post"], url_path="recalcular")
    def recalcular(self, request, pk=None):
        bono = self.get_object()
        _recalcular_desde_registros(bono)
        return Response(BonoProduccionSerializer(bono).data, status=status.HTTP_200_OK)


class RegistroDiarioViewSet(viewsets.ModelViewSet):
    queryset = RegistroDiarioProduccion.objects.select_related("bono__empleado", "bono__periodo")
    serializer_class = RegistroDiarioSerializer
    permission_classes = [IsAuthenticated, CanAccessBonosProduccion]

    def get_queryset(self):
        qs = super().get_queryset()
        bono_id = self.request.query_params.get("bono")
        if bono_id:
            qs = qs.filter(bono_id=bono_id)
        return qs

    def perform_create(self, serializer):
        instance = serializer.save(capturado_por=self.request.user if self.request.user.is_authenticated else None)
        _recalcular_desde_registros(instance.bono)

    def perform_update(self, serializer):
        instance = serializer.save()
        _recalcular_desde_registros(instance.bono)


class PermisosProduccionEquipoViewSet(BasePermisosEquipoViewSet):
    permission_classes = [IsAuthenticated, CanAccessBonosProduccion]
    origen_solicitud = "bonos_produccion"

    def _context_param(self, key):
        value = self.request.query_params.get(key)
        if value not in (None, ""):
            return value
        data = getattr(self.request, "data", {})
        if hasattr(data, "get"):
            return data.get(key)
        return None

    def _bonos_periodo_queryset(self):
        mes = self._context_param("mes")
        anio = self._context_param("anio")
        if not (mes and anio):
            return None
        qs = BonoProduccionEmpleado.objects.filter(periodo__mes=mes, periodo__anio=anio).select_related("empleado")
        area = self._context_param("area")
        if area:
            area_normalizada = normalizar_area_produccion(area)
            areas_validas = {code for code, _ in AREAS_PRODUCCION}
            if area_normalizada not in areas_validas:
                return BonoProduccionEmpleado.objects.none()
            qs = qs.filter(area=area_normalizada)
        return qs

    def _equipo_directo_queryset(self):
        jefe = _empleado_de_usuario(self.request.user)
        if not jefe:
            return Empleado.objects.none()
        return Empleado.objects.filter(jefe_directo=jefe, activo=True)

    def _prioridad_equipo_directo(self, empleado):
        puesto_operativo = (empleado.puesto_operativo or "").upper()
        puesto_texto = normalizar_nombre(f"{empleado.puesto or ''} {puesto_operativo}")
        if puesto_operativo == "SUPERVISION_PRODUCCION" or "SUPERVIS" in puesto_texto:
            return 0
        if puesto_operativo == "ENCARGADA_PRODUCCION" or "ENCARGAD" in puesto_texto:
            return 1
        if puesto_operativo == "JEFATURA" or "JEFE" in puesto_texto:
            return 2
        return 10

    def _equipo_directo_ordenado(self):
        return sorted(
            self._equipo_directo_queryset(),
            key=lambda empleado: (self._prioridad_equipo_directo(empleado), empleado.nombre),
        )

    def _con_equipo_directo(self, qs):
        return (qs | self._equipo_directo_queryset()).distinct()

    def _empleados_area_queryset(self, area_normalizada):
        empleados = Empleado.objects.filter(
            Q(participa_bonos_produccion=True) | Q(area__in=[*{code for code, _ in AREAS_PRODUCCION}, "PRODUCCION"]),
            activo=True,
        )
        ids = [empleado.id for empleado in empleados if area_bono_produccion_empleado(empleado) == area_normalizada]
        return Empleado.objects.filter(id__in=ids)

    def list(self, request):
        bonos_periodo = self._bonos_periodo_queryset()
        if bonos_periodo is None:
            return super().list(request)
        bonos = list(bonos_periodo.filter(empleado__activo=True).order_by("area", "empleado__nombre"))
        empleados = []
        empleados_ids = set()
        for empleado in self._equipo_directo_ordenado():
            payload = _empleado_payload(empleado)
            payload["area"] = area_bono_produccion_empleado(empleado) or empleado.departamento or AREA_PRODUCCION
            empleados.append(payload)
            empleados_ids.add(empleado.id)
        for bono in bonos:
            if bono.empleado_id in empleados_ids:
                continue
            payload = _empleado_payload(bono.empleado)
            payload["area"] = bono.area
            empleados.append(payload)
            empleados_ids.add(bono.empleado_id)
        for empleado in self._empleados():
            if empleado.id in empleados_ids:
                continue
            payload = _empleado_payload(empleado)
            payload["area"] = area_bono_produccion_empleado(empleado) or empleado.departamento or AREA_PRODUCCION
            empleados.append(payload)
            empleados_ids.add(empleado.id)
        permisos = list(self._permisos())
        return Response(
            {
                "empleados": empleados,
                "permisos": [_permiso_payload(permiso) for permiso in permisos],
            }
        )

    def empleados_queryset(self):
        areas_validas = {code for code, _ in AREAS_PRODUCCION}
        area = self._context_param("area")
        mes = self._context_param("mes")
        anio = self._context_param("anio")
        if area:
            area_normalizada = normalizar_area_produccion(area)
            if area_normalizada not in areas_validas:
                return Empleado.objects.none()
            if mes and anio:
                empleados_periodo = BonoProduccionEmpleado.objects.filter(
                    periodo__mes=mes,
                    periodo__anio=anio,
                    area=area_normalizada,
                ).values_list("empleado_id", flat=True)
                return self._con_equipo_directo(
                    Empleado.objects.filter(id__in=empleados_periodo) | self._empleados_area_queryset(area_normalizada)
                )
            return self._con_equipo_directo(self._empleados_area_queryset(area_normalizada))
        if mes and anio:
            empleados_periodo = BonoProduccionEmpleado.objects.filter(
                periodo__mes=mes,
                periodo__anio=anio,
            ).values_list("empleado_id", flat=True)
            return self._con_equipo_directo(Empleado.objects.filter(id__in=empleados_periodo))
        return self._con_equipo_directo(Empleado.objects.filter(
            Q(participa_bonos_produccion=True) | Q(area__in=[*areas_validas, "PRODUCCION"]),
            activo=True,
        ))
