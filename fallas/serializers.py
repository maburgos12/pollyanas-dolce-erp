from django.contrib.auth import get_user_model
from django.db import transaction
from rest_framework import serializers

from core.models import Sucursal
from activos.models import Activo

from .models import BitacoraFalla, CategoriaFalla, EvidenciaSeguimientoFalla, ReporteFalla


class SucursalFallaSerializer(serializers.ModelSerializer):
    class Meta:
        model = Sucursal
        fields = ["id", "codigo", "nombre"]


class ActivoFallaSerializer(serializers.ModelSerializer):
    display = serializers.SerializerMethodField()

    def get_display(self, obj):
        parts = [obj.codigo, obj.nombre]
        if obj.categoria:
            parts.append(obj.categoria)
        if obj.ubicacion:
            parts.append(obj.ubicacion)
        return " · ".join(part for part in parts if part)

    class Meta:
        model = Activo
        fields = ["id", "codigo", "nombre", "categoria", "ubicacion", "display"]


class CategoriaFallaSerializer(serializers.ModelSerializer):
    tipo_display = serializers.CharField(source="get_tipo_display", read_only=True)

    class Meta:
        model = CategoriaFalla
        fields = ["id", "nombre", "tipo", "tipo_display", "activo", "orden"]


class EvidenciaSeguimientoSerializer(serializers.ModelSerializer):
    url = serializers.SerializerMethodField()

    def get_url(self, obj):
        request = self.context.get("request")
        if not obj.archivo:
            return ""
        url = obj.archivo.url
        return request.build_absolute_uri(url) if request else url

    class Meta:
        model = EvidenciaSeguimientoFalla
        fields = ["id", "nombre", "url", "creado_en"]


class BitacoraSerializer(serializers.ModelSerializer):
    usuario_nombre = serializers.SerializerMethodField()
    estatus_nuevo_display = serializers.SerializerMethodField()
    evidencias = EvidenciaSeguimientoSerializer(many=True, read_only=True)

    def get_usuario_nombre(self, obj):
        return obj.usuario.get_full_name() or obj.usuario.username

    def get_estatus_nuevo_display(self, obj):
        choices = dict(ReporteFalla.ESTATUS)
        return choices.get(obj.estatus_nuevo, obj.estatus_nuevo)

    class Meta:
        model = BitacoraFalla
        fields = [
            "id",
            "usuario_nombre",
            "estatus_anterior",
            "estatus_nuevo",
            "estatus_nuevo_display",
            "comentario",
            "evidencias",
            "timestamp",
        ]


class ReporteFallaListSerializer(serializers.ModelSerializer):
    sucursal_nombre = serializers.CharField(source="sucursal.nombre", read_only=True)
    categoria_nombre = serializers.CharField(source="categoria.nombre", read_only=True)
    estatus_display = serializers.CharField(source="get_estatus_display", read_only=True)
    prioridad_display = serializers.CharField(source="get_prioridad_display", read_only=True)
    area_display = serializers.CharField(source="get_area_display", read_only=True)
    reportado_por_nombre = serializers.SerializerMethodField()
    puede_editar = serializers.SerializerMethodField()

    def get_reportado_por_nombre(self, obj):
        return obj.reportado_por.get_full_name() or obj.reportado_por.username

    def get_puede_editar(self, obj):
        request = self.context.get("request")
        user = getattr(request, "user", None)
        return bool(
            user
            and user.is_authenticated
            and obj.reportado_por_id == user.id
            and obj.estatus == ReporteFalla.ESTATUS_ABIERTO
        )

    class Meta:
        model = ReporteFalla
        fields = [
            "id",
            "sucursal_nombre",
            "titulo",
            "descripcion",
            "categoria_nombre",
            "prioridad",
            "prioridad_display",
            "estatus",
            "estatus_display",
            "area",
            "area_display",
            "fecha_reporte",
            "latitud",
            "longitud",
            "reportado_por_nombre",
            "foto_evidencia",
            "puede_editar",
        ]


class ReporteFallaCreateSerializer(serializers.ModelSerializer):
    """Usado por la PWA al crear un reporte."""

    class Meta:
        model = ReporteFalla
        fields = [
            "sucursal",
            "activo_relacionado",
            "categoria",
            "titulo",
            "descripcion",
            "prioridad",
            "foto_evidencia",
            "latitud",
            "longitud",
            "area",
        ]

    def validate_foto_evidencia(self, value):
        if not value:
            raise serializers.ValidationError("La foto de evidencia es obligatoria.")
        return value

    def create(self, validated_data):
        validated_data["reportado_por"] = self.context["request"].user
        with transaction.atomic():
            reporte = super().create(validated_data)
            BitacoraFalla.objects.create(
                reporte=reporte,
                usuario=validated_data["reportado_por"],
                estatus_nuevo=ReporteFalla.ESTATUS_ABIERTO,
                comentario="Reporte creado desde aplicación móvil.",
            )
            try:
                from .tasks import notificar_nuevo_reporte

                transaction.on_commit(lambda: notificar_nuevo_reporte.delay(reporte.pk))
            except Exception:
                pass
        return reporte


class ReporteFallaUpdateSerializer(serializers.ModelSerializer):
    """Permite al creador corregir un reporte propio antes de seguimiento."""

    class Meta:
        model = ReporteFalla
        fields = [
            "sucursal",
            "activo_relacionado",
            "categoria",
            "titulo",
            "descripcion",
            "prioridad",
            "foto_evidencia",
            "latitud",
            "longitud",
            "area",
        ]
        extra_kwargs = {"foto_evidencia": {"required": False}}

    def update(self, instance, validated_data):
        with transaction.atomic():
            reporte = super().update(instance, validated_data)
            BitacoraFalla.objects.create(
                reporte=reporte,
                usuario=self.context["request"].user,
                estatus_anterior=ReporteFalla.ESTATUS_ABIERTO,
                estatus_nuevo=ReporteFalla.ESTATUS_ABIERTO,
                comentario="Reporte editado por el usuario que lo levantó.",
            )
        return reporte


class ReporteFallaDetailSerializer(ReporteFallaListSerializer):
    bitacora = BitacoraSerializer(many=True, read_only=True)
    activo_nombre = serializers.SerializerMethodField()

    def get_activo_nombre(self, obj):
        return str(obj.activo_relacionado) if obj.activo_relacionado_id else ""

    class Meta(ReporteFallaListSerializer.Meta):
        fields = ReporteFallaListSerializer.Meta.fields + [
            "sucursal",
            "categoria",
            "descripcion",
            "activo_relacionado",
            "activo_nombre",
            "latitud",
            "longitud",
            "asignado_a",
            "costo_estimado",
            "costo_real",
            "proveedor_servicio",
            "notas_internas",
            "fecha_asignacion",
            "fecha_resolucion",
            "fecha_cierre",
            "tiempo_respuesta_horas",
            "tiempo_resolucion_horas",
            "bitacora",
        ]


class CambioEstatusSerializer(serializers.Serializer):
    estatus = serializers.ChoiceField(choices=ReporteFalla.ESTATUS, required=False)
    comentario = serializers.CharField(required=False, allow_blank=True)
    asignado_a = serializers.IntegerField(required=False, allow_null=True)
    costo_estimado = serializers.DecimalField(max_digits=10, decimal_places=2, required=False, allow_null=True)
    costo_real = serializers.DecimalField(max_digits=10, decimal_places=2, required=False, allow_null=True)
    proveedor_servicio = serializers.CharField(required=False, allow_blank=True)

    def validate_asignado_a(self, value):
        if value is None:
            return value
        if not get_user_model().objects.filter(pk=value, is_active=True).exists():
            raise serializers.ValidationError("Usuario asignado no encontrado o inactivo.")
        return value
