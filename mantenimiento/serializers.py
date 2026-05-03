"""Serializers para la PWA de mantenimiento.

Este módulo no define modelos propios: adapta la PWA a los modelos
existentes de activos y logística.
"""

from decimal import Decimal

from rest_framework import serializers

from activos.models import Activo, BitacoraMantenimiento, OrdenMantenimiento
from logistica.models import ReparacionUnidad, ServicioRealizadoUnidad, TipoServicioUnidad, Unidad


class ActivoListSerializer(serializers.ModelSerializer):
    sucursal_nombre = serializers.CharField(source="sucursal.nombre", read_only=True, default="")

    class Meta:
        model = Activo
        fields = [
            "id",
            "codigo",
            "nombre",
            "categoria",
            "ubicacion",
            "sucursal",
            "sucursal_nombre",
            "estado",
            "criticidad",
        ]


class OrdenMantenimientoCreateSerializer(serializers.ModelSerializer):
    costo_real = serializers.DecimalField(max_digits=18, decimal_places=2, required=False, write_only=True)
    proveedor_servicio = serializers.CharField(required=False, allow_blank=True, write_only=True)
    foto = serializers.ImageField(required=False, allow_null=True, write_only=True)

    class Meta:
        model = OrdenMantenimiento
        fields = [
            "activo_ref",
            "tipo",
            "prioridad",
            "descripcion",
            "responsable",
            "costo_repuestos",
            "costo_mano_obra",
            "costo_otros",
            "costo_real",
            "proveedor_servicio",
            "fecha_programada",
            "foto",
        ]
        extra_kwargs = {
            "prioridad": {"required": False},
            "responsable": {"required": False, "allow_blank": True},
            "costo_repuestos": {"required": False},
            "costo_mano_obra": {"required": False},
            "costo_otros": {"required": False},
            "fecha_programada": {"required": False},
        }

    def create(self, validated_data):
        request = self.context["request"]
        costo_real = validated_data.pop("costo_real", None)
        proveedor = validated_data.pop("proveedor_servicio", "")
        foto = validated_data.pop("foto", None)
        if costo_real is not None and not validated_data.get("costo_otros"):
            validated_data["costo_otros"] = costo_real
        if proveedor and not validated_data.get("responsable"):
            validated_data["responsable"] = proveedor
        validated_data["creado_por"] = request.user
        validated_data["estatus"] = OrdenMantenimiento.ESTATUS_EN_PROCESO
        orden = super().create(validated_data)
        comentario = "Orden creada desde PWA de mantenimiento."
        extras = []
        if proveedor:
            extras.append(f"Proveedor: {proveedor}")
        if costo_real is not None:
            extras.append(f"Costo capturado: ${costo_real}")
        if foto:
            extras.append(f"Foto adjunta: {foto.name}")
        if extras:
            comentario = f"{comentario} " + " | ".join(extras)
        BitacoraMantenimiento.objects.create(
            orden=orden,
            usuario=request.user,
            accion="Orden creada desde PWA",
            comentario=comentario,
            costo_adicional=costo_real or Decimal("0"),
        )
        return orden


class OrdenMantenimientoListSerializer(serializers.ModelSerializer):
    activo_nombre = serializers.CharField(source="activo_ref.nombre", read_only=True)
    activo_codigo = serializers.CharField(source="activo_ref.codigo", read_only=True)
    sucursal_nombre = serializers.CharField(source="activo_ref.sucursal.nombre", read_only=True, default="")
    tipo_display = serializers.CharField(source="get_tipo_display", read_only=True)
    estatus_display = serializers.CharField(source="get_estatus_display", read_only=True)
    prioridad_display = serializers.CharField(source="get_prioridad_display", read_only=True)
    costo_total = serializers.DecimalField(max_digits=18, decimal_places=2, read_only=True)

    class Meta:
        model = OrdenMantenimiento
        fields = [
            "id",
            "folio",
            "activo_nombre",
            "activo_codigo",
            "sucursal_nombre",
            "tipo",
            "tipo_display",
            "prioridad",
            "prioridad_display",
            "estatus",
            "estatus_display",
            "descripcion",
            "responsable",
            "costo_repuestos",
            "costo_mano_obra",
            "costo_otros",
            "costo_total",
            "fecha_programada",
            "fecha_inicio",
            "fecha_cierre",
        ]


class UnidadListSerializer(serializers.ModelSerializer):
    sucursal_nombre = serializers.CharField(source="sucursal.nombre", read_only=True, default="")

    class Meta:
        model = Unidad
        fields = ["id", "codigo", "descripcion", "marca", "modelo", "placa", "sucursal", "sucursal_nombre", "activa"]


class TipoServicioSerializer(serializers.ModelSerializer):
    class Meta:
        model = TipoServicioUnidad
        fields = ["id", "nombre", "tipo_intervalo", "intervalo_km", "intervalo_meses"]


class ReparacionCreateSerializer(serializers.ModelSerializer):
    costo = serializers.DecimalField(max_digits=12, decimal_places=2, required=False, write_only=True)
    foto = serializers.ImageField(required=False, allow_null=True, write_only=True)

    class Meta:
        model = ReparacionUnidad
        fields = [
            "unidad",
            "fecha_ingreso",
            "descripcion_falla",
            "descripcion_reparacion",
            "proveedor",
            "costo_total",
            "costo",
            "fecha_entrega",
            "archivo_factura",
            "foto_nota",
            "foto",
            "notas",
        ]
        extra_kwargs = {
            "descripcion_reparacion": {"required": False, "allow_blank": True},
            "proveedor": {"required": False, "allow_blank": True},
            "costo_total": {"required": False, "allow_null": True},
            "fecha_entrega": {"required": False, "allow_null": True},
            "archivo_factura": {"required": False, "allow_null": True},
            "foto_nota": {"required": False, "allow_null": True},
            "notas": {"required": False, "allow_blank": True},
        }

    def validate_descripcion_falla(self, value):
        if not value or not value.strip():
            raise serializers.ValidationError("La descripción de la falla es obligatoria.")
        return value.strip()

    def create(self, validated_data):
        request = self.context["request"]
        costo = validated_data.pop("costo", None)
        foto = validated_data.pop("foto", None)
        if costo is not None and validated_data.get("costo_total") is None:
            validated_data["costo_total"] = costo
        if foto and not validated_data.get("foto_nota"):
            validated_data["foto_nota"] = foto
        validated_data["registrado_por"] = request.user
        return super().create(validated_data)


class ReparacionListSerializer(serializers.ModelSerializer):
    unidad_descripcion = serializers.CharField(source="unidad.descripcion", read_only=True)
    unidad_codigo = serializers.CharField(source="unidad.codigo", read_only=True)
    costo = serializers.DecimalField(source="costo_total", max_digits=12, decimal_places=2, read_only=True)

    class Meta:
        model = ReparacionUnidad
        fields = [
            "id",
            "unidad_codigo",
            "unidad_descripcion",
            "fecha_ingreso",
            "fecha_entrega",
            "descripcion_falla",
            "descripcion_reparacion",
            "proveedor",
            "costo",
            "costo_total",
            "foto_nota",
            "notas",
        ]


class ServicioCreateSerializer(serializers.ModelSerializer):
    fecha = serializers.DateField(required=False, write_only=True)
    km_servicio = serializers.IntegerField(required=False, allow_null=True, write_only=True)

    class Meta:
        model = ServicioRealizadoUnidad
        fields = [
            "unidad",
            "tipo_servicio",
            "fecha_servicio",
            "fecha",
            "km_al_servicio",
            "km_servicio",
            "proveedor",
            "costo",
            "archivo_factura",
            "notas",
            "proxima_fecha",
            "proximos_km",
        ]
        extra_kwargs = {
            "fecha_servicio": {"required": False},
            "km_al_servicio": {"required": False, "allow_null": True},
            "proveedor": {"required": False, "allow_blank": True},
            "costo": {"required": False, "allow_null": True},
            "archivo_factura": {"required": False, "allow_null": True},
            "notas": {"required": False, "allow_blank": True},
            "proxima_fecha": {"required": False, "allow_null": True},
            "proximos_km": {"required": False, "allow_null": True},
        }

    def create(self, validated_data):
        request = self.context["request"]
        fecha = validated_data.pop("fecha", None)
        km_servicio = validated_data.pop("km_servicio", None)
        if fecha and not validated_data.get("fecha_servicio"):
            validated_data["fecha_servicio"] = fecha
        if km_servicio is not None and validated_data.get("km_al_servicio") is None:
            validated_data["km_al_servicio"] = km_servicio
        validated_data["registrado_por"] = request.user
        return super().create(validated_data)


class ServicioListSerializer(serializers.ModelSerializer):
    unidad_descripcion = serializers.CharField(source="unidad.descripcion", read_only=True)
    unidad_codigo = serializers.CharField(source="unidad.codigo", read_only=True)
    tipo_nombre = serializers.CharField(source="tipo_servicio.nombre", read_only=True)
    fecha = serializers.DateField(source="fecha_servicio", read_only=True)
    km_servicio = serializers.IntegerField(source="km_al_servicio", read_only=True)

    class Meta:
        model = ServicioRealizadoUnidad
        fields = [
            "id",
            "unidad_codigo",
            "unidad_descripcion",
            "tipo_nombre",
            "fecha",
            "fecha_servicio",
            "km_servicio",
            "km_al_servicio",
            "proveedor",
            "costo",
            "proxima_fecha",
            "proximos_km",
            "notas",
        ]
