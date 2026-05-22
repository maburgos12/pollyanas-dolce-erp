"""Serializers para la PWA de mantenimiento.

Este módulo no define modelos propios: adapta la PWA a los modelos
existentes de activos y logística.
"""

from decimal import Decimal

from django.utils import timezone
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


class ActivoQuickCreateSerializer(serializers.ModelSerializer):
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
            "notas",
        ]
        read_only_fields = ["id", "codigo", "estado", "criticidad", "sucursal_nombre"]
        extra_kwargs = {
            "nombre": {"required": True, "allow_blank": False},
            "sucursal": {"required": True, "allow_null": False},
            "categoria": {"required": False, "allow_blank": True},
            "ubicacion": {"required": False, "allow_blank": True},
            "notas": {"required": False, "allow_blank": True},
        }

    def validate_nombre(self, value):
        value = value.strip()
        if len(value) < 5:
            raise serializers.ValidationError("El nombre debe identificar claramente el punto mantenible.")
        return value

    def create(self, validated_data):
        if not (validated_data.get("categoria") or "").strip():
            validated_data["categoria"] = "Infraestructura"
        validated_data["estado"] = Activo.ESTADO_OPERATIVO
        validated_data["criticidad"] = Activo.CRITICIDAD_MEDIA
        validated_data["activo"] = True
        validated_data["codigo"] = self._next_code(validated_data["sucursal"])
        return super().create(validated_data)

    def _next_code(self, sucursal):
        base = (sucursal.codigo or "SUC").upper().replace(" ", "_")[:10]
        prefix = f"PM-{base}-"
        seq = Activo.objects.filter(codigo__startswith=prefix).count() + 1
        codigo = f"{prefix}{seq:04d}"
        while Activo.objects.filter(codigo=codigo).exists():
            seq += 1
            codigo = f"{prefix}{seq:04d}"
        return codigo


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
    activo_categoria = serializers.CharField(source="activo_ref.categoria", read_only=True)
    activo_ubicacion = serializers.CharField(source="activo_ref.ubicacion", read_only=True)
    activo_estado = serializers.CharField(source="activo_ref.estado", read_only=True)
    activo_criticidad = serializers.CharField(source="activo_ref.criticidad", read_only=True)
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
            "activo_categoria",
            "activo_ubicacion",
            "activo_estado",
            "activo_criticidad",
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


class BitacoraMantenimientoSerializer(serializers.ModelSerializer):
    usuario_nombre = serializers.SerializerMethodField()
    fecha_display = serializers.DateTimeField(source="fecha", format="%d/%m/%Y %H:%M", read_only=True)

    class Meta:
        model = BitacoraMantenimiento
        fields = ["id", "fecha", "fecha_display", "accion", "comentario", "usuario_nombre", "costo_adicional"]

    def get_usuario_nombre(self, obj):
        if not obj.usuario:
            return ""
        return obj.usuario.get_full_name() or obj.usuario.username


class OrdenMantenimientoDetailSerializer(OrdenMantenimientoListSerializer):
    bitacora = BitacoraMantenimientoSerializer(many=True, read_only=True)

    class Meta(OrdenMantenimientoListSerializer.Meta):
        fields = OrdenMantenimientoListSerializer.Meta.fields + ["bitacora"]


class OrdenMantenimientoSeguimientoSerializer(serializers.Serializer):
    estatus = serializers.ChoiceField(choices=OrdenMantenimiento.ESTATUS_CHOICES, required=False)
    comentario = serializers.CharField(required=False, allow_blank=True)
    costo_adicional = serializers.DecimalField(max_digits=18, decimal_places=2, required=False)
    responsable = serializers.CharField(required=False, allow_blank=True)

    def validate(self, attrs):
        if not attrs:
            raise serializers.ValidationError("No hay cambios para guardar.")
        return attrs

    def save(self, **kwargs):
        orden = self.context["orden"]
        request = self.context["request"]
        cambios = []
        estatus = self.validated_data.get("estatus")
        responsable = self.validated_data.get("responsable")
        comentario = self.validated_data.get("comentario", "").strip()
        costo_adicional = self.validated_data.get("costo_adicional")

        if estatus and estatus != orden.estatus:
            orden.estatus = estatus
            cambios.append(f"Estatus: {orden.get_estatus_display()}")
            today = timezone.localdate()
            if estatus == OrdenMantenimiento.ESTATUS_EN_PROCESO and not orden.fecha_inicio:
                orden.fecha_inicio = today
            if estatus == OrdenMantenimiento.ESTATUS_CERRADA and not orden.fecha_cierre:
                orden.fecha_cierre = today

        if responsable is not None and responsable.strip() != orden.responsable:
            orden.responsable = responsable.strip()
            cambios.append(f"Responsable: {orden.responsable or 'Sin responsable'}")

        if costo_adicional:
            orden.costo_otros = (orden.costo_otros or Decimal("0")) + costo_adicional
            cambios.append(f"Costo adicional: ${costo_adicional}")

        update_fields = ["estatus", "responsable", "fecha_inicio", "fecha_cierre", "costo_otros", "actualizado_en"]
        orden.save(update_fields=update_fields)

        accion = "Seguimiento actualizado"
        bitacora_texto = comentario
        if cambios:
            bitacora_texto = " | ".join(cambios + ([comentario] if comentario else []))
        BitacoraMantenimiento.objects.create(
            orden=orden,
            usuario=request.user,
            accion=accion,
            comentario=bitacora_texto,
            costo_adicional=costo_adicional or Decimal("0"),
        )
        return orden


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
