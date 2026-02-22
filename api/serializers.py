from rest_framework import serializers

from compras.models import OrdenCompra, SolicitudCompra


class MRPRequestSerializer(serializers.Serializer):
    receta_id = serializers.IntegerField()
    multiplicador = serializers.DecimalField(max_digits=18, decimal_places=6, required=False, default=1)

class MRPItemSerializer(serializers.Serializer):
    insumo_id = serializers.IntegerField(allow_null=True)
    nombre = serializers.CharField()
    cantidad = serializers.DecimalField(max_digits=18, decimal_places=6)
    unidad = serializers.CharField(allow_blank=True)
    costo = serializers.FloatField()

class MRPResponseSerializer(serializers.Serializer):
    receta_id = serializers.IntegerField()
    receta_nombre = serializers.CharField()
    multiplicador = serializers.DecimalField(max_digits=18, decimal_places=6)
    costo_total = serializers.FloatField()
    items = MRPItemSerializer(many=True)


class RecetaCostoVersionSerializer(serializers.Serializer):
    version_num = serializers.IntegerField()
    creado_en = serializers.DateTimeField()
    fuente = serializers.CharField()
    lote_referencia = serializers.DecimalField(max_digits=18, decimal_places=6)
    driver_scope = serializers.CharField(allow_blank=True)
    driver_nombre = serializers.CharField(allow_blank=True)
    mo_pct = serializers.DecimalField(max_digits=8, decimal_places=4)
    indirecto_pct = serializers.DecimalField(max_digits=8, decimal_places=4)
    mo_fijo = serializers.DecimalField(max_digits=18, decimal_places=6)
    indirecto_fijo = serializers.DecimalField(max_digits=18, decimal_places=6)
    costo_mp = serializers.DecimalField(max_digits=18, decimal_places=6)
    costo_mo = serializers.DecimalField(max_digits=18, decimal_places=6)
    costo_indirecto = serializers.DecimalField(max_digits=18, decimal_places=6)
    costo_total = serializers.DecimalField(max_digits=18, decimal_places=6)
    rendimiento_cantidad = serializers.DecimalField(max_digits=18, decimal_places=6, allow_null=True)
    rendimiento_unidad = serializers.CharField(allow_blank=True)
    costo_por_unidad_rendimiento = serializers.DecimalField(max_digits=18, decimal_places=6, allow_null=True)


class RecetaCostoHistoricoResponseSerializer(serializers.Serializer):
    receta_id = serializers.IntegerField()
    receta_nombre = serializers.CharField()
    puntos = RecetaCostoVersionSerializer(many=True)
    comparativo = serializers.DictField(required=False)


class MRPRequerimientoItemInputSerializer(serializers.Serializer):
    receta_id = serializers.IntegerField()
    cantidad = serializers.DecimalField(max_digits=18, decimal_places=6)


class MRPRequerimientosRequestSerializer(serializers.Serializer):
    plan_id = serializers.IntegerField(required=False)
    fecha_referencia = serializers.DateField(required=False)
    periodo = serializers.CharField(max_length=7, required=False, allow_blank=True)
    periodo_tipo = serializers.ChoiceField(
        choices=["mes", "q1", "q2"],
        required=False,
        default="mes",
    )
    items = MRPRequerimientoItemInputSerializer(many=True, required=False)

    def validate(self, attrs):
        plan_id = attrs.get("plan_id")
        items = attrs.get("items") or []
        periodo_raw = (attrs.get("periodo") or "").strip()

        selected_sources = int(bool(plan_id)) + int(bool(items)) + int(bool(periodo_raw))
        if selected_sources == 0:
            raise serializers.ValidationError("Debes enviar uno de: plan_id, items o periodo.")
        if selected_sources > 1:
            raise serializers.ValidationError(
                "Envía una sola fuente por request: plan_id, items o periodo (no combinados)."
            )

        if periodo_raw:
            parts = periodo_raw.split("-")
            if len(parts) != 2:
                raise serializers.ValidationError({"periodo": "Usa formato YYYY-MM."})
            try:
                year = int(parts[0])
                month = int(parts[1])
            except ValueError:
                raise serializers.ValidationError({"periodo": "Usa formato YYYY-MM."})
            if year < 2000 or year > 2200 or month < 1 or month > 12:
                raise serializers.ValidationError({"periodo": "Periodo fuera de rango válido (YYYY-MM)."})
            attrs["periodo"] = f"{year:04d}-{month:02d}"

        return attrs


class ComprasSolicitudCreateSerializer(serializers.Serializer):
    area = serializers.CharField(max_length=120)
    solicitante = serializers.CharField(max_length=120, required=False, allow_blank=True)
    insumo_id = serializers.IntegerField()
    cantidad = serializers.DecimalField(max_digits=18, decimal_places=3)
    fecha_requerida = serializers.DateField(required=False)
    estatus = serializers.ChoiceField(
        choices=[choice[0] for choice in SolicitudCompra.STATUS_CHOICES],
        required=False,
        default=SolicitudCompra.STATUS_BORRADOR,
    )
    auto_crear_orden = serializers.BooleanField(required=False, default=False)
    orden_estatus = serializers.ChoiceField(
        choices=[choice[0] for choice in OrdenCompra.STATUS_CHOICES],
        required=False,
        default=OrdenCompra.STATUS_BORRADOR,
    )

    def validate_cantidad(self, value):
        if value <= 0:
            raise serializers.ValidationError("La cantidad debe ser mayor a 0.")
        return value

    def validate(self, attrs):
        attrs = super().validate(attrs)
        if attrs.get("auto_crear_orden") and attrs.get("estatus") != SolicitudCompra.STATUS_APROBADA:
            raise serializers.ValidationError(
                {"auto_crear_orden": "Para crear OC automática, la solicitud debe ir en estatus APROBADA."}
            )
        return attrs
