from rest_framework import serializers

from compras.models import OrdenCompra, RecepcionCompra, SolicitudCompra
from inventario.models import AjusteInventario
from recetas.models import SolicitudVenta


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


class PlanDesdePronosticoRequestSerializer(serializers.Serializer):
    periodo = serializers.CharField(max_length=7)
    fecha_produccion = serializers.DateField(required=False)
    nombre = serializers.CharField(max_length=140, required=False, allow_blank=True)
    incluir_preparaciones = serializers.BooleanField(required=False, default=False)

    def validate_periodo(self, value):
        raw = (value or "").strip()
        parts = raw.split("-")
        if len(parts) != 2:
            raise serializers.ValidationError("Usa formato YYYY-MM.")
        try:
            year = int(parts[0])
            month = int(parts[1])
        except ValueError:
            raise serializers.ValidationError("Usa formato YYYY-MM.")
        if year < 2000 or year > 2200 or month < 1 or month > 12:
            raise serializers.ValidationError("Periodo fuera de rango válido (YYYY-MM).")
        return f"{year:04d}-{month:02d}"


class PlanProduccionItemCreateSerializer(serializers.Serializer):
    receta_id = serializers.IntegerField()
    cantidad = serializers.DecimalField(max_digits=18, decimal_places=3)
    notas = serializers.CharField(max_length=160, required=False, allow_blank=True, default="")

    def validate_cantidad(self, value):
        if value <= 0:
            raise serializers.ValidationError("La cantidad debe ser mayor a 0.")
        return value


class PlanProduccionCreateSerializer(serializers.Serializer):
    nombre = serializers.CharField(max_length=140, required=False, allow_blank=True, default="")
    fecha_produccion = serializers.DateField(required=False)
    notas = serializers.CharField(required=False, allow_blank=True, default="")
    items = PlanProduccionItemCreateSerializer(many=True)

    def validate_items(self, value):
        if not value:
            raise serializers.ValidationError("Debes enviar al menos una fila en items.")
        if len(value) > 400:
            raise serializers.ValidationError("Máximo 400 renglones por plan.")
        return value


class PlanProduccionUpdateSerializer(serializers.Serializer):
    nombre = serializers.CharField(max_length=140, required=False, allow_blank=True)
    fecha_produccion = serializers.DateField(required=False)
    notas = serializers.CharField(required=False, allow_blank=True)

    def validate(self, attrs):
        attrs = super().validate(attrs)
        if not attrs:
            raise serializers.ValidationError("Envía al menos un campo para actualizar.")
        return attrs


class PlanProduccionItemUpdateSerializer(serializers.Serializer):
    receta_id = serializers.IntegerField(required=False)
    cantidad = serializers.DecimalField(max_digits=18, decimal_places=3, required=False)
    notas = serializers.CharField(max_length=160, required=False, allow_blank=True)

    def validate(self, attrs):
        attrs = super().validate(attrs)
        if not attrs:
            raise serializers.ValidationError("Envía al menos un campo para actualizar.")
        return attrs

    def validate_cantidad(self, value):
        if value <= 0:
            raise serializers.ValidationError("La cantidad debe ser mayor a 0.")
        return value


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


class ComprasSolicitudStatusSerializer(serializers.Serializer):
    estatus = serializers.ChoiceField(choices=[choice[0] for choice in SolicitudCompra.STATUS_CHOICES])


class ComprasCrearOrdenSerializer(serializers.Serializer):
    proveedor_id = serializers.IntegerField(required=False)
    estatus = serializers.ChoiceField(
        choices=[choice[0] for choice in OrdenCompra.STATUS_CHOICES],
        required=False,
        default=OrdenCompra.STATUS_BORRADOR,
    )
    fecha_emision = serializers.DateField(required=False)
    fecha_entrega_estimada = serializers.DateField(required=False)


class ComprasOrdenStatusSerializer(serializers.Serializer):
    estatus = serializers.ChoiceField(choices=[choice[0] for choice in OrdenCompra.STATUS_CHOICES])


class ComprasRecepcionCreateSerializer(serializers.Serializer):
    fecha_recepcion = serializers.DateField(required=False)
    conformidad_pct = serializers.DecimalField(max_digits=5, decimal_places=2, required=False, default=100)
    estatus = serializers.ChoiceField(
        choices=[choice[0] for choice in RecepcionCompra.STATUS_CHOICES],
        required=False,
        default=RecepcionCompra.STATUS_PENDIENTE,
    )
    observaciones = serializers.CharField(max_length=255, required=False, allow_blank=True, default="")

    def validate_conformidad_pct(self, value):
        if value < 0 or value > 100:
            raise serializers.ValidationError("conformidad_pct debe estar entre 0 y 100.")
        return value


class ComprasRecepcionStatusSerializer(serializers.Serializer):
    estatus = serializers.ChoiceField(choices=[choice[0] for choice in RecepcionCompra.STATUS_CHOICES])


class ForecastEstadisticoRequestSerializer(serializers.Serializer):
    alcance = serializers.ChoiceField(choices=["mes", "semana", "fin_semana"], required=False, default="mes")
    periodo = serializers.CharField(max_length=7, required=False, allow_blank=True)
    fecha_base = serializers.DateField(required=False)
    sucursal_id = serializers.IntegerField(required=False, allow_null=True)
    incluir_preparaciones = serializers.BooleanField(required=False, default=False)
    safety_pct = serializers.DecimalField(max_digits=6, decimal_places=2, required=False, default=0)
    include_solicitud_compare = serializers.BooleanField(required=False, default=True)
    top = serializers.IntegerField(required=False, min_value=1, max_value=500, default=120)

    def validate_safety_pct(self, value):
        if value < -30 or value > 100:
            raise serializers.ValidationError("safety_pct debe estar entre -30 y 100.")
        return value

    def validate_periodo(self, value):
        raw = (value or "").strip()
        if raw == "":
            return raw
        parts = raw.split("-")
        if len(parts) != 2:
            raise serializers.ValidationError("Usa formato YYYY-MM.")
        try:
            year = int(parts[0])
            month = int(parts[1])
        except ValueError:
            raise serializers.ValidationError("Usa formato YYYY-MM.")
        if year < 2000 or year > 2200 or month < 1 or month > 12:
            raise serializers.ValidationError("Periodo fuera de rango válido (YYYY-MM).")
        return f"{year:04d}-{month:02d}"


class ForecastBacktestRequestSerializer(serializers.Serializer):
    alcance = serializers.ChoiceField(choices=["mes", "semana", "fin_semana"], required=False, default="mes")
    fecha_base = serializers.DateField(required=False)
    sucursal_id = serializers.IntegerField(required=False, allow_null=True)
    incluir_preparaciones = serializers.BooleanField(required=False, default=False)
    safety_pct = serializers.DecimalField(max_digits=6, decimal_places=2, required=False, default=0)
    periods = serializers.IntegerField(required=False, min_value=1, max_value=12, default=3)
    top = serializers.IntegerField(required=False, min_value=1, max_value=50, default=10)

    def validate_safety_pct(self, value):
        if value < -30 or value > 100:
            raise serializers.ValidationError("safety_pct debe estar entre -30 y 100.")
        return value


class PronosticoVentaBulkRowSerializer(serializers.Serializer):
    receta_id = serializers.IntegerField(required=False)
    receta = serializers.CharField(max_length=220, required=False, allow_blank=True)
    codigo_point = serializers.CharField(max_length=100, required=False, allow_blank=True)
    periodo = serializers.CharField(max_length=7)
    cantidad = serializers.DecimalField(max_digits=18, decimal_places=3)

    def validate_cantidad(self, value):
        if value < 0:
            raise serializers.ValidationError("La cantidad no puede ser negativa.")
        return value

    def validate_periodo(self, value):
        raw = (value or "").strip()
        parts = raw.split("-")
        if len(parts) != 2:
            raise serializers.ValidationError("Usa formato YYYY-MM.")
        try:
            year = int(parts[0])
            month = int(parts[1])
        except ValueError:
            raise serializers.ValidationError("Usa formato YYYY-MM.")
        if year < 2000 or year > 2200 or month < 1 or month > 12:
            raise serializers.ValidationError("Periodo fuera de rango válido (YYYY-MM).")
        return f"{year:04d}-{month:02d}"

    def validate(self, attrs):
        attrs = super().validate(attrs)
        has_receta_ref = bool(attrs.get("receta_id")) or bool((attrs.get("receta") or "").strip()) or bool(
            (attrs.get("codigo_point") or "").strip()
        )
        if not has_receta_ref:
            raise serializers.ValidationError("Debes enviar receta_id o receta/codigo_point.")
        return attrs


class PronosticoVentaBulkSerializer(serializers.Serializer):
    rows = PronosticoVentaBulkRowSerializer(many=True)
    modo = serializers.ChoiceField(choices=["replace", "accumulate"], required=False, default="replace")
    fuente = serializers.CharField(max_length=40, required=False, allow_blank=True, default="API_PRON_BULK")
    dry_run = serializers.BooleanField(required=False, default=True)
    stop_on_error = serializers.BooleanField(required=False, default=False)
    top = serializers.IntegerField(required=False, min_value=1, max_value=500, default=120)

    def validate_rows(self, value):
        if not value:
            raise serializers.ValidationError("Debes enviar al menos una fila.")
        if len(value) > 5000:
            raise serializers.ValidationError("Máximo 5000 filas por request.")
        return value


class SolicitudVentaUpsertSerializer(serializers.Serializer):
    receta_id = serializers.IntegerField()
    sucursal_id = serializers.IntegerField(required=False, allow_null=True)
    alcance = serializers.ChoiceField(choices=["mes", "semana", "fin_semana"], required=False, default="mes")
    periodo = serializers.CharField(max_length=7, required=False, allow_blank=True)
    fecha_base = serializers.DateField(required=False)
    fecha_inicio = serializers.DateField(required=False)
    fecha_fin = serializers.DateField(required=False)
    cantidad = serializers.DecimalField(max_digits=18, decimal_places=3)
    fuente = serializers.CharField(max_length=40, required=False, allow_blank=True, default="API_SOL_VENTAS")

    def validate_cantidad(self, value):
        if value <= 0:
            raise serializers.ValidationError("La cantidad debe ser mayor a 0.")
        return value

    def validate_periodo(self, value):
        raw = (value or "").strip()
        if raw == "":
            return raw
        parts = raw.split("-")
        if len(parts) != 2:
            raise serializers.ValidationError("Usa formato YYYY-MM.")
        try:
            year = int(parts[0])
            month = int(parts[1])
        except ValueError:
            raise serializers.ValidationError("Usa formato YYYY-MM.")
        if year < 2000 or year > 2200 or month < 1 or month > 12:
            raise serializers.ValidationError("Periodo fuera de rango válido (YYYY-MM).")
        return f"{year:04d}-{month:02d}"


class SolicitudVentaAplicarForecastSerializer(serializers.Serializer):
    alcance = serializers.ChoiceField(choices=["mes", "semana", "fin_semana"], required=False, default="mes")
    periodo = serializers.CharField(max_length=7, required=False, allow_blank=True)
    fecha_base = serializers.DateField(required=False)
    sucursal_id = serializers.IntegerField(required=True)
    incluir_preparaciones = serializers.BooleanField(required=False, default=False)
    safety_pct = serializers.DecimalField(max_digits=6, decimal_places=2, required=False, default=0)
    modo = serializers.ChoiceField(
        choices=["desviadas", "sobre", "bajo", "receta", "todas"],
        required=False,
        default="desviadas",
    )
    receta_id = serializers.IntegerField(required=False)
    fuente = serializers.CharField(max_length=40, required=False, allow_blank=True, default="API_FORECAST_ADJUST")
    top = serializers.IntegerField(required=False, min_value=1, max_value=500, default=120)

    def validate_safety_pct(self, value):
        if value < -30 or value > 100:
            raise serializers.ValidationError("safety_pct debe estar entre -30 y 100.")
        return value

    def validate_periodo(self, value):
        raw = (value or "").strip()
        if raw == "":
            return raw
        parts = raw.split("-")
        if len(parts) != 2:
            raise serializers.ValidationError("Usa formato YYYY-MM.")
        try:
            year = int(parts[0])
            month = int(parts[1])
        except ValueError:
            raise serializers.ValidationError("Usa formato YYYY-MM.")
        if year < 2000 or year > 2200 or month < 1 or month > 12:
            raise serializers.ValidationError("Periodo fuera de rango válido (YYYY-MM).")
        return f"{year:04d}-{month:02d}"

    def validate(self, attrs):
        attrs = super().validate(attrs)
        if attrs.get("modo") == "receta" and not attrs.get("receta_id"):
            raise serializers.ValidationError({"receta_id": "Es requerido cuando modo=receta."})
        return attrs


class VentaHistoricaBulkRowSerializer(serializers.Serializer):
    receta_id = serializers.IntegerField(required=False)
    receta = serializers.CharField(max_length=220, required=False, allow_blank=True)
    codigo_point = serializers.CharField(max_length=100, required=False, allow_blank=True)
    fecha = serializers.DateField()
    cantidad = serializers.DecimalField(max_digits=18, decimal_places=3)
    sucursal_id = serializers.IntegerField(required=False, allow_null=True)
    sucursal = serializers.CharField(max_length=120, required=False, allow_blank=True)
    sucursal_codigo = serializers.CharField(max_length=40, required=False, allow_blank=True)
    tickets = serializers.IntegerField(required=False, default=0, min_value=0)
    monto_total = serializers.DecimalField(max_digits=18, decimal_places=2, required=False, allow_null=True)

    def validate_cantidad(self, value):
        if value < 0:
            raise serializers.ValidationError("La cantidad no puede ser negativa.")
        return value

    def validate(self, attrs):
        attrs = super().validate(attrs)
        has_receta_ref = bool(attrs.get("receta_id")) or bool((attrs.get("receta") or "").strip()) or bool(
            (attrs.get("codigo_point") or "").strip()
        )
        if not has_receta_ref:
            raise serializers.ValidationError("Debes enviar receta_id o receta/codigo_point.")
        return attrs


class VentaHistoricaBulkSerializer(serializers.Serializer):
    rows = VentaHistoricaBulkRowSerializer(many=True)
    modo = serializers.ChoiceField(choices=["replace", "accumulate"], required=False, default="replace")
    fuente = serializers.CharField(max_length=40, required=False, allow_blank=True, default="API_VENTAS_BULK")
    sucursal_default_id = serializers.IntegerField(required=False, allow_null=True)
    dry_run = serializers.BooleanField(required=False, default=True)
    stop_on_error = serializers.BooleanField(required=False, default=False)
    top = serializers.IntegerField(required=False, min_value=1, max_value=500, default=120)

    def validate_rows(self, value):
        if not value:
            raise serializers.ValidationError("Debes enviar al menos una fila.")
        if len(value) > 5000:
            raise serializers.ValidationError("Máximo 5000 filas por request.")
        return value


class SolicitudVentaBulkRowSerializer(serializers.Serializer):
    receta_id = serializers.IntegerField(required=False)
    receta = serializers.CharField(max_length=220, required=False, allow_blank=True)
    codigo_point = serializers.CharField(max_length=100, required=False, allow_blank=True)
    sucursal_id = serializers.IntegerField(required=False, allow_null=True)
    sucursal = serializers.CharField(max_length=120, required=False, allow_blank=True)
    sucursal_codigo = serializers.CharField(max_length=40, required=False, allow_blank=True)
    alcance = serializers.ChoiceField(choices=["mes", "semana", "fin_semana"], required=False, default="mes")
    periodo = serializers.CharField(max_length=7, required=False, allow_blank=True)
    fecha_base = serializers.DateField(required=False)
    fecha_inicio = serializers.DateField(required=False)
    fecha_fin = serializers.DateField(required=False)
    cantidad = serializers.DecimalField(max_digits=18, decimal_places=3)

    def validate_cantidad(self, value):
        if value <= 0:
            raise serializers.ValidationError("La cantidad debe ser mayor a 0.")
        return value

    def validate_periodo(self, value):
        raw = (value or "").strip()
        if raw == "":
            return raw
        parts = raw.split("-")
        if len(parts) != 2:
            raise serializers.ValidationError("Usa formato YYYY-MM.")
        try:
            year = int(parts[0])
            month = int(parts[1])
        except ValueError:
            raise serializers.ValidationError("Usa formato YYYY-MM.")
        if year < 2000 or year > 2200 or month < 1 or month > 12:
            raise serializers.ValidationError("Periodo fuera de rango válido (YYYY-MM).")
        return f"{year:04d}-{month:02d}"

    def validate(self, attrs):
        attrs = super().validate(attrs)
        has_receta_ref = bool(attrs.get("receta_id")) or bool((attrs.get("receta") or "").strip()) or bool(
            (attrs.get("codigo_point") or "").strip()
        )
        if not has_receta_ref:
            raise serializers.ValidationError("Debes enviar receta_id o receta/codigo_point.")
        return attrs


class SolicitudVentaBulkSerializer(serializers.Serializer):
    rows = SolicitudVentaBulkRowSerializer(many=True)
    modo = serializers.ChoiceField(choices=["replace", "accumulate"], required=False, default="replace")
    fuente = serializers.CharField(max_length=40, required=False, allow_blank=True, default="API_SOL_BULK")
    sucursal_default_id = serializers.IntegerField(required=False, allow_null=True)
    dry_run = serializers.BooleanField(required=False, default=True)
    stop_on_error = serializers.BooleanField(required=False, default=False)
    top = serializers.IntegerField(required=False, min_value=1, max_value=500, default=120)

    def validate_rows(self, value):
        if not value:
            raise serializers.ValidationError("Debes enviar al menos una fila.")
        if len(value) > 5000:
            raise serializers.ValidationError("Máximo 5000 filas por request.")
        return value


class InventarioAjusteCreateSerializer(serializers.Serializer):
    insumo_id = serializers.IntegerField()
    cantidad_sistema = serializers.DecimalField(max_digits=18, decimal_places=3)
    cantidad_fisica = serializers.DecimalField(max_digits=18, decimal_places=3)
    motivo = serializers.CharField(max_length=255, required=False, allow_blank=True, default="")
    aplicar_inmediato = serializers.BooleanField(required=False, default=False)
    comentario_revision = serializers.CharField(max_length=255, required=False, allow_blank=True, default="")

    def validate(self, attrs):
        attrs = super().validate(attrs)
        if attrs["cantidad_sistema"] < 0 or attrs["cantidad_fisica"] < 0:
            raise serializers.ValidationError("cantidad_sistema y cantidad_fisica no pueden ser negativas.")
        attrs["motivo"] = (attrs.get("motivo") or "").strip() or "Sin motivo"
        attrs["comentario_revision"] = (attrs.get("comentario_revision") or "").strip()[:255]
        return attrs


class InventarioAjusteDecisionSerializer(serializers.Serializer):
    action = serializers.ChoiceField(choices=["approve", "apply", "reject"])
    comentario_revision = serializers.CharField(max_length=255, required=False, allow_blank=True, default="")

    def validate(self, attrs):
        attrs = super().validate(attrs)
        attrs["action"] = str(attrs.get("action") or "").strip().lower()
        attrs["comentario_revision"] = (attrs.get("comentario_revision") or "").strip()[:255]
        return attrs


class InventarioAjusteResponseSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    folio = serializers.CharField()
    insumo_id = serializers.IntegerField()
    insumo = serializers.CharField()
    cantidad_sistema = serializers.DecimalField(max_digits=18, decimal_places=3)
    cantidad_fisica = serializers.DecimalField(max_digits=18, decimal_places=3)
    delta = serializers.DecimalField(max_digits=18, decimal_places=3)
    motivo = serializers.CharField()
    estatus = serializers.ChoiceField(choices=[choice[0] for choice in AjusteInventario.STATUS_CHOICES])
    solicitado_por = serializers.CharField(allow_blank=True)
    aprobado_por = serializers.CharField(allow_blank=True)
    comentario_revision = serializers.CharField(allow_blank=True)
    creado_en = serializers.DateTimeField()
    aprobado_en = serializers.DateTimeField(allow_null=True)
    aplicado_en = serializers.DateTimeField(allow_null=True)
