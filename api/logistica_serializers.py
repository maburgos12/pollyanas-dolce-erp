from __future__ import annotations

from rest_framework import serializers

from logistica.models import EntregaRuta, RutaEntrega


class LogisticaRutaSerializer(serializers.ModelSerializer):
    class Meta:
        model = RutaEntrega
        fields = [
            "id",
            "folio",
            "nombre",
            "fecha_ruta",
            "chofer",
            "unidad",
            "estatus",
            "km_estimado",
            "notas",
            "total_entregas",
            "entregas_completadas",
            "entregas_incidencia",
            "monto_estimado_total",
            "created_at",
            "updated_at",
        ]
        read_only_fields = [
            "id",
            "folio",
            "total_entregas",
            "entregas_completadas",
            "entregas_incidencia",
            "monto_estimado_total",
            "created_at",
            "updated_at",
        ]


class LogisticaEntregaSerializer(serializers.ModelSerializer):
    pedido_folio = serializers.CharField(source="pedido.folio", read_only=True)

    class Meta:
        model = EntregaRuta
        fields = [
            "id",
            "ruta",
            "secuencia",
            "pedido",
            "pedido_folio",
            "cliente_nombre",
            "direccion",
            "contacto",
            "telefono",
            "ventana_inicio",
            "ventana_fin",
            "estatus",
            "monto_estimado",
            "comentario",
            "evidencia_url",
            "entregado_at",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "ruta", "entregado_at", "created_at", "updated_at", "pedido_folio"]


class LogisticaEntregaCreateSerializer(serializers.Serializer):
    secuencia = serializers.IntegerField(min_value=1, required=False, default=1)
    pedido_id = serializers.IntegerField(required=False, allow_null=True)
    cliente_nombre = serializers.CharField(required=False, allow_blank=True, default="")
    direccion = serializers.CharField(required=False, allow_blank=True, default="")
    contacto = serializers.CharField(required=False, allow_blank=True, default="")
    telefono = serializers.CharField(required=False, allow_blank=True, default="")
    ventana_inicio = serializers.DateTimeField(required=False, allow_null=True)
    ventana_fin = serializers.DateTimeField(required=False, allow_null=True)
    estatus = serializers.ChoiceField(choices=EntregaRuta.ESTATUS_CHOICES, required=False, default=EntregaRuta.ESTATUS_PENDIENTE)
    monto_estimado = serializers.DecimalField(max_digits=12, decimal_places=2, required=False, default=0)
    comentario = serializers.CharField(required=False, allow_blank=True, default="")
