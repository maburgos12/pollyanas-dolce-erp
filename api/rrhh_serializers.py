from __future__ import annotations

from rest_framework import serializers

from rrhh.models import Empleado, NominaLinea, NominaPeriodo


class RRHHEmpleadoSerializer(serializers.ModelSerializer):
    class Meta:
        model = Empleado
        fields = [
            "id",
            "codigo",
            "nombre",
            "area",
            "puesto",
            "tipo_contrato",
            "fecha_ingreso",
            "salario_diario",
            "telefono",
            "email",
            "sucursal",
            "activo",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "codigo", "created_at", "updated_at"]


class RRHHNominaPeriodoSerializer(serializers.ModelSerializer):
    class Meta:
        model = NominaPeriodo
        fields = [
            "id",
            "folio",
            "tipo_periodo",
            "fecha_inicio",
            "fecha_fin",
            "estatus",
            "total_bruto",
            "total_descuentos",
            "total_neto",
            "notas",
            "created_at",
            "updated_at",
        ]
        read_only_fields = [
            "id",
            "folio",
            "total_bruto",
            "total_descuentos",
            "total_neto",
            "created_at",
            "updated_at",
        ]


class RRHHNominaLineaSerializer(serializers.ModelSerializer):
    empleado_nombre = serializers.CharField(source="empleado.nombre", read_only=True)
    empleado_codigo = serializers.CharField(source="empleado.codigo", read_only=True)

    class Meta:
        model = NominaLinea
        fields = [
            "id",
            "periodo",
            "empleado",
            "empleado_nombre",
            "empleado_codigo",
            "dias_trabajados",
            "salario_base",
            "bonos",
            "descuentos",
            "total_percepciones",
            "neto_calculado",
            "observaciones",
            "created_at",
            "updated_at",
        ]
        read_only_fields = [
            "id",
            "total_percepciones",
            "neto_calculado",
            "created_at",
            "updated_at",
            "empleado_nombre",
            "empleado_codigo",
        ]


class RRHHNominaLineaUpsertSerializer(serializers.Serializer):
    empleado_id = serializers.IntegerField(min_value=1)
    dias_trabajados = serializers.DecimalField(max_digits=6, decimal_places=2)
    salario_base = serializers.DecimalField(max_digits=12, decimal_places=2, required=False, default=0)
    bonos = serializers.DecimalField(max_digits=12, decimal_places=2, required=False, default=0)
    descuentos = serializers.DecimalField(max_digits=12, decimal_places=2, required=False, default=0)
    observaciones = serializers.CharField(required=False, allow_blank=True, default="")
