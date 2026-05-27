from django.contrib import admin
from django.utils import timezone

from .models import (
    AsistenciaEmpleado,
    BonoEsquema,
    Empleado,
    EmpleadoBaja,
    HoraExtra,
    ImportacionChecador,
    ImportacionNominaContpaq,
    NominaConceptoLinea,
    NominaImportacion,
    NominaLinea,
    NominaPeriodo,
    PermisoSalida,
    PlantillaAutorizada,
    Prestamo,
    PrestamoCuota,
    Turno,
    VacanteRRHH,
)


@admin.register(BonoEsquema)
class BonoEsquemaAdmin(admin.ModelAdmin):
    list_display = ("codigo", "nombre", "departamento", "area", "activo", "actualizado_en")
    list_filter = ("activo", "departamento", "area")
    search_fields = ("codigo", "nombre", "area", "descripcion")


@admin.register(Empleado)
class EmpleadoAdmin(admin.ModelAdmin):
    list_display = (
        "codigo",
        "nombre",
        "departamento_origen",
        "departamento",
        "area",
        "puesto",
        "puesto_operativo",
        "jefe_directo",
        "tipo_personal",
        "participa_bonos_ventas",
        "participa_bonos_produccion",
        "salario_diario",
        "activo",
    )
    list_filter = (
        "activo",
        "tipo_contrato",
        "departamento_origen",
        "departamento",
        "puesto_operativo",
        "tipo_personal",
        "participa_bonos_ventas",
        "participa_bonos_produccion",
        "area",
    )
    search_fields = (
        "codigo",
        "nombre",
        "rfc",
        "curp",
        "nss",
        "telefono",
        "email",
        "area",
        "puesto",
        "puesto_operativo",
        "jefe_directo__nombre",
    )


class NominaConceptoLineaInline(admin.TabularInline):
    model = NominaConceptoLinea
    extra = 0


class NominaLineaInline(admin.TabularInline):
    model = NominaLinea
    extra = 0


@admin.register(NominaPeriodo)
class NominaPeriodoAdmin(admin.ModelAdmin):
    list_display = ("folio", "tipo_periodo", "fecha_inicio", "fecha_fin", "estatus", "total_neto")
    list_filter = ("tipo_periodo", "estatus")
    search_fields = ("folio",)
    inlines = [NominaLineaInline]


@admin.register(NominaLinea)
class NominaLineaAdmin(admin.ModelAdmin):
    list_display = (
        "periodo",
        "empleado",
        "dias_trabajados",
        "horas_trabajadas",
        "horas_extra",
        "total_percepciones",
        "descuentos",
        "neto_calculado",
    )
    list_filter = ("periodo__tipo_periodo",)
    search_fields = ("periodo__folio", "empleado__nombre", "empleado__codigo")
    inlines = [NominaConceptoLineaInline]


@admin.register(NominaConceptoLinea)
class NominaConceptoLineaAdmin(admin.ModelAdmin):
    list_display = ("linea", "tipo", "codigo_concepto", "nombre", "valor", "importe")
    list_filter = ("tipo", "codigo_concepto")
    search_fields = ("linea__periodo__folio", "linea__empleado__nombre", "nombre", "codigo_concepto")


@admin.register(NominaImportacion)
class NominaImportacionAdmin(admin.ModelAdmin):
    list_display = (
        "archivo_nombre",
        "estatus",
        "periodo",
        "empleados_detectados",
        "total_percepciones",
        "total_deducciones",
        "total_neto",
        "created_at",
    )
    list_filter = ("estatus", "created_at")
    search_fields = ("archivo_nombre", "archivo_hash", "periodo__folio")


@admin.register(EmpleadoBaja)
class EmpleadoBajaAdmin(admin.ModelAdmin):
    list_display = ("fecha_baja", "nombre", "area", "motivo", "fecha_ingreso", "antiguedad_meses", "creado_por")
    list_filter = ("motivo", "area", "fecha_baja")
    search_fields = ("nombre", "empleado__codigo", "empleado__nombre", "observacion")


@admin.register(PlantillaAutorizada)
class PlantillaAutorizadaAdmin(admin.ModelAdmin):
    list_display = ("anio", "mes", "area", "puesto", "cantidad", "actualizado_por", "actualizado_en")
    list_filter = ("anio", "mes", "area")
    search_fields = ("area", "puesto", "notas")


@admin.register(VacanteRRHH)
class VacanteRRHHAdmin(admin.ModelAdmin):
    list_display = ("fecha_solicitada", "area", "puesto", "estado", "fecha_cubierta", "dias_en_cubrir", "creado_por")
    list_filter = ("estado", "area", "fecha_solicitada")
    search_fields = ("area", "puesto", "motivo_no_cubierta", "sugerencias")


@admin.register(AsistenciaEmpleado)
class AsistenciaAdmin(admin.ModelAdmin):
    list_display = ("empleado", "fecha", "entrada", "salida", "minutos_trabajados", "fuente")
    list_filter = ("fuente", "sucursal", "fecha")
    search_fields = ("empleado__nombre", "empleado__codigo")


@admin.register(HoraExtra)
class HoraExtraAdmin(admin.ModelAdmin):
    list_display = ("empleado", "fecha", "horas", "monto_calculado", "estado", "autorizado_por")
    list_filter = ("estado", "fecha")
    search_fields = ("empleado__nombre", "empleado__codigo")
    actions = ["autorizar_seleccionadas"]

    def autorizar_seleccionadas(self, request, queryset):
        from .services import calcular_monto_hora_extra

        for he in queryset.filter(estado=HoraExtra.ESTADO_PENDIENTE):
            he.estado = HoraExtra.ESTADO_AUTORIZADO
            he.autorizado_por = request.user
            calcular_monto_hora_extra(he)
            he.save(update_fields=["estado", "autorizado_por"])
        self.message_user(request, "Horas extra autorizadas.")

    autorizar_seleccionadas.short_description = "Autorizar horas extra seleccionadas"


@admin.register(PermisoSalida)
class PermisoAdmin(admin.ModelAdmin):
    list_display = (
        "folio",
        "empleado",
        "tipo",
        "fecha_inicio",
        "origen_solicitud",
        "estado_jefe",
        "requiere_direccion",
        "estado_direccion",
        "estado",
        "goce_sueldo",
        "autorizado_jefe_por",
        "autorizado_direccion_por",
        "autorizado_por",
    )
    list_filter = (
        "tipo",
        "origen_solicitud",
        "estado_jefe",
        "requiere_direccion",
        "estado_direccion",
        "estado",
        "goce_sueldo",
        "fecha_inicio",
    )
    search_fields = ("folio", "empleado__nombre", "empleado__codigo", "motivo")
    readonly_fields = ("folio",)


@admin.register(Turno)
class TurnoAdmin(admin.ModelAdmin):
    list_display = ("nombre", "hora_entrada", "hora_salida", "tolerancia_minutos", "activo")
    list_filter = ("activo",)


@admin.register(ImportacionChecador)
class ImportacionAdmin(admin.ModelAdmin):
    list_display = ("creado_en", "metodo", "registros_procesados", "errores", "creado_por")
    list_filter = ("metodo", "creado_en")
    readonly_fields = ("log",)


class PrestamoCuotaInline(admin.TabularInline):
    model = PrestamoCuota
    extra = 0
    readonly_fields = ("fuente", "fecha_cobro", "registrado_por")
    fields = ("numero_quincena", "fecha_quincena", "monto_esperado", "monto_cobrado", "estado", "fuente", "nota")


@admin.register(Prestamo)
class PrestamoAdmin(admin.ModelAdmin):
    list_display = ("folio", "empleado", "jefe_directo", "importe", "saldo_actual", "num_quincenas", "estado", "fecha_solicitud")
    list_filter = ("estado", "metodo_pago", "fecha_solicitud")
    search_fields = ("empleado__nombre", "empleado__codigo", "folio")
    readonly_fields = ("folio", "saldo_actual", "creado_en", "actualizado_en")
    inlines = [PrestamoCuotaInline]
    actions = ["autorizar_jefe_bulk"]

    def autorizar_jefe_bulk(self, request, queryset):
        for prestamo in queryset.filter(estado=Prestamo.ESTADO_SOLICITADO):
            prestamo.firma_jefe = True
            prestamo.autorizado_jefe = request.user
            prestamo.fecha_auth_jefe = timezone.now()
            prestamo.estado = Prestamo.ESTADO_AUTORIZADO
            prestamo.save(update_fields=["firma_jefe", "autorizado_jefe", "fecha_auth_jefe", "estado", "actualizado_en"])
        self.message_user(request, "Préstamos autorizados por jefe.")

    autorizar_jefe_bulk.short_description = "Autorizar como jefe inmediato"


@admin.register(PrestamoCuota)
class PrestamoCuotaAdmin(admin.ModelAdmin):
    list_display = ("prestamo", "numero_quincena", "fecha_quincena", "monto_esperado", "monto_cobrado", "estado", "fuente")
    list_filter = ("estado", "fuente", "fecha_quincena")
    search_fields = ("prestamo__folio", "prestamo__empleado__nombre", "prestamo__empleado__codigo")


@admin.register(ImportacionNominaContpaq)
class ImportContpaqAdmin(admin.ModelAdmin):
    list_display = (
        "creado_en",
        "periodo_inicio",
        "periodo_fin",
        "empleados_leidos",
        "prestamos_aplicados",
        "diferencias_detectadas",
        "creado_por",
    )
    readonly_fields = ("log",)
