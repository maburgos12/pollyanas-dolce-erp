from django.contrib import admin

from .models import (
    AjusteAsistencia,
    AsistenciaEmpleado,
    BonoEsquema,
    CatalogoFuncionOperativa,
    Empleado,
    EmpleadoBaja,
    EmpleadoIdentidadPendiente,
    HoraExtra,
    IncidenciaAsistencia,
    IncidenciaAsistenciaBitacora,
    ImportacionChecador,
    ImportacionNominaContpaq,
    NominaConceptoLinea,
    NominaImportacion,
    NominaLinea,
    NominaPeriodo,
    MovimientoVacaciones,
    PermisoSalida,
    PermisoSalidaCambio,
    PlantillaAutorizada,
    PoliticaVacaciones,
    PrenominaCorte,
    PrenominaEmpleadoResumen,
    PrenominaEquivalenciaCONTPAQi,
    PrenominaMovimiento,
    Prestamo,
    PrestamoCuota,
    SolicitudVacaciones,
    SuspensionEmpleado,
    Turno,
    VacanteCobertura,
    VacanteMovimiento,
    VacanteRRHH,
    VacanteSeguimiento,
)


@admin.register(CatalogoFuncionOperativa)
class CatalogoFuncionOperativaAdmin(admin.ModelAdmin):
    list_display = (
        "codigo",
        "etiqueta",
        "departamento_origen",
        "departamento_actual",
        "puesto_operativo",
        "nivel_organizacional",
        "activo",
        "sistema",
    )
    list_filter = ("activo", "sistema", "departamento_origen", "departamento_actual", "nivel_organizacional")
    search_fields = ("codigo", "etiqueta", "puesto_operativo")


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
        "nivel_organizacional",
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
        "nivel_organizacional",
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
        "nivel_organizacional",
        "jefe_directo__nombre",
    )


@admin.register(EmpleadoIdentidadPendiente)
class EmpleadoIdentidadPendienteAdmin(admin.ModelAdmin):
    list_display = ("codigo_externo", "nombre_externo", "fuente", "empleado_sugerido", "estado", "actualizado_en")
    list_filter = ("fuente", "estado")
    search_fields = ("codigo_externo", "nombre_externo", "empleado_sugerido__nombre", "empleado_sugerido__codigo")
    readonly_fields = ("nombre_normalizado", "creado_en", "actualizado_en", "resuelto_en")


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
    list_display = (
        "folio",
        "fecha_solicitada",
        "area",
        "puesto",
        "cantidad_solicitada",
        "estado",
        "prioridad",
        "tipo_autorizacion",
        "autorizador_asignado",
        "fecha_cubierta",
        "dias_en_cubrir",
        "solicitado_por",
    )
    list_filter = (
        "estado",
        "prioridad",
        "tipo_solicitud",
        "tipo_autorizacion",
        "requiere_direccion",
        "departamento",
        "area",
        "fecha_solicitada",
    )
    search_fields = ("folio", "area", "puesto", "motivo_solicitud", "motivo_no_cubierta", "sugerencias")


@admin.register(VacanteMovimiento)
class VacanteMovimientoAdmin(admin.ModelAdmin):
    list_display = ("vacante", "estado_anterior", "estado_nuevo", "actor", "creado_en")
    list_filter = ("estado_nuevo", "creado_en")
    search_fields = ("vacante__folio", "vacante__area", "vacante__puesto", "comentario")


@admin.register(VacanteCobertura)
class VacanteCoberturaAdmin(admin.ModelAdmin):
    list_display = ("vacante", "empleado", "fecha_cobertura", "creado_por")
    list_filter = ("fecha_cobertura",)
    search_fields = ("vacante__folio", "empleado__nombre", "nota")


@admin.register(VacanteSeguimiento)
class VacanteSeguimientoAdmin(admin.ModelAdmin):
    list_display = ("vacante", "etapa", "candidato", "fecha", "creado_por", "creado_en")
    list_filter = ("etapa", "fecha", "creado_en")
    search_fields = ("vacante__folio", "vacante__area", "vacante__puesto", "candidato", "comentario")


@admin.register(AsistenciaEmpleado)
class AsistenciaAdmin(admin.ModelAdmin):
    list_display = (
        "empleado",
        "fecha",
        "entrada",
        "salida_comida",
        "regreso_comida",
        "salida",
        "minutos_comida",
        "minutos_trabajados",
        "fuente",
    )
    list_filter = ("fuente", "sucursal", "fecha")
    search_fields = ("empleado__nombre", "empleado__codigo")


@admin.register(HoraExtra)
class HoraExtraAdmin(admin.ModelAdmin):
    list_display = ("empleado", "fecha", "horas", "monto_calculado", "estado", "jefe_directo", "autorizado_por")
    list_filter = ("estado", "fecha", "jefe_directo")
    search_fields = ("empleado__nombre", "empleado__codigo")


@admin.register(IncidenciaAsistencia)
class IncidenciaAsistenciaAdmin(admin.ModelAdmin):
    list_display = (
        "empleado",
        "fecha",
        "tipo",
        "estado",
        "severidad",
        "minutos",
        "goce_sueldo",
        "editado_manual",
        "actualizado_en",
    )
    list_filter = ("tipo", "estado", "severidad", "fecha", "goce_sueldo", "editado_manual")
    search_fields = ("empleado__nombre", "empleado__codigo", "detalle")
    readonly_fields = (
        "empleado",
        "fecha",
        "tipo",
        "estado",
        "severidad",
        "asistencia",
        "permiso",
        "solicitud_vacaciones",
        "hora_extra",
        "minutos",
        "goce_sueldo",
        "ventana_inicio",
        "ventana_fin",
        "conteo_retardos_15d",
        "conteo_faltas_30d",
        "detalle",
        "metadata",
        "editado_manual",
        "creado_en",
        "actualizado_en",
    )

    def has_add_permission(self, request):
        return False


@admin.register(IncidenciaAsistenciaBitacora)
class IncidenciaAsistenciaBitacoraAdmin(admin.ModelAdmin):
    list_display = ("incidencia", "usuario", "campo", "creado_en")
    list_filter = ("campo", "creado_en")
    search_fields = ("incidencia__empleado__nombre", "incidencia__empleado__codigo", "campo", "comentario")
    readonly_fields = ("incidencia", "usuario", "campo", "valor_anterior", "valor_nuevo", "comentario", "creado_en")

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


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
    readonly_fields = (
        "folio",
        "estado",
        "estado_jefe",
        "requiere_direccion",
        "estado_direccion",
        "autorizado_jefe_por",
        "fecha_autorizacion_jefe",
        "autorizado_direccion_por",
        "fecha_autorizacion_direccion",
        "autorizado_por",
    )


@admin.register(PermisoSalidaCambio)
class PermisoSalidaCambioAdmin(admin.ModelAdmin):
    list_display = ("folio", "empleado_nombre", "accion", "realizado_por", "creado_en")
    list_filter = ("accion", "creado_en")
    search_fields = ("folio", "empleado_nombre", "motivo", "realizado_por__username")
    readonly_fields = ("permiso", "folio", "empleado_nombre", "accion", "motivo", "cambios", "realizado_por", "creado_en")

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(PoliticaVacaciones)
class PoliticaVacacionesAdmin(admin.ModelAdmin):
    list_display = (
        "nombre",
        "antiguedad_desde",
        "antiguedad_hasta",
        "dias_laborables",
        "prima_porcentaje",
        "vigente_desde",
        "vigente_hasta",
        "activo",
    )
    list_filter = ("activo", "vigente_desde", "vigente_hasta")
    search_fields = ("nombre", "notas")


@admin.register(SolicitudVacaciones)
class SolicitudVacacionesAdmin(admin.ModelAdmin):
    list_display = (
        "folio",
        "empleado",
        "fecha_inicio",
        "fecha_fin",
        "dias_laborables",
        "estado",
        "jefe_directo",
        "preautorizado_por",
        "aprobado_rrhh_por",
    )
    list_filter = ("estado", "fecha_inicio", "fecha_fin", "jefe_directo")
    search_fields = ("folio", "empleado__nombre", "empleado__codigo", "motivo", "notas_rrhh")
    readonly_fields = ("folio", "creado_en", "actualizado_en")
    autocomplete_fields = ("empleado", "jefe_directo", "preautorizado_por", "aprobado_rrhh_por", "creado_por")


@admin.register(MovimientoVacaciones)
class MovimientoVacacionesAdmin(admin.ModelAdmin):
    list_display = ("empleado", "tipo", "dias", "periodo_anio", "solicitud", "actor", "creado_en")
    list_filter = ("tipo", "periodo_anio", "creado_en")
    search_fields = ("empleado__nombre", "empleado__codigo", "solicitud__folio", "descripcion")
    readonly_fields = ("creado_en",)
    autocomplete_fields = ("empleado", "solicitud", "actor")


@admin.register(SuspensionEmpleado)
class SuspensionEmpleadoAdmin(admin.ModelAdmin):
    list_display = ("empleado", "fecha_inicio", "fecha_fin", "estado", "con_goce", "aplicada_por")
    list_filter = ("estado", "con_goce", "fecha_inicio")
    search_fields = ("empleado__nombre", "empleado__codigo", "motivo", "comentario_cancelacion")


@admin.register(Turno)
class TurnoAdmin(admin.ModelAdmin):
    list_display = ("nombre", "hora_entrada", "hora_salida", "tolerancia_minutos", "activo")
    list_filter = ("activo",)


@admin.register(ImportacionChecador)
class ImportacionAdmin(admin.ModelAdmin):
    list_display = ("creado_en", "metodo", "registros_procesados", "errores", "creado_por")
    list_filter = ("metodo", "creado_en")
    readonly_fields = ("log",)


class PrenominaEmpleadoResumenInline(admin.TabularInline):
    model = PrenominaEmpleadoResumen
    extra = 0
    fields = (
        "empleado",
        "dias_periodo",
        "dias_laborables",
        "dias_no_laborados_pre_ingreso",
        "faltas",
        "horas_extra_autorizadas",
        "estado",
    )


@admin.register(PrenominaCorte)
class PrenominaCorteAdmin(admin.ModelAdmin):
    list_display = ("folio", "tipo_periodo", "fecha_inicio", "fecha_fin", "fecha_corte", "estado", "creado_por")
    list_filter = ("tipo_periodo", "estado", "fecha_corte", "sucursal", "area")
    search_fields = ("folio", "sucursal", "area", "notas")
    readonly_fields = ("folio", "creado_en", "actualizado_en")
    inlines = [PrenominaEmpleadoResumenInline]


@admin.register(PrenominaEmpleadoResumen)
class PrenominaEmpleadoResumenAdmin(admin.ModelAdmin):
    list_display = (
        "corte",
        "empleado",
        "dias_periodo",
        "dias_laborables",
        "dias_no_laborados_pre_ingreso",
        "faltas",
        "horas_extra_autorizadas",
        "estado",
    )
    list_filter = ("estado", "corte__tipo_periodo", "corte__fecha_corte")
    search_fields = ("corte__folio", "empleado__nombre", "empleado__codigo", "observaciones")


@admin.register(PrenominaMovimiento)
class PrenominaMovimientoAdmin(admin.ModelAdmin):
    list_display = (
        "corte",
        "empleado",
        "fecha",
        "tipo_movimiento_erp",
        "clave_contpaqi",
        "estado",
        "valor",
        "horas",
        "importe",
    )
    list_filter = ("tipo_movimiento_erp", "estado", "fecha", "corte__fecha_corte")
    search_fields = ("corte__folio", "empleado__nombre", "empleado__codigo", "clave_contpaqi", "referencia", "fuente")


@admin.register(PrenominaEquivalenciaCONTPAQi)
class PrenominaEquivalenciaCONTPAQiAdmin(admin.ModelAdmin):
    list_display = (
        "tipo_movimiento_erp",
        "clave_contpaqi",
        "descripcion",
        "aplica_valor",
        "aplica_horas",
        "aplica_importe",
        "activo",
    )
    list_filter = ("activo", "aplica_valor", "aplica_horas", "aplica_importe")
    search_fields = ("tipo_movimiento_erp", "clave_contpaqi", "descripcion")


@admin.register(AjusteAsistencia)
class AjusteAsistenciaAdmin(admin.ModelAdmin):
    list_display = ("empleado", "fecha", "tipo_ajuste", "estado", "solicitado_por", "autorizado_por", "aplicado_por")
    list_filter = ("tipo_ajuste", "estado", "fecha")
    search_fields = ("empleado__nombre", "empleado__codigo", "motivo")
    readonly_fields = ("solicitado_en", "actualizado_en")


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
    readonly_fields = (
        "folio",
        "saldo_actual",
        "estado",
        "firma_jefe",
        "autorizado_jefe",
        "fecha_auth_jefe",
        "firma_direccion",
        "autorizado_dg",
        "fecha_auth_dg",
        "creado_en",
        "actualizado_en",
    )
    inlines = [PrestamoCuotaInline]


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
