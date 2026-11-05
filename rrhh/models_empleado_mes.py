"""Evaluación mensual CEDIS; identidades y auditoría permanecen en sus fuentes existentes."""
from django.conf import settings
from django.db import models

SECCIONES = (
    ("Desempeño y logros", (("calidad_cantidad", "Calidad y cantidad de trabajo"), ("metas", "Cumplimiento de metas y objetivos"), ("expectativas", "Superación de expectativas"), ("innovacion", "Innovación y creatividad"), ("tiempo", "Gestión del tiempo"))),
    ("Actitud y comportamiento", (("equipo", "Trabajo en equipo"), ("profesionalismo", "Comportamiento profesional"), ("asistencia", "Puntualidad y asistencia"), ("aprendizaje", "Disposición para aprender y desarrollarse"), ("motivacion", "Motivación y entusiasmo"))),
    ("Contribución al equipo y a la empresa", (("proyectos", "Participación en proyectos"), ("apoyo", "Ayuda a los compañeros"), ("problemas", "Solución de problemas"), ("mejoras", "Mejora de procesos"))),
)
CRITERIOS = dict(item for _, items in SECCIONES for item in items)


class PeriodoEmpleadoMes(models.Model):
    mes = models.DateField(unique=True)
    fecha_limite = models.DateField()
    evaluador_produccion = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="periodos_empleado_produccion")
    evaluador_administracion = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="periodos_empleado_administracion")
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="periodos_empleado_creados")
    creado_en = models.DateTimeField(auto_now_add=True)
    ganador = models.ForeignKey("rrhh.EvaluacionEmpleadoMes", on_delete=models.PROTECT, null=True, blank=True, related_name="periodos_ganados")
    motivo_decision = models.TextField(blank=True)
    elegido_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, null=True, blank=True, related_name="periodos_empleado_elegidos")
    elegido_en = models.DateTimeField(null=True, blank=True)
    premio_tipo = models.CharField(max_length=20, blank=True, choices=[("efectivo", "Efectivo"), ("vales", "Vales"), ("descanso", "Día de descanso")])
    premio_detalle = models.TextField(blank=True)
    premio_importe = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    premio_aprobado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, null=True, blank=True, related_name="premios_empleado_aprobados")
    premio_aprobado_en = models.DateTimeField(null=True, blank=True)
    entrega_fecha = models.DateField(null=True, blank=True)
    entrega_responsable = models.CharField(max_length=180, blank=True)
    entrega_comprobante = models.TextField(blank=True)
    entregado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, null=True, blank=True, related_name="premios_empleado_entregados")
    entregado_en = models.DateTimeField(null=True, blank=True)
    version = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ["-mes"]

    @property
    def estado_display(self):
        return "Entregado" if self.entregado_en else "Premio aprobado" if self.premio_aprobado_en else "Elegido" if self.ganador_id else "En evaluación"


class EvaluacionEmpleadoMes(models.Model):
    periodo = models.ForeignKey(PeriodoEmpleadoMes, on_delete=models.PROTECT, related_name="evaluaciones")
    empleado = models.ForeignKey("rrhh.Empleado", on_delete=models.PROTECT, related_name="evaluaciones_empleado_mes")
    evaluador = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="evaluaciones_empleado_mes")
    nombre_snapshot = models.CharField(max_length=180)
    codigo_snapshot = models.CharField(max_length=80, blank=True)
    puesto_snapshot = models.CharField(max_length=120, blank=True)
    departamento_snapshot = models.CharField(max_length=40)
    estado = models.CharField(max_length=12, choices=[("borrador", "Borrador"), ("enviada", "Enviada")], default="borrador")
    calificaciones = models.JSONField(default=dict, blank=True)
    comentarios = models.JSONField(default=dict, blank=True)
    reconocimiento = models.BooleanField(default=False)
    reconocimiento_detalle = models.TextField(blank=True)
    recomienda = models.BooleanField(null=True, blank=True)
    justificacion = models.TextField(blank=True)
    enviada_en = models.DateTimeField(null=True, blank=True)
    version = models.PositiveIntegerField(default=0)

    class Meta:
        constraints = [models.UniqueConstraint(fields=["periodo", "empleado"], name="empleado_mes_periodo_empleado_unico")]
        ordering = ["departamento_snapshot", "nombre_snapshot"]

    @property
    def total(self):
        return sum(v for v in self.calificaciones.values() if type(v) is int and 1 <= v <= 5)

    @property
    def completa(self):
        return set(self.calificaciones) == set(CRITERIOS) and all(type(v) is int and 1 <= v <= 5 for v in self.calificaciones.values())

    @property
    def estado_display(self):
        return self.get_estado_display()
