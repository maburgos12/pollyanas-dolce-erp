from __future__ import annotations

from django.conf import settings
from django.db import models
from django.utils import timezone


class VisitaSucursal(models.Model):
    TIPO_NORMAL = "NORMAL"
    TIPO_QUINCENAL = "QUINCENAL"
    TIPO_SEGUIMIENTO = "SEGUIMIENTO"
    TIPO_EXTRAORDINARIA = "EXTRAORDINARIA"
    TIPO_CHOICES = [
        (TIPO_NORMAL, "Visita normal"),
        (TIPO_QUINCENAL, "Auditoría quincenal"),
        (TIPO_SEGUIMIENTO, "Seguimiento"),
        (TIPO_EXTRAORDINARIA, "Extraordinaria"),
    ]

    ESTATUS_PROGRAMADA = "PROGRAMADA"
    ESTATUS_REALIZADA = "REALIZADA"
    ESTATUS_CANCELADA = "CANCELADA"
    ESTATUS_CHOICES = [
        (ESTATUS_PROGRAMADA, "Programada"),
        (ESTATUS_REALIZADA, "Realizada"),
        (ESTATUS_CANCELADA, "Cancelada"),
    ]

    sucursal = models.ForeignKey("core.Sucursal", on_delete=models.PROTECT, related_name="visitas_comerciales")
    fecha_programada = models.DateField(default=timezone.localdate, db_index=True)
    fecha_real = models.DateField(null=True, blank=True)
    tipo = models.CharField(max_length=16, choices=TIPO_CHOICES, default=TIPO_QUINCENAL)
    estatus = models.CharField(max_length=16, choices=ESTATUS_CHOICES, default=ESTATUS_PROGRAMADA, db_index=True)
    responsable = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="visitas_sucursal_responsable",
    )
    auditor = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="visitas_sucursal_auditor",
    )
    realizada_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="visitas_sucursal_realizadas",
    )
    realizada_en = models.DateTimeField(null=True, blank=True)
    gps_latitud = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    gps_longitud = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    gps_precision_m = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)
    gps_distancia_sucursal_m = models.PositiveIntegerField(null=True, blank=True)
    gps_dentro_geocerca = models.BooleanField(null=True, blank=True)
    personal_presente = models.ManyToManyField("rrhh.Empleado", blank=True, related_name="visitas_sucursal_presentes")
    observaciones = models.TextField(blank=True, default="")
    creado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="visitas_sucursal_creadas",
    )
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-fecha_programada", "-id"]
        verbose_name = "Visita a sucursal"
        verbose_name_plural = "Visitas a sucursal"

    def __str__(self) -> str:
        return f"{self.sucursal} · {self.fecha_programada:%d/%m/%Y}"

    @property
    def porcentaje_cumplimiento(self) -> int:
        respuestas = list(self.checklist.exclude(respuesta=ChecklistVisita.RESPUESTA_NA))
        if not respuestas:
            return 0
        cumplidas = sum(1 for item in respuestas if item.respuesta == ChecklistVisita.RESPUESTA_SI)
        return round((cumplidas / len(respuestas)) * 100)


class ChecklistVisita(models.Model):
    RESPUESTA_PENDIENTE = "PENDIENTE"
    RESPUESTA_SI = "SI"
    RESPUESTA_NO = "NO"
    RESPUESTA_NA = "NA"
    RESPUESTA_CHOICES = [
        (RESPUESTA_PENDIENTE, "Pendiente"),
        (RESPUESTA_SI, "Cumple"),
        (RESPUESTA_NO, "No cumple"),
        (RESPUESTA_NA, "No aplica"),
    ]

    visita = models.ForeignKey(VisitaSucursal, on_delete=models.CASCADE, related_name="checklist")
    categoria = models.CharField(max_length=80)
    titulo = models.CharField(max_length=220)
    respuesta = models.CharField(max_length=12, choices=RESPUESTA_CHOICES, default=RESPUESTA_PENDIENTE)
    observaciones = models.TextField(blank=True, default="")
    orden = models.PositiveSmallIntegerField(default=0)

    class Meta:
        ordering = ["orden", "id"]
        verbose_name = "Checklist de visita"
        verbose_name_plural = "Checklist de visita"

    def __str__(self) -> str:
        return self.titulo


class FotoVisita(models.Model):
    visita = models.ForeignKey(VisitaSucursal, on_delete=models.CASCADE, related_name="fotos")
    foto = models.ImageField(upload_to="visitas_sucursal/%Y/%m/")
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-creado_en", "-id"]
        verbose_name = "Foto de visita"
        verbose_name_plural = "Fotos de visita"

    def __str__(self) -> str:
        return f"Foto visita #{self.visita_id}"


class HallazgoVisita(models.Model):
    PRIORIDAD_BAJA = "BAJA"
    PRIORIDAD_MEDIA = "MEDIA"
    PRIORIDAD_ALTA = "ALTA"
    PRIORIDAD_CRITICA = "CRITICA"
    PRIORIDAD_CHOICES = [
        (PRIORIDAD_BAJA, "Baja"),
        (PRIORIDAD_MEDIA, "Media"),
        (PRIORIDAD_ALTA, "Alta"),
        (PRIORIDAD_CRITICA, "Crítica"),
    ]

    ESTATUS_ABIERTO = "ABIERTO"
    ESTATUS_EN_PROCESO = "EN_PROCESO"
    ESTATUS_CERRADO = "CERRADO"
    ESTATUS_CHOICES = [
        (ESTATUS_ABIERTO, "Abierto"),
        (ESTATUS_EN_PROCESO, "En proceso"),
        (ESTATUS_CERRADO, "Cerrado"),
    ]

    visita = models.ForeignKey(VisitaSucursal, on_delete=models.CASCADE, related_name="hallazgos")
    categoria = models.CharField(max_length=80)
    descripcion = models.TextField()
    accion_correctiva = models.TextField(blank=True, default="")
    responsable = models.CharField(max_length=160, blank=True, default="")
    fecha_compromiso = models.DateField(null=True, blank=True)
    prioridad = models.CharField(max_length=10, choices=PRIORIDAD_CHOICES, default=PRIORIDAD_MEDIA)
    estatus = models.CharField(max_length=16, choices=ESTATUS_CHOICES, default=ESTATUS_ABIERTO, db_index=True)
    requiere_falla = models.BooleanField(default=False)
    reporte_falla = models.ForeignKey(
        "fallas.ReporteFalla",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="hallazgos_visita",
    )
    creado_por = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    creado_en = models.DateTimeField(auto_now_add=True)
    actualizado_en = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["estatus", "-prioridad", "fecha_compromiso", "id"]
        verbose_name = "Hallazgo de visita"
        verbose_name_plural = "Hallazgos de visita"

    def __str__(self) -> str:
        return f"{self.visita.sucursal} · {self.categoria}"
