from __future__ import annotations

from django.conf import settings
from django.db import models
from django.utils import timezone


class SeguimientoItem(models.Model):
    TIPO_MINUTA = "MINUTA"
    TIPO_PROYECTO = "PROYECTO"
    TIPO_COMPROMISO = "COMPROMISO"
    TIPO_CHOICES = [
        (TIPO_MINUTA, "Minuta"),
        (TIPO_PROYECTO, "Proyecto"),
        (TIPO_COMPROMISO, "Compromiso"),
    ]

    ESTATUS_PENDIENTE = "PENDIENTE"
    ESTATUS_EN_PROCESO = "EN_PROCESO"
    ESTATUS_EN_REVISION = "EN_REVISION"
    ESTATUS_COMPLETADO = "COMPLETADO"
    ESTATUS_BLOQUEADO = "BLOQUEADO"
    ESTATUS_CANCELADO = "CANCELADO"
    ESTATUS_CHOICES = [
        (ESTATUS_PENDIENTE, "Pendiente"),
        (ESTATUS_EN_PROCESO, "En proceso"),
        (ESTATUS_EN_REVISION, "En revisión"),
        (ESTATUS_COMPLETADO, "Completado"),
        (ESTATUS_BLOQUEADO, "Bloqueado"),
        (ESTATUS_CANCELADO, "Cancelado"),
    ]

    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES, db_index=True)
    titulo = models.CharField(max_length=220)
    descripcion = models.TextField(blank=True, default="")
    entregable_esperado = models.TextField(blank=True, default="")
    responsable_user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        related_name="seguimiento_items",
        null=True,
        blank=True,
    )
    responsable_empleado = models.ForeignKey(
        "rrhh.Empleado",
        on_delete=models.SET_NULL,
        related_name="seguimiento_items",
        null=True,
        blank=True,
    )
    participantes_user = models.ManyToManyField(
        settings.AUTH_USER_MODEL,
        related_name="seguimiento_participaciones",
        blank=True,
    )
    participantes_empleado = models.ManyToManyField(
        "rrhh.Empleado",
        related_name="seguimiento_participaciones",
        blank=True,
    )
    area = models.CharField(max_length=120, blank=True, default="")
    fecha_limite = models.DateTimeField(null=True, blank=True, db_index=True)
    estatus = models.CharField(max_length=20, choices=ESTATUS_CHOICES, default=ESTATUS_PENDIENTE, db_index=True)
    requiere_aprobacion = models.BooleanField(default=True)
    aprobado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        related_name="seguimiento_items_aprobados",
        null=True,
        blank=True,
    )
    aprobado_at = models.DateTimeField(null=True, blank=True)
    origen = models.CharField(max_length=80, blank=True, default="ERP")
    referencia_externa = models.CharField(max_length=160, blank=True, default="")
    metadata = models.JSONField(blank=True, default=dict)
    creado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        related_name="seguimiento_items_creados",
        null=True,
        blank=True,
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["estatus", "fecha_limite", "-updated_at", "id"]
        indexes = [
            models.Index(fields=["responsable_user", "estatus", "fecha_limite"], name="seg_item_resp_status_idx"),
            models.Index(fields=["tipo", "estatus"], name="seg_item_tipo_status_idx"),
        ]
        verbose_name = "Seguimiento"
        verbose_name_plural = "Seguimientos"

    def __str__(self) -> str:
        return self.titulo

    @property
    def esta_cerrado(self) -> bool:
        return self.estatus in {self.ESTATUS_COMPLETADO, self.ESTATUS_CANCELADO}

    @property
    def esta_vencido(self) -> bool:
        return bool(self.fecha_limite and self.fecha_limite < timezone.now() and not self.esta_cerrado)


class SeguimientoChecklistItem(models.Model):
    seguimiento = models.ForeignKey(SeguimientoItem, on_delete=models.CASCADE, related_name="checklist")
    titulo = models.CharField(max_length=220)
    descripcion = models.TextField(blank=True, default="")
    orden = models.PositiveIntegerField(default=0)
    completado = models.BooleanField(default=False, db_index=True)
    completado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        related_name="seguimiento_checks_completados",
        null=True,
        blank=True,
    )
    completado_at = models.DateTimeField(null=True, blank=True)
    # Detalle del paso espejado desde el Agente DG (solo lectura en el ERP)
    origen_step_id = models.IntegerField(null=True, blank=True, db_index=True)
    entregable = models.TextField(blank=True, default="")
    responsable_nombre = models.CharField(max_length=160, blank=True, default="")
    aprobador_nombre = models.CharField(max_length=160, blank=True, default="")
    requiere_aprobacion = models.BooleanField(default=False)
    vence = models.DateTimeField(null=True, blank=True)
    prioridad = models.CharField(max_length=40, blank=True, default="")
    tipo = models.CharField(max_length=40, blank=True, default="")
    estatus_origen = models.CharField(max_length=40, blank=True, default="")
    sub_checklist = models.JSONField(blank=True, default=list)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["orden", "id"]
        verbose_name = "Checklist de seguimiento"
        verbose_name_plural = "Checklist de seguimiento"

    def __str__(self) -> str:
        return self.titulo


class SeguimientoComentario(models.Model):
    TIPO_FEEDBACK = "FEEDBACK"
    TIPO_REVISION_DG = "REVISION_DG"
    TIPO_BLOQUEO = "BLOQUEO"
    TIPO_CHOICES = [
        (TIPO_FEEDBACK, "Retroalimentación del colaborador"),
        (TIPO_REVISION_DG, "Revisión DG"),
        (TIPO_BLOQUEO, "Bloqueo"),
    ]

    seguimiento = models.ForeignKey(SeguimientoItem, on_delete=models.CASCADE, related_name="comentarios")
    usuario = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="seguimiento_comentarios")
    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES, default=TIPO_FEEDBACK)
    comentario = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at", "-id"]
        verbose_name = "Comentario de seguimiento"
        verbose_name_plural = "Comentarios de seguimiento"

    def __str__(self) -> str:
        return f"{self.get_tipo_display()} · {self.seguimiento_id}"


class SeguimientoEvidencia(models.Model):
    ESTATUS_SUBIDA = "SUBIDA"
    ESTATUS_APROBADA = "APROBADA"
    ESTATUS_RECHAZADA = "RECHAZADA"
    ESTATUS_CHOICES = [
        (ESTATUS_SUBIDA, "Subida"),
        (ESTATUS_APROBADA, "Aprobada"),
        (ESTATUS_RECHAZADA, "Rechazada"),
    ]

    seguimiento = models.ForeignKey(SeguimientoItem, on_delete=models.CASCADE, related_name="evidencias")
    usuario = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="seguimiento_evidencias")
    archivo = models.FileField(upload_to="seguimiento/evidencias/%Y/%m/")
    nombre_original = models.CharField(max_length=255)
    comentario = models.TextField(blank=True, default="")
    estatus = models.CharField(max_length=20, choices=ESTATUS_CHOICES, default=ESTATUS_SUBIDA)
    revisado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        related_name="seguimiento_evidencias_revisadas",
        null=True,
        blank=True,
    )
    revisado_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at", "-id"]
        verbose_name = "Evidencia de seguimiento"
        verbose_name_plural = "Evidencias de seguimiento"

    def __str__(self) -> str:
        return self.nombre_original


class SeguimientoProrrogaSolicitud(models.Model):
    ESTATUS_PENDIENTE = "PENDIENTE"
    ESTATUS_APROBADA = "APROBADA"
    ESTATUS_RECHAZADA = "RECHAZADA"
    ESTATUS_CHOICES = [
        (ESTATUS_PENDIENTE, "Pendiente"),
        (ESTATUS_APROBADA, "Aprobada"),
        (ESTATUS_RECHAZADA, "Rechazada"),
    ]

    seguimiento = models.ForeignKey(SeguimientoItem, on_delete=models.CASCADE, related_name="prorrogas")
    usuario = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="seguimiento_prorrogas")
    fecha_solicitada = models.DateField()
    motivo = models.TextField()
    estatus = models.CharField(max_length=20, choices=ESTATUS_CHOICES, default=ESTATUS_PENDIENTE, db_index=True)
    resuelto_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        related_name="seguimiento_prorrogas_resueltas",
        null=True,
        blank=True,
    )
    resuelto_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at", "-id"]
        verbose_name = "Solicitud de prórroga"
        verbose_name_plural = "Solicitudes de prórroga"
        indexes = [
            models.Index(fields=["seguimiento", "estatus"], name="seg_prorroga_item_status_idx"),
            models.Index(fields=["usuario", "estatus"], name="seg_prorroga_user_status_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.seguimiento_id} · {self.fecha_solicitada:%d/%m/%Y}"
