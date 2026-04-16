from __future__ import annotations

import hashlib
import uuid

from django.conf import settings
from django.db import models
from django.db.models import Q
from django.utils import timezone
from unidecode import unidecode

from core.models import Sucursal


def normalize_text(value: str) -> str:
    return " ".join(unidecode((value or "")).lower().strip().split())


class SucursalAlias(models.Model):
    SOURCE_MANUAL = "MANUAL"
    SOURCE_IMPORTED = "IMPORTED"
    SOURCE_AI = "AI_SUGGESTED"
    SOURCE_CHOICES = [
        (SOURCE_MANUAL, "Manual"),
        (SOURCE_IMPORTED, "Importado"),
        (SOURCE_AI, "Sugerido IA"),
    ]

    sucursal = models.ForeignKey(Sucursal, on_delete=models.CASCADE, related_name="horarios_aliases")
    alias = models.CharField(max_length=120)
    alias_normalizado = models.CharField(max_length=140, db_index=True, blank=True, default="")
    source = models.CharField(max_length=20, choices=SOURCE_CHOICES, default=SOURCE_MANUAL)
    is_active = models.BooleanField(default=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="horarios_especiales_aliases_creados",
    )
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "special_hours_branch_aliases"
        ordering = ["alias"]
        constraints = [
            models.UniqueConstraint(
                fields=["alias_normalizado"],
                condition=Q(is_active=True),
                name="he_branch_alias_active_unique",
            )
        ]
        indexes = [
            models.Index(fields=["sucursal", "is_active"], name="he_alias_branch_active_idx"),
        ]

    def save(self, *args, **kwargs):
        self.alias_normalizado = normalize_text(self.alias)
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"{self.alias} -> {self.sucursal.codigo}"


class SucursalPlataformaExterna(models.Model):
    PLATFORM_GOOGLE = "GOOGLE_BUSINESS_PROFILE"
    PLATFORM_CHOICES = [
        (PLATFORM_GOOGLE, "Google Business Profile"),
    ]

    sucursal = models.ForeignKey(Sucursal, on_delete=models.CASCADE, related_name="plataformas_horario")
    platform = models.CharField(max_length=40, choices=PLATFORM_CHOICES, default=PLATFORM_GOOGLE)
    external_account_id = models.CharField(max_length=120, blank=True, default="")
    external_location_id = models.CharField(max_length=120, blank=True, default="")
    external_location_name = models.CharField(max_length=160, blank=True, default="")
    settings_json = models.JSONField(default=dict, blank=True)
    is_active = models.BooleanField(default=True)
    last_validated_at = models.DateTimeField(null=True, blank=True)
    last_published_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "special_hours_platform_configs"
        ordering = ["platform", "sucursal__codigo"]
        constraints = [
            models.UniqueConstraint(fields=["sucursal", "platform"], name="he_platform_config_branch_unique"),
            models.UniqueConstraint(
                fields=["platform", "external_location_name"],
                condition=~Q(external_location_name=""),
                name="he_platform_config_location_unique",
            ),
        ]
        indexes = [
            models.Index(fields=["platform", "is_active"], name="he_platform_config_status_idx"),
        ]

    def save(self, *args, **kwargs):
        if self.external_location_name and not self.external_location_id:
            self.external_location_id = self.external_location_name.split("/")[-1]
        if self.external_location_id and not self.external_location_name:
            self.external_location_name = f"locations/{self.external_location_id}"
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"{self.sucursal.codigo} · {self.platform}"


class SolicitudHorarioEspecial(models.Model):
    STATUS_BORRADOR = "BORRADOR"
    STATUS_VALIDADO = "VALIDADO"
    STATUS_APROBADO = "APROBADO"
    STATUS_EJECUTADO = "EJECUTADO"
    STATUS_FALLIDO = "FALLIDO"
    STATUS_CANCELADO = "CANCELADO"
    STATUS_CHOICES = [
        (STATUS_BORRADOR, "Borrador"),
        (STATUS_VALIDADO, "Validado"),
        (STATUS_APROBADO, "Aprobado"),
        (STATUS_EJECUTADO, "Ejecutado"),
        (STATUS_FALLIDO, "Fallido"),
        (STATUS_CANCELADO, "Cancelado"),
    ]

    SOURCE_WEB = "WEB"
    SOURCE_API = "API"
    SOURCE_AI = "IA_PRIVADA"
    SOURCE_CHOICES = [
        (SOURCE_WEB, "ERP Web"),
        (SOURCE_API, "API"),
        (SOURCE_AI, "IA privada"),
    ]

    request_code = models.CharField(max_length=40, unique=True, blank=True, db_index=True)
    raw_command = models.TextField()
    source_channel = models.CharField(max_length=20, choices=SOURCE_CHOICES, default=SOURCE_WEB)
    reason = models.CharField(max_length=255, blank=True, default="")
    canonical_payload = models.JSONField(default=dict, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_BORRADOR, db_index=True)
    idempotency_key = models.CharField(max_length=64, blank=True, default="", db_index=True)
    execution_summary_json = models.JSONField(default=dict, blank=True)
    requested_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="horarios_especiales_solicitados",
    )
    approved_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="horarios_especiales_aprobados",
    )
    executed_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="horarios_especiales_ejecutados",
    )
    approved_at = models.DateTimeField(null=True, blank=True)
    executed_at = models.DateTimeField(null=True, blank=True)
    cancelled_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "special_hours_requests"
        ordering = ["-created_at", "-id"]
        indexes = [
            models.Index(fields=["status", "created_at"], name="he_request_status_created_idx"),
        ]

    def save(self, *args, **kwargs):
        if not self.request_code:
            stamp = timezone.localtime().strftime("%Y%m%d%H%M%S")
            self.request_code = f"HE-{stamp}-{uuid.uuid4().hex[:6].upper()}"
        if not self.idempotency_key and self.canonical_payload:
            self.idempotency_key = hashlib.sha256(str(self.canonical_payload).encode("utf-8")).hexdigest()
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"{self.request_code} · {self.status}"


class HorarioEspecialDetalle(models.Model):
    EXEC_STATUS_PENDING = "PENDIENTE"
    EXEC_STATUS_SUCCESS = "EXITOSO"
    EXEC_STATUS_FAILED = "FALLIDO"
    EXEC_STATUS_CANCELLED = "CANCELADO"
    EXEC_STATUS_CHOICES = [
        (EXEC_STATUS_PENDING, "Pendiente"),
        (EXEC_STATUS_SUCCESS, "Exitoso"),
        (EXEC_STATUS_FAILED, "Fallido"),
        (EXEC_STATUS_CANCELLED, "Cancelado"),
    ]

    request = models.ForeignKey(
        SolicitudHorarioEspecial,
        on_delete=models.CASCADE,
        related_name="details",
    )
    sucursal = models.ForeignKey(Sucursal, on_delete=models.PROTECT, related_name="horarios_especiales_detalle")
    target_date = models.DateField(db_index=True)
    closed_all_day = models.BooleanField(default=False)
    time_windows_json = models.JSONField(default=list, blank=True)
    execution_status = models.CharField(
        max_length=20,
        choices=EXEC_STATUS_CHOICES,
        default=EXEC_STATUS_PENDING,
        db_index=True,
    )
    validation_errors_json = models.JSONField(default=list, blank=True)
    platform_payload_json = models.JSONField(default=dict, blank=True)
    published_snapshot_json = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "special_hours_request_details"
        ordering = ["target_date", "sucursal__codigo", "id"]
        constraints = [
            models.UniqueConstraint(
                fields=["request", "sucursal", "target_date"],
                name="he_request_detail_unique",
            )
        ]
        indexes = [
            models.Index(fields=["target_date", "execution_status"], name="he_detail_date_exec_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.request.request_code} · {self.sucursal.codigo} · {self.target_date}"


class HorarioEspecialIntentoPublicacion(models.Model):
    PLATFORM_GOOGLE = SucursalPlataformaExterna.PLATFORM_GOOGLE
    PLATFORM_CHOICES = SucursalPlataformaExterna.PLATFORM_CHOICES

    STATUS_PENDING = "PENDING"
    STATUS_RUNNING = "RUNNING"
    STATUS_SUCCESS = "SUCCESS"
    STATUS_FAILED = "FAILED"
    STATUS_SKIPPED = "SKIPPED"
    STATUS_CANCELLED = "CANCELLED"
    STATUS_CHOICES = [
        (STATUS_PENDING, "Pending"),
        (STATUS_RUNNING, "Running"),
        (STATUS_SUCCESS, "Success"),
        (STATUS_FAILED, "Failed"),
        (STATUS_SKIPPED, "Skipped"),
        (STATUS_CANCELLED, "Cancelled"),
    ]

    detail = models.ForeignKey(
        HorarioEspecialDetalle,
        on_delete=models.CASCADE,
        related_name="publication_attempts",
    )
    platform = models.CharField(max_length=40, choices=PLATFORM_CHOICES, default=PLATFORM_GOOGLE)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_PENDING, db_index=True)
    attempt_no = models.PositiveIntegerField(default=1)
    idempotency_key = models.CharField(max_length=64, blank=True, default="")
    request_payload_json = models.JSONField(default=dict, blank=True)
    response_payload_json = models.JSONField(default=dict, blank=True)
    error_payload_json = models.JSONField(default=dict, blank=True)
    error_message = models.TextField(blank=True, default="")
    external_operation_id = models.CharField(max_length=120, blank=True, default="")
    started_at = models.DateTimeField(default=timezone.now)
    finished_at = models.DateTimeField(null=True, blank=True)
    executed_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="horarios_especiales_intentos_ejecutados",
    )

    class Meta:
        db_table = "special_hours_publication_attempts"
        ordering = ["-started_at", "-id"]
        constraints = [
            models.UniqueConstraint(
                fields=["detail", "platform", "attempt_no"],
                name="he_publication_attempt_unique",
            )
        ]
        indexes = [
            models.Index(fields=["platform", "status"], name="he_attempt_platform_status_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.detail_id} · {self.platform} · {self.attempt_no}"


class HorarioEspecialBitacora(models.Model):
    ACTION_CREATED = "CREATED"
    ACTION_VALIDATED = "VALIDATED"
    ACTION_APPROVED = "APPROVED"
    ACTION_EXECUTION_REQUESTED = "EXECUTION_REQUESTED"
    ACTION_EXECUTION_SUCCESS = "EXECUTION_SUCCESS"
    ACTION_EXECUTION_FAILED = "EXECUTION_FAILED"
    ACTION_RETRY_REQUESTED = "RETRY_REQUESTED"
    ACTION_CANCELLED = "CANCELLED"
    ACTION_CHOICES = [
        (ACTION_CREATED, "Created"),
        (ACTION_VALIDATED, "Validated"),
        (ACTION_APPROVED, "Approved"),
        (ACTION_EXECUTION_REQUESTED, "Execution requested"),
        (ACTION_EXECUTION_SUCCESS, "Execution success"),
        (ACTION_EXECUTION_FAILED, "Execution failed"),
        (ACTION_RETRY_REQUESTED, "Retry requested"),
        (ACTION_CANCELLED, "Cancelled"),
    ]

    request = models.ForeignKey(
        SolicitudHorarioEspecial,
        on_delete=models.CASCADE,
        related_name="audit_entries",
    )
    detail = models.ForeignKey(
        HorarioEspecialDetalle,
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="audit_entries",
    )
    action = models.CharField(max_length=40, choices=ACTION_CHOICES)
    payload_json = models.JSONField(default=dict, blank=True)
    actor_user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="horarios_especiales_audit_entries",
    )
    actor_role = models.CharField(max_length=30, blank=True, default="")
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "special_hours_audit_log"
        ordering = ["-created_at", "-id"]
        indexes = [
            models.Index(fields=["request", "created_at"], name="he_audit_request_created_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.request.request_code} · {self.action}"

