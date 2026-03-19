from __future__ import annotations

from django.conf import settings
from django.db import models
from django.utils import timezone


class PointSyncJob(models.Model):
    JOB_TYPE_INVENTORY = "inventory"
    JOB_TYPE_SALES = "sales"
    JOB_TYPE_CHOICES = [
        (JOB_TYPE_INVENTORY, "Inventory"),
        (JOB_TYPE_SALES, "Sales"),
    ]

    STATUS_PENDING = "PENDING"
    STATUS_RUNNING = "RUNNING"
    STATUS_SUCCESS = "SUCCESS"
    STATUS_FAILED = "FAILED"
    STATUS_PARTIAL = "PARTIAL"
    STATUS_CHOICES = [
        (STATUS_PENDING, "Pending"),
        (STATUS_RUNNING, "Running"),
        (STATUS_SUCCESS, "Success"),
        (STATUS_FAILED, "Failed"),
        (STATUS_PARTIAL, "Partial"),
    ]

    job_type = models.CharField(max_length=32, choices=JOB_TYPE_CHOICES, default=JOB_TYPE_INVENTORY)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_PENDING, db_index=True)
    started_at = models.DateTimeField(default=timezone.now)
    finished_at = models.DateTimeField(null=True, blank=True)
    error_message = models.TextField(blank=True, default="")
    parameters = models.JSONField(default=dict, blank=True)
    result_summary = models.JSONField(default=dict, blank=True)
    artifacts = models.JSONField(default=dict, blank=True)
    attempt_count = models.PositiveIntegerField(default=0)
    triggered_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="point_sync_jobs",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "pos_bridge_sync_jobs"
        ordering = ["-started_at", "-id"]
        verbose_name = "Point sync job"
        verbose_name_plural = "Point sync jobs"

    def __str__(self) -> str:
        return f"{self.job_type} #{self.id} {self.status}"


class PointExtractionLog(models.Model):
    LEVEL_DEBUG = "DEBUG"
    LEVEL_INFO = "INFO"
    LEVEL_WARNING = "WARNING"
    LEVEL_ERROR = "ERROR"
    LEVEL_CHOICES = [
        (LEVEL_DEBUG, "Debug"),
        (LEVEL_INFO, "Info"),
        (LEVEL_WARNING, "Warning"),
        (LEVEL_ERROR, "Error"),
    ]

    sync_job = models.ForeignKey(PointSyncJob, on_delete=models.CASCADE, related_name="logs")
    level = models.CharField(max_length=16, choices=LEVEL_CHOICES, default=LEVEL_INFO)
    message = models.TextField()
    context = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "pos_bridge_extraction_logs"
        ordering = ["created_at", "id"]
        verbose_name = "Point extraction log"
        verbose_name_plural = "Point extraction logs"

    def __str__(self) -> str:
        return f"{self.sync_job_id} {self.level} {self.message[:60]}"
