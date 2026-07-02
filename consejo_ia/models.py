from __future__ import annotations

from django.conf import settings
from django.db import models
from django.utils import timezone


class ConsejoConsulta(models.Model):
    VEREDICTO_APROBAR = "APROBAR"
    VEREDICTO_RECHAZAR = "RECHAZAR"
    VEREDICTO_POSPONER = "POSPONER"
    VEREDICTO_PILOTO = "PILOTO"
    VEREDICTO_PEDIR_DATOS = "PEDIR_DATOS"
    VEREDICTO_CHOICES = [
        (VEREDICTO_APROBAR, "Aprobar"),
        (VEREDICTO_RECHAZAR, "Rechazar"),
        (VEREDICTO_POSPONER, "Posponer"),
        (VEREDICTO_PILOTO, "Probar piloto"),
        (VEREDICTO_PEDIR_DATOS, "Pedir más datos"),
    ]

    pregunta = models.TextField()
    snapshot_json = models.JSONField(default=dict, blank=True)
    respuestas_json = models.JSONField(default=dict, blank=True)
    veredicto_ceo = models.CharField(max_length=20, choices=VEREDICTO_CHOICES, blank=True, default="")
    resumen_ejecutivo_ceo = models.TextField(blank=True, default="")
    creado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="consultas_consejo_ia",
    )
    creado_en = models.DateTimeField(default=timezone.now)

    class Meta:
        ordering = ["-creado_en"]
        verbose_name = "Consulta al Consejo Estratégico de IA"
        verbose_name_plural = "Consultas al Consejo Estratégico de IA"

    def __str__(self) -> str:
        return f"{self.creado_en:%Y-%m-%d} · {self.pregunta[:60]}"
