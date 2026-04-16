from __future__ import annotations

from decimal import Decimal

from django.conf import settings
from django.db import models
from django.utils import timezone

from core.models import Sucursal
from maestros.models import Insumo, Proveedor
from recetas.models import Receta


def _normalize_code(value: str) -> str:
    return "".join(ch for ch in (value or "").strip().upper() if ch.isalnum() or ch in {"-", "_"})


class EventoVenta(models.Model):
    STATUS_BORRADOR = "BORRADOR"
    STATUS_RECOPILANDO = "RECOPILANDO_DATOS"
    STATUS_MODELADO = "EN_MODELADO"
    STATUS_LISTO_REVISION = "LISTO_PARA_REVISION"
    STATUS_PENDIENTE_DG = "PENDIENTE_DIRECCION"
    STATUS_APROBADO = "APROBADO"
    STATUS_APROBADO_AJUSTES = "APROBADO_CON_AJUSTES"
    STATUS_RECHAZADO = "RECHAZADO"
    STATUS_ENVIADO_PROD = "ENVIADO_A_PRODUCCION"
    STATUS_VALIDADO_PROD = "VALIDADO_POR_PRODUCCION"
    STATUS_ENVIADO_COMPRAS = "ENVIADO_A_COMPRAS"
    STATUS_EN_EJECUCION = "EN_EJECUCION"
    STATUS_CERRADO = "CERRADO"
    STATUS_EVALUADO = "EVALUADO_POST_EVENTO"
    STATUS_CHOICES = [
        (STATUS_BORRADOR, "Borrador"),
        (STATUS_RECOPILANDO, "Recopilando datos"),
        (STATUS_MODELADO, "En modelado"),
        (STATUS_LISTO_REVISION, "Listo para revisión"),
        (STATUS_PENDIENTE_DG, "Pendiente dirección"),
        (STATUS_APROBADO, "Aprobado"),
        (STATUS_APROBADO_AJUSTES, "Aprobado con ajustes"),
        (STATUS_RECHAZADO, "Rechazado"),
        (STATUS_ENVIADO_PROD, "Enviado a producción"),
        (STATUS_VALIDADO_PROD, "Validado por producción"),
        (STATUS_ENVIADO_COMPRAS, "Enviado a compras"),
        (STATUS_EN_EJECUCION, "En ejecución"),
        (STATUS_CERRADO, "Cerrado"),
        (STATUS_EVALUADO, "Evaluado post-evento"),
    ]

    PRIORIDAD_BAJA = "BAJA"
    PRIORIDAD_MEDIA = "MEDIA"
    PRIORIDAD_ALTA = "ALTA"
    PRIORIDAD_URGENTE = "URGENTE"
    PRIORIDAD_CHOICES = [
        (PRIORIDAD_BAJA, "Baja"),
        (PRIORIDAD_MEDIA, "Media"),
        (PRIORIDAD_ALTA, "Alta"),
        (PRIORIDAD_URGENTE, "Urgente"),
    ]

    SCENARIO_CONSERVADOR = "CONSERVADOR"
    SCENARIO_BASE = "BASE"
    SCENARIO_AGRESIVO = "AGRESIVO"
    SCENARIO_CHOICES = [
        (SCENARIO_CONSERVADOR, "Conservador"),
        (SCENARIO_BASE, "Base"),
        (SCENARIO_AGRESIVO, "Agresivo"),
    ]

    code = models.CharField(max_length=40, unique=True, blank=True, db_index=True)
    name = models.CharField(max_length=180)
    event_type = models.CharField(max_length=120, blank=True, default="")
    main_date = models.DateField()
    analysis_start_date = models.DateField()
    analysis_end_date = models.DateField()
    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default=STATUS_BORRADOR)
    objective_type = models.CharField(max_length=80, blank=True, default="")
    objective_notes = models.TextField(blank=True, default="")
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ventas_eventos_creados",
    )
    approved_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ventas_eventos_aprobados",
    )
    rejected_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ventas_eventos_rechazados",
    )
    approved_at = models.DateTimeField(null=True, blank=True)
    rejected_at = models.DateTimeField(null=True, blank=True)
    approval_deadline = models.DateField(null=True, blank=True)
    priority = models.CharField(max_length=20, choices=PRIORIDAD_CHOICES, default=PRIORIDAD_MEDIA)
    scenario_focus = models.CharField(max_length=20, choices=SCENARIO_CHOICES, default=SCENARIO_BASE)
    guamuchil_comparable_branch = models.ForeignKey(
        Sucursal,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ventas_eventos_comparable_guamuchil",
    )
    conservative_pct = models.DecimalField(max_digits=6, decimal_places=2, default=Decimal("0.90"))
    aggressive_pct = models.DecimalField(max_digits=6, decimal_places=2, default=Decimal("1.10"))
    version = models.PositiveIntegerField(default=1)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    deleted_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "sales_events"
        ordering = ["-main_date", "-id"]
        indexes = [
            models.Index(fields=["status", "main_date"], name="sales_events_status_date_idx"),
        ]

    def save(self, *args, **kwargs):
        if not self.code:
            base = _normalize_code(self.name)[:16] or "EVT"
            ymd = self.main_date.strftime("%y%m%d") if self.main_date else timezone.localdate().strftime("%y%m%d")
            last = EventoVenta.objects.filter(code__startswith=f"{base}-{ymd}").count() + 1
            self.code = f"{base}-{ymd}-{last:03d}"
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"{self.code} · {self.name}"


class EventoVentaSucursal(models.Model):
    sales_event = models.ForeignKey(EventoVenta, on_delete=models.CASCADE, related_name="branches")
    branch = models.ForeignKey(Sucursal, on_delete=models.PROTECT, related_name="sales_events")
    comparable_branch = models.ForeignKey(
        Sucursal,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ventas_eventos_branch_comparable",
    )
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "sales_event_branches"
        unique_together = [("sales_event", "branch")]
        ordering = ["branch__codigo"]

    def __str__(self) -> str:
        return f"{self.sales_event.code} · {self.branch.codigo}"


class EventoVentaProducto(models.Model):
    SOURCE_MANUAL = "MANUAL"
    SOURCE_AI = "AI_SUGGESTED"
    SOURCE_HISTORICAL = "HISTORICAL"
    SOURCE_REPLACEMENT = "REPLACEMENT"
    SOURCE_CHOICES = [
        (SOURCE_MANUAL, "Manual"),
        (SOURCE_AI, "AI sugerido"),
        (SOURCE_HISTORICAL, "Histórico"),
        (SOURCE_REPLACEMENT, "Reemplazo"),
    ]

    sales_event = models.ForeignKey(EventoVenta, on_delete=models.CASCADE, related_name="products")
    product = models.ForeignKey(Receta, on_delete=models.PROTECT, related_name="sales_event_products")
    source_type = models.CharField(max_length=20, choices=SOURCE_CHOICES, default=SOURCE_MANUAL)
    inclusion_reason = models.CharField(max_length=255, blank=True, default="")
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "sales_event_products"
        unique_together = [("sales_event", "product")]
        ordering = ["product__nombre"]

    def __str__(self) -> str:
        return f"{self.sales_event.code} · {self.product.nombre}"


class EventoVentaForecast(models.Model):
    sales_event = models.ForeignKey(EventoVenta, on_delete=models.CASCADE, related_name="forecasts")
    branch = models.ForeignKey(Sucursal, on_delete=models.PROTECT, related_name="sales_event_forecasts")
    product = models.ForeignKey(Receta, on_delete=models.PROTECT, related_name="sales_event_forecasts")
    forecast_date = models.DateField(db_index=True)
    base_demand = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    event_uplift = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    trend_adjustment = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    final_forecast = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    conservative_forecast = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    aggressive_forecast = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    confidence_score = models.DecimalField(max_digits=5, decimal_places=2, default=0)
    model_version = models.CharField(max_length=40, blank=True, default="v1-hybrid")
    explanation_json = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "sales_event_forecasts"
        ordering = ["forecast_date", "branch__codigo", "product__nombre"]
        indexes = [
            models.Index(fields=["sales_event", "forecast_date"], name="se_forecast_event_date_idx"),
            models.Index(fields=["branch", "product", "forecast_date"], name="se_fcst_br_prod_dt_idx"),
        ]
        unique_together = [("sales_event", "branch", "product", "forecast_date")]

    def __str__(self) -> str:
        return f"{self.sales_event.code} · {self.forecast_date} · {self.branch.codigo} · {self.product.nombre}"


class EventoVentaSubstitutionWeight(models.Model):
    SOURCE_BRANCH = "branch"
    SOURCE_GLOBAL = "global"
    SOURCE_BLENDED = "blended"
    SOURCE_LEVEL_CHOICES = [
        (SOURCE_BRANCH, "Sucursal"),
        (SOURCE_GLOBAL, "Cadena/global"),
        (SOURCE_BLENDED, "Mezcla"),
    ]

    CONFIDENCE_LOW = "low"
    CONFIDENCE_MEDIUM = "medium"
    CONFIDENCE_HIGH = "high"
    CONFIDENCE_CHOICES = [
        (CONFIDENCE_LOW, "Baja"),
        (CONFIDENCE_MEDIUM, "Media"),
        (CONFIDENCE_HIGH, "Alta"),
    ]

    group_key = models.CharField(max_length=180, db_index=True)
    winner_product = models.ForeignKey(
        Receta,
        on_delete=models.CASCADE,
        related_name="sales_event_substitution_winner_weights",
    )
    loser_product = models.ForeignKey(
        Receta,
        on_delete=models.CASCADE,
        related_name="sales_event_substitution_loser_weights",
    )
    branch = models.ForeignKey(
        Sucursal,
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="sales_event_substitution_weights",
    )
    source_level = models.CharField(max_length=10, choices=SOURCE_LEVEL_CHOICES, default=SOURCE_GLOBAL)
    weight = models.DecimalField(max_digits=8, decimal_places=5, default=0)
    sample_size = models.PositiveIntegerField(default=0)
    confidence = models.CharField(max_length=10, choices=CONFIDENCE_CHOICES, default=CONFIDENCE_LOW)
    window_start = models.DateField()
    window_end = models.DateField()
    version = models.CharField(max_length=32, default="v7.2-learned")
    metadata_json = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "sales_event_substitution_weights"
        ordering = ["group_key", "winner_product__nombre", "loser_product__nombre", "branch__codigo"]
        indexes = [
            models.Index(fields=["group_key", "version"], name="se_subwt_group_ver_idx"),
            models.Index(fields=["branch", "version"], name="se_subwt_branch_ver_idx"),
            models.Index(fields=["winner_product", "loser_product"], name="se_subwt_pair_idx"),
        ]
        unique_together = [("group_key", "winner_product", "loser_product", "branch", "version")]

    def __str__(self) -> str:
        branch_code = self.branch.codigo if self.branch else "GLOBAL"
        return f"{self.group_key} · {branch_code} · {self.winner_product.nombre} -> {self.loser_product.nombre}"


class EventoVentaProjectionArtifact(models.Model):
    TYPE_WEEK = "SEMANA"
    TYPE_DAY = "DIA_EXACTO"
    TYPE_DAILY = "POR_DIA"
    TYPE_DASHBOARD = "DASHBOARD"
    TYPE_PACKAGE = "PAQUETE"
    TYPE_CHOICES = [
        (TYPE_WEEK, "Proyección semanal"),
        (TYPE_DAY, "Proyección día exacto"),
        (TYPE_DAILY, "Proyección por día"),
        (TYPE_DASHBOARD, "Dashboard ejecutivo"),
        (TYPE_PACKAGE, "Paquete completo"),
    ]

    sales_event = models.ForeignKey(EventoVenta, on_delete=models.CASCADE, related_name="projection_artifacts")
    export_type = models.CharField(max_length=20, choices=TYPE_CHOICES)
    forecast_version = models.PositiveIntegerField(default=1)
    scope_start = models.DateField(null=True, blank=True)
    scope_end = models.DateField(null=True, blank=True)
    generated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ventas_projection_artifacts",
    )
    file_name = models.CharField(max_length=255)
    file_path = models.CharField(max_length=500)
    size_bytes = models.PositiveBigIntegerField(default=0)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "sales_event_projection_artifacts"
        ordering = ["-created_at", "-id"]
        indexes = [
            models.Index(fields=["sales_event", "export_type", "forecast_version"], name="se_artifact_evt_tp_ver_idx"),
        ]
        unique_together = [("sales_event", "export_type", "forecast_version")]

    def __str__(self) -> str:
        return f"{self.sales_event.code} · {self.export_type} · v{self.forecast_version}"


class EventoVentaDetailSnapshot(models.Model):
    sales_event = models.OneToOneField(
        EventoVenta,
        on_delete=models.CASCADE,
        related_name="detail_snapshot",
    )
    snapshot_version = models.PositiveIntegerField(default=1)
    source_hash = models.CharField(max_length=64, blank=True, default="", db_index=True)
    payload_json = models.JSONField(default=dict, blank=True)
    generated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ventas_eventos_detail_snapshots",
    )
    generated_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "sales_event_detail_snapshots"
        ordering = ["-generated_at", "-id"]
        indexes = [
            models.Index(fields=["sales_event", "snapshot_version"], name="se_detail_snapshot_evt_ver_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.sales_event.code} · detail · v{self.snapshot_version}"


class VentaAutoritativaPoint(models.Model):
    branch = models.ForeignKey(
        Sucursal,
        on_delete=models.PROTECT,
        related_name="ventas_autoritativas_point",
    )
    product = models.ForeignKey(
        Receta,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ventas_autoritativas_point",
    )
    sale_date = models.DateField(db_index=True)
    product_code = models.CharField(max_length=80, blank=True, default="", db_index=True)
    point_name = models.CharField(max_length=250, blank=True, default="")
    category = models.CharField(max_length=120, blank=True, default="")
    quantity = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    gross_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    discount_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    total_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    tax_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    net_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    source_file = models.CharField(max_length=300, blank=True, default="")
    source_sheet = models.CharField(max_length=120, blank=True, default="")
    raw_payload = models.JSONField(default=dict, blank=True)
    imported_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "ventas_autoritativas_point"
        ordering = ["-sale_date", "branch__codigo", "point_name"]
        unique_together = [("branch", "sale_date", "product_code")]
        indexes = [
            models.Index(fields=["sale_date", "branch"], name="vap_day_branch_idx"),
            models.Index(fields=["product", "sale_date"], name="vap_product_day_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.sale_date} · {self.branch.codigo} · {self.point_name or self.product_code}"


class EventoVentaApproval(models.Model):
    STAGE_DIRECCION = "DIRECCION"
    STAGE_PRODUCCION = "PRODUCCION"
    STAGE_COMPRAS = "COMPRAS"
    STAGE_CHOICES = [
        (STAGE_DIRECCION, "Dirección"),
        (STAGE_PRODUCCION, "Producción"),
        (STAGE_COMPRAS, "Compras"),
    ]

    STATUS_PENDIENTE = "PENDIENTE"
    STATUS_APROBADO = "APROBADO"
    STATUS_RECHAZADO = "RECHAZADO"
    STATUS_AJUSTES = "AJUSTES"
    STATUS_CHOICES = [
        (STATUS_PENDIENTE, "Pendiente"),
        (STATUS_APROBADO, "Aprobado"),
        (STATUS_RECHAZADO, "Rechazado"),
        (STATUS_AJUSTES, "Ajustes solicitados"),
    ]

    sales_event = models.ForeignKey(EventoVenta, on_delete=models.CASCADE, related_name="approvals")
    approval_stage = models.CharField(max_length=20, choices=STAGE_CHOICES)
    role_required = models.CharField(max_length=40, blank=True, default="")
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_PENDIENTE)
    requested_to_user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ventas_eventos_aprobaciones_pedidas",
    )
    responded_by_user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ventas_eventos_aprobaciones_resueltas",
    )
    comments = models.TextField(blank=True, default="")
    snapshot_json = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    responded_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = "sales_event_approvals"
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return f"{self.sales_event.code} · {self.approval_stage} · {self.status}"


class EventoVentaAdjustment(models.Model):
    sales_event = models.ForeignKey(EventoVenta, on_delete=models.CASCADE, related_name="adjustments")
    branch = models.ForeignKey(Sucursal, null=True, blank=True, on_delete=models.SET_NULL)
    product = models.ForeignKey(Receta, null=True, blank=True, on_delete=models.SET_NULL)
    field_name = models.CharField(max_length=80)
    old_value = models.CharField(max_length=200, blank=True, default="")
    new_value = models.CharField(max_length=200, blank=True, default="")
    adjustment_reason = models.CharField(max_length=255, blank=True, default="")
    adjusted_by = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "sales_event_adjustments"
        ordering = ["-created_at"]


class EventoVentaAdjustmentDraft(models.Model):
    SCOPE_RANGE = "RANGO"
    SCOPE_DAY = "DIA"
    SCOPE_CHOICES = [
        (SCOPE_RANGE, "Rango del evento"),
        (SCOPE_DAY, "Día principal"),
    ]

    STATUS_DRAFT = "BORRADOR"
    STATUS_FINALIZED = "FINALIZADO"
    STATUS_CANCELED = "CANCELADO"
    STATUS_CHOICES = [
        (STATUS_DRAFT, "Borrador"),
        (STATUS_FINALIZED, "Finalizado"),
        (STATUS_CANCELED, "Cancelado"),
    ]

    sales_event = models.ForeignKey(EventoVenta, on_delete=models.CASCADE, related_name="adjustment_drafts")
    scope_mode = models.CharField(max_length=20, choices=SCOPE_CHOICES, default=SCOPE_RANGE)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_DRAFT)
    forecast_version = models.PositiveIntegerField(default=1)
    notes = models.TextField(blank=True, default="")
    entries_json = models.JSONField(default=list, blank=True)
    preview_json = models.JSONField(default=dict, blank=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ventas_eventos_borradores_ajuste_creados",
    )
    finalized_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ventas_eventos_borradores_ajuste_finalizados",
    )
    finalized_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "sales_event_adjustment_drafts"
        ordering = ["-updated_at", "-id"]
        indexes = [
            models.Index(fields=["sales_event", "status", "scope_mode"], name="se_adjdraft_evt_st_scope_idx"),
        ]

    def __str__(self) -> str:
        return f"{self.sales_event.code} · {self.scope_mode} · {self.status}"


class EventoVentaCapacityRule(models.Model):
    sales_event = models.ForeignKey(EventoVenta, on_delete=models.CASCADE, related_name="capacity_rules")
    capacity_date = models.DateField(null=True, blank=True, db_index=True)
    product = models.ForeignKey(Receta, null=True, blank=True, on_delete=models.SET_NULL, related_name="sales_event_capacity_rules")
    branch = models.ForeignKey(Sucursal, null=True, blank=True, on_delete=models.SET_NULL, related_name="sales_event_capacity_rules")
    max_production_qty = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    notes = models.CharField(max_length=255, blank=True, default="")
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "sales_event_capacity_rules"
        ordering = ["capacity_date", "product__nombre", "branch__codigo", "id"]


class EventoVentaProductionPlan(models.Model):
    STATUS_BORRADOR = "BORRADOR"
    STATUS_CONFIRMADO = "CONFIRMADO"
    STATUS_CHOICES = [
        (STATUS_BORRADOR, "Borrador"),
        (STATUS_CONFIRMADO, "Confirmado"),
    ]

    sales_event = models.ForeignKey(EventoVenta, on_delete=models.CASCADE, related_name="production_plans")
    plan_date = models.DateField(db_index=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_BORRADOR)
    approved_by_production = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="ventas_eventos_produccion_aprobados",
    )
    approved_at = models.DateTimeField(null=True, blank=True)
    notes = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "production_plans"
        ordering = ["plan_date", "id"]
        unique_together = [("sales_event", "plan_date")]


class EventoVentaProductionLine(models.Model):
    production_plan = models.ForeignKey(EventoVentaProductionPlan, on_delete=models.CASCADE, related_name="lines")
    product = models.ForeignKey(Receta, on_delete=models.PROTECT, related_name="sales_event_production_lines")
    branch = models.ForeignKey(Sucursal, null=True, blank=True, on_delete=models.SET_NULL)
    required_qty = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    planned_qty = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    existing_finished_stock = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    net_qty_to_produce = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    capacity_limit_qty = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    capacity_gap_qty = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    constraint_reason = models.CharField(max_length=255, blank=True, default="")
    production_day = models.DateField(null=True, blank=True)
    priority = models.CharField(max_length=20, blank=True, default="")
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "production_plan_lines"
        ordering = ["production_day", "product__nombre", "id"]


class EventoVentaInputRequirement(models.Model):
    RISK_LOW = "BAJO"
    RISK_MEDIUM = "MEDIO"
    RISK_HIGH = "ALTO"
    RISK_CHOICES = [
        (RISK_LOW, "Bajo"),
        (RISK_MEDIUM, "Medio"),
        (RISK_HIGH, "Alto"),
    ]

    sales_event = models.ForeignKey(EventoVenta, on_delete=models.CASCADE, related_name="input_requirements")
    production_plan = models.ForeignKey(
        EventoVentaProductionPlan,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="input_requirements",
    )
    input_item = models.ForeignKey(Insumo, on_delete=models.PROTECT, related_name="sales_event_requirements")
    required_qty = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    on_hand_qty = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    reserved_qty = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    net_shortage_qty = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    unit_cost_estimate = models.DecimalField(max_digits=18, decimal_places=4, default=0)
    risk_level = models.CharField(max_length=10, choices=RISK_CHOICES, default=RISK_LOW)
    required_by_date = models.DateField(null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "input_requirements"
        ordering = ["-net_shortage_qty", "input_item__nombre"]


class EventoVentaPurchaseRequirement(models.Model):
    STATUS_PENDIENTE = "PENDIENTE"
    STATUS_CONFIRMADO = "CONFIRMADO"
    STATUS_OBSERVADO = "OBSERVADO"
    STATUS_CHOICES = [
        (STATUS_PENDIENTE, "Pendiente"),
        (STATUS_CONFIRMADO, "Confirmado"),
        (STATUS_OBSERVADO, "Observado"),
    ]

    sales_event = models.ForeignKey(EventoVenta, on_delete=models.CASCADE, related_name="purchase_requirements")
    input_requirement = models.ForeignKey(EventoVentaInputRequirement, on_delete=models.CASCADE, related_name="purchase_links")
    suggested_purchase_qty = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    purchase_deadline = models.DateField(null=True, blank=True)
    priority = models.CharField(max_length=20, blank=True, default="")
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=STATUS_PENDIENTE)
    supplier = models.ForeignKey(Proveedor, null=True, blank=True, on_delete=models.SET_NULL)
    estimated_cost = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "purchase_requirements"
        ordering = ["-created_at"]


class EventoVentaFinancial(models.Model):
    SCENARIO_CONSERVADOR = "CONSERVADOR"
    SCENARIO_BASE = "BASE"
    SCENARIO_AGRESIVO = "AGRESIVO"
    SCENARIO_CHOICES = [
        (SCENARIO_CONSERVADOR, "Conservador"),
        (SCENARIO_BASE, "Base"),
        (SCENARIO_AGRESIVO, "Agresivo"),
    ]

    sales_event = models.ForeignKey(EventoVenta, on_delete=models.CASCADE, related_name="financials")
    scenario = models.CharField(max_length=20, choices=SCENARIO_CHOICES, default=SCENARIO_BASE)
    estimated_sales = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    estimated_cogs = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    estimated_gross_profit = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    estimated_margin = models.DecimalField(max_digits=6, decimal_places=2, default=0)
    incremental_investment = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    break_even_sales = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    expected_roi = models.DecimalField(max_digits=8, decimal_places=2, default=0)
    sensitivity_json = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "sales_event_financials"
        ordering = ["sales_event", "scenario"]
        unique_together = [("sales_event", "scenario")]


class EventoVentaExecutionMetric(models.Model):
    sales_event = models.ForeignKey(EventoVenta, on_delete=models.CASCADE, related_name="execution_metrics")
    metric_date = models.DateField(db_index=True)
    branch = models.ForeignKey(Sucursal, null=True, blank=True, on_delete=models.SET_NULL)
    product = models.ForeignKey(Receta, null=True, blank=True, on_delete=models.SET_NULL)
    forecast_qty = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    actual_qty = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    forecast_sales = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    actual_sales = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    variance_qty = models.DecimalField(max_digits=18, decimal_places=3, default=0)
    variance_sales = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "event_execution_metrics"
        ordering = ["-metric_date"]


class EventoVentaAuditLog(models.Model):
    sales_event = models.ForeignKey(EventoVenta, on_delete=models.CASCADE, related_name="audit_logs")
    entity_name = models.CharField(max_length=120)
    entity_id = models.CharField(max_length=64, blank=True, default="")
    action_type = models.CharField(max_length=40)
    old_data_json = models.JSONField(default=dict, blank=True)
    new_data_json = models.JSONField(default=dict, blank=True)
    actor_user = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    actor_role = models.CharField(max_length=30, blank=True, default="")
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "sales_event_audit_log"
        ordering = ["-created_at"]


class EventoVentaNotification(models.Model):
    STATUS_PENDIENTE = "PENDIENTE"
    STATUS_LEIDA = "LEIDA"
    STATUS_CHOICES = [
        (STATUS_PENDIENTE, "Pendiente"),
        (STATUS_LEIDA, "Leída"),
    ]

    SEVERITY_INFO = "INFO"
    SEVERITY_WARN = "WARN"
    SEVERITY_CRIT = "CRIT"
    SEVERITY_CHOICES = [
        (SEVERITY_INFO, "Info"),
        (SEVERITY_WARN, "Advertencia"),
        (SEVERITY_CRIT, "Crítica"),
    ]

    sales_event = models.ForeignKey(EventoVenta, on_delete=models.CASCADE, related_name="notifications")
    message = models.CharField(max_length=255)
    severity = models.CharField(max_length=10, choices=SEVERITY_CHOICES, default=SEVERITY_INFO)
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default=STATUS_PENDIENTE)
    created_at = models.DateTimeField(default=timezone.now)

    class Meta:
        db_table = "sales_event_notifications"
        ordering = ["-created_at"]
