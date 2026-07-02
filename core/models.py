from django.db import models
from django.db.models import Q
from django.conf import settings
from django.utils import timezone

class Sucursal(models.Model):
    codigo = models.CharField(max_length=20, unique=True)
    nombre = models.CharField(max_length=120)
    activa = models.BooleanField(default=True)
    fecha_apertura = models.DateField(null=True, blank=True, db_index=True)

    class Meta:
        verbose_name = "Sucursal"
        verbose_name_plural = "Sucursales"

    def __str__(self) -> str:
        return f"{self.codigo} - {self.nombre}"

    def esta_operativa(self, reference_date=None) -> bool:
        reference_date = reference_date or timezone.localdate()
        return self.activa and (self.fecha_apertura is None or self.fecha_apertura <= reference_date)


def sucursales_operativas_q(reference_date=None) -> Q:
    reference_date = reference_date or timezone.localdate()
    return Q(activa=True) & (Q(fecha_apertura__isnull=True) | Q(fecha_apertura__lte=reference_date))


def sucursales_operativas(reference_date=None):
    return Sucursal.objects.filter(sucursales_operativas_q(reference_date)).order_by("codigo", "nombre")

class Departamento(models.Model):
    codigo = models.CharField(max_length=30, unique=True)
    nombre = models.CharField(max_length=120)

    class Meta:
        verbose_name = "Departamento"
        verbose_name_plural = "Departamentos"

    def __str__(self) -> str:
        return f"{self.codigo} - {self.nombre}"

class UserProfile(models.Model):
    user = models.OneToOneField(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    departamento = models.ForeignKey(Departamento, null=True, blank=True, on_delete=models.SET_NULL)
    sucursal = models.ForeignKey(Sucursal, null=True, blank=True, on_delete=models.SET_NULL)
    telefono = models.CharField(max_length=30, blank=True, default="")
    modo_captura_sucursal = models.BooleanField(default=False)
    lock_maestros = models.BooleanField(default=False)
    lock_recetas = models.BooleanField(default=False)
    lock_compras = models.BooleanField(default=False)
    lock_inventario = models.BooleanField(default=False)
    lock_reportes = models.BooleanField(default=False)
    lock_crm = models.BooleanField(default=False)
    lock_logistica = models.BooleanField(default=False)
    lock_rrhh = models.BooleanField(default=False)
    lock_captura_piso = models.BooleanField(default=False)
    lock_auditoria = models.BooleanField(default=False)

    class Meta:
        verbose_name = "Perfil de usuario"
        verbose_name_plural = "Perfiles de usuario"

    def __str__(self) -> str:
        return f"Perfil: {self.user.username}"


class UserModuleAccess(models.Model):
    ACCESS_NONE = "none"
    ACCESS_VIEW = "view"
    ACCESS_MANAGE = "manage"

    MODULOS = [
        ("logistica", "Logística"),
        ("mantenimiento", "Mantenimiento"),
        ("seguimiento", "Seguimiento personal"),
        ("seguimiento.calendario", "Seguimiento - Calendario"),
        ("fallas", "Fallas / Mantenimiento"),
        ("mermas", "Mermas"),
        ("mermas.captura", "Mermas - App sucursal"),
        ("mermas.recepcion", "Mermas - Recepción CEDIS"),
        ("mermas.dashboard", "Mermas - Panel ERP"),
        ("compras", "Compras"),
        ("ventas", "Ventas"),
        ("ventas.visitas_sucursal", "Ventas - Visitas a sucursal"),
        ("reportes", "Reportes"),
        ("produccion", "Producción"),
        ("rrhh", "RRHH"),
        ("rrhh.dashboard", "RRHH - Indicadores"),
        ("rrhh.organizacion", "RRHH - Organización"),
        ("rrhh.empleados", "RRHH - Empleados"),
        ("rrhh.permisos", "RRHH - Permisos"),
        ("rrhh.vacaciones", "RRHH - Vacaciones"),
        ("rrhh.horas_extra", "RRHH - Horas extra"),
        ("rrhh.asistencias", "RRHH - Asistencias"),
        ("rrhh.importar_checador", "RRHH - Checador"),
        ("rrhh.vacantes", "RRHH - Vacantes"),
        ("rrhh.prestamos", "RRHH - Préstamos"),
        ("rrhh.nomina", "RRHH - Nómina"),
        ("rrhh.asignacion_sucursal", "RRHH - Asignación sucursal"),
        ("inventario", "Inventario"),
        ("recetas", "Recetas"),
        ("crm", "CRM"),
        ("auditoria", "Auditoría"),
        ("maestros", "Maestros"),
    ]

    ACCESS_CHOICES = [
        (ACCESS_NONE, "Sin acceso"),
        (ACCESS_VIEW, "Solo ver"),
        (ACCESS_MANAGE, "Ver y editar"),
    ]

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="module_access",
    )
    module = models.CharField(max_length=40, choices=MODULOS)
    access = models.CharField(max_length=10, choices=ACCESS_CHOICES, default=ACCESS_NONE)
    updated_at = models.DateTimeField(auto_now=True)
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        related_name="module_access_updated",
    )

    class Meta:
        unique_together = [("user", "module")]
        verbose_name = "Acceso por módulo"
        verbose_name_plural = "Accesos por módulo"

    def __str__(self) -> str:
        return f"{self.user} · {self.get_module_display()} · {self.get_access_display()}"


class AuditLog(models.Model):
    timestamp = models.DateTimeField(default=timezone.now)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)
    action = models.CharField(max_length=64)  # CREATE/UPDATE/DELETE/APPROVE/IMPORT
    model = models.CharField(max_length=128)
    object_id = models.CharField(max_length=64, blank=True, default="")
    payload = models.JSONField(default=dict, blank=True)

    class Meta:
        verbose_name = "Bitácora (Audit)"
        verbose_name_plural = "Bitácora (Audit)"
        ordering = ["-timestamp"]

    def __str__(self) -> str:
        return f"{self.timestamp:%Y-%m-%d %H:%M} {self.action} {self.model} {self.object_id}"


class Notificacion(models.Model):
    TIPO_PERMISO = "permiso"
    TIPO_HORA_EXTRA = "hora_extra"
    TIPO_PRESTAMO = "prestamo"
    TIPO_SISTEMA = "sistema"
    TIPO_SEGUIMIENTO = "seguimiento"
    TIPO_CHOICES = [
        (TIPO_PERMISO, "Permiso"),
        (TIPO_HORA_EXTRA, "Hora extra"),
        (TIPO_PRESTAMO, "Préstamo"),
        (TIPO_SISTEMA, "Sistema"),
        (TIPO_SEGUIMIENTO, "Seguimiento"),
    ]

    PRIORIDAD_NORMAL = "normal"
    PRIORIDAD_ALTA = "alta"
    PRIORIDAD_CHOICES = [
        (PRIORIDAD_NORMAL, "Normal"),
        (PRIORIDAD_ALTA, "Alta"),
    ]

    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="notificaciones",
    )
    actor = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="notificaciones_generadas",
    )
    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES, default=TIPO_SISTEMA, db_index=True)
    prioridad = models.CharField(max_length=10, choices=PRIORIDAD_CHOICES, default=PRIORIDAD_NORMAL, db_index=True)
    titulo = models.CharField(max_length=160)
    mensaje = models.TextField(blank=True, default="")
    url = models.CharField(max_length=300, blank=True, default="")
    objeto_tipo = models.CharField(max_length=80, blank=True, default="")
    objeto_id = models.CharField(max_length=80, blank=True, default="")
    leida = models.BooleanField(default=False, db_index=True)
    creado_en = models.DateTimeField(auto_now_add=True, db_index=True)
    leido_en = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["leida", "-creado_en"]
        indexes = [
            models.Index(fields=["usuario", "leida", "-creado_en"]),
            models.Index(fields=["tipo", "objeto_tipo", "objeto_id"]),
        ]
        verbose_name = "Notificación"
        verbose_name_plural = "Notificaciones"

    def __str__(self) -> str:
        return f"{self.usuario} · {self.titulo}"

    def marcar_leida(self):
        if not self.leida:
            self.leida = True
            self.leido_en = timezone.now()
            self.save(update_fields=["leida", "leido_en"])
