from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="SolicitudCancelacion",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("tipo", models.CharField(choices=[("falla", "Reporte de falla"), ("unidad", "Reporte de unidad logística"), ("orden", "Orden de mantenimiento")], max_length=10)),
                ("objeto_id", models.PositiveIntegerField()),
                ("referencia", models.CharField(max_length=200)),
                ("motivo", models.TextField()),
                ("estatus", models.CharField(choices=[("pendiente", "Pendiente"), ("aprobada", "Aprobada y eliminada"), ("rechazada", "Rechazada")], default="pendiente", max_length=12)),
                ("notas_resolucion", models.TextField(blank=True, default="")),
                ("creado_en", models.DateTimeField(default=django.utils.timezone.now)),
                ("resuelto_en", models.DateTimeField(blank=True, null=True)),
                ("solicitado_por", models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="solicitudes_cancelacion", to=settings.AUTH_USER_MODEL)),
                ("resuelto_por", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="cancelaciones_resueltas", to=settings.AUTH_USER_MODEL)),
            ],
            options={"ordering": ["-creado_en"], "verbose_name": "Solicitud de cancelación", "verbose_name_plural": "Solicitudes de cancelación"},
        ),
    ]
