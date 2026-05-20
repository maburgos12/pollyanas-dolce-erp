from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("rrhh", "0004_prestamos_contpaq_v1"),
    ]

    operations = [
        migrations.AddField(
            model_name="permisosalida",
            name="estado_jefe",
            field=models.CharField(
                choices=[
                    ("pendiente", "Pendiente de jefe"),
                    ("preautorizado", "Preautorizado por jefe"),
                    ("rechazado", "Rechazado por jefe"),
                ],
                default="pendiente",
                max_length=16,
            ),
        ),
        migrations.AddField(
            model_name="permisosalida",
            name="autorizado_jefe_por",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="permisos_preautorizados_jefe",
                to=settings.AUTH_USER_MODEL,
            ),
        ),
        migrations.AddField(
            model_name="permisosalida",
            name="fecha_autorizacion_jefe",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="permisosalida",
            name="origen_solicitud",
            field=models.CharField(
                choices=[
                    ("rrhh", "Capital Humano"),
                    ("bonos_ventas", "Bonos ventas"),
                    ("bonos_produccion", "Bonos producción"),
                ],
                default="rrhh",
                max_length=24,
            ),
        ),
    ]
