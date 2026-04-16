from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("reportes", "0023_operationsmetricsnapshot_and_more"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.AddField(
            model_name="proyectoinversionescenario",
            name="capturado_por",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="proyectos_inversion_escenarios_capturados",
                to=settings.AUTH_USER_MODEL,
            ),
        ),
        migrations.AddField(
            model_name="proyectoinversionescenario",
            name="estatus_simulacion",
            field=models.CharField(
                choices=[
                    ("EN_REVISION", "En revision"),
                    ("CANDIDATO", "Candidato"),
                    ("DESCARTADO", "Descartado"),
                    ("APROBADO_PRELIMINAR", "Aprobado preliminar"),
                ],
                db_index=True,
                default="EN_REVISION",
                max_length=30,
            ),
        ),
        migrations.AddField(
            model_name="proyectoinversionescenario",
            name="simulacion_hash",
            field=models.CharField(blank=True, db_index=True, default="", max_length=64),
        ),
    ]
