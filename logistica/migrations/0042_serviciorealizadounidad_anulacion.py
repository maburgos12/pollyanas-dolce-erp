from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("logistica", "0041_rutacarga_point_activa_unica"),
    ]

    operations = [
        migrations.AddField(
            model_name="serviciorealizadounidad",
            name="anulado_en",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="serviciorealizadounidad",
            name="anulado_por",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="servicios_unidad_anulados",
                to=settings.AUTH_USER_MODEL,
            ),
        ),
        migrations.AddField(
            model_name="serviciorealizadounidad",
            name="duplicado_de",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="duplicados_anulados",
                to="logistica.serviciorealizadounidad",
            ),
        ),
        migrations.AddField(
            model_name="serviciorealizadounidad",
            name="motivo_anulacion",
            field=models.TextField(blank=True),
        ),
    ]
