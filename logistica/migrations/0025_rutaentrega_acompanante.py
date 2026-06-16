from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("logistica", "0024_rutacargachecklistlinea_rutacarga_linea_source_global_unica"),
    ]

    operations = [
        migrations.AddField(
            model_name="rutaentrega",
            name="acompanante",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="rutas_acompanadas",
                to="logistica.repartidor",
            ),
        ),
        migrations.AddField(
            model_name="rutaentrega",
            name="acompanante_manual",
            field=models.CharField(blank=True, default="", max_length=120),
        ),
    ]
