from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("logistica", "0014_lavado_partes_multiples"),
    ]

    operations = [
        migrations.AlterField(
            model_name="reporteunidad",
            name="repartidor",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="reportes_unidad",
                to="logistica.repartidor",
            ),
        ),
    ]
