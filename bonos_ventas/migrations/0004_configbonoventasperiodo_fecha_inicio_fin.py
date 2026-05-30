from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("bonos_ventas", "0003_config_bono_ventas_cancelacion"),
    ]

    operations = [
        migrations.AddField(
            model_name="configbonoventasperiodo",
            name="fecha_inicio",
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="configbonoventasperiodo",
            name="fecha_fin",
            field=models.DateField(blank=True, null=True),
        ),
    ]
