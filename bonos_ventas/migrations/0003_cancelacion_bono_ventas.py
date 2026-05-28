from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("bonos_ventas", "0002_repartidor_campos_bitacora"),
    ]

    operations = [
        migrations.AddField(
            model_name="configbonoventasperiodo",
            name="cancela_por_asistencia",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="configbonoventasperiodo",
            name="limite_asistencia_cancelacion",
            field=models.PositiveSmallIntegerField(default=2),
        ),
        migrations.AddField(
            model_name="configbonoventasperiodo",
            name="cancela_por_puntualidad",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="configbonoventasperiodo",
            name="limite_retardos_cancelacion",
            field=models.PositiveSmallIntegerField(default=3),
        ),
    ]
