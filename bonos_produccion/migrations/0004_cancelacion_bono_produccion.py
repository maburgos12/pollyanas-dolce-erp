from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("bonos_produccion", "0003_configbonoarea"),
    ]

    operations = [
        migrations.AddField(
            model_name="configbonoarea",
            name="cancela_por_asistencia",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="configbonoarea",
            name="limite_asistencia_cancelacion",
            field=models.PositiveSmallIntegerField(default=2),
        ),
        migrations.AddField(
            model_name="configbonoarea",
            name="cancela_por_puntualidad",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="configbonoarea",
            name="limite_retardos_cancelacion",
            field=models.PositiveSmallIntegerField(default=3),
        ),
    ]
