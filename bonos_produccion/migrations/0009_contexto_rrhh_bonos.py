from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("bonos_produccion", "0008_areas_preparacion_y_cuartos_frios"),
    ]

    operations = [
        migrations.AddField(
            model_name="bonoproduccionempleado",
            name="faltas_rrhh",
            field=models.PositiveSmallIntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="registrodiarioproduccion",
            name="estado_rrhh",
            field=models.CharField(blank=True, default="", max_length=32),
        ),
        migrations.AddField(
            model_name="registrodiarioproduccion",
            name="falta_penalizable",
            field=models.BooleanField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="registrodiarioproduccion",
            name="fecha",
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="registrodiarioproduccion",
            name="motivo_rrhh",
            field=models.CharField(blank=True, default="", max_length=200),
        ),
    ]
