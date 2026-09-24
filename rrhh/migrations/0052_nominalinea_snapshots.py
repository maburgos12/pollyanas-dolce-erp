import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0023_cumpleanos_activos_y_avisos"),
        ("rrhh", "0051_jornadas_semanales"),
    ]

    operations = [
        migrations.AddField(
            model_name="nominalinea",
            name="departamento_snapshot",
            field=models.CharField(blank=True, default="", max_length=50),
        ),
        migrations.AddField(
            model_name="nominalinea",
            name="sucursal_snapshot",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="lineas_nomina_snapshot",
                to="core.sucursal",
            ),
        ),
        migrations.AddField(
            model_name="nominalinea",
            name="snapshot_capturado_en",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="nominalinea",
            name="snapshot_origen",
            field=models.CharField(
                blank=True,
                choices=[
                    ("CREACION", "Creación de línea"),
                    ("LISTA_RAYA", "Importación de lista de raya"),
                    ("CAPTURA_EXPLICITA", "Captura explícita posterior"),
                ],
                default="",
                max_length=24,
            ),
        ),
    ]
