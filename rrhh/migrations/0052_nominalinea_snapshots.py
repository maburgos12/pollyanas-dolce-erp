import django.db.models.deletion
from django.db import migrations, models


def backfill_nomina_snapshots(apps, schema_editor):
    NominaLinea = apps.get_model("rrhh", "NominaLinea")
    lineas = list(
        NominaLinea.objects.select_related("empleado").only(
            "id",
            "empleado__sucursal_ref_id",
            "empleado__departamento",
        )
    )
    for linea in lineas:
        linea.sucursal_snapshot_id = linea.empleado.sucursal_ref_id
        linea.departamento_snapshot = linea.empleado.departamento or ""
    if lineas:
        NominaLinea.objects.bulk_update(
            lineas,
            ["sucursal_snapshot", "departamento_snapshot"],
            batch_size=500,
        )


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
        migrations.RunPython(backfill_nomina_snapshots, migrations.RunPython.noop),
    ]
