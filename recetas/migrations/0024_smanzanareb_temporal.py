from django.db import migrations


def mark_smanzanareb_temporal(apps, schema_editor):
    Receta = apps.get_model("recetas", "Receta")
    Receta.objects.filter(codigo_point__iexact="SMANZANAREB").update(
        temporalidad="TEMPORAL",
        temporalidad_detalle="Temporada manzana",
    )


def noop(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("recetas", "0023_recetacostosemanal"),
    ]

    operations = [
        migrations.RunPython(mark_smanzanareb_temporal, noop),
    ]
