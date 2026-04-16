from django.db import migrations


def set_colorante_rojo_ml(apps, schema_editor):
    Insumo = apps.get_model("maestros", "Insumo")
    UnidadMedida = apps.get_model("maestros", "UnidadMedida")

    ml_unit = UnidadMedida.objects.filter(codigo="ml").first()
    if ml_unit is None:
        return

    targets = Insumo.objects.filter(codigo_point="085")
    for insumo in targets:
        insumo.unidad_base_id = ml_unit.id
        insumo.save(update_fields=["unidad_base"])


def noop(apps, schema_editor):
    return


class Migration(migrations.Migration):
    dependencies = [
        ("maestros", "0008_pointpendingmatch_visibility_and_classification"),
    ]

    operations = [
        migrations.RunPython(set_colorante_rojo_ml, noop),
    ]
