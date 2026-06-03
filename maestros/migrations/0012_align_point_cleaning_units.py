from django.db import migrations


UNIT_FIXES = {
    "4001": "GLI",
    "4019": "GLI",
    "4036": "GLI",
    "00142": "GLI",
    "6001": "Gfn",
    "6019": "Gfn",
    "6020": "Gfn",
    "6036": "Gfn",
}


def align_point_cleaning_units(apps, schema_editor):
    Insumo = apps.get_model("maestros", "Insumo")
    UnidadMedida = apps.get_model("maestros", "UnidadMedida")

    units = {
        unit.codigo: unit
        for unit in UnidadMedida.objects.filter(codigo__in=set(UNIT_FIXES.values()))
    }
    for point_code, unit_code in UNIT_FIXES.items():
        unit = units.get(unit_code)
        if unit is None:
            continue
        Insumo.objects.filter(codigo_point=point_code).update(unidad_base_id=unit.id)


def restore_piece_units(apps, schema_editor):
    Insumo = apps.get_model("maestros", "Insumo")
    UnidadMedida = apps.get_model("maestros", "UnidadMedida")

    piece = UnidadMedida.objects.filter(codigo="pza").first()
    if piece is None:
        return
    Insumo.objects.filter(codigo_point__in=UNIT_FIXES.keys()).update(unidad_base_id=piece.id)


class Migration(migrations.Migration):
    dependencies = [
        ("maestros", "0011_add_company_point_units"),
    ]

    operations = [
        migrations.RunPython(align_point_cleaning_units, restore_piece_units),
    ]
