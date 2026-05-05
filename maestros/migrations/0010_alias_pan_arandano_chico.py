from django.db import migrations


def crear_alias(apps, schema_editor):
    Insumo = apps.get_model("maestros", "Insumo")
    InsumoAlias = apps.get_model("maestros", "InsumoAlias")

    insumo = Insumo.objects.filter(nombre__iexact="Pan Vainilla Dawn Arándano - Chico").first()
    if not insumo:
        return

    InsumoAlias.objects.get_or_create(
        nombre_normalizado="pan arandano chico",
        defaults={
            "nombre": "Pan Arándano Chico",
            "insumo": insumo,
        },
    )


def revertir_alias(apps, schema_editor):
    InsumoAlias = apps.get_model("maestros", "InsumoAlias")
    InsumoAlias.objects.filter(nombre_normalizado="pan arandano chico").delete()


class Migration(migrations.Migration):
    dependencies = [
        ("maestros", "0009_fix_colorante_rojo_volume_unit"),
    ]

    operations = [
        migrations.RunPython(crear_alias, revertir_alias),
    ]
