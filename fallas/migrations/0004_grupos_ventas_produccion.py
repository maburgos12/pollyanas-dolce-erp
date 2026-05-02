from django.db import migrations


def crear_grupos(apps, schema_editor):
    Group = apps.get_model("auth", "Group")
    for nombre in ["ventas", "produccion"]:
        Group.objects.get_or_create(name=nombre)


class Migration(migrations.Migration):
    dependencies = [
        ("fallas", "0003_reportefalla_add_area"),
    ]

    operations = [
        migrations.RunPython(crear_grupos, migrations.RunPython.noop),
    ]
