from django.db import migrations


def crear_grupos(apps, schema_editor):
    Group = apps.get_model("auth", "Group")
    for nombre in ["personal_sucursal", "compras_logistica", "dg"]:
        Group.objects.get_or_create(name=nombre)


class Migration(migrations.Migration):
    dependencies = [
        ("fallas", "0001_initial"),
    ]

    operations = [
        migrations.RunPython(crear_grupos, migrations.RunPython.noop),
    ]
