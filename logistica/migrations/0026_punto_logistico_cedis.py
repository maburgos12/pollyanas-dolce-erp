from django.db import migrations


def crear_punto_cedis(apps, schema_editor):
    Sucursal = apps.get_model("core", "Sucursal")
    PuntoLogistico = apps.get_model("logistica", "PuntoLogistico")
    cedis = Sucursal.objects.filter(codigo="CEDIS").first()
    matriz = Sucursal.objects.filter(codigo="MATRIZ").first()
    PuntoLogistico.objects.update_or_create(
        tipo="CEDIS",
        nombre="CEDIS",
        defaults={
            "sucursal": cedis or matriz,
            "latitud": "25.567916",
            "longitud": "-108.459969",
            "radio_geocerca_metros": 120,
            "activo": True,
            "notas": "Punto operativo para recargas de ruta en CEDIS.",
        },
    )


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0018_alter_usermoduleaccess_module"),
        ("logistica", "0025_rutaentrega_acompanante"),
    ]

    operations = [
        migrations.RunPython(crear_punto_cedis, migrations.RunPython.noop),
    ]
