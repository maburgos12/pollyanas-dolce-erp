from django.db import migrations


def mapear_branches_point(apps, schema_editor):
    Sucursal = apps.get_model("core", "Sucursal")
    PointBranch = apps.get_model("pos_bridge", "PointBranch")

    cedis = Sucursal.objects.filter(codigo="CEDIS").first()
    if not cedis:
        return

    PointBranch.objects.filter(id__in=[3, 13, 20, 22]).update(erp_branch=cedis)
    PointBranch.objects.filter(name__iexact="CEDIS").update(erp_branch=cedis)
    PointBranch.objects.filter(name__iexact="Produccion Crucero").update(erp_branch=cedis)


def revertir_mapeo(apps, schema_editor):
    Sucursal = apps.get_model("core", "Sucursal")
    PointBranch = apps.get_model("pos_bridge", "PointBranch")

    cedis = Sucursal.objects.filter(codigo="CEDIS").first()
    if not cedis:
        return

    PointBranch.objects.filter(id__in=[13, 22], erp_branch=cedis).update(erp_branch=None)


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0007_crear_sucursales_cedis_devoluciones"),
        ("pos_bridge", "0016_add_point_conversion_line"),
    ]

    operations = [
        migrations.RunPython(mapear_branches_point, revertir_mapeo),
    ]
