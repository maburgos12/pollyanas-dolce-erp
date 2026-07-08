from __future__ import annotations

from django.db import migrations


def backfill_sucursal_ref(apps, schema_editor):
    """Puebla Empleado.sucursal_ref desde el texto libre `sucursal` usando el
    resolver canónico normalizado (nombre/código, tolerante a prefijo/acentos).
    Additivo: solo escribe sucursal_ref; no toca ningún otro campo. Los que no
    resuelven quedan en null."""
    from core.branch_catalog import indice_sucursales_por_texto, resolver_sucursal_por_texto

    Empleado = apps.get_model("rrhh", "Empleado")
    Sucursal = apps.get_model("core", "Sucursal")

    indice = indice_sucursales_por_texto(Sucursal.objects.filter(activa=True))
    for empleado in Empleado.objects.filter(sucursal_ref__isnull=True).exclude(sucursal="").iterator():
        sucursal = resolver_sucursal_por_texto(empleado.sucursal, indice=indice)
        if sucursal is not None:
            Empleado.objects.filter(pk=empleado.pk).update(sucursal_ref_id=sucursal.pk)


def limpiar_sucursal_ref(apps, schema_editor):
    # Reversa segura: limpiar el FK (el texto original se conserva intacto).
    Empleado = apps.get_model("rrhh", "Empleado")
    Empleado.objects.update(sucursal_ref=None)


class Migration(migrations.Migration):

    dependencies = [
        ('rrhh', '0034_empleado_sucursal_ref'),
    ]

    operations = [
        migrations.RunPython(backfill_sucursal_ref, limpiar_sucursal_ref),
    ]
