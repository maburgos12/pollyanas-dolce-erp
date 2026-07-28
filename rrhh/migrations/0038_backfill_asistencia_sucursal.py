from __future__ import annotations

from django.db import migrations


def backfill_asistencia_sucursal(apps, schema_editor):
    """Rellena AsistenciaEmpleado.sucursal en las filas que quedaron en null por
    el resolver frágil del checador (comparaba texto libre por igualdad exacta).
    Additivo: solo escribe donde sucursal es null; nunca sobreescribe una
    sucursal ya asignada ni toca marcajes, turnos ni incidencias."""
    from core.branch_catalog import indice_sucursales_por_texto, resolver_sucursal_por_texto

    AsistenciaEmpleado = apps.get_model("rrhh", "AsistenciaEmpleado")
    Empleado = apps.get_model("rrhh", "Empleado")
    Sucursal = apps.get_model("core", "Sucursal")

    indice = indice_sucursales_por_texto(Sucursal.objects.filter(activa=True))
    empleados_pendientes = Empleado.objects.filter(
        pk__in=AsistenciaEmpleado.objects.filter(sucursal__isnull=True).values("empleado_id")
    )
    for empleado in empleados_pendientes.iterator():
        sucursal_id = empleado.sucursal_ref_id
        if not sucursal_id:
            sucursal = resolver_sucursal_por_texto(empleado.sucursal, indice=indice)
            sucursal_id = sucursal.pk if sucursal else None
        if sucursal_id:
            AsistenciaEmpleado.objects.filter(
                empleado_id=empleado.pk, sucursal__isnull=True
            ).update(sucursal_id=sucursal_id)


class Migration(migrations.Migration):

    dependencies = [
        ('rrhh', '0037_empleado_exento_checador_and_more'),
    ]

    operations = [
        # Sin reversa: no se puede distinguir lo rellenado aquí de lo que ya
        # estaba asignado, y borrarlo perdería datos operativos.
        migrations.RunPython(backfill_asistencia_sucursal, migrations.RunPython.noop),
    ]
