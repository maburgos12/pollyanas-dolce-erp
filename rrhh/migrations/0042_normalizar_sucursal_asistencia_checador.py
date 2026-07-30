from __future__ import annotations

from django.db import migrations
from django.db.models import Q

CHECADOR_SUCURSAL_CODIGO = "MATRIZ"
CHECADOR_SITIO_CODIGOS = ("MATRIZ", "CEDIS")
FUENTES_CHECADOR = ("hikconnect_api", "hikconnect_excel")


def normalizar_sucursal_checador(apps, schema_editor):
    """Normaliza la sucursal de las asistencias del checador al sitio del aparato.

    Hay un solo checador físico y está en Matriz/CEDIS; el resto de las
    sucursales checa por Point. Las filas que quedaron con otra sucursal traían
    la asignación administrativa vencida del empleado (caso Crucero, que como
    ubicación física dejó de existir) y las que quedaron en null son de personal
    rotativo o sin sucursal asignada. En ambos casos la marca prueba presencia
    en Matriz. No se tocan las filas que ya son Matriz/CEDIS ni ninguna de otra
    fuente, ni marcajes, turnos o incidencias.
    """
    AsistenciaEmpleado = apps.get_model("rrhh", "AsistenciaEmpleado")
    Sucursal = apps.get_model("core", "Sucursal")

    matriz = Sucursal.objects.filter(codigo=CHECADOR_SUCURSAL_CODIGO).first()
    if not matriz:
        return
    ids_sitio = list(
        Sucursal.objects.filter(codigo__in=CHECADOR_SITIO_CODIGOS).values_list("id", flat=True)
    )
    AsistenciaEmpleado.objects.filter(fuente__in=FUENTES_CHECADOR).filter(
        Q(sucursal_id__isnull=True) | ~Q(sucursal_id__in=ids_sitio)
    ).update(sucursal_id=matriz.pk)


class Migration(migrations.Migration):

    dependencies = [
        ('rrhh', '0041_eventohikcloud_conflict_count_and_more'),
    ]

    operations = [
        # Sin reversa: la sucursal anterior era justamente el dato incorrecto.
        migrations.RunPython(normalizar_sucursal_checador, migrations.RunPython.noop),
    ]
