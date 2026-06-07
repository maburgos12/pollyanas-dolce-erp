from django.db import migrations


def aplicar_regla_por_tipo(apps, schema_editor):
    """Solo minutas y proyectos requieren aprobación del DG.

    Los compromisos son actividades de desempeño que gestiona el colaborador, así que:
    - no requieren aprobación, y
    - los que el Agente DG dejó atorados en 'En revisión' regresan a 'En proceso'.
    """
    SeguimientoItem = apps.get_model("seguimiento", "SeguimientoItem")

    # Minutas y proyectos: requieren aprobación
    SeguimientoItem.objects.filter(tipo__in=["MINUTA", "PROYECTO"]).update(requiere_aprobacion=True)

    # Compromisos: no requieren aprobación
    SeguimientoItem.objects.filter(tipo="COMPROMISO").update(requiere_aprobacion=False)

    # Compromisos atorados en revisión por el mapeo del Agente DG → regresan a En proceso
    SeguimientoItem.objects.filter(tipo="COMPROMISO", estatus="EN_REVISION").update(estatus="EN_PROCESO")


def revertir(apps, schema_editor):
    # No-op: no revertimos datos operativos.
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("seguimiento", "0003_seguimientoprorrogasolicitud"),
    ]

    operations = [
        migrations.RunPython(aplicar_regla_por_tipo, revertir),
    ]
