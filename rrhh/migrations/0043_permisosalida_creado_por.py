from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


def recuperar_capturista_desde_notificaciones(apps, schema_editor):
    Notificacion = apps.get_model("core", "Notificacion")
    PermisoSalida = apps.get_model("rrhh", "PermisoSalida")

    notificaciones = (
        Notificacion.objects.filter(
            objeto_tipo="rrhh.PermisoSalida",
            actor_id__isnull=False,
        )
        .values_list("objeto_id", "actor_id")
        .order_by("id")
    )
    for objeto_id, actor_id in notificaciones.iterator():
        try:
            permiso_id = int(objeto_id)
        except (TypeError, ValueError):
            continue
        PermisoSalida.objects.filter(pk=permiso_id, creado_por_id__isnull=True).update(
            creado_por_id=actor_id
        )


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("core", "0013_notificacion"),
        ("rrhh", "0042_normalizar_sucursal_asistencia_checador"),
    ]

    operations = [
        migrations.AddField(
            model_name="permisosalida",
            name="creado_por",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="permisos_capturados",
                to=settings.AUTH_USER_MODEL,
            ),
        ),
        migrations.RunPython(
            recuperar_capturista_desde_notificaciones,
            migrations.RunPython.noop,
        ),
    ]
