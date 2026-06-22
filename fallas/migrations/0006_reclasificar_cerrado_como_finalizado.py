from django.db import migrations, models
from django.utils import timezone


def reclasificar_cerrados(apps, schema_editor):
    ReporteFalla = apps.get_model("fallas", "ReporteFalla")
    BitacoraFalla = apps.get_model("fallas", "BitacoraFalla")
    now = timezone.now()
    reportes = list(ReporteFalla.objects.filter(estatus="cerrado"))
    bitacora = []

    for reporte in reportes:
        if not reporte.fecha_resolucion:
            reporte.fecha_resolucion = reporte.fecha_cierre or now
        reporte.estatus = "resuelto"
        bitacora.append(
            BitacoraFalla(
                reporte_id=reporte.id,
                usuario_id=reporte.cerrado_por_id or reporte.reportado_por_id,
                estatus_anterior="cerrado",
                estatus_nuevo="resuelto",
                comentario="Ajuste operativo: cerrado reclasificado como finalizado.",
                timestamp=now,
            )
        )

    if reportes:
        ReporteFalla.objects.bulk_update(reportes, ["estatus", "fecha_resolucion"])
        BitacoraFalla.objects.bulk_create(bitacora)


class Migration(migrations.Migration):

    dependencies = [
        ("fallas", "0005_evidenciaseguimientofalla"),
    ]

    operations = [
        migrations.AlterField(
            model_name="reportefalla",
            name="estatus",
            field=models.CharField(
                choices=[
                    ("abierto", "Abierto"),
                    ("en_revision", "En revisión"),
                    ("en_proceso", "En proceso"),
                    ("resuelto", "Finalizado"),
                    ("cerrado", "Cerrado / validado"),
                    ("cancelado", "Cancelado"),
                ],
                default="abierto",
                max_length=15,
            ),
        ),
        migrations.RunPython(reclasificar_cerrados, migrations.RunPython.noop),
    ]
