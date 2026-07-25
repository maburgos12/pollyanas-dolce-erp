from django.db import migrations, models
from django.db.models import Count, Q


NOTA_AUDITORIA = (
    "Fila duplicada de la misma línea Point; se conserva solo para auditoría."
)


def superar_duplicados_point_activos(apps, schema_editor):
    Linea = apps.get_model("logistica", "RutaCargaChecklistLinea")

    duplicados = (
        Linea.objects.exclude(point_transfer_line_id=None)
        .exclude(estatus="SUPERADA")
        .values("point_transfer_line_id")
        .annotate(total=Count("id"))
        .filter(total__gt=1)
    )
    for grupo in duplicados.iterator():
        lineas = list(
            Linea.objects.filter(
                point_transfer_line_id=grupo["point_transfer_line_id"],
            )
            .exclude(estatus="SUPERADA")
            .select_related("point_transfer_line")
            .order_by("id")
        )
        point_line = lineas[0].point_transfer_line
        canonica = max(
            lineas,
            key=lambda linea: (
                linea.source_hash == point_line.source_hash,
                linea.detail_external_id == point_line.detail_external_id,
                linea.transfer_external_id == point_line.transfer_external_id,
                -linea.id,
            ),
        )
        for linea in lineas:
            if linea.id == canonica.id:
                continue
            notas = (linea.notas or "").strip()
            if NOTA_AUDITORIA not in notas:
                notas = " ".join(value for value in [notas, NOTA_AUDITORIA] if value)
            linea.estatus = "SUPERADA"
            linea.superada_por_id = canonica.id
            linea.notas = notas
            linea.save(update_fields=["estatus", "superada_por", "notas"])


class Migration(migrations.Migration):
    # La limpieza confirma sus UPDATE antes de que PostgreSQL construya el
    # índice parcial; así no quedan eventos de triggers FK pendientes.
    atomic = False

    dependencies = [
        ("logistica", "0040_repartidor_tutorial_carga_sucursal_visto_en"),
    ]

    operations = [
        migrations.RunPython(
            superar_duplicados_point_activos,
            reverse_code=migrations.RunPython.noop,
            atomic=True,
        ),
        migrations.AddConstraint(
            model_name="rutacargachecklistlinea",
            constraint=models.UniqueConstraint(
                fields=("point_transfer_line",),
                condition=Q(point_transfer_line__isnull=False) & ~Q(estatus="SUPERADA"),
                name="rutacarga_point_activa_unica",
            ),
        ),
    ]
