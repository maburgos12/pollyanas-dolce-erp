from django.db import migrations, models
from django.db.models import F, Q


def backfill_route_sequence(apps, schema_editor):
    SolicitudDomicilio = apps.get_model("logistica", "SolicitudDomicilio")
    repartidor_ids = (
        SolicitudDomicilio.objects.exclude(repartidor_id=None)
        .order_by()
        .values_list("repartidor_id", flat=True)
        .distinct()
    )
    for repartidor_id in repartidor_ids.iterator():
        solicitud_ids = (
            SolicitudDomicilio.objects.filter(repartidor_id=repartidor_id)
            .order_by(
                F("ventana_inicio").asc(nulls_last=True),
                F("ventana_fin").asc(nulls_last=True),
                "id",
            )
            .values_list("id", flat=True)
        )
        for sequence, solicitud_id in enumerate(solicitud_ids.iterator(), start=1):
            SolicitudDomicilio.objects.filter(pk=solicitud_id).update(
                route_sequence=sequence,
            )


class Migration(migrations.Migration):
    dependencies = [
        ("logistica", "0048_domicilio_driver_journey"),
    ]

    operations = [
        migrations.AddField(
            model_name="solicituddomicilio",
            name="route_sequence",
            field=models.PositiveIntegerField(
                blank=True,
                editable=False,
                null=True,
            ),
        ),
        migrations.RunPython(
            backfill_route_sequence,
            reverse_code=migrations.RunPython.noop,
        ),
        migrations.AddConstraint(
            model_name="solicituddomicilio",
            constraint=models.UniqueConstraint(
                condition=Q(
                    repartidor__isnull=False,
                    route_sequence__isnull=False,
                ),
                fields=("repartidor", "route_sequence"),
                name="logistica_domicilio_repartidor_secuencia_unica",
            ),
        ),
    ]
