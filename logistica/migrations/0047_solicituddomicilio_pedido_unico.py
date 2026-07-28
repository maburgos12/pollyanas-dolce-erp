from django.db import migrations, models
import django.db.models.deletion


def abort_if_duplicate_deliveries(apps, schema_editor):
    Solicitud = apps.get_model("logistica", "SolicitudDomicilio")
    duplicate_groups = list(
        Solicitud.objects.exclude(pedido_cliente_id=None)
        .values("pedido_cliente_id")
        .annotate(total=models.Count("id"))
        .filter(total__gt=1)
        .order_by("pedido_cliente_id")
        .values_list("pedido_cliente_id", "total")[:20]
    )
    if not duplicate_groups:
        return
    total_groups = (
        Solicitud.objects.exclude(pedido_cliente_id=None)
        .values("pedido_cliente_id")
        .annotate(total=models.Count("id"))
        .filter(total__gt=1)
        .count()
    )
    sample = ", ".join(
        f"pedido_cliente_id={pedido_id} ({total})"
        for pedido_id, total in duplicate_groups
    )
    raise RuntimeError(
        "No se puede aplicar logistica.0047: se detectaron "
        f"{total_groups} pedido(s) con más de un domicilio. "
        "La migración no elige, cancela ni modifica registros automáticamente. "
        f"Concilia manualmente y vuelve a ejecutar. Muestra: {sample}"
    )


class Migration(migrations.Migration):
    dependencies = [
        ("logistica", "0046_solicituddomicilio_operacion_canonica"),
    ]

    operations = [
        migrations.AddField(
            model_name="solicituddomicilio",
            name="duplicado_de",
            field=models.ForeignKey(
                blank=True,
                editable=False,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="duplicados_reconciliados",
                to="logistica.solicituddomicilio",
            ),
        ),
        migrations.AddField(
            model_name="solicituddomicilio",
            name="pedido_cliente_original",
            field=models.ForeignKey(
                blank=True,
                editable=False,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="solicitudes_domicilio_reconciliadas",
                to="crm.pedidocliente",
            ),
        ),
        migrations.RunPython(
            abort_if_duplicate_deliveries,
            migrations.RunPython.noop,
        ),
        migrations.AddConstraint(
            model_name="solicituddomicilio",
            constraint=models.UniqueConstraint(
                condition=models.Q(("pedido_cliente__isnull", False)),
                fields=("pedido_cliente",),
                name="logistica_solicitud_pedido_unico",
            ),
        ),
    ]
