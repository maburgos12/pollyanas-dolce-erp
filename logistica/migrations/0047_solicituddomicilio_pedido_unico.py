import base64
import json
import re

from django.db import migrations, models
import django.db.models.deletion


MARKER_PREFIX = "ROLLBACK_DOMICILIO_0047:"
MARKER_RE = re.compile(r"\n\[CONCILIACION_DOMICILIO_0047[^\]]*\]\n\[ROLLBACK_DOMICILIO_0047:([A-Za-z0-9_=-]+)\]$")


def _encode_state(solicitud):
    payload = {
        "pedido_cliente_id": solicitud.pedido_cliente_id,
        "estatus": solicitud.estatus,
        "legacy_without_point": solicitud.legacy_without_point,
        "cancelacion_motivo": solicitud.cancelacion_motivo,
        "notas": solicitud.notas,
    }
    return base64.urlsafe_b64encode(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")


def reconcile_duplicate_deliveries(apps, schema_editor):
    Solicitud = apps.get_model("logistica", "SolicitudDomicilio")
    duplicate_order_ids = (
        Solicitud.objects.exclude(pedido_cliente_id=None)
        .values("pedido_cliente_id")
        .annotate(total=models.Count("id"))
        .filter(total__gt=1)
        .values_list("pedido_cliente_id", flat=True)
    )
    for pedido_id in duplicate_order_ids.iterator():
        solicitudes = list(
            Solicitud.objects.filter(pedido_cliente_id=pedido_id).order_by("id")
        )
        canonical = solicitudes[0]
        for duplicate in solicitudes[1:]:
            token = _encode_state(duplicate)
            marker = (
                f"\n[CONCILIACION_DOMICILIO_0047 canónico={canonical.id}]"
                f"\n[{MARKER_PREFIX}{token}]"
            )
            updates = {
                "pedido_cliente_id": None,
                "pedido_cliente_original_id": pedido_id,
                "duplicado_de_id": canonical.id,
                "legacy_without_point": True,
                "notas": f"{duplicate.notas}{marker}",
            }
            if duplicate.estatus not in {"ENTREGADO", "CANCELADO"}:
                updates["estatus"] = "CANCELADO"
                updates["cancelacion_motivo"] = (
                    "Conciliación 0047: domicilio duplicado; "
                    f"se conserva como histórico del canónico #{canonical.id}."
                )
            elif duplicate.estatus == "CANCELADO" and not duplicate.cancelacion_motivo:
                updates["cancelacion_motivo"] = (
                    "Conciliación 0047: domicilio duplicado; "
                    f"se conserva como histórico del canónico #{canonical.id}."
                )
            Solicitud._base_manager.filter(pk=duplicate.pk).update(**updates)


def restore_duplicate_deliveries(apps, schema_editor):
    Solicitud = apps.get_model("logistica", "SolicitudDomicilio")
    for duplicate in Solicitud.objects.exclude(duplicado_de_id=None).iterator():
        match = MARKER_RE.search(duplicate.notas or "")
        if not match:
            continue
        payload = json.loads(
            base64.urlsafe_b64decode(match.group(1).encode("ascii")).decode("utf-8")
        )
        Solicitud._base_manager.filter(pk=duplicate.pk).update(
            pedido_cliente_id=payload["pedido_cliente_id"],
            estatus=payload["estatus"],
            legacy_without_point=payload["legacy_without_point"],
            cancelacion_motivo=payload["cancelacion_motivo"],
            notas=payload["notas"],
            duplicado_de_id=None,
            pedido_cliente_original_id=None,
        )


class Migration(migrations.Migration):
    # La reversión debe confirmar la restauración de las FK antes de retirar
    # el campo de trazabilidad; PostgreSQL no permite ALTER con triggers FK
    # diferidos pendientes dentro de una sola transacción de migración.
    atomic = False

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
            reconcile_duplicate_deliveries,
            restore_duplicate_deliveries,
            atomic=True,
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
