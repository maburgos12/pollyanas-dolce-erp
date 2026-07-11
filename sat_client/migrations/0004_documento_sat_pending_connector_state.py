from django.db import migrations


PENDING_CONNECTOR_MESSAGE = "Conector de portal SAT pendiente"


def mark_pending_connector_requests(apps, schema_editor):
    SolicitudDocumentoSat = apps.get_model("sat_client", "SolicitudDocumentoSat")
    SolicitudDocumentoSat.objects.filter(
        estado="error",
        mensaje__startswith=PENDING_CONNECTOR_MESSAGE,
    ).update(estado="pendiente")


def reverse_pending_connector_requests(apps, schema_editor):
    SolicitudDocumentoSat = apps.get_model("sat_client", "SolicitudDocumentoSat")
    SolicitudDocumentoSat.objects.filter(
        estado="pendiente",
        mensaje__startswith=PENDING_CONNECTOR_MESSAGE,
    ).update(estado="error")


class Migration(migrations.Migration):
    dependencies = [
        ("sat_client", "0003_solicituddocumentosat"),
    ]

    operations = [
        migrations.RunPython(mark_pending_connector_requests, reverse_pending_connector_requests),
    ]
