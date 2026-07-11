from django.db import migrations


TIPOS_HISTORICOS_RECARGA = ["recarga_cedis", "recarga_cedis_pwa"]


def normalizar_recargas_cedis(apps, schema_editor):
    EventoRuta = apps.get_model("logistica", "EventoRuta")
    EventoRuta.objects.filter(
        tipo="INCIDENCIA_MANUAL",
        metadata__tipo__in=TIPOS_HISTORICOS_RECARGA,
    ).update(tipo="RECARGA_CEDIS")


def revertir_recargas_cedis(apps, schema_editor):
    EventoRuta = apps.get_model("logistica", "EventoRuta")
    EventoRuta.objects.filter(
        tipo="RECARGA_CEDIS",
        metadata__tipo__in=TIPOS_HISTORICOS_RECARGA,
    ).update(tipo="INCIDENCIA_MANUAL")


class Migration(migrations.Migration):
    dependencies = [
        ("logistica", "0032_alter_eventoruta_tipo_and_more"),
    ]

    operations = [
        migrations.RunPython(normalizar_recargas_cedis, revertir_recargas_cedis),
    ]
