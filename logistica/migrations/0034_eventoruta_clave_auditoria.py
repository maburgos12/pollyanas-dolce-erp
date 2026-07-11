from django.db import migrations, models


def adoptar_claves_legacy(apps, schema_editor):
    EventoRuta = apps.get_model("logistica", "EventoRuta")
    claves_adoptadas = set()
    eventos = EventoRuta.objects.filter(
        tipo="INCONSISTENCIA_ENTREGA",
        clave_auditoria__isnull=True,
    ).order_by("id")
    for evento in eventos.iterator():
        clave = str((evento.metadata or {}).get("clave") or "").strip()
        if not clave or clave in claves_adoptadas:
            continue
        evento.clave_auditoria = clave
        evento.save(update_fields=["clave_auditoria"])
        claves_adoptadas.add(clave)


class Migration(migrations.Migration):
    dependencies = [("logistica", "0033_normalizar_eventos_recarga_cedis")]

    operations = [
        migrations.AddField(
            model_name="eventoruta",
            name="clave_auditoria",
            field=models.CharField(
                blank=True,
                db_index=True,
                editable=False,
                max_length=255,
                null=True,
                unique=True,
            ),
        ),
        migrations.RunPython(adoptar_claves_legacy, migrations.RunPython.noop),
    ]
