import hashlib

from django.db import migrations, models


def _construir_clave_auditoria(*, regla, ruta_id, parada_id, hecho):
    regla_legible = str(regla or "LEGACY").strip()[:100] or "LEGACY"
    hecho_canonico = str(hecho or "")
    digest = hashlib.sha256(hecho_canonico.encode("utf-8")).hexdigest()
    return f"{regla_legible}:{ruta_id}:{parada_id}:{digest}"


def _normalizar_metadata_legacy(evento):
    metadata = evento.metadata or {}
    clave_legacy = str(metadata.get("clave") or "").strip()
    if not clave_legacy:
        return None
    partes = clave_legacy.split(":", 3)
    regla = metadata.get("regla") or (partes[0] if partes else "LEGACY")
    ruta_id = metadata.get("ruta_id") or (partes[1] if len(partes) > 1 else evento.ruta_id)
    parada_id = metadata.get("parada_id") or (partes[2] if len(partes) > 2 else evento.parada_id)
    hecho = metadata.get("hecho")
    if hecho is None:
        hecho = partes[3] if len(partes) > 3 else clave_legacy
    return _construir_clave_auditoria(
        regla=regla,
        ruta_id=ruta_id,
        parada_id=parada_id,
        hecho=hecho,
    )


def adoptar_claves_legacy(apps, schema_editor):
    EventoRuta = apps.get_model("logistica", "EventoRuta")
    claves_adoptadas = set()
    eventos = EventoRuta.objects.filter(
        tipo="INCONSISTENCIA_ENTREGA",
        clave_auditoria__isnull=True,
    ).order_by("id")
    for evento in eventos.iterator():
        clave = _normalizar_metadata_legacy(evento)
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
