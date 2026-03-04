from django.db import migrations, models
from unidecode import unidecode


def _norm(value: str) -> str:
    raw = unidecode(str(value or "")).lower().strip()
    return " ".join(raw.split())


def backfill_tipo_item(apps, schema_editor):
    Insumo = apps.get_model("maestros", "Insumo")
    total = 0
    updated = 0
    for insumo in Insumo.objects.all().iterator(chunk_size=1000):
        total += 1
        codigo = (insumo.codigo or "").strip().upper()
        categoria = _norm(insumo.categoria or "")
        nombre = _norm(insumo.nombre or "")
        tipo = "MATERIA_PRIMA"

        if codigo.startswith("DERIVADO:RECETA:"):
            tipo = "INSUMO_INTERNO"
        elif (
            "empaque" in categoria
            or "empaque" in nombre
            or any(
                token in nombre
                for token in ("domo", "etiqueta", "caja", "charola", "blonda", "base carton", "base carton")
            )
        ):
            tipo = "EMPAQUE"

        if insumo.tipo_item != tipo:
            insumo.tipo_item = tipo
            insumo.save(update_fields=["tipo_item"])
            updated += 1


def noop_reverse(apps, schema_editor):
    return


class Migration(migrations.Migration):
    dependencies = [
        ("maestros", "0006_insumo_categoria"),
    ]

    operations = [
        migrations.AddField(
            model_name="insumo",
            name="tipo_item",
            field=models.CharField(
                choices=[
                    ("MATERIA_PRIMA", "Materia prima (compra directa)"),
                    ("INSUMO_INTERNO", "Insumo interno (producido)"),
                    ("EMPAQUE", "Empaque"),
                ],
                default="MATERIA_PRIMA",
                max_length=20,
                db_index=True,
            ),
        ),
        migrations.RunPython(backfill_tipo_item, noop_reverse),
    ]
