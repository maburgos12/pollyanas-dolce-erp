from django.db import migrations, models


def _normalize_product_name(value: str) -> str:
    return (value or "").strip().upper()


def backfill_normalized_name(apps, schema_editor):
    ProductBusinessRule = apps.get_model("reportes", "ProductBusinessRule")
    collisions: dict[str, list[str]] = {}
    rows = list(ProductBusinessRule.objects.all().only("id", "product_name"))

    for row in rows:
        normalized_name = _normalize_product_name(row.product_name)
        collisions.setdefault(normalized_name, []).append(row.product_name or "")

    duplicated = {
        normalized_name: names
        for normalized_name, names in collisions.items()
        if normalized_name and len(names) > 1
    }
    if duplicated:
        details = "; ".join(
            f"{normalized_name}: {sorted(set(names))}"
            for normalized_name, names in sorted(duplicated.items())
        )
        raise RuntimeError(
            "ProductBusinessRule has semantic duplicates by normalized_name. "
            f"Resolve them before applying this migration: {details}"
        )

    for row in rows:
        ProductBusinessRule.objects.filter(pk=row.pk).update(
            normalized_name=_normalize_product_name(row.product_name)
        )


def clear_normalized_name(apps, schema_editor):
    ProductBusinessRule = apps.get_model("reportes", "ProductBusinessRule")
    ProductBusinessRule.objects.all().update(normalized_name=None)


class Migration(migrations.Migration):
    dependencies = [
        ("reportes", "0009_productbusinessrule_alter_presupuestoimport_tipo"),
    ]

    operations = [
        migrations.AddField(
            model_name="productbusinessrule",
            name="normalized_name",
            field=models.CharField(
                blank=True,
                editable=False,
                max_length=255,
                null=True,
            ),
        ),
        migrations.RunPython(backfill_normalized_name, reverse_code=clear_normalized_name),
        migrations.AlterField(
            model_name="productbusinessrule",
            name="normalized_name",
            field=models.CharField(
                db_index=True,
                editable=False,
                max_length=255,
                unique=True,
            ),
        ),
    ]
