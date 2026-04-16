from django.db import migrations


FIXED_REVENTA_PRODUCTS = [
    "CAJA CH PARA VENTA",
    "CAJA G PARA VENTA",
    "COCA-COLA 450 ML",
    "TE DEL JARDIN",
    "CAFE STARBUCKS FRAPPUCINO",
]


def _normalize_product_name(value: str) -> str:
    return (value or "").strip().upper()


def seed_fixed_reventa_rules(apps, schema_editor):
    ProductBusinessRule = apps.get_model("reportes", "ProductBusinessRule")

    for product_name in FIXED_REVENTA_PRODUCTS:
        normalized_name = _normalize_product_name(product_name)
        ProductBusinessRule.objects.update_or_create(
            normalized_name=normalized_name,
            defaults={
                "product_name": product_name,
                "classification": "REVENTA",
                "is_fixed": True,
            },
        )


class Migration(migrations.Migration):
    dependencies = [
        ("reportes", "0010_productbusinessrule_normalized_name"),
    ]

    operations = [
        migrations.RunPython(seed_fixed_reventa_rules, reverse_code=migrations.RunPython.noop),
    ]
