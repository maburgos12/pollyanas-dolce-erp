from decimal import Decimal

from django.db import migrations


EQUIVALENCIAS = [
    {
        "porcion_id": 111,
        "porcion_nombre": "Pay de Plátano Rebanada",
        "padre_id": 61,
        "padre_nombre": "Pay de Plátano Grande",
        "factor": Decimal("8"),
    },
    {
        "porcion_id": 118,
        "porcion_nombre": "Pastel Arándano R",
        "padre_id": 96,
        "padre_nombre": "Pastel Arándano Mediano",
        "factor": Decimal("10"),
    },
    {
        "porcion_id": 123,
        "porcion_nombre": "Pastel Lotus R",
        "padre_id": 95,
        "padre_nombre": "Pastel Lotus Mediano",
        "factor": Decimal("10"),
    },
]


def _get_recipe(Receta, recipe_id, name):
    recipe = Receta.objects.filter(id=recipe_id, tipo="PRODUCTO_FINAL").first()
    if recipe:
        return recipe
    return Receta.objects.filter(nombre__iexact=name, tipo="PRODUCTO_FINAL").first()


def registrar_equivalencias(apps, schema_editor):
    Receta = apps.get_model("recetas", "Receta")
    RecetaEquivalencia = apps.get_model("recetas", "RecetaEquivalencia")

    for item in EQUIVALENCIAS:
        porcion = _get_recipe(Receta, item["porcion_id"], item["porcion_nombre"])
        padre = _get_recipe(Receta, item["padre_id"], item["padre_nombre"])
        if not porcion or not padre:
            print(f"WARNING: No se encontró equivalencia {item['porcion_nombre']} -> {item['padre_nombre']}")
            continue

        equivalencia, created = RecetaEquivalencia.objects.get_or_create(
            receta_porcion=porcion,
            defaults={
                "receta_padre": padre,
                "factor_conversion": item["factor"],
                "activo": True,
                "fuente": "migration_0032",
                "metadata": {
                    "notas": "Equivalencia histórica para Producido vs Vendido",
                    "porcion_id_verificado": item["porcion_id"],
                    "padre_id_verificado": item["padre_id"],
                },
            },
        )
        if not created and not equivalencia.activo:
            equivalencia.activo = True
            equivalencia.save(update_fields=["activo", "actualizado_en"])


def revertir_equivalencias(apps, schema_editor):
    Receta = apps.get_model("recetas", "Receta")
    RecetaEquivalencia = apps.get_model("recetas", "RecetaEquivalencia")
    porcion_ids = [item["porcion_id"] for item in EQUIVALENCIAS]
    porcion_names = [item["porcion_nombre"] for item in EQUIVALENCIAS]
    recetas = Receta.objects.filter(id__in=porcion_ids) | Receta.objects.filter(nombre__in=porcion_names)
    RecetaEquivalencia.objects.filter(
        receta_porcion__in=recetas,
        fuente="migration_0032",
    ).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("recetas", "0031_product_closure_physical_inventory"),
    ]

    operations = [
        migrations.RunPython(registrar_equivalencias, revertir_equivalencias),
    ]
