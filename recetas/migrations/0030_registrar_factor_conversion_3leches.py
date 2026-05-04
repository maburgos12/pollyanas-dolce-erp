from decimal import Decimal

from django.db import migrations


def registrar_equivalencia(apps, schema_editor):
    Receta = apps.get_model("recetas", "Receta")
    RecetaEquivalencia = apps.get_model("recetas", "RecetaEquivalencia")

    try:
        padre = Receta.objects.get(nombre__icontains="3 Leches Mediano", tipo="PRODUCTO_FINAL")
        porcion = Receta.objects.get(nombre__icontains="3 Leches Rebanada", tipo="PRODUCTO_FINAL")
    except Receta.DoesNotExist:
        padre = (
            Receta.objects.filter(nombre__icontains="3 Leches", tipo="PRODUCTO_FINAL")
            .filter(nombre__icontains="Mediano")
            .first()
        )
        porcion = (
            Receta.objects.filter(nombre__icontains="3 Leches", tipo="PRODUCTO_FINAL")
            .filter(nombre__icontains="Rebanada")
            .first()
        )
        if not padre or not porcion:
            print("WARNING: No se encontraron las recetas de 3 Leches, omitiendo")
            return

    RecetaEquivalencia.objects.get_or_create(
        receta_padre=padre,
        receta_porcion=porcion,
        defaults={
            "factor_conversion": Decimal("6"),
            "activo": True,
            "fuente": "migration_0030",
            "metadata": {"notas": "Pastel mediano rectangular rebana en 6 cubos"},
        },
    )
    print(f"Equivalencia registrada: {padre.nombre} -> {porcion.nombre} x6")


def revertir(apps, schema_editor):
    Receta = apps.get_model("recetas", "Receta")
    RecetaEquivalencia = apps.get_model("recetas", "RecetaEquivalencia")
    porcion = (
        Receta.objects.filter(nombre__icontains="3 Leches", tipo="PRODUCTO_FINAL")
        .filter(nombre__icontains="Rebanada")
        .first()
    )
    if porcion:
        RecetaEquivalencia.objects.filter(
            receta_porcion=porcion,
            factor_conversion=Decimal("6"),
            fuente="migration_0030",
        ).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("recetas", "0029_planproduccion_autorizado_and_more"),
    ]

    operations = [
        migrations.RunPython(registrar_equivalencia, revertir),
    ]
