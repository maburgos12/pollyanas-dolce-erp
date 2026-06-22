from django.db import migrations


def apply_fix(apps, schema_editor):
    Insumo = apps.get_model("maestros", "Insumo")
    LineaReceta = apps.get_model("recetas", "LineaReceta")

    canonical = {
        "Galleta Para Pay": Insumo.objects.filter(codigo_point__iexact="01GP13").order_by("id").first(),
        "Mermelada Fresa": Insumo.objects.filter(codigo_point__iexact="01MF06").order_by("id").first(),
    }
    addon_codes = {"03SPFREB", "SGALLETACAJETAG", "SGALLETACAJETAM", "03SPGCCREB"}
    for old_name, target in canonical.items():
        if target is None:
            continue
        (
            LineaReceta.objects.filter(
                receta__codigo_point__in=addon_codes,
                insumo__nombre__iexact=old_name,
            )
            .update(
                insumo=target,
                insumo_texto=target.nombre,
                match_score=1,
                match_method="EXACT",
                match_status="AUTO_APPROVED",
            )
        )

    lotus = Insumo.objects.filter(codigo_point__iexact="660734").order_by("id").first()
    if lotus is not None:
        lotus.tipo_item = "MATERIA_PRIMA"
        lotus.activo = True
        lotus.save(update_fields=["tipo_item", "activo"])


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("maestros", "0012_align_point_cleaning_units"),
        ("recetas", "0038_politica_margen_precio"),
    ]

    operations = [
        migrations.RunPython(apply_fix, noop_reverse),
    ]
