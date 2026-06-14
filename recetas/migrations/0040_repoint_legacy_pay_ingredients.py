from django.db import migrations


def apply_fix(apps, schema_editor):
    Insumo = apps.get_model("maestros", "Insumo")
    LineaReceta = apps.get_model("recetas", "LineaReceta")

    canonical_cookie = Insumo.objects.filter(codigo_point__iexact="01GP13").order_by("id").first()
    legacy_cookie_ids = list(
        Insumo.objects.filter(codigo_point="", nombre__iexact="Galleta Para Pay").values_list("id", flat=True)
    )
    if canonical_cookie is not None and legacy_cookie_ids:
        LineaReceta.objects.filter(
            insumo_id__in=legacy_cookie_ids,
            insumo_texto__iexact="Galleta Pay",
        ).update(
            insumo=canonical_cookie,
            insumo_texto=canonical_cookie.nombre,
            match_score=1,
            match_method="EXACT",
            match_status="AUTO_APPROVED",
        )
        Insumo.objects.filter(id__in=legacy_cookie_ids).update(activo=False)

    canonical_jam = Insumo.objects.filter(codigo_point__iexact="01MF06").order_by("id").first()
    legacy_jam_ids = list(
        Insumo.objects.filter(codigo_point="", nombre__iexact="Mermelada Fresa").values_list("id", flat=True)
    )
    if canonical_jam is not None and legacy_jam_ids:
        LineaReceta.objects.filter(
            insumo_id__in=legacy_jam_ids,
            insumo_texto__iexact="Mermelada Fresa Liquida",
        ).update(
            insumo=canonical_jam,
            insumo_texto=canonical_jam.nombre,
            match_score=1,
            match_method="EXACT",
            match_status="AUTO_APPROVED",
        )
        Insumo.objects.filter(id__in=legacy_jam_ids).update(activo=False)


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("recetas", "0039_fix_point_canonical_recipe_inputs"),
    ]

    operations = [
        migrations.RunPython(apply_fix, noop_reverse),
    ]
