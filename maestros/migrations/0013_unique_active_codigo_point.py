from django.db import migrations, models
from django.db.models import Count, Q


def consolidate_active_point_duplicates(apps, schema_editor):
    Insumo = apps.get_model("maestros", "Insumo")
    InsumoAlias = apps.get_model("maestros", "InsumoAlias")
    CostoInsumo = apps.get_model("maestros", "CostoInsumo")
    LineaReceta = apps.get_model("recetas", "LineaReceta")
    SolicitudCompra = apps.get_model("compras", "SolicitudCompra")
    MovimientoInventario = apps.get_model("inventario", "MovimientoInventario")
    AjusteInventario = apps.get_model("inventario", "AjusteInventario")
    ExistenciaInsumo = apps.get_model("inventario", "ExistenciaInsumo")

    referenced_inactive_point_ids = (
        LineaReceta.objects.exclude(match_status="REJECTED")
        .exclude(tipo_linea="SUBSECCION")
        .filter(insumo__activo=False)
        .exclude(insumo__codigo_point="")
        .values_list("insumo_id", flat=True)
        .distinct()
    )
    Insumo.objects.filter(id__in=list(referenced_inactive_point_ids)).update(activo=True)

    duplicate_codes = (
        Insumo.objects.filter(activo=True)
        .exclude(codigo_point="")
        .values("codigo_point")
        .annotate(total=Count("id"))
        .filter(total__gt=1)
        .values_list("codigo_point", flat=True)
    )

    for code in duplicate_codes:
        insumos = list(Insumo.objects.filter(activo=True, codigo_point=code).order_by("id"))
        if len(insumos) < 2:
            continue
        ids = [item.id for item in insumos]

        recipe_counts = dict(
            LineaReceta.objects.filter(insumo_id__in=ids)
            .values("insumo_id")
            .annotate(total=Count("id"))
            .values_list("insumo_id", "total")
        )
        movement_counts = dict(
            MovimientoInventario.objects.filter(insumo_id__in=ids)
            .values("insumo_id")
            .annotate(total=Count("id"))
            .values_list("insumo_id", "total")
        )
        existence_ids = set(ExistenciaInsumo.objects.filter(insumo_id__in=ids).values_list("insumo_id", flat=True))
        cost_counts = dict(
            CostoInsumo.objects.filter(insumo_id__in=ids)
            .values("insumo_id")
            .annotate(total=Count("id"))
            .values_list("insumo_id", "total")
        )

        def priority(insumo):
            return (
                recipe_counts.get(insumo.id, 0) * 1000
                + movement_counts.get(insumo.id, 0) * 100
                + (50 if insumo.id in existence_ids else 0)
                + cost_counts.get(insumo.id, 0)
                - insumo.id / 1000000
            )

        canonical = sorted(insumos, key=priority, reverse=True)[0]
        for duplicate in insumos:
            if duplicate.id == canonical.id:
                continue

            LineaReceta.objects.filter(insumo_id=duplicate.id).update(insumo=canonical)
            CostoInsumo.objects.filter(insumo_id=duplicate.id).update(insumo=canonical)
            InsumoAlias.objects.filter(insumo_id=duplicate.id).update(insumo=canonical)
            SolicitudCompra.objects.filter(insumo_id=duplicate.id).update(insumo=canonical)
            MovimientoInventario.objects.filter(insumo_id=duplicate.id).update(insumo=canonical)
            AjusteInventario.objects.filter(insumo_id=duplicate.id).update(insumo=canonical)

            duplicate_existence = ExistenciaInsumo.objects.filter(insumo_id=duplicate.id).first()
            canonical_has_existence = ExistenciaInsumo.objects.filter(insumo_id=canonical.id).exists()
            if duplicate_existence and not canonical_has_existence:
                duplicate_existence.insumo = canonical
                duplicate_existence.save(update_fields=["insumo"])

            duplicate.activo = False
            duplicate.save(update_fields=["activo"])


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("compras", "0008_compras_read_indexes"),
        ("inventario", "0012_alter_existenciainsumo_options_and_more"),
        ("maestros", "0012_align_point_cleaning_units"),
        ("recetas", "0040_repoint_legacy_pay_ingredients"),
    ]

    operations = [
        migrations.RunPython(consolidate_active_point_duplicates, noop_reverse),
        migrations.AddConstraint(
            model_name="insumo",
            constraint=models.UniqueConstraint(
                fields=("codigo_point",),
                condition=Q(activo=True) & ~Q(codigo_point=""),
                name="uniq_active_insumo_codigo_point",
            ),
        ),
    ]
