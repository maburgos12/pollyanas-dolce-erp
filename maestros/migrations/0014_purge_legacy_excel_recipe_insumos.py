from django.db import migrations


LEGACY_TO_CANONICAL = {
    "Galleta Para Pay": "01GP13",
    "Mermelada Fresa": "01MF06",
}


def purge_legacy_excel_recipe_insumos(apps, schema_editor):
    Insumo = apps.get_model("maestros", "Insumo")
    CostoInsumo = apps.get_model("maestros", "CostoInsumo")
    ExistenciaInsumo = apps.get_model("inventario", "ExistenciaInsumo")
    MovimientoInventario = apps.get_model("inventario", "MovimientoInventario")
    ConsumoInsumoMensual = apps.get_model("inventario", "ConsumoInsumoMensual")
    PointTransferLine = apps.get_model("pos_bridge", "PointTransferLine")
    FactInventarioDiario = apps.get_model("reportes", "FactInventarioDiario")

    for legacy_name, canonical_code in LEGACY_TO_CANONICAL.items():
        canonical = Insumo.objects.filter(codigo_point=canonical_code, activo=True).first()
        if not canonical:
            continue

        legacy_items = list(Insumo.objects.filter(nombre=legacy_name, codigo_point="").order_by("id"))
        for legacy in legacy_items:
            CostoInsumo.objects.filter(insumo=legacy).update(insumo=canonical)
            MovimientoInventario.objects.filter(insumo=legacy).update(insumo=canonical)
            ConsumoInsumoMensual.objects.filter(insumo=legacy).update(insumo=canonical)
            PointTransferLine.objects.filter(insumo=legacy).update(insumo=canonical)
            FactInventarioDiario.objects.filter(insumo=legacy).update(insumo=canonical)

            legacy_existence = ExistenciaInsumo.objects.filter(insumo=legacy).first()
            if legacy_existence:
                if ExistenciaInsumo.objects.filter(insumo=canonical).exists():
                    legacy_existence.delete()
                else:
                    legacy_existence.insumo = canonical
                    legacy_existence.save(update_fields=["insumo"])

            legacy.delete()


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("inventario", "0012_alter_existenciainsumo_options_and_more"),
        ("maestros", "0013_unique_active_codigo_point"),
        ("pos_bridge", "0017_alter_pointsyncjob_job_type"),
        ("reportes", "0031_dgoperacionsnapshot"),
    ]

    operations = [
        migrations.RunPython(purge_legacy_excel_recipe_insumos, noop_reverse),
    ]
