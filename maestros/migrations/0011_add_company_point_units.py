from decimal import Decimal

from django.db import migrations


UNITS = {
    "GLI": {
        "nombre": "Galón",
        "tipo": "VOLUME",
        "factor_to_base": Decimal("3785.411784"),
    },
    "Gfn": {
        "nombre": "Garrafón",
        "tipo": "VOLUME",
        "factor_to_base": Decimal("20000"),
    },
    "CJA": {
        "nombre": "Caja",
        "tipo": "UNIT",
        "factor_to_base": Decimal("1"),
    },
}


def add_company_point_units(apps, schema_editor):
    UnidadMedida = apps.get_model("maestros", "UnidadMedida")
    for code, defaults in UNITS.items():
        unit = UnidadMedida.objects.filter(codigo__iexact=code).first()
        if unit is None:
            UnidadMedida.objects.create(codigo=code, **defaults)
            continue
        for field, value in defaults.items():
            setattr(unit, field, value)
        unit.save(update_fields=["nombre", "tipo", "factor_to_base"])


def remove_company_point_units(apps, schema_editor):
    UnidadMedida = apps.get_model("maestros", "UnidadMedida")
    UnidadMedida.objects.filter(codigo__in=UNITS.keys()).delete()


class Migration(migrations.Migration):
    dependencies = [
        ("maestros", "0010_alias_pan_arandano_chico"),
    ]

    operations = [
        migrations.RunPython(add_company_point_units, remove_company_point_units),
    ]
