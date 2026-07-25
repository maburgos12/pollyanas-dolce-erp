from django.db import migrations


def normalize_repartidor_area(apps, schema_editor):
    Empleado = apps.get_model("rrhh", "Empleado")
    BonoEsquema = apps.get_model("rrhh", "BonoEsquema")
    CatalogoFuncionOperativa = apps.get_model("rrhh", "CatalogoFuncionOperativa")

    Empleado.objects.filter(area__iexact="REPARTIDORES").update(area="REPARTIDOR")
    BonoEsquema.objects.filter(area__iexact="REPARTIDORES").update(area="REPARTIDOR")

    legacy = CatalogoFuncionOperativa.objects.filter(codigo="REPARTIDORES").first()
    canonical = CatalogoFuncionOperativa.objects.filter(codigo="REPARTIDOR").first()
    if legacy and canonical and legacy.pk != canonical.pk:
        if not canonical.etiqueta:
            canonical.etiqueta = legacy.etiqueta
        if not canonical.departamento_origen:
            canonical.departamento_origen = legacy.departamento_origen
        if not canonical.departamento_actual:
            canonical.departamento_actual = legacy.departamento_actual
        if not canonical.puesto_operativo:
            canonical.puesto_operativo = legacy.puesto_operativo
        if not canonical.nivel_organizacional:
            canonical.nivel_organizacional = legacy.nivel_organizacional
        canonical.activo = canonical.activo or legacy.activo
        canonical.sistema = canonical.sistema or legacy.sistema
        canonical.etiqueta = "Repartidor"
        canonical.save(
            update_fields=[
                "etiqueta",
                "departamento_origen",
                "departamento_actual",
                "puesto_operativo",
                "nivel_organizacional",
                "activo",
                "sistema",
                "actualizado_en",
            ]
        )
        legacy.delete()
    elif legacy:
        legacy.codigo = "REPARTIDOR"
        legacy.etiqueta = "Repartidor"
        legacy.save(update_fields=["codigo", "etiqueta", "actualizado_en"])
    elif canonical:
        canonical.etiqueta = "Repartidor"
        canonical.save(update_fields=["etiqueta", "actualizado_en"])


class Migration(migrations.Migration):

    dependencies = [
        ("rrhh", "0033_incapacidad_fecha_check"),
    ]

    operations = [
        migrations.RunPython(normalize_repartidor_area, migrations.RunPython.noop),
    ]
