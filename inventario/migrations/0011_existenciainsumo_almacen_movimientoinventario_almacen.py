from django.db import migrations, models


ALMACEN_CHOICES = [
    ("ALMACEN_1", "Almacén 1 (principal)"),
    ("ALMACEN_CASA_1", "Almacén Casa 1"),
    ("ALMACEN_CASA_2", "Almacén Casa 2"),
    ("CUARTO_FRIO", "Cuarto Frío"),
    ("VELAS", "Almacén de Velas"),
    ("LIMPIEZA", "Almacén de Limpieza"),
    ("OTRO", "Otro"),
]


class Migration(migrations.Migration):

    dependencies = [
        ("inventario", "0010_conteofisicomensual_lineaconteofisico_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="existenciainsumo",
            name="almacen",
            field=models.CharField(
                max_length=20,
                choices=ALMACEN_CHOICES,
                default="ALMACEN_1",
                verbose_name="Almacén / Ubicación",
                db_index=True,
            ),
        ),
        migrations.AddField(
            model_name="movimientoinventario",
            name="almacen",
            field=models.CharField(
                max_length=20,
                choices=ALMACEN_CHOICES,
                default="ALMACEN_1",
                blank=True,
                verbose_name="Almacén",
            ),
        ),
        migrations.AddField(
            model_name="movimientoinventario",
            name="notas",
            field=models.CharField(
                max_length=255,
                blank=True,
                default="",
                verbose_name="Notas / destino",
            ),
        ),
        migrations.AddField(
            model_name="movimientoinventario",
            name="registrado_por",
            field=models.CharField(
                max_length=120,
                blank=True,
                default="",
                verbose_name="Registrado por",
            ),
        ),
    ]
