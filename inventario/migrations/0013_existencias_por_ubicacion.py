from django.db import migrations, models
import django.db.models.deletion


ALMACEN_CHOICES = [
    ("ALMACEN_1", "Almacén 1 (principal)"),
    ("CFP_1_1", "CFP 1.1"),
    ("ARMADO", "Armado"),
    ("CFP_1", "CFP 1"),
    ("ALMACEN_CASA_1", "Almacén Casa 1"),
    ("ALMACEN_CASA_2", "Almacén Casa 2"),
    ("CUARTO_FRIO", "Cuarto Frío"),
    ("VELAS", "Almacén de Velas"),
    ("LIMPIEZA", "Almacén de Limpieza"),
    ("OTRO", "Otro"),
]


class Migration(migrations.Migration):
    dependencies = [
        ("inventario", "0012_alter_existenciainsumo_options_and_more"),
    ]

    operations = [
        migrations.AlterField(
            model_name="existenciainsumo",
            name="insumo",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                related_name="existencias",
                to="maestros.insumo",
            ),
        ),
        migrations.AlterField(
            model_name="existenciainsumo",
            name="almacen",
            field=models.CharField(
                choices=ALMACEN_CHOICES,
                db_index=True,
                default="ALMACEN_1",
                max_length=20,
                verbose_name="Almacén / Ubicación",
            ),
        ),
        migrations.AlterField(
            model_name="movimientoinventario",
            name="almacen",
            field=models.CharField(
                blank=True,
                choices=ALMACEN_CHOICES,
                default="ALMACEN_1",
                max_length=20,
            ),
        ),
        migrations.AddConstraint(
            model_name="existenciainsumo",
            constraint=models.UniqueConstraint(
                fields=("insumo", "almacen"),
                name="uniq_existencia_insumo_almacen",
            ),
        ),
    ]
