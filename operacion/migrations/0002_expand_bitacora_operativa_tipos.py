# Expands operational bitacora choices for production capture formats.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("operacion", "0001_initial"),
    ]

    operations = [
        migrations.AlterField(
            model_name="bitacoraoperativa",
            name="tipo",
            field=models.CharField(
                choices=[
                    ("SALIDAS_CFP1", "Salidas CFP1 a sucursales"),
                    ("INVENTARIO_CFP1", "Inventario Diario CFP1"),
                    ("PLAGAS", "Registro de control de plagas"),
                    ("CFP11", "Control de Inventario Diario CFP 1.1"),
                    ("ROTACION", "Rotación de producto bitácora"),
                    ("REBANADO", "Producto Rebanado"),
                    ("TEMPERATURA", "Bitácora de temperatura"),
                    ("HORNOS", "Control de producción - Hornos"),
                    ("ARMADO", "Control de producción - Armado"),
                    ("INOCUIDAD", "Checklist inocuidad producción"),
                    ("LIMPIEZA", "Checklist limpieza"),
                    ("MERMA", "Registro de merma"),
                    ("RECHAZOS", "Registro de rechazos por sucursal"),
                ],
                max_length=32,
            ),
        ),
    ]
