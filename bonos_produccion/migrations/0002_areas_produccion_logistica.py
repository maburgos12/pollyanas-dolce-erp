from decimal import Decimal

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("bonos_produccion", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="configbonoperiodo",
            name="monto_area_produccion",
            field=models.DecimalField(decimal_places=2, default=Decimal("850.00"), max_digits=8),
        ),
        migrations.AddField(
            model_name="configbonoperiodo",
            name="monto_logistica",
            field=models.DecimalField(decimal_places=2, default=Decimal("850.00"), max_digits=8),
        ),
        migrations.AlterField(
            model_name="bonoproduccionempleado",
            name="area",
            field=models.CharField(
                choices=[
                    ("HORNOS", "Hornos"),
                    ("PRODUCCION", "Producción"),
                    ("ARMADO", "Armado"),
                    ("LOGISTICA", "Logística"),
                    ("CRUCERO", "Crucero"),
                ],
                max_length=20,
            ),
        ),
    ]
