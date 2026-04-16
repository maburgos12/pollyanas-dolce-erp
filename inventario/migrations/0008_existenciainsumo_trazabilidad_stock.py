from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("inventario", "0007_ajusteinventario_aplicado_en_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="existenciainsumo",
            name="trazabilidad_stock",
            field=models.JSONField(blank=True, default=dict),
        ),
    ]
