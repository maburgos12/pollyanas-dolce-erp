from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("logistica", "0039_rutacargasucursalevento_discrepancialogistica_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="repartidor",
            name="tutorial_carga_sucursal_visto_en",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
