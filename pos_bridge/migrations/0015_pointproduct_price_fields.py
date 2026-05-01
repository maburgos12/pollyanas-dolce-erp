from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("pos_bridge", "0014_optimize_inventory_latest_snapshots"),
    ]

    operations = [
        migrations.AddField(
            model_name="pointproduct",
            name="precio",
            field=models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True),
        ),
        migrations.AddField(
            model_name="pointproduct",
            name="precio_temporada",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="pointproduct",
            name="precio_activo",
            field=models.BooleanField(default=True),
        ),
        migrations.AddField(
            model_name="pointproduct",
            name="precio_actualizado_en",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
