from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("recetas", "0025_productomonthclosure_and_more"),
    ]

    operations = [
        migrations.AlterField(
            model_name="productomonthclosure",
            name="opening_source",
            field=models.CharField(
                blank=True,
                choices=[
                    ("PREVIOUS_CLOSURE", "Cierre previo"),
                    ("POINT_SNAPSHOT", "Snapshot Point"),
                    ("BOOTSTRAP_SEED", "Bootstrap historico"),
                ],
                default="",
                max_length=32,
            ),
        ),
    ]
