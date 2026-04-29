from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("pos_bridge", "0012_pointtransferline_is_open"),
    ]

    operations = [
        migrations.CreateModel(
            name="PointProductCategory",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("codigo_point", models.CharField(max_length=50, unique=True)),
                ("nombre", models.CharField(max_length=200)),
                (
                    "category",
                    models.CharField(
                        choices=[
                            ("REVENTA", "Reventa"),
                            ("SERVICIO_ACCESORIO", "Servicio / Accesorio"),
                            ("TOPPING", "Topping"),
                        ],
                        max_length=30,
                    ),
                ),
                ("notas", models.TextField(blank=True, default="")),
                ("creado_en", models.DateTimeField(auto_now_add=True)),
                ("actualizado_en", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Categoría producto Point",
                "verbose_name_plural": "Categorías productos Point",
            },
        ),
    ]
