from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("recetas", "0026_alter_productomonthclosure_opening_source"),
    ]

    operations = [
        migrations.AddField(
            model_name="receta",
            name="modo_costeo",
            field=models.CharField(
                choices=[
                    ("FABRICADO", "Fabricado"),
                    ("REVENTA", "Reventa"),
                    ("SERVICIO_ACCESORIO", "Servicio/Accesorio"),
                ],
                db_index=True,
                default="FABRICADO",
                max_length=20,
            ),
        ),
    ]
