from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("recetas", "0015_receta_categoria_receta_familia_alter_receta_tipo"),
    ]

    operations = [
        migrations.AddField(
            model_name="receta",
            name="temporalidad",
            field=models.CharField(
                choices=[
                    ("PERMANENTE", "Permanente"),
                    ("TEMPORAL", "Temporal"),
                    ("FECHA_ESPECIAL", "Fecha especial"),
                ],
                db_index=True,
                default="PERMANENTE",
                max_length=20,
            ),
        ),
        migrations.AddField(
            model_name="receta",
            name="temporalidad_detalle",
            field=models.CharField(blank=True, default="", max_length=120),
        ),
    ]
