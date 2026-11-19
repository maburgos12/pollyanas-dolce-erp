from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("recetas", "0042_protect_locked_product_closure_lines")]

    operations = [
        migrations.AddField(
            model_name="recetaagrupacionaddon",
            name="metadata",
            field=models.JSONField(blank=True, default=dict),
        ),
    ]
