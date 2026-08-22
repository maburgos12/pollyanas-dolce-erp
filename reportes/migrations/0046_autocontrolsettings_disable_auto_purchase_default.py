from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("reportes", "0045_fuente_unica_presupuesto"),
    ]

    operations = [
        migrations.AlterField(
            model_name="autocontrolsettings",
            name="enable_auto_purchase",
            field=models.BooleanField(default=False),
        ),
    ]
