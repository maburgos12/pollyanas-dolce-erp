from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("reportes", "0050_periodicidad_gasto_recurrente_multimensual"),
    ]

    operations = [
        migrations.AlterField(
            model_name="documentocedulaimss",
            name="clase",
            field=models.CharField(
                max_length=12,
                choices=[
                    ("SUA_XLS", "SUA XLS"),
                    ("EMA_PDF", "EMA PDF"),
                    ("EBA_PDF", "EBA PDF"),
                    ("SIPARE_PDF", "Comprobante de pago SIPARE PDF"),
                ],
            ),
        ),
    ]
