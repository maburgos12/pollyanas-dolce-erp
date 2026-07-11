from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("logistica", "0036_auditoriaentregacursor_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="reporteunidad",
            name="fecha_cierre",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
