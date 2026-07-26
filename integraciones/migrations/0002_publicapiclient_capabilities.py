from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("integraciones", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="publicapiclient",
            name="capabilities",
            field=models.JSONField(blank=True, default=list),
        ),
    ]
