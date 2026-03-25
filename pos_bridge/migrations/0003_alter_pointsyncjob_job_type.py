from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("pos_bridge", "0002_alter_pointsyncjob_job_type_pointdailysale"),
    ]

    operations = [
        migrations.AlterField(
            model_name="pointsyncjob",
            name="job_type",
            field=models.CharField(
                choices=[
                    ("inventory", "Inventory"),
                    ("sales", "Sales"),
                    ("recipes", "Recipes"),
                ],
                default="inventory",
                max_length=32,
            ),
        ),
    ]
