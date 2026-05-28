from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("pos_bridge", "0016_add_point_conversion_line"),
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
                    ("waste", "Waste"),
                    ("production", "Production"),
                    ("transfers", "Transfers"),
                    ("attendance", "Attendance"),
                ],
                default="inventory",
                max_length=32,
            ),
        ),
    ]
