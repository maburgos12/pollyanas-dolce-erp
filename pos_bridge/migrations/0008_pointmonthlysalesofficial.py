from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("pos_bridge", "0007_pointrecipeextractionrun_pointrecipenode_and_more"),
    ]

    operations = [
        migrations.CreateModel(
            name="PointMonthlySalesOfficial",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("month_start", models.DateField(db_index=True, unique=True)),
                ("month_end", models.DateField()),
                ("total_quantity", models.DecimalField(decimal_places=3, default=0, max_digits=18)),
                ("gross_amount", models.DecimalField(decimal_places=2, default=0, max_digits=18)),
                ("discount_amount", models.DecimalField(decimal_places=2, default=0, max_digits=18)),
                ("total_amount", models.DecimalField(decimal_places=2, default=0, max_digits=18)),
                ("tax_amount", models.DecimalField(decimal_places=2, default=0, max_digits=18)),
                ("net_amount", models.DecimalField(decimal_places=2, default=0, max_digits=18)),
                ("report_path", models.CharField(blank=True, default="", max_length=300)),
                ("source_endpoint", models.CharField(blank=True, default="/Report/PrintReportes?idreporte=3", max_length=160)),
                ("raw_payload", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "pos_bridge_monthly_sales_official",
                "ordering": ["-month_start", "id"],
                "verbose_name": "Point monthly official sale",
                "verbose_name_plural": "Point monthly official sales",
            },
        ),
    ]
