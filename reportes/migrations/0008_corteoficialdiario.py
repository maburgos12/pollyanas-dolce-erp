from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ("reportes", "0007_presupuestolineamensual_audit_fields"),
    ]

    operations = [
        migrations.CreateModel(
            name="CorteOficialDiario",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("corte_date", models.DateField(db_index=True, unique=True)),
                ("total_amount", models.DecimalField(decimal_places=2, default=0, max_digits=18)),
                ("total_tickets", models.PositiveIntegerField(default=0)),
                ("avg_ticket", models.DecimalField(decimal_places=2, default=0, max_digits=18)),
                ("contado_amount", models.DecimalField(decimal_places=2, default=0, max_digits=18)),
                ("credito_amount", models.DecimalField(decimal_places=2, default=0, max_digits=18)),
                ("discounts_amount", models.DecimalField(decimal_places=2, default=0, max_digits=18)),
                ("new_customers", models.PositiveIntegerField(default=0)),
                ("branch_scope", models.CharField(blank=True, default="Todas las sucursales", max_length=120)),
                ("source_label", models.CharField(blank=True, default="Corte oficial diario", max_length=120)),
                ("evidence_path", models.CharField(blank=True, default="", max_length=500)),
                ("notes", models.TextField(blank=True, default="")),
                ("metadata", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Corte oficial diario",
                "verbose_name_plural": "Cortes oficiales diarios",
                "ordering": ["-corte_date"],
            },
        ),
    ]
