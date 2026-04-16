from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("recetas", "0024_smanzanareb_temporal"),
    ]

    operations = [
        migrations.CreateModel(
            name="ProductoMonthClosure",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("month_start", models.DateField(db_index=True, unique=True)),
                ("month_end", models.DateField()),
                ("status", models.CharField(choices=[("DRAFT", "Borrador"), ("BUILT", "Construido"), ("LOCKED", "Bloqueado")], db_index=True, default="DRAFT", max_length=20)),
                ("opening_source", models.CharField(blank=True, choices=[("PREVIOUS_CLOSURE", "Cierre previo"), ("POINT_SNAPSHOT", "Snapshot Point")], default="", max_length=32)),
                ("opening_reference_date", models.DateField(blank=True, null=True)),
                ("upstream_sync_cutoff_at", models.DateTimeField(blank=True, null=True)),
                ("built_at", models.DateTimeField(blank=True, null=True)),
                ("notes", models.TextField(blank=True, default="")),
                ("metadata", models.JSONField(blank=True, default=dict)),
                ("is_locked", models.BooleanField(default=False)),
                ("created_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("built_by", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="product_month_closures_built", to=settings.AUTH_USER_MODEL)),
            ],
            options={
                "verbose_name": "Cierre mensual de producto",
                "verbose_name_plural": "Cierres mensuales de producto",
                "ordering": ["-month_start", "-id"],
            },
        ),
        migrations.CreateModel(
            name="ProductoMonthClosureLine",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("inventario_inicial_teorico", models.DecimalField(decimal_places=6, default=0, max_digits=18)),
                ("produccion_mes", models.DecimalField(decimal_places=6, default=0, max_digits=18)),
                ("venta_directa_enteros", models.DecimalField(decimal_places=6, default=0, max_digits=18)),
                ("venta_derivada_equivalente", models.DecimalField(decimal_places=6, default=0, max_digits=18)),
                ("venta_total_equivalente", models.DecimalField(decimal_places=6, default=0, max_digits=18)),
                ("merma_directa_enteros", models.DecimalField(decimal_places=6, default=0, max_digits=18)),
                ("merma_derivada_equivalente", models.DecimalField(decimal_places=6, default=0, max_digits=18)),
                ("merma_total_equivalente", models.DecimalField(decimal_places=6, default=0, max_digits=18)),
                ("inventario_final_teorico", models.DecimalField(decimal_places=6, default=0, max_digits=18)),
                ("source_snapshot_count", models.PositiveIntegerField(default=0)),
                ("source_sale_rows", models.PositiveIntegerField(default=0)),
                ("source_production_rows", models.PositiveIntegerField(default=0)),
                ("source_waste_rows", models.PositiveIntegerField(default=0)),
                ("has_catalog_issue", models.BooleanField(default=False)),
                ("catalog_issue_note", models.CharField(blank=True, default="", max_length=255)),
                ("metadata", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(default=django.utils.timezone.now)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("closure", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="lines", to="recetas.productomonthclosure")),
                ("receta_padre", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name="product_month_closure_lines", to="recetas.receta")),
            ],
            options={
                "verbose_name": "Linea cierre mensual de producto",
                "verbose_name_plural": "Lineas cierre mensual de producto",
                "ordering": ["receta_padre__nombre", "id"],
                "unique_together": {("closure", "receta_padre")},
            },
        ),
    ]
