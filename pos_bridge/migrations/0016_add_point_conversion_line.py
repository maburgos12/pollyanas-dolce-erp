from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0007_crear_sucursales_cedis_devoluciones"),
        ("pos_bridge", "0015_pointproduct_price_fields"),
        ("recetas", "0029_planproduccion_autorizado_and_more"),
    ]

    operations = [
        migrations.CreateModel(
            name="PointConversionLine",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("movement_external_id", models.CharField(db_index=True, max_length=40)),
                ("source_hash", models.CharField(db_index=True, max_length=64, unique=True)),
                ("movement_at", models.DateTimeField(db_index=True)),
                ("item_name", models.CharField(max_length=250)),
                ("item_code", models.CharField(blank=True, default="", max_length=80)),
                ("quantity", models.DecimalField(decimal_places=3, default=0, max_digits=18)),
                ("unit", models.CharField(blank=True, default="", max_length=40)),
                ("unit_cost", models.DecimalField(decimal_places=6, default=0, max_digits=18)),
                ("total_cost", models.DecimalField(decimal_places=2, default=0, max_digits=18)),
                (
                    "source_item_name",
                    models.CharField(
                        blank=True,
                        default="",
                        help_text="Producto origen de la conversión (el pastel entero)",
                        max_length=250,
                    ),
                ),
                ("source_item_code", models.CharField(blank=True, default="", max_length=80)),
                ("source_endpoint", models.CharField(blank=True, default="/Report/crea_Reporte_Largo", max_length=160)),
                ("raw_payload", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "branch",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="conversion_lines",
                        to="pos_bridge.pointbranch",
                    ),
                ),
                (
                    "erp_branch",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="conversion_lines",
                        to="core.sucursal",
                    ),
                ),
                (
                    "receta",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="conversion_lines",
                        to="recetas.receta",
                    ),
                ),
                (
                    "sync_job",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="conversion_lines",
                        to="pos_bridge.pointsyncjob",
                    ),
                ),
            ],
            options={
                "verbose_name": "Point conversion line",
                "verbose_name_plural": "Point conversion lines",
                "db_table": "pos_bridge_conversion_lines",
                "ordering": ["-movement_at", "branch__name"],
                "indexes": [
                    models.Index(fields=["movement_at", "branch"], name="pbconv_date_branch_idx"),
                    models.Index(fields=["item_code"], name="pbconv_item_code_idx"),
                ],
            },
        ),
    ]
