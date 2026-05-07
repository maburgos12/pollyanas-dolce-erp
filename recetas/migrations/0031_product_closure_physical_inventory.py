from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("recetas", "0030_registrar_factor_conversion_3leches"),
    ]

    operations = [
        migrations.AddField(
            model_name="productomonthclosureline",
            name="inventario_final_point_cedis",
            field=models.DecimalField(decimal_places=6, default=0, max_digits=18),
        ),
        migrations.AddField(
            model_name="productomonthclosureline",
            name="inventario_final_point_sucursales",
            field=models.DecimalField(decimal_places=6, default=0, max_digits=18),
        ),
        migrations.AddField(
            model_name="productomonthclosureline",
            name="inventario_final_point_total",
            field=models.DecimalField(decimal_places=6, default=0, max_digits=18),
        ),
        migrations.AddField(
            model_name="productomonthclosureline",
            name="diferencia_teorico_vs_point",
            field=models.DecimalField(decimal_places=6, default=0, max_digits=18),
        ),
        migrations.AddField(
            model_name="productomonthclosureline",
            name="estado_auditoria",
            field=models.CharField(
                choices=[
                    ("CUADRA", "Cuadra"),
                    ("CUADRA_CON_MERMA", "Cuadra con merma"),
                    ("SOBRANTE_FISICO", "Sobrante físico"),
                    ("FALTANTE_NO_EXPLICADO", "Faltante no explicado"),
                    ("SIN_INVENTARIO_FISICO", "Sin inventario físico"),
                    ("REVISAR_CATALOGO", "Revisar catálogo"),
                ],
                db_index=True,
                default="SIN_INVENTARIO_FISICO",
                max_length=32,
            ),
        ),
        migrations.AddField(
            model_name="productomonthclosureline",
            name="detalle_auditoria",
            field=models.CharField(blank=True, default="", max_length=255),
        ),
        migrations.AddField(
            model_name="productomonthclosureline",
            name="source_closing_snapshot_count",
            field=models.PositiveIntegerField(default=0),
        ),
    ]
