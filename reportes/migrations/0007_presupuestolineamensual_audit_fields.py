from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("reportes", "0006_alter_insumocostohistoricomensual_metodo_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="presupuestolineamensual",
            name="audit_source",
            field=models.CharField(blank=True, default="", max_length=60),
        ),
        migrations.AddField(
            model_name="presupuestolineamensual",
            name="audit_status",
            field=models.CharField(
                choices=[
                    ("PENDING", "Pendiente"),
                    ("OK", "OK"),
                    ("DESVIACION", "Desviación"),
                    ("MALA_FORMULA", "Mala fórmula"),
                    ("SIN_SOPORTE_DETALLE", "Sin soporte detalle"),
                    ("EXCLUIDO_TOTALIZADOR", "Excluido totalizador"),
                    ("EXCLUIDO_EXTRAORDINARIO", "Excluido extraordinario"),
                    ("EXCLUIDO_DUPLICADO", "Excluido duplicado"),
                ],
                db_index=True,
                default="PENDING",
                max_length=30,
            ),
        ),
    ]
