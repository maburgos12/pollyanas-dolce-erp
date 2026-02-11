# Generated manually for Sprint 1
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone

class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="UnidadMedida",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("codigo", models.CharField(max_length=20, unique=True)),
                ("nombre", models.CharField(max_length=60)),
                ("tipo", models.CharField(choices=[("MASS", "Masa"), ("VOLUME", "Volumen"), ("UNIT", "Pieza")], default="UNIT", max_length=10)),
                ("factor_to_base", models.DecimalField(decimal_places=6, default=1, max_digits=18)),
            ],
        ),
        migrations.CreateModel(
            name="Proveedor",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nombre", models.CharField(max_length=200, unique=True)),
                ("lead_time_dias", models.PositiveIntegerField(default=0)),
                ("activo", models.BooleanField(default=True)),
            ],
        ),
        migrations.CreateModel(
            name="Insumo",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("codigo", models.CharField(blank=True, default="", max_length=60)),
                ("nombre", models.CharField(max_length=250)),
                ("nombre_normalizado", models.CharField(db_index=True, max_length=260)),
                ("activo", models.BooleanField(default=True)),
                ("creado_en", models.DateTimeField(default=django.utils.timezone.now)),
                ("proveedor_principal", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to="maestros.proveedor")),
                ("unidad_base", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to="maestros.unidadmedida")),
            ],
        ),
        migrations.CreateModel(
            name="CostoInsumo",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("fecha", models.DateField(default=django.utils.timezone.now)),
                ("moneda", models.CharField(default="MXN", max_length=10)),
                ("costo_unitario", models.DecimalField(decimal_places=6, max_digits=18)),
                ("source_hash", models.CharField(max_length=64, unique=True)),
                ("raw", models.JSONField(blank=True, default=dict)),
                ("insumo", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="maestros.insumo")),
                ("proveedor", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to="maestros.proveedor")),
            ],
            options={"ordering": ["-fecha", "insumo__nombre"]},
        ),
        migrations.AddIndex(
            model_name="insumo",
            index=models.Index(fields=["nombre_normalizado"], name="maestros_in_nombre_n_3f6c1d_idx"),
        ),
    ]
