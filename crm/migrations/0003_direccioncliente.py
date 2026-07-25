from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("crm", "0002_pedidocliente_sucursal_ref_pickupreservation_and_more"),
    ]

    operations = [
        migrations.CreateModel(
            name="DireccionCliente",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("alias", models.CharField(blank=True, default="", max_length=80)),
                ("direccion", models.CharField(max_length=300)),
                ("direccion_normalizada", models.CharField(editable=False, max_length=300)),
                ("referencias", models.CharField(blank=True, default="", max_length=300)),
                ("latitud", models.DecimalField(blank=True, decimal_places=6, max_digits=9, null=True)),
                ("longitud", models.DecimalField(blank=True, decimal_places=6, max_digits=9, null=True)),
                ("place_id", models.CharField(blank=True, default="", max_length=255)),
                ("es_predeterminada", models.BooleanField(default=False)),
                ("activa", models.BooleanField(default=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "cliente",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="direcciones",
                        to="crm.cliente",
                    ),
                ),
            ],
            options={
                "ordering": ["-es_predeterminada", "-updated_at", "-id"],
            },
        ),
        migrations.AddConstraint(
            model_name="direccioncliente",
            constraint=models.UniqueConstraint(
                fields=("cliente", "direccion_normalizada"),
                name="crm_direccion_cliente_normalizada_unica",
            ),
        ),
        migrations.AddConstraint(
            model_name="direccioncliente",
            constraint=models.CheckConstraint(
                check=(
                    models.Q(("latitud__isnull", True), ("longitud__isnull", True))
                    | models.Q(("latitud__isnull", False), ("longitud__isnull", False))
                ),
                name="crm_direccion_gps_par_completo",
            ),
        ),
    ]
