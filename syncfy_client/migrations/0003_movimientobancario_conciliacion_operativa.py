from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("syncfy_client", "0002_cuentabancaria_origen_alter_cuentabancaria_banco"),
    ]

    operations = [
        migrations.AddField(
            model_name="movimientobancario",
            name="tipo_conciliacion",
            field=models.CharField(
                blank=True,
                choices=[
                    ("cfdi", "CFDI"),
                    ("traspaso_cuentas", "Traspaso entre cuentas"),
                    ("linea_credito", "Linea de credito"),
                    ("comision_bancaria", "Comision bancaria"),
                    ("fiscal", "Fiscal"),
                    ("soporte", "Soporte sin CFDI"),
                ],
                max_length=30,
            ),
        ),
        migrations.AddField(
            model_name="movimientobancario",
            name="movimiento_relacionado",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="movimientos_relacionados",
                to="syncfy_client.movimientobancario",
            ),
        ),
        migrations.AddField(
            model_name="movimientobancario",
            name="nota_conciliacion",
            field=models.TextField(blank=True),
        ),
        migrations.AddField(
            model_name="movimientobancario",
            name="conciliado_por",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="movimientos_conciliados",
                to=settings.AUTH_USER_MODEL,
            ),
        ),
        migrations.AddField(
            model_name="movimientobancario",
            name="conciliado_en",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
