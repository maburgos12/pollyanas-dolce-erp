from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("crm", "0004_pedidocliente_direccion_entrega_and_external_origin"),
        ("logistica", "0042_serviciorealizadounidad_anulacion"),
    ]

    operations = [
        migrations.AddField(
            model_name="solicituddomicilio",
            name="cliente",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="solicitudes_domicilio",
                to="crm.cliente",
            ),
        ),
        migrations.AddField(
            model_name="solicituddomicilio",
            name="direccion_cliente",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="solicitudes_domicilio",
                to="crm.direccioncliente",
            ),
        ),
    ]
