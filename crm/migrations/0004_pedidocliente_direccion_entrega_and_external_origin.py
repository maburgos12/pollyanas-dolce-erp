from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("crm", "0003_direccioncliente"),
    ]

    operations = [
        migrations.AddField(
            model_name="pedidocliente",
            name="direccion_entrega",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="pedidos",
                to="crm.direccioncliente",
            ),
        ),
        migrations.AddField(
            model_name="pedidocliente",
            name="external_id",
            field=models.CharField(blank=True, db_index=True, default="", max_length=120),
        ),
        migrations.AddField(
            model_name="pedidocliente",
            name="external_source",
            field=models.CharField(blank=True, db_index=True, default="", max_length=40),
        ),
        migrations.AddConstraint(
            model_name="pedidocliente",
            constraint=models.UniqueConstraint(
                condition=~models.Q(("external_source", "")) & ~models.Q(("external_id", "")),
                fields=("external_source", "external_id"),
                name="crm_pedido_origen_externo_unico",
            ),
        ),
    ]
