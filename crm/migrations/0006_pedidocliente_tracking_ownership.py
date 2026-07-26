import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("crm", "0005_pedidocliente_payload_snapshot"),
        ("integraciones", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="pedidocliente",
            name="public_api_client",
            field=models.ForeignKey(
                blank=True,
                editable=False,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="pedidos_omnichannel",
                to="integraciones.publicapiclient",
            ),
        ),
        migrations.AddField(
            model_name="pedidocliente",
            name="tracking_token",
            field=models.UUIDField(
                blank=True,
                editable=False,
                null=True,
                unique=True,
            ),
        ),
    ]
