from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("crm", "0004_pedidocliente_direccion_entrega_and_external_origin"),
    ]

    operations = [
        migrations.AddField(
            model_name="pedidocliente",
            name="payload_snapshot",
            field=models.JSONField(blank=True, default=dict, editable=False),
        ),
    ]
