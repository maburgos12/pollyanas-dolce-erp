from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("integraciones", "0002_publicapiclient_capabilities"),
        ("logistica", "0043_solicituddomicilio_cliente_direccion"),
    ]

    operations = [
        migrations.AddField(
            model_name="publicapiclient",
            name="repartidores_logistica_autorizados",
            field=models.ManyToManyField(
                blank=True,
                related_name="api_clients_logistica_autorizados",
                to="logistica.repartidor",
            ),
        ),
    ]
