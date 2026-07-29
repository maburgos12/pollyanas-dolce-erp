from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("logistica", "0049_domicilio_route_sequence"),
    ]

    operations = [
        migrations.AlterField(
            model_name="solicituddomiciliostatusoperation",
            name="repartidor",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="domicilio_status_operations",
                to="logistica.repartidor",
            ),
        ),
    ]
