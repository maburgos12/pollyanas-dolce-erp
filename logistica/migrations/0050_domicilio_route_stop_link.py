from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("crm", "0008_point_link_fingerprint_indexes"),
        ("logistica", "0049_domicilio_route_sequence"),
    ]

    operations = [
        migrations.AddField(
            model_name="puntologistico",
            name="direccion_cliente",
            field=models.OneToOneField(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.PROTECT,
                related_name="punto_logistico",
                to="crm.direccioncliente",
            ),
        ),
        migrations.AlterField(
            model_name="puntologistico",
            name="tipo",
            field=models.CharField(
                choices=[
                    ("CEDIS", "CEDIS"),
                    ("SUCURSAL", "Sucursal"),
                    ("PROVEEDOR", "Proveedor"),
                    ("TALLER", "Taller"),
                    ("BANCO", "Banco"),
                    ("AUTORIZADO", "Punto autorizado"),
                    ("DOMICILIO", "Domicilio de cliente"),
                ],
                default="SUCURSAL",
                max_length=20,
            ),
        ),
        migrations.AddField(
            model_name="solicituddomicilio",
            name="parada_ruta",
            field=models.OneToOneField(
                blank=True,
                editable=False,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="solicitud_domicilio",
                to="logistica.paradaruta",
            ),
        ),
    ]
