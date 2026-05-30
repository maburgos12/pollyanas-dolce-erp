from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("bonos_produccion", "0004_configbonoarea_cancelacion"),
    ]

    operations = [
        migrations.AddField(
            model_name="configbonoperiodo",
            name="fecha_inicio",
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="configbonoperiodo",
            name="fecha_fin",
            field=models.DateField(blank=True, null=True),
        ),
    ]
