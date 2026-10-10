from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [('compras', '0020_procedencia_adquisicion')]
    operations = [migrations.AlterField(
        model_name='comprarealizadadepartamental', name='comprobante',
        field=models.FileField(blank=True, upload_to='compras/cotizaciones/compras/%Y/%m/'),
    )]
