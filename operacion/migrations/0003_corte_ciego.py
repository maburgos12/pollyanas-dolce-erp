import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('operacion', '0002_expand_bitacoras_trazables'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.AddField(
            model_name='bitacoraoperativa',
            name='conteo_guardado_en',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='bitacoraoperativa',
            name='conteo_guardado_por',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='cortes_ciegos_guardados', to=settings.AUTH_USER_MODEL),
        ),
    ]
