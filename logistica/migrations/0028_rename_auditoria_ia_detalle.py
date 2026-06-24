from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("logistica", "0027_cargacombustibleunidad_auditoria_analizada_en_and_more"),
    ]

    operations = [
        migrations.RenameField(
            model_name="cargacombustibleunidad",
            old_name="auditoria_ia",
            new_name="auditoria_detalle",
        ),
    ]
