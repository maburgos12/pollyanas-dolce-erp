from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("logistica", "0034_eventoruta_clave_auditoria")]

    operations = [
        migrations.AlterField(
            model_name="paradaruta",
            name="revision_entrega_estado",
            field=models.CharField(
                choices=[
                    ("NO_REQUERIDA", "No requerida"),
                    ("PENDIENTE", "Pendiente"),
                    ("AUTORIZADA", "Autorizada"),
                    ("RECHAZADA", "Rechazada"),
                    ("CORREGIDA", "Corregida"),
                ],
                default="NO_REQUERIDA",
                max_length=20,
            ),
        ),
    ]
