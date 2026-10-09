import uuid
from django.db import migrations, models


def emitir_existentes(apps, schema_editor):
    Empleado = apps.get_model("rrhh", "Empleado")
    empleados = Empleado.objects.using(schema_editor.connection.alias)
    for pk in empleados.filter(activo=True, gafete_token__isnull=True).values_list("pk", flat=True).iterator():
        empleados.filter(pk=pk, gafete_token__isnull=True).update(gafete_token=uuid.uuid4())


class Migration(migrations.Migration):
    dependencies = [("rrhh", "0053_horaextra_seguimiento_prenomina")]
    operations = [
        migrations.AddField(model_name="empleado", name="gafete_token", field=models.UUIDField(editable=False, null=True, unique=True)),
        migrations.RunPython(emitir_existentes, migrations.RunPython.noop),
        migrations.AlterField(model_name="empleado", name="gafete_token", field=models.UUIDField(default=uuid.uuid4, editable=False, null=True, unique=True)),
    ]
