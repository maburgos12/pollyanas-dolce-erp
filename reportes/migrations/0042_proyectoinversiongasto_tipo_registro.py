from django.db import migrations, models
from django.db.models import Q, Sum


def classify_planned_lines(apps, schema_editor):
    expense_model = apps.get_model("reportes", "ProyectoInversionGasto")
    project_model = apps.get_model("reportes", "ProyectoInversion")
    planned_lines = expense_model.objects.filter(
        Q(referencia_contable__startswith="PLAN_")
        | Q(referencia_contable__startswith="BAMOA_PLAN_")
    )
    affected_project_ids = list(
        planned_lines.values_list("proyecto_id", flat=True).distinct()
    )
    planned_lines.update(tipo_registro="PLANEADO")
    for project_id in affected_project_ids:
        actual_total = (
            expense_model.objects.filter(
                proyecto_id=project_id,
                tipo_registro="REAL",
            ).aggregate(total=Sum("monto_total"))["total"]
            or 0
        )
        project_model.objects.filter(pk=project_id).update(
            monto_inversion_real=actual_total
        )


class Migration(migrations.Migration):
    dependencies = [("reportes", "0041_alter_reglafuenterubro_tipo_fuente")]

    operations = [
        migrations.AddField(
            model_name="proyectoinversiongasto",
            name="tipo_registro",
            field=models.CharField(
                choices=[
                    ("PLANEADO", "Presupuesto planeado"),
                    ("REAL", "Gasto real"),
                ],
                db_index=True,
                default="REAL",
                max_length=12,
            ),
        ),
        migrations.RunPython(classify_planned_lines, migrations.RunPython.noop),
        migrations.AddIndex(
            model_name="proyectoinversiongasto",
            index=models.Index(
                fields=["proyecto", "tipo_registro"],
                name="rpinvexp_project_type_idx",
            ),
        ),
    ]
