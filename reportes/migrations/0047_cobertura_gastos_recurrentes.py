from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("reportes", "0046_autocontrolsettings_disable_auto_purchase_default")]

    operations = [
        migrations.AddField(
            model_name="gastooperativomensual", name="cobertura_mes_inicio",
            field=models.DateField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name="gastooperativomensual", name="cobertura_mes_fin",
            field=models.DateField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name="gastorecurrenteversion", name="periodicidad_meses",
            field=models.PositiveSmallIntegerField(choices=[(1, "Mensual"), (2, "Bimestral")], default=1),
        ),
        migrations.AddConstraint(
            model_name="gastooperativomensual",
            constraint=models.CheckConstraint(
                check=(
                    models.Q(cobertura_mes_inicio__isnull=True, cobertura_mes_fin__isnull=True)
                    | models.Q(
                        cobertura_mes_inicio__isnull=False, cobertura_mes_fin__isnull=False,
                        cobertura_mes_inicio__day=1, cobertura_mes_fin__day=1,
                        cobertura_mes_fin__gte=models.F("cobertura_mes_inicio"),
                    )
                ),
                name="gasto_cobertura_mensual_valida",
            ),
        ),
        migrations.AddConstraint(
            model_name="gastorecurrenteversion",
            constraint=models.CheckConstraint(
                check=models.Q(periodicidad_meses__in=(1, 2)), name="gasto_rec_periodicidad_valida",
            ),
        ),
    ]
