from decimal import Decimal

from django.db import migrations, models
import django.db.models.deletion


AREAS = ["HORNOS", "PRODUCCION", "ARMADO", "LOGISTICA", "CRUCERO"]


def defaults_for_area(area, periodo):
    defaults = {
        "pct_produccion": periodo.pct_produccion,
        "pct_asistencia": periodo.pct_asistencia,
        "pct_puntualidad": periodo.pct_puntualidad,
        "pct_uniforme": periodo.pct_uniforme,
        "limite_uniforme": periodo.limite_uniforme,
        "limite_asistencia": periodo.limite_asistencia,
        "limite_puntualidad": periodo.limite_puntualidad,
        "limite_produccion": periodo.limite_produccion,
        "usa_produccion": True,
    }
    if area == "LOGISTICA":
        defaults.update(
            {
                "pct_produccion": Decimal("0.00"),
                "pct_asistencia": Decimal("50.00"),
                "pct_puntualidad": Decimal("30.00"),
                "pct_uniforme": Decimal("20.00"),
                "limite_produccion": 0,
                "usa_produccion": False,
            }
        )
    return defaults


def crear_reglas_area(apps, schema_editor):
    ConfigBonoPeriodo = apps.get_model("bonos_produccion", "ConfigBonoPeriodo")
    ConfigBonoArea = apps.get_model("bonos_produccion", "ConfigBonoArea")
    for periodo in ConfigBonoPeriodo.objects.all():
        for area in AREAS:
            ConfigBonoArea.objects.get_or_create(
                periodo=periodo,
                area=area,
                defaults=defaults_for_area(area, periodo),
            )


class Migration(migrations.Migration):
    dependencies = [
        ("bonos_produccion", "0002_areas_produccion_logistica"),
    ]

    operations = [
        migrations.CreateModel(
            name="ConfigBonoArea",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("area", models.CharField(choices=[("HORNOS", "Hornos"), ("PRODUCCION", "Producción"), ("ARMADO", "Armado"), ("LOGISTICA", "Logística"), ("CRUCERO", "Crucero")], max_length=20)),
                ("pct_produccion", models.DecimalField(decimal_places=2, default=Decimal("65.00"), max_digits=5)),
                ("pct_asistencia", models.DecimalField(decimal_places=2, default=Decimal("15.00"), max_digits=5)),
                ("pct_puntualidad", models.DecimalField(decimal_places=2, default=Decimal("15.00"), max_digits=5)),
                ("pct_uniforme", models.DecimalField(decimal_places=2, default=Decimal("5.00"), max_digits=5)),
                ("limite_uniforme", models.PositiveSmallIntegerField(default=1)),
                ("limite_asistencia", models.PositiveSmallIntegerField(default=2)),
                ("limite_puntualidad", models.PositiveSmallIntegerField(default=2)),
                ("limite_produccion", models.PositiveSmallIntegerField(default=2)),
                ("usa_produccion", models.BooleanField(default=True)),
                ("actualizado_en", models.DateTimeField(auto_now=True)),
                ("periodo", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="reglas_area", to="bonos_produccion.configbonoperiodo")),
            ],
            options={
                "verbose_name": "Regla de bono por área",
                "verbose_name_plural": "Reglas de bonos por área",
                "ordering": ["area"],
                "unique_together": {("periodo", "area")},
            },
        ),
        migrations.RunPython(crear_reglas_area, migrations.RunPython.noop),
    ]
