# Generated manually for Sprint 1
from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone

class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ("maestros", "0001_initial"),
        ("core", "0001_initial"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="Receta",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("nombre", models.CharField(max_length=250)),
                ("nombre_normalizado", models.CharField(db_index=True, max_length=260)),
                ("sheet_name", models.CharField(blank=True, default="", max_length=120)),
                ("hash_contenido", models.CharField(max_length=64, unique=True)),
                ("creado_en", models.DateTimeField(default=django.utils.timezone.now)),
            ],
            options={"ordering": ["nombre"]},
        ),
        migrations.CreateModel(
            name="LineaReceta",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("posicion", models.PositiveIntegerField(default=0)),
                ("insumo_texto", models.CharField(max_length=250)),
                ("cantidad", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("unidad_texto", models.CharField(blank=True, default="", max_length=40)),
                ("costo_linea_excel", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("costo_unitario_snapshot", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("match_score", models.FloatField(default=0)),
                ("match_method", models.CharField(default="NO_MATCH", max_length=20)),
                ("match_status", models.CharField(choices=[("AUTO_APPROVED", "Auto"), ("NEEDS_REVIEW", "Needs review"), ("REJECTED", "Rejected")], default="REJECTED", max_length=20)),
                ("aprobado_en", models.DateTimeField(blank=True, null=True)),
                ("aprobado_por", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to=settings.AUTH_USER_MODEL)),
                ("insumo", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to="maestros.insumo")),
                ("receta", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="lineas", to="recetas.receta")),
                ("unidad", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to="maestros.unidadmedida")),
            ],
            options={"ordering": ["receta", "posicion"]},
        ),
    ]
