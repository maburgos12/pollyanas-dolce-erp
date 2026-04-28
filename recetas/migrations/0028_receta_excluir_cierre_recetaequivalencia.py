from decimal import Decimal

import django.db.models.deletion
from django.db import migrations, models
from django.utils import timezone


class Migration(migrations.Migration):

    dependencies = [
        ("recetas", "0027_receta_modo_costeo"),
    ]

    operations = [
        migrations.AddField(
            model_name="receta",
            name="excluir_cierre",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.CreateModel(
            name="RecetaEquivalencia",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "factor_conversion",
                    models.DecimalField(decimal_places=6, default=Decimal("1"), max_digits=18),
                ),
                ("activo", models.BooleanField(db_index=True, default=True)),
                ("fuente", models.CharField(blank=True, default="", max_length=80)),
                ("metadata", models.JSONField(blank=True, default=dict)),
                ("creado_en", models.DateTimeField(default=timezone.now)),
                ("actualizado_en", models.DateTimeField(auto_now=True)),
                (
                    "receta_padre",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="equivalencias_hijas_cierre",
                        to="recetas.receta",
                    ),
                ),
                (
                    "receta_porcion",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="equivalencia_cierre",
                        to="recetas.receta",
                    ),
                ),
            ],
            options={
                "verbose_name": "Equivalencia de receta para cierre",
                "verbose_name_plural": "Equivalencias de recetas para cierre",
                "ordering": ["receta_porcion__nombre"],
            },
        ),
    ]
