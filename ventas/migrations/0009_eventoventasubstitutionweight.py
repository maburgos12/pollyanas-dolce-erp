from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0001_initial"),
        ("recetas", "0001_initial"),
        ("ventas", "0008_alter_eventoventaprojectionartifact_export_type_and_more"),
    ]

    operations = [
        migrations.CreateModel(
            name="EventoVentaSubstitutionWeight",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("group_key", models.CharField(db_index=True, max_length=180)),
                (
                    "source_level",
                    models.CharField(
                        choices=[("branch", "Sucursal"), ("global", "Cadena/global"), ("blended", "Mezcla")],
                        default="global",
                        max_length=10,
                    ),
                ),
                ("weight", models.DecimalField(decimal_places=5, default=0, max_digits=8)),
                ("sample_size", models.PositiveIntegerField(default=0)),
                (
                    "confidence",
                    models.CharField(
                        choices=[("low", "Baja"), ("medium", "Media"), ("high", "Alta")],
                        default="low",
                        max_length=10,
                    ),
                ),
                ("window_start", models.DateField()),
                ("window_end", models.DateField()),
                ("version", models.CharField(default="v7.2-learned", max_length=32)),
                ("metadata_json", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(default=django.utils.timezone.now)),
                (
                    "branch",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="sales_event_substitution_weights",
                        to="core.sucursal",
                    ),
                ),
                (
                    "loser_product",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="sales_event_substitution_loser_weights",
                        to="recetas.receta",
                    ),
                ),
                (
                    "winner_product",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="sales_event_substitution_winner_weights",
                        to="recetas.receta",
                    ),
                ),
            ],
            options={
                "db_table": "sales_event_substitution_weights",
                "ordering": ["group_key", "winner_product__nombre", "loser_product__nombre", "branch__codigo"],
                "unique_together": {("group_key", "winner_product", "loser_product", "branch", "version")},
            },
        ),
        migrations.AddIndex(
            model_name="eventoventasubstitutionweight",
            index=models.Index(fields=["group_key", "version"], name="se_subwt_group_ver_idx"),
        ),
        migrations.AddIndex(
            model_name="eventoventasubstitutionweight",
            index=models.Index(fields=["branch", "version"], name="se_subwt_branch_ver_idx"),
        ),
        migrations.AddIndex(
            model_name="eventoventasubstitutionweight",
            index=models.Index(fields=["winner_product", "loser_product"], name="se_subwt_pair_idx"),
        ),
    ]
