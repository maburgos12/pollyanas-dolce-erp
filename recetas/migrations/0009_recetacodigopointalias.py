from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ("recetas", "0008_receta_codigo_point"),
    ]

    operations = [
        migrations.CreateModel(
            name="RecetaCodigoPointAlias",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("codigo_point", models.CharField(max_length=80)),
                ("codigo_point_normalizado", models.CharField(db_index=True, max_length=90, unique=True)),
                ("nombre_point", models.CharField(blank=True, default="", max_length=250)),
                ("activo", models.BooleanField(default=True)),
                ("creado_en", models.DateTimeField(default=django.utils.timezone.now)),
                ("actualizado_en", models.DateTimeField(auto_now=True)),
                (
                    "receta",
                    models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="codigos_point_aliases", to="recetas.receta"),
                ),
            ],
            options={
                "verbose_name": "Alias código Point de receta",
                "verbose_name_plural": "Aliases código Point de recetas",
                "ordering": ["codigo_point"],
            },
        ),
    ]
