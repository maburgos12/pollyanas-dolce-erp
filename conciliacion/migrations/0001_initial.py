# Generated manually for Sprint 3 conciliacion bancaria.

import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("syncfy_client", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="ImportacionBancaria",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "fuente",
                    models.CharField(
                        choices=[("manual_csv", "Carga manual CSV"), ("manual_excel", "Carga manual Excel")],
                        max_length=20,
                    ),
                ),
                (
                    "estado",
                    models.CharField(
                        choices=[("preview", "Preview"), ("importada", "Importada"), ("error", "Error")],
                        default="preview",
                        max_length=20,
                    ),
                ),
                ("archivo_nombre", models.CharField(max_length=255)),
                ("archivo_hash", models.CharField(db_index=True, max_length=64)),
                ("total_filas", models.IntegerField(default=0)),
                ("movimientos_nuevos", models.IntegerField(default=0)),
                ("movimientos_duplicados", models.IntegerField(default=0)),
                ("filas_con_error", models.IntegerField(default=0)),
                ("preview", models.JSONField(blank=True, default=list)),
                ("errores", models.JSONField(blank=True, default=list)),
                ("creado_en", models.DateTimeField(auto_now_add=True)),
                ("actualizado_en", models.DateTimeField(auto_now=True)),
                (
                    "creado_por",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "cuenta",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="importaciones_bancarias",
                        to="syncfy_client.cuentabancaria",
                    ),
                ),
            ],
            options={
                "verbose_name": "Importacion bancaria",
                "verbose_name_plural": "Importaciones bancarias",
                "ordering": ["-creado_en"],
            },
        ),
        migrations.AddIndex(
            model_name="importacionbancaria",
            index=models.Index(fields=["cuenta", "creado_en"], name="conciliacio_cuenta__efe6fe_idx"),
        ),
        migrations.AddIndex(
            model_name="importacionbancaria",
            index=models.Index(fields=["estado"], name="conciliacio_estado_86e91b_idx"),
        ),
    ]
