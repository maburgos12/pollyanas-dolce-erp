from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        ("reportes", "0030_stockmensualsucursal"),
    ]

    operations = [
        migrations.CreateModel(
            name="DgOperacionSnapshot",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("fecha_operacion", models.DateField(db_index=True, unique=True)),
                ("payload", models.JSONField(blank=True, default=dict)),
                (
                    "status",
                    models.CharField(
                        choices=[("READY", "Listo"), ("ERROR", "Error"), ("STALE", "Desactualizado")],
                        db_index=True,
                        default="READY",
                        max_length=20,
                    ),
                ),
                ("source_cutoff_at", models.DateTimeField(blank=True, null=True)),
                ("generated_at", models.DateTimeField(db_index=True, default=django.utils.timezone.now)),
                ("metadata", models.JSONField(blank=True, default=dict)),
                ("last_error", models.TextField(blank=True, default="")),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Snapshot Operación DG",
                "verbose_name_plural": "Snapshots Operación DG",
                "ordering": ["-fecha_operacion"],
            },
        ),
        migrations.AddIndex(
            model_name="dgoperacionsnapshot",
            index=models.Index(fields=["status", "fecha_operacion"], name="rdg_ops_status_date_idx"),
        ),
    ]
