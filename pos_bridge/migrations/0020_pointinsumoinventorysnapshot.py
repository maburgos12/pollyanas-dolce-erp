from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):
    dependencies = [
        ("maestros", "0016_alter_insumo_tipo_item"),
        ("pos_bridge", "0019_alter_pointsyncjob_job_type"),
    ]

    operations = [
        migrations.CreateModel(
            name="PointInsumoInventorySnapshot",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("point_code", models.CharField(db_index=True, max_length=80)),
                ("point_name", models.CharField(blank=True, default="", max_length=250)),
                ("point_quantity", models.DecimalField(decimal_places=6, max_digits=18)),
                ("point_unit", models.CharField(max_length=20)),
                ("quantity_base", models.DecimalField(decimal_places=6, max_digits=18)),
                ("captured_at", models.DateTimeField(db_index=True, default=django.utils.timezone.now)),
                ("raw_payload", models.JSONField(blank=True, default=dict)),
                ("branch", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name="insumo_snapshots", to="pos_bridge.pointbranch")),
                ("insumo", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name="point_inventory_snapshots", to="maestros.insumo")),
                ("sync_job", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name="insumo_snapshots", to="pos_bridge.pointsyncjob")),
            ],
            options={
                "db_table": "pos_bridge_insumo_inventory_snapshots",
                "ordering": ["-captured_at", "-id"],
            },
        ),
        migrations.AddConstraint(
            model_name="pointinsumoinventorysnapshot",
            constraint=models.UniqueConstraint(fields=("sync_job", "branch", "insumo"), name="uniq_point_insumo_snapshot_cycle"),
        ),
        migrations.AddIndex(
            model_name="pointinsumoinventorysnapshot",
            index=models.Index(fields=["branch", "insumo", "-captured_at", "-id"], name="pb_insumo_latest_idx"),
        ),
    ]
