from django.db import migrations, models
import django.utils.timezone


MV_SQL = """
CREATE MATERIALIZED VIEW mv_dashboard_full AS
SELECT
    months_window,
    payload,
    metadata,
    generated_at,
    updated_at
FROM reportes_dashboardfullsnapshot
"""

MV_REVERSE_SQL = "DROP MATERIALIZED VIEW IF EXISTS mv_dashboard_full"

MV_INDEX_SQL = """
CREATE UNIQUE INDEX mv_dashboard_full_months_window_uidx
ON mv_dashboard_full(months_window)
"""

MV_INDEX_REVERSE_SQL = "DROP INDEX IF EXISTS mv_dashboard_full_months_window_uidx"


class Migration(migrations.Migration):

    dependencies = [
        ("reportes", "0016_mv_dashboard_daily_ops"),
    ]

    operations = [
        migrations.CreateModel(
            name="DashboardFullSnapshot",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("months_window", models.PositiveSmallIntegerField(db_index=True, unique=True)),
                ("payload", models.JSONField(blank=True, default=dict)),
                ("metadata", models.JSONField(blank=True, default=dict)),
                ("generated_at", models.DateTimeField(db_index=True, default=django.utils.timezone.now)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "verbose_name": "Snapshot dashboard ejecutivo",
                "verbose_name_plural": "Snapshots dashboard ejecutivo",
                "ordering": ["months_window"],
            },
        ),
        migrations.RunSQL(sql=MV_SQL, reverse_sql=MV_REVERSE_SQL),
        migrations.RunSQL(sql=MV_INDEX_SQL, reverse_sql=MV_INDEX_REVERSE_SQL),
    ]
