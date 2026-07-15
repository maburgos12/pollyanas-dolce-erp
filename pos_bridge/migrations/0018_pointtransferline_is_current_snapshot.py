from django.db import migrations, models


def reconcile_current_transfer_snapshots(apps, schema_editor):
    transfer_line = apps.get_model("pos_bridge", "PointTransferLine")
    sync_job = apps.get_model("pos_bridge", "PointSyncJob")
    quote = schema_editor.connection.ops.quote_name
    lines_table = quote(transfer_line._meta.db_table)
    jobs_table = quote(sync_job._meta.db_table)
    schema_editor.execute(
        f"""
        WITH latest_snapshot AS (
            SELECT
                line.transfer_external_id,
                COALESCE(
                    MAX(CASE WHEN job.status IN ('SUCCESS', 'PARTIAL') THEN line.sync_job_id END),
                    MAX(line.sync_job_id)
                ) AS latest_job_id
            FROM {lines_table} AS line
            LEFT JOIN {jobs_table} AS job ON job.id = line.sync_job_id
            WHERE line.transfer_external_id <> ''
            GROUP BY line.transfer_external_id
        )
        UPDATE {lines_table} AS line
        SET is_current_snapshot = (line.sync_job_id = latest.latest_job_id)
        FROM latest_snapshot AS latest
        WHERE line.transfer_external_id = latest.transfer_external_id
          AND latest.latest_job_id IS NOT NULL
        """
    )


class Migration(migrations.Migration):
    dependencies = [
        ("pos_bridge", "0017_alter_pointsyncjob_job_type"),
    ]

    operations = [
        migrations.AddField(
            model_name="pointtransferline",
            name="is_current_snapshot",
            field=models.BooleanField(db_index=True, default=True),
        ),
        migrations.RunPython(
            reconcile_current_transfer_snapshots,
            migrations.RunPython.noop,
        ),
    ]
