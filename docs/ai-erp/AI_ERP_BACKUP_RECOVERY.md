# ERP AI F1 — SQL and active media recovery

The daily VPS backup contains PostgreSQL, the two private evidence archives, and
`backup_<timestamp>.media.json`. The media manifest records every active file's
relative path, byte length, and SHA-256. It is **not** a copy of those files.
`storage/media` must be received by the NAS through a separate private HBS pull.
Neither a local backup manifest nor a successful HBS status proves recovery.

## Private transport

Keep the existing `erp` SQL and `erp_media_archive` historical modules unchanged.
Add an rsync daemon module on the VPS with these settings, reusing the existing
NAS-to-VPS WireGuard route and HBS rsync identity. No ERP process receives NAS
backup credentials or access to the SQL backup share.

```ini
[erp_media_active]
    path = /opt/pastelerias-erp/storage/media
    comment = ERP active media backup source
    uid = erp_backup_reader
    gid = erp_backup_reader
    read only = yes
    list = no
    auth users = erp_backup_reader
    secrets file = /etc/rsyncd-erp.secrets
    hosts allow = 10.77.216.2
    hosts deny = *
```

The global daemon configuration remains bound to `10.77.216.1:873`, chrooted,
read-only, and limited to two connections. Before enabling HBS, check that the
reader can traverse and read every active media file, while another network host
cannot connect. Do not copy credentials into the repository or logs.

Create an HBS **backup** job on NASDOLCE from `erp_media_active` to a dedicated
folder under `ERP_BACKUPS`, with version history/retention that preserves the
media generation paired to each SQL manifest. Schedule it after the existing
daily SQL job, with no overlapping rsync sessions. Verify the configured source,
destination, schedule, retry/error notifications and version behavior in HBS.
The old photo archive job and its restricted ERP reader are separate.

## Backup run and failure boundary

`scripts/backup_db.sh` runs `pg_dump`, archives private evidence, hashes the active
media tree, and publishes the four small payloads plus SHA manifest atomically.
The media root must exist and be readable; a missing tree, symlink, changing file
or failed hash aborts before publication/rotation. The historical local retention
setting is unchanged. HBS/NAS outage does not make the VPS SQL backup script
claim media receipt; monitor failed HBS runs and keep existing local restore
points until transfer is repaired. Do not extend local retention or purge old
backups without a separate reviewed capacity/retention decision.

## Recovery proof for one timestamp

1. On the actual NAS, identify the HBS generation received after the SQL backup.
   Compare the NAS `backup_<timestamp>.manifest` bytes and run SHA-256 checks on
   its SQL and evidence payloads. A VPS staging file or HBS green badge is not
   this proof.
2. Restore SQL to an isolated PostgreSQL 16 database. Never restore into live ERP.
   Confirm schema/table counts and selected media references from the restored DB.
3. Restore that HBS media generation to a separate temporary directory. Run
   `python3 scripts/media_manifest.py verify RESTORED_MEDIA backup_<timestamp>.media.json`.
   Every listed byte must match; missing/mutated files fail. Reconcile a bounded
   sample of restored DB file references with this directory. Check private access
   for sensitive files, not just HTTP 200.
4. Preserve manifest, HBS generation ID, counts, hashes, reference checks,
   capacity before/after and exact cleanup evidence. Report synthetic rehearsal,
   bounded restore and full restore as distinct results.

For a NAS outage, SQL/evidence backup can continue locally while media stays on
the VPS. The pair is **not NAS recoverable** until an HBS generation has been
received and verified. If HBS misses multiple days, escalate before existing
local retention rotates the only recent SQL points. One NAS unit is not an
independent second copy of historical files already removed from the VPS.

## Deployment and rollback

Merge only after CI, local PostgreSQL checks, rsync reader checks, HBS schedule
review, and an isolated restore rehearsal. Deploy through
`scripts/deploy_web_safe.sh` without a prior manual `git pull`. Verify a real
daily backup set and HBS receipt on NAS. Roll back code through Git and disable
only the new HBS job/module if it fails; preserve all preexisting backup sets,
historical photo jobs, originals and NAS generations.
