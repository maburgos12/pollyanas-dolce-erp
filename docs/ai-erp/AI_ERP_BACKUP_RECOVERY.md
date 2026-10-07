# ERP AI F1 — SQL and active media recovery

The daily VPS backup contains PostgreSQL, the two private evidence archives, and
`backup_<timestamp>.media.json`. The media manifest records every active file's
relative path, byte length, and SHA-256. It is **not** a copy of those files.
`storage/media` must be received by the NAS through a separate private HBS pull.
Neither a local backup manifest nor a successful HBS status proves recovery.

## Private transport

Keep the existing `erp` SQL and `erp_media_archive` historical modules unchanged.
The VPS exposes `storage/media` at `hbs-export/active-media` through an enabled,
read-only systemd bind mount. The `erp` daemon module stays bound to WireGuard
`10.77.216.1:873`, chrooted, read-only, and limited to NAS `10.77.216.2`. The
existing HBS storage connection reads this second folder without a new password
or a full media copy on the VPS; the SQL job still selects only `daily`. Verify
the bind mount is read-only after a VPS reboot. No ERP process receives NAS
backup credentials or access to the SQL backup share.

HBS Active Sync job `ERP medios activos VPS a NAS - diario` pulls `active-media`
into `ERP_BACKUPS/ERP_ACTIVE_MEDIA_STAGING` after the existing SQL pull. Its Copy
policy keeps received files when the source deletes them. Confirm failure
notifications have a configured delivery method before relying on alerts.
Active Sync alone is not a versioned backup: an updated source file replaces
its prior NAS copy. Local HBS job `ERP medios activos - 7 versiones` backs up the
received staging folder to `ERP_BACKUPS/ERP_ACTIVE_MEDIA_VERSIONS` after Active
Sync finishes, with seven retained generations, no destination deletion, and
weekly quick/content integrity checks. Confirm a matching SQL/media restore
before declaring the configuration complete. The old photo archive job and its
restricted ERP reader are separate.

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
