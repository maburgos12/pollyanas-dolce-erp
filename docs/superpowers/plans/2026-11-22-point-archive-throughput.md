# Point archive throughput implementation plan

> Execute inline using executing-plans; retain independent code review before integration.

**Goal:** Finish eligible Point history with fresh integrity checks, verified NAS recovery, exact approvals and real disk measurements.

**Architecture:** Reuse one bounded management-command process per Broker operation. Run Django/system checks once, call the existing inventory/parse handler afresh per request, close each database connection and fail closed on protocol or process errors. Raise only future manifest limits to 100 files after tests; retain 100 MB and 128 KiB.

**Stack:** Existing Python standard library, Django and PostgreSQL 16; no daemon or dependency.

## Scope and sequence

- [ ] Register isolated worktree and PostgreSQL16 environment; migrate/check baseline before application edits.
- [ ] Add failing subprocess tests in scripts/tests/test_point_archive.py: two requests use one child; wrong ID, oversize, EOF, timeout and stderr flood fail closed without retry; close reaps own process.
- [ ] Add failing PostgreSQL tests in pos_bridge/tests/test_archive_inventory.py: two stream requests use existing fresh handler, changed reference blocks next request, invalid request aborts and per-request connections close.
- [ ] Add bounded session transport in erp_point_archive.py and --stream mode in point_archive_inventory.py. Wire deterministic close through Broker main, cleanup revalidate_proposal and HostAdapter/once. Preserve the synchronous CLI API.
- [ ] Test 100/101 entries, 100 MB and 128 KiB; update producer and validator coherently, retaining exact manifest digest/nonce approvals. Existing proposals remain unchanged.
- [ ] Run affected stdlib suites and Django PostgreSQL suites, check/migrate --check, inspect complete diff, independent review and commit only task files.
- [ ] Draft PR, one CI watcher, exact HEAD test SUCCESS, normal merge. Coordinate one official deployment with timers/oneshots excluded and fresh runtime pins; update installed validator by official refresh without enabling publisher.
- [ ] Measure native real ten-file path and close/reap before relying on larger volumes. Continue approvals and eligible scanning, preserve all excluded incidents, prove NAS recovery before every retirement, record native free bytes before/after.
- [ ] Close own test resources with verified backup and recorded lifecycle; restore temporary generic-prepare pause when safe. Close global objective only with complete scope/retention accounting.

## Test commands

`python -m unittest scripts.tests.test_point_archive scripts.tests.test_cleanup_execute scripts.tests.test_telegram_cleanup`

`python manage.py test pos_bridge.tests.test_archive_inventory pos_bridge.tests.test_archive_readers --settings=config.settings_test --keepdb`

Safety acceptance: no cached eligibility, fresh transaction under lease before unlink, NAS full bytes/hash/parser retained, one request in flight, bounded output while reading, inner process deadline and no ambiguous resend. Native subprocess tests must prove reuse and actual reaping, not only mock calls.

## Approved design

User “ya” follows the concrete proposal to reuse the bounded session and allow at most100 files. Each actual removal still requires approval of its exact frozen Telegram manifest. Full design is retained in private task artifact throughput-design-proposed.md; no model/schema/eligibility changes.

## Ampliación autorizada: sucesora supervisada

Mauricio autorizó expresamente una nueva aprobación exacta para el mismo lote bloqueado, conservando sus tres intentos. Se integra en este PR antes del despliegue: vínculo de hashes al padre inmutable, manifiesto idéntico, ninguna solicitud automática y resolución ancestral sólo después de completed con cierre confirmado. Revisión del delta y CI del SHA conjunto sustituyen los anteriores.
