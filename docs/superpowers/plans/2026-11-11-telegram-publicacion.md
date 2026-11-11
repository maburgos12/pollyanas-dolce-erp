# Telegram publication implementation plan

Goal: authenticated approve/reject in Mauricio's private Telegram chat, GitHub audit, and exactly approved release deployment; no cleanup authorization.
Architecture: reuse the single Point decision receiver, add a separate signed callback namespace and root-owned durable ledger. A separate trusted publisher validates current PR/base, CI and one-time human decision; it merges only the expected head and invokes the official deploy pinned to the verified merge revision. No production capability is added to the model or autofix generator.

Authorization: Mauricio in this thread, 11 November 2026, explicitly approved replacing the prior GitHub-comment gate with authenticated Telegram decisions and a constrained publisher; cleaning remains separate.

- [x] Read existing lifecycle records, Factory and existing native approval/Telegram receiver; no duplicate consumer.
- [x] Register isolated worktree/task and synthetic PostgreSQL16 resources; migrate baseline, check and migrate --check.
- [x] Extend official deploy with expected revision/base and common process lock; real disposable Git tests ensure later main commits are not deployed.
- [ ] Implement request ledger, signed identity-bound callback, expiry/rejection/revocation/replay and fixed-repository GitHub audit.
- [ ] Implement separate publication executor with current CI/head/base checks, bounded one-time claim, expected merge tree/parents, fixed deployment, and outcome notification.
- [ ] Install through Git with pinned existing receiver hash; preserve Point callbacks and monitor schedules. No new network listener.
- [ ] Negative tests plus integration fixtures; independent specification review then code-quality review; PostgreSQL checks, full CI and surgical commit/PR.
- [ ] Bootstrap infrastructure only via authorized operator and concrete reviewed revision. Ask Mauricio to press real approve/reject buttons; never simulate his identity or approval. Verify receipt, GitHub trace and deployed revision at the real consumer.
- [ ] Official cleanup or documented handoff of synthetic resources; no productive cleanup.

Files: scripts/deploy_web_safe.sh, scripts/test_deploy_web_safe.sh, ops/erp-telegram-release controller/installation/units/runbook, scripts/tests/test_telegram_release.py, single CI test command. Scope excludes business data, inventory retries, permissions beyond this explicitly authorized approval bridge, and cleanup routines.

Risks: unapproved code via moving main, concurrent publications, lost responses causing repeated effects, replay/forged callback, stale CI/base, two Telegram pollers, false success after availability-only check. All must fail closed with durable reconciliation state and exact revision evidence.

Activation gates discovered during review: existing main protection strict=false. User approval requested to enable strict up-to-date checks globally. Existing GitHubApp and reviewer token cannot read protection; REST forbidden and GraphQL FORBIDDEN. User approval requested for Administration:read for publisher only. Neither change performed. Controller and installer must fail closed until prerequisites can be verified.
