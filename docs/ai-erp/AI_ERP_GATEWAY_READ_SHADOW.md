# ERP AI Gateway: assets READ/SHADOW

This additive cut provides Python entry points for a later server-controlled
caller. It adds no planner, provider, user, endpoint, UI, migration or operational
write. Existing Gateway callers retain their contracts and approval workflow.

`AI_GATEWAY_ASSETS_ENABLED` is read from Django settings with a default of `False` (only the boolean `True` enables it).
No environment/configuration activation is included. While disabled, the three
registered tools are absent from catalogs/OpenAPI and denied on execution.
Activation and a real model/provider pilot require a separate authorized step.

```python
from api.ai_gateway_services import list_read_shadow_tools, invoke_read_shadow_tool

# The authenticated actor and mode come from the server, never model arguments.
catalog = list_read_shadow_tools(user=user, mode="SHADOW")
result = invoke_read_shadow_tool(
    user=user, mode="SHADOW", tool_key="erp.get_asset_context",
    arguments={"activo_id": asset_id},
)
```

Both entry points accept only `READ`/`SHADOW`; the catalog reports the chosen mode.
Execution retains the existing `tool | scope | result` envelope. Identity, current
ACL/profile and authorized branches are re-read at each boundary. Only these
three keys are accepted; unknown/legacy reads, sync, draft and approval actions
never reach a handler or approval path through this entry point.

| Tool | Arguments | Bounds / meaning |
| --- | --- | --- |
| `erp.search_assets` | `q?`, `sucursal_id?`, `limit?` | q max 180 characters; limit 1–50, default 20. Multiple matches return choices and `ambiguous`, never select an asset. |
| `erp.get_asset_context` | `activo_id` | Positive JSON integer; asset fetched inside existing authorized queryset. Up to 10 open failures and 10 recent orders, plus last closure and next plan. |
| `erp.get_pending_maintenance` | `sucursal_id?`, `fecha_hasta?`, `limit?` | ISO YYYY-MM-DD date; horizon defaults to today + 30 days. Limit 1–50 (default 20) per separate group: overdue, upcoming, missing schedule, inactive/paused. |

Schemas derive from the same DRF serializers that enforce execution. Unknown
keys, null/scalar/array arguments, boolean/string/fractional integers, nonpositive
IDs, invalid dates and excessive bounds fail before the handler. Out-of-scope
and nonexistent object IDs both return `no_data`. Existing HTTP null behavior
remains unchanged (400 from the existing JSONField).

Asset and maintenance authority is reused; see the
[source ficha](../data-reuse/ai-erp-gateway-read-shadow.md). Historical failure
reports additionally intersect their own branch with the actor's authorized
branches when an asset moved. Existing passport UI behavior is unchanged.
Financial fields follow `can_view_costs`; invoice file URLs, document contents and
ORM instances are omitted. Dates are ISO strings; Decimal values are exact strings,
including null when missing. Event history explicitly remains partial; truncation
is declared. Open orders are not plans; missing plans do not establish absence of
historical service. Results name their source, unit, timestamp and timezone.

READ and SHADOW have identical read behavior. SQL tests allow only technical
`AuditLog` inserts; no business records/jobs/notifications are created. Audit keeps
actor, mode, scoped identifiers, bounds and result status; free-text search is
recorded only by length and result contents are not logged. Denied/invalid/failed
attempts also produce a bounded audit: unknown keys and invalid modes are normalized,
raw invalid arguments and exception messages are omitted. Audit failure propagates.
Data strings have no policy authority. No external model has consumed this DTO in
this cut; model/provider robustness and a real user-facing pilot remain unverified.
