"""Bounded Google projection of approved ERP intent; no public claim from PATCH."""
from contextlib import ExitStack, contextmanager
from datetime import date, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from django.db import connection
from django.utils import timezone

from core.models import Sucursal
from horarios_especiales.models import (
    HorarioEspecialDetalle, HorarioEspecialIntentoPublicacion, SolicitudHorarioEspecial,
    SucursalPlataformaExterna,
)
from horarios_especiales.services.audit import log_special_hours_event
from horarios_especiales.services.rules import PRECEDENCE, rule_applies, validate_rule_payload
from integraciones.services.google_business_profile.publisher import GoogleBusinessProfilePublisher, google_time, hours_equal

DAYS = ("MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY")
HORIZON_DAYS = 90


@contextmanager
def branch_lock(branch_id):
    # Session lock keeps the pre-mutation backup committed while excluding other workers.
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_lock(%s, %s)", [72419, branch_id])
        locked = cursor.fetchone()[0]
    if not locked:
        raise ValueError("PUBLICATION_BUSY: hay otra publicación para esta sucursal.")
    try:
        yield
    finally:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_unlock(%s, %s)", [72419, branch_id])


def approved_rules(branch_id):
    rows = []
    for obj in SolicitudHorarioEspecial.objects.filter(
        canonical_payload__schema_version=2,
        canonical_payload__locations__contains=[{"branch_id": branch_id}],
        status__in=["APROBADO", "EJECUTADO", "FALLIDO"], approved_at__isnull=False,
        cancelled_at__isnull=True,
    ).order_by("approved_at", "pk"):
        payload = obj.canonical_payload
        if validate_rule_payload(payload) or not obj.audit_entries.filter(
            action="APPROVED", payload_json__revision=payload.get("revision"),
        ).exists():
            raise ValueError("Revisión aprobada inválida; requiere revisión humana.")
        for index, rule in enumerate(payload["rules"]):
            rows.append(((PRECEDENCE[rule["kind"]], obj.approved_at, obj.pk, index), rule, payload["revision"]))
    return rows


def projection_fields(*, current, rows, today, owned):
    end = today + timedelta(days=HORIZON_DAYS - 1)
    regular = {}
    covered = set()
    for weekday in range(7):
        candidates = [row for row in rows if row[1]["kind"] == "weekly" and not row[1].get("end_date")
                      and row[1]["start_date"] <= today.isoformat() and weekday in row[1]["weekdays"]]
        if candidates:
            winner = max(candidates, key=lambda row: row[0])
            regular[weekday] = winner[1]
            covered.add((winner[0][2], winner[2]))
    dated = {}
    for offset in range(HORIZON_DAYS):
        day = today + timedelta(days=offset)
        candidates = [row for row in rows if rule_applies(row[1], day)
                      and not (row[1]["kind"] == "weekly" and not row[1].get("end_date")
                               and row[1]["start_date"] > today.isoformat())]
        if candidates:
            winner = max(candidates, key=lambda row: row[0])
            rule = winner[1]
            if rule["kind"] != "weekly" or rule.get("end_date"):
                dated[day.isoformat()] = rule
                covered.add((winner[0][2], winner[2]))
    fields = {}
    if regular:
        periods = list((current.get("regularHours") or {}).get("periods") or [])
        # Overnight regular intervals can span a targeted day; refuse ambiguous rewrites.
        if any(p.get("openDay") != p.get("closeDay") for p in periods):
            raise ValueError("Horario semanal nocturno requiere revisión manual.")
        periods = [p for p in periods if p.get("openDay") not in {DAYS[d] for d in regular}]
        for weekday, rule in regular.items():
            periods.extend({"openDay": DAYS[weekday], "closeDay": DAYS[weekday],
                            "openTime": google_time(w["open"]), "closeTime": google_time(w["close"])}
                           for w in rule["time_windows"])
        fields["regularHours"] = {"periods": periods}
    existing = list((current.get("specialHours") or {}).get("specialHourPeriods") or [])
    merged = []
    for period in existing:
        start = period.get("startDate") or {}
        try:
            day = date(start["year"], start["month"], start["day"])
        except (KeyError, ValueError, TypeError):
            raise ValueError("Excepción Google sin fecha válida; no se sobrescribe.")
        key = day.isoformat()
        weekly_target = day >= today and day.weekday() in regular
        finite_weekly = key in dated and dated[key]["kind"] == "weekly"
        is_owned = any(hours_equal(
            {"specialHours": {"specialHourPeriods": [period]}},
            {"specialHours": {"specialHourPeriods": [expected]}},
        ) for expected in owned.get(key, []))
        if (weekly_target or finite_weekly) and key not in dated and not is_owned:
            raise ValueError(f"MANUAL_EXCEPTION_CONFLICT: {key}")
        if finite_weekly and not is_owned:
            raise ValueError(f"MANUAL_EXCEPTION_CONFLICT: {key}")
        if key in dated or (weekly_target and is_owned):
            end_date = period.get("endDate") or start
            if end_date != start:
                raise ValueError(f"Excepción nocturna Google requiere revisión: {key}")
            continue
        merged.append(period)
    new_owned = {key: periods for key, periods in owned.items() if key >= today.isoformat()}
    for key, rule in dated.items():
        periods = GoogleBusinessProfilePublisher._build_special_hour_periods(detail=SimpleNamespace(
            target_date=date.fromisoformat(key), closed_all_day=rule["closed_all_day"],
            time_windows_json=rule["time_windows"]))
        merged.extend(periods)
        new_owned[key] = periods
    if dated or merged != existing:
        if not (fields.get("regularHours", current.get("regularHours")) or {}).get("periods"):
            raise ValueError("Google requiere regularHours antes de specialHours.")
        fields["specialHours"] = {"specialHourPeriods": merged}
    ongoing = any((r["kind"] == "weekly" and not r.get("end_date")) or
                  (r.get("end_date") or r["start_date"]) > end.isoformat() for _, r, _ in rows)
    return fields, new_owned, end, ongoing, [
        {"request_id": request_id, "revision": revision} for request_id, revision in sorted(covered)
    ]


def replacement_receipt(rows, request_id, today, covered_revisions):
    own = [rule for key, rule, _ in rows if key[2] == request_id]
    for offset in range(HORIZON_DAYS):
        day = today + timedelta(days=offset)
        if not any(rule_applies(rule, day) for rule in own):
            continue
        winner = max((row for row in rows if rule_applies(row[1], day)), key=lambda row: row[0])
        if winner[0][2] != request_id:
            covered = any(row['request_id'] == request_id for row in covered_revisions)
            return 'PARTIALLY_SUPERSEDED' if covered else 'SUPERSEDED'
    return None


def execute_rules(*, request_obj, actor=None, today=None, publisher=None):
    today = today or timezone.localdate(timezone=ZoneInfo("America/Mazatlan"))
    request_obj.refresh_from_db()
    if request_obj.telegramhoursupdate_set.exists():
        from horarios_especiales.services.telegram_bot import require_approver
        require_approver(request_obj.approved_by)
    if request_obj.status not in {"APROBADO", "FALLIDO", "EJECUTADO"} or request_obj.cancelled_at or not request_obj.approved_at:
        raise ValueError("La solicitud debe estar aprobada antes de ejecutarse.")
    if validate_rule_payload(request_obj.canonical_payload) or not request_obj.audit_entries.filter(
        action="APPROVED", payload_json__revision=request_obj.canonical_payload.get("revision"),
    ).exists():
        raise ValueError("La revisión no coincide con su aprobación.")
    summary = {"details": [], "success": 0, "failed": 0, "skipped": 0,
               "public_confirmed": False, "projection_start": today.isoformat(),
               "projection_end": (today + timedelta(days=HORIZON_DAYS - 1)).isoformat(),
               "next_reconcile_at": (timezone.now() + timedelta(minutes=15)).isoformat(),
               "revision": request_obj.canonical_payload["revision"]}
    expired = all(not (r["kind"] == "weekly" and not r.get("end_date"))
                  and (r.get("end_date") or r["start_date"]) < today.isoformat()
                  for r in request_obj.canonical_payload["rules"])
    if expired:
        summary["details"] = [{"branch_id": row["branch_id"], "branch_code": row.get("branch_code"),
                               "status": "NO_APPLICABLE_DATES", "public_confirmed": False}
                              for row in request_obj.canonical_payload["locations"]]
        summary["skipped"] = len(summary["details"])
    for location in ([] if expired else request_obj.canonical_payload["locations"]):
        branch_id = location["branch_id"]
        attempt = None
        detail = None
        locks = ExitStack()
        try:
            locks.enter_context(branch_lock(branch_id))
            request_obj.refresh_from_db()
            if request_obj.cancelled_at:
                raise ValueError("Solicitud cancelada.")
            branch = Sucursal.objects.get(pk=branch_id)
            config = SucursalPlataformaExterna.objects.get(sucursal_id=branch_id,
                platform=SucursalPlataformaExterna.PLATFORM_GOOGLE, is_active=True)
            rows = approved_rules(branch_id)
            publisher = publisher or GoogleBusinessProfilePublisher()
            current = publisher.client.get_location(location_name=config.external_location_name)
            detail, _ = HorarioEspecialDetalle.objects.get_or_create(request=request_obj,
                sucursal=branch, target_date=today)
            attempt = HorarioEspecialIntentoPublicacion.objects.create(detail=detail,
                platform=config.platform, status="RUNNING",
                attempt_no=(detail.publication_attempts.order_by("-attempt_no").values_list("attempt_no", flat=True).first() or 0) + 1,
                executed_by=actor if getattr(actor, "is_authenticated", False) else None)
            attempt.response_payload_json = {"before": current, "mutation_started": False}
            attempt.save(update_fields=["response_payload_json"])
            owned = dict(config.settings_json.get("hours_agent_owned_specials", {}))
            # Recover ownership after a process dies between PATCH and recording its outcome.
            recovery = HorarioEspecialIntentoPublicacion.objects.filter(
                detail__sucursal_id=branch_id, response_payload_json__mutation_started=True,
            ).exclude(error_payload_json={})
            if config.last_published_at:
                recovery = recovery.filter(started_at__gte=config.last_published_at)
            for previous in recovery.order_by("pk").iterator():
                owned.update(previous.error_payload_json.get("intended_owned", {}))
            fields, owned, end, ongoing, covered_revisions = projection_fields(current=current, rows=rows, today=today, owned=owned)
            # Only winning revisions contribute; revoked historical rules do not block their replacements.
            for contributing in SolicitudHorarioEspecial.objects.filter(
                    pk__in=[row['request_id'] for row in covered_revisions]).select_related('approved_by'):
                if contributing.telegramhoursupdate_set.exists():
                    from horarios_especiales.services.telegram_bot import require_approver
                    require_approver(contributing.approved_by)
            attempt.error_payload_json = {"intended_owned": owned, "covered_revisions": covered_revisions}
            attempt.save(update_fields=["error_payload_json"])
            if fields and HorarioEspecialDetalle.objects.filter(sucursal_id=branch_id,
                target_date__gte=today, target_date__lte=end,
                request__status__in=["APROBADO", "EJECUTADO", "FALLIDO"],
                request__cancelled_at__isnull=True).exclude(
                    request__canonical_payload__contains={"schema_version": 2}).exists():
                raise ValueError("LEGACY_EXCEPTION_CONFLICT: revisar las excepciones ERP anteriores.")
            if not fields:
                result = {"receipt": "PROGRAMMED" if ongoing else "NO_APPLICABLE_DATES", "noop": True,
                          "response_payload": attempt.response_payload_json, "request_payload": {}}
            else:
                result = publisher.publish_fields(location_name=config.external_location_name,
                    current=current, fields=fields, attempt=attempt)
                if approved_rules(branch_id) != rows:
                    result["receipt"] = "PENDING_RECONCILE"
                if result["receipt"] not in {"READBACK_MISMATCH", "PENDING_RECONCILE"}:
                    config.settings_json = {**config.settings_json, "hours_agent_owned_specials": owned}
                    config.last_published_at = timezone.now()
                    config.save(update_fields=["settings_json", "last_published_at", "updated_at"])
            projection_receipt = result["receipt"]
            future_pending = any(
                (r["kind"] == "weekly" and not r.get("end_date") and r["start_date"] > today.isoformat())
                or (r.get("end_date") or r["start_date"]) > end.isoformat()
                for r in request_obj.canonical_payload["rules"])
            receipt = "PROGRAMMED" if future_pending and projection_receipt == "GOOGLE_CONFIRMED" else projection_receipt
            if receipt == "GOOGLE_CONFIRMED":
                receipt = replacement_receipt(rows, request_obj.pk, today, covered_revisions) or receipt
            result["response_payload"].update(receipt=receipt, projection_receipt=projection_receipt,
                                               future_pending=future_pending)
            attempt.status = "FAILED" if receipt == "READBACK_MISMATCH" else "SKIPPED" if result["noop"] else "SUCCESS"
            attempt.request_payload_json = result["request_payload"]
            attempt.response_payload_json = result["response_payload"]
            attempt.finished_at = timezone.now()
            attempt.save(update_fields=["status", "request_payload_json", "response_payload_json", "finished_at"])
            detail.execution_status = "EXITOSO" if receipt == "GOOGLE_CONFIRMED" else "PENDIENTE"
            detail.platform_payload_json = result["request_payload"]
            detail.published_snapshot_json = result["response_payload"]
            detail.save(update_fields=["execution_status", "platform_payload_json", "published_snapshot_json", "updated_at"])
            summary["details"].append({"branch_id": branch_id, "branch_code": branch.codigo,
                "detail_id": detail.pk, "status": receipt, "public_confirmed": receipt == "GOOGLE_CONFIRMED",
                "ongoing_projection": ongoing, "future_pending": future_pending,
                "projection_receipt": projection_receipt, "noop": result["noop"]})
            summary["failed" if receipt == "READBACK_MISMATCH" else "skipped" if result["noop"] else "success"] += 1
            log_special_hours_event(request_obj=request_obj, detail=detail, actor=actor,
                action="EXECUTION_FAILED" if receipt == "READBACK_MISMATCH" else "EXECUTION_SUCCESS",
                payload={"receipt": receipt, "projection_end": end.isoformat(), "ongoing_projection": ongoing})
        except Exception as exc:
            if attempt:
                attempt.status, attempt.error_message, attempt.finished_at = "FAILED", str(exc), timezone.now()
                attempt.save(update_fields=["status", "error_message", "finished_at"])
            if detail:
                detail.execution_status = "FALLIDO"
                detail.save(update_fields=["execution_status", "updated_at"])
            summary["failed"] += 1
            summary["details"].append({"branch_id": branch_id, "branch_code": location.get("branch_code"),
                                        "status": "FAILED", "error": str(exc), "public_confirmed": False})
            log_special_hours_event(request_obj=request_obj, detail=detail, actor=actor,
                                    action="EXECUTION_FAILED", payload={"error": str(exc)})
        finally:
            locks.close()
    summary["public_confirmed"] = bool(summary["details"]) and all(d["public_confirmed"] for d in summary["details"])
    summary["ongoing_projection"] = any(d.get("ongoing_projection") for d in summary["details"])
    summary["projection_expired"] = bool(summary["details"]) and all(
        d["status"] == "NO_APPLICABLE_DATES" for d in summary["details"])
    if summary["projection_expired"]:
        summary["next_reconcile_at"] = None
    elif summary["public_confirmed"] or all(d["status"] == "PROGRAMMED" for d in summary["details"]):
        summary["next_reconcile_at"] = (timezone.now() + timedelta(days=1)).isoformat()
    # ERP approval remains authoritative even when Google is unavailable or pending.
    SolicitudHorarioEspecial.objects.filter(pk=request_obj.pk, cancelled_at__isnull=True).update(
        execution_summary_json=summary, executed_at=timezone.now(),
        executed_by=actor if getattr(actor, "is_authenticated", False) else None)
    request_obj.refresh_from_db()
    return summary
