from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any
from urllib.parse import urljoin

from django.db import transaction
from django.utils import timezone
from django.utils.dateparse import parse_time

from core.audit import log_event
from core.models import Sucursal
from pos_bridge.config import PointBridgeSettings, load_point_bridge_settings
from pos_bridge.models import PointBranch, PointExtractionLog, PointSyncJob
from pos_bridge.services.alert_service import PointAlertService
from pos_bridge.services.point_http_session_service import PointAuthenticatedSession, PointHttpSessionService
from pos_bridge.utils.exceptions import ExtractionError, PersistenceError
from pos_bridge.utils.helpers import normalize_text, sanitize_sensitive_data
from pos_bridge.utils.logger import get_job_logger, get_pos_bridge_logger
from rrhh.models import AsistenciaEmpleado, Empleado, Turno
from rrhh.services import generar_horas_extra_automatico


@dataclass(frozen=True)
class PointAttendanceBranch:
    external_id: str
    name: str
    plaza: str = ""


@dataclass(frozen=True)
class PointAttendancePayload:
    branch: PointBranch
    attendance_date: date
    employee_code: str
    employee_name: str
    position: str
    entry_at: datetime | None
    exit_at: datetime | None
    scheduled_entry: time | None
    scheduled_exit: time | None
    point_row_id: str
    point_worked_hours: float | None
    is_late: bool
    is_absence: bool
    is_out_of_range: bool
    raw_payload: dict[str, Any]


class PointAttendanceSyncService:
    BRANCHES_PATH = "/Home/Get_Sucursales_ByZona"
    ATTENDANCE_PATH = "/Attendance/AsistenciaDiaria"
    ABSENCES_PATH = "/Attendance/Inasistencias"
    OBSERVATION_PREFIX = "[Point]"

    def __init__(
        self,
        bridge_settings: PointBridgeSettings | None = None,
        http_session_service: PointHttpSessionService | None = None,
    ):
        self.settings = bridge_settings or load_point_bridge_settings()
        self.http_session_service = http_session_service or PointHttpSessionService(self.settings)
        self.logger = get_pos_bridge_logger()
        self.alert_service = PointAlertService()

    def _base_url(self) -> str:
        return self.settings.base_url.rstrip("/") + "/"

    def _request_url(self, path: str) -> str:
        return urljoin(self._base_url(), path.lstrip("/"))

    @staticmethod
    def _decode_payload(response, *, label: str):
        try:
            payload = response.json()
        except ValueError:
            try:
                payload = json.loads(response.text or "[]")
            except json.JSONDecodeError as exc:
                raise ExtractionError(f"Point devolvio una respuesta no JSON en {label}.") from exc
        if isinstance(payload, str):
            try:
                payload = json.loads(payload or "[]")
            except json.JSONDecodeError as exc:
                raise ExtractionError(f"Point devolvio JSON anidado invalido en {label}.") from exc
        return payload

    @staticmethod
    def _coerce_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        return str(value).strip().lower() in {"1", "true", "t", "yes", "si"}

    @staticmethod
    def _parse_point_datetime(value: Any) -> datetime | None:
        text = str(value or "").strip()
        if not text or text.lower() in {"none", "null"}:
            return None
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if timezone.is_naive(parsed):
            return timezone.make_aware(parsed, timezone.get_current_timezone())
        return parsed

    @staticmethod
    def _point_epoch_ms(value: date) -> int:
        local_dt = timezone.make_aware(datetime.combine(value, time.min), timezone.get_current_timezone())
        return int(local_dt.timestamp() * 1000)

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _resolve_erp_branch(external_id: str, name: str) -> Sucursal | None:
        external_id = str(external_id or "").strip()
        name = str(name or "").strip()
        match = None
        if external_id:
            match = Sucursal.objects.filter(codigo__iexact=external_id).first()
        if match is None and name:
            match = Sucursal.objects.filter(nombre__iexact=name).first()
            match = match or Sucursal.objects.filter(codigo__iexact=name).first()
        if match is None and name:
            normalized = normalize_text(name)
            for sucursal in Sucursal.objects.all().only("id", "nombre", "codigo"):
                if normalize_text(sucursal.nombre) == normalized or normalize_text(sucursal.codigo) == normalized:
                    return sucursal
        return match

    def _upsert_branch(self, branch: PointAttendanceBranch) -> PointBranch:
        point_branch, _ = PointBranch.objects.get_or_create(
            external_id=branch.external_id,
            defaults={
                "name": branch.name,
                "status": PointBranch.STATUS_ACTIVE,
                "erp_branch": self._resolve_erp_branch(branch.external_id, branch.name),
                "metadata": {"plaza": branch.plaza, "source": "attendance"},
                "last_seen_at": timezone.now(),
            },
        )
        metadata = point_branch.metadata or {}
        metadata.update({"attendance_plaza": branch.plaza, "attendance_source": "attendance"})
        point_branch.name = branch.name
        point_branch.status = PointBranch.STATUS_ACTIVE
        point_branch.erp_branch = self._resolve_erp_branch(branch.external_id, branch.name)
        point_branch.metadata = metadata
        point_branch.last_seen_at = timezone.now()
        point_branch.save(update_fields=["name", "status", "erp_branch", "metadata", "last_seen_at", "updated_at"])
        return point_branch

    @staticmethod
    def _resolve_turno(
        scheduled_entry: time | None,
        scheduled_exit: time | None,
        entry_at: datetime | None,
    ) -> Turno | None:
        if scheduled_entry and scheduled_exit:
            exact = Turno.objects.filter(
                activo=True,
                hora_entrada=scheduled_entry,
                hora_salida=scheduled_exit,
            ).first()
            if exact:
                return exact
        if not entry_at:
            return None
        local_entry = timezone.localtime(entry_at)
        base = local_entry.date()
        best = None
        best_diff = None
        for turno in Turno.objects.filter(activo=True):
            turno_start = datetime.combine(base, turno.hora_entrada)
            entry_naive = datetime.combine(base, local_entry.time())
            diff = abs((entry_naive - turno_start).total_seconds() / 60)
            if diff <= 90 and (best_diff is None or diff < best_diff):
                best = turno
                best_diff = diff
        return best

    @classmethod
    def _merge_point_observation(cls, existing: str, point_observation: str) -> str:
        lines = [line for line in (existing or "").splitlines() if not line.startswith(cls.OBSERVATION_PREFIX)]
        lines.append(point_observation)
        return "\n".join(line for line in lines if line).strip()

    @staticmethod
    def _name_tokens(value: str) -> set[str]:
        return {token for token in normalize_text(value).split() if len(token) > 1}

    def _resolve_employee(self, payload: PointAttendancePayload) -> tuple[Empleado | None, str, str]:
        if payload.employee_code:
            empleado = Empleado.objects.filter(codigo=payload.employee_code).first()
            if empleado is not None:
                return empleado, "code", ""

        name_key = normalize_text(payload.employee_name)
        if not name_key:
            return None, "", "missing_code" if not payload.employee_code else "missing_employee"

        exact_matches = list(Empleado.objects.filter(activo=True, nombre_normalizado=name_key).order_by("id")[:2])
        if len(exact_matches) == 1:
            return exact_matches[0], "name", ""
        if len(exact_matches) > 1:
            return None, "", "ambiguous_employee"

        point_tokens = self._name_tokens(payload.employee_name)
        if len(point_tokens) < 2:
            return None, "", "missing_employee"

        token_matches = []
        for candidato in Empleado.objects.filter(activo=True).only("id", "nombre", "nombre_normalizado").order_by("id"):
            erp_tokens = self._name_tokens(candidato.nombre_normalizado or candidato.nombre)
            if point_tokens.issubset(erp_tokens):
                token_matches.append(candidato)
                if len(token_matches) > 1:
                    return None, "", "ambiguous_employee"
        if len(token_matches) == 1:
            return token_matches[0], "name_tokens", ""
        return None, "", "missing_employee"

    def create_job(
        self,
        *,
        start_date: date,
        end_date: date,
        branch_filter: str | None,
        triggered_by=None,
        attempt_count: int = 1,
    ) -> PointSyncJob:
        return PointSyncJob.objects.create(
            job_type=PointSyncJob.JOB_TYPE_ATTENDANCE,
            status=PointSyncJob.STATUS_RUNNING,
            started_at=timezone.now(),
            triggered_by=triggered_by,
            parameters={
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "branch_filter": branch_filter or "",
                "source": self.ATTENDANCE_PATH,
                "settings": self.settings.safe_dict(),
            },
            attempt_count=attempt_count,
        )

    def record_log(self, sync_job: PointSyncJob, level: str, message: str, *, context: dict | None = None) -> None:
        context = sanitize_sensitive_data(context or {})
        PointExtractionLog.objects.create(sync_job=sync_job, level=level, message=message, context=context)
        get_job_logger(sync_job.id).log(
            {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}.get(level, 20),
            "%s | %s",
            message,
            context,
        )

    def mark_success(self, sync_job: PointSyncJob, summary: dict) -> PointSyncJob:
        summary = sanitize_sensitive_data(summary)
        sync_job.status = PointSyncJob.STATUS_SUCCESS
        sync_job.finished_at = timezone.now()
        sync_job.error_message = ""
        sync_job.result_summary = summary
        sync_job.save(update_fields=["status", "finished_at", "error_message", "result_summary", "updated_at"])
        log_event(
            sync_job.triggered_by,
            "POS_BRIDGE_SYNC_SUCCESS",
            "pos_bridge.PointSyncJob",
            str(sync_job.id),
            payload=summary,
        )
        return sync_job

    def mark_partial(self, sync_job: PointSyncJob, summary: dict, *, warning_message: str) -> PointSyncJob:
        summary = sanitize_sensitive_data(summary)
        sync_job.status = PointSyncJob.STATUS_PARTIAL
        sync_job.finished_at = timezone.now()
        sync_job.error_message = warning_message
        sync_job.result_summary = summary
        sync_job.save(update_fields=["status", "finished_at", "error_message", "result_summary", "updated_at"])
        self.record_log(sync_job, PointExtractionLog.LEVEL_WARNING, warning_message, context=summary)
        log_event(
            sync_job.triggered_by,
            "POS_BRIDGE_SYNC_PARTIAL",
            "pos_bridge.PointSyncJob",
            str(sync_job.id),
            payload=summary,
        )
        return sync_job

    def mark_failure(self, sync_job: PointSyncJob, exc: Exception) -> PointSyncJob:
        context = sanitize_sensitive_data(getattr(exc, "context", {}) or {})
        sync_job.status = PointSyncJob.STATUS_FAILED
        sync_job.finished_at = timezone.now()
        sync_job.error_message = str(exc)
        sync_job.artifacts = {**sync_job.artifacts, **context}
        sync_job.save(update_fields=["status", "finished_at", "error_message", "artifacts", "updated_at"])
        self.record_log(sync_job, PointExtractionLog.LEVEL_ERROR, str(exc), context=context)
        self.alert_service.emit_failure(job_id=sync_job.id, message=str(exc), context=context)
        log_event(
            sync_job.triggered_by,
            "POS_BRIDGE_SYNC_FAILED",
            "pos_bridge.PointSyncJob",
            str(sync_job.id),
            payload={"error": str(exc), "context": context},
        )
        return sync_job

    def fetch_branches(self, auth_session: PointAuthenticatedSession) -> list[PointAttendanceBranch]:
        response = auth_session.session.get(
            self._request_url(self.BRANCHES_PATH),
            timeout=self.settings.timeout_ms / 1000,
        )
        response.raise_for_status()
        payload = self._decode_payload(response, label="sucursales Point asistencia")
        if not isinstance(payload, list):
            raise ExtractionError("Point devolvio sucursales de asistencia en formato invalido.")
        branches: list[PointAttendanceBranch] = []
        for plaza in payload:
            plaza_name = str((plaza or {}).get("Plaza") or "").strip()
            for item in (plaza or {}).get("Sucursales") or []:
                external_id = str(item.get("PK_Sucursal") or "").strip()
                name = str(item.get("Sucursal") or "").strip()
                if external_id and name:
                    branches.append(PointAttendanceBranch(external_id=external_id, name=name, plaza=plaza_name))
        return branches

    def _filter_branches(
        self,
        branches: list[PointAttendanceBranch],
        branch_filter: str | None,
    ) -> list[PointAttendanceBranch]:
        token = str(branch_filter or "").strip()
        if not token:
            return branches
        normalized = normalize_text(token)
        exact = [
            branch
            for branch in branches
            if branch.external_id == token
            or normalize_text(branch.name) == normalized
            or normalize_text(branch.plaza) == normalized
        ]
        if exact:
            return exact
        return [
            branch
            for branch in branches
            if normalized in normalize_text(branch.name) or normalized in normalize_text(branch.plaza)
        ]

    def fetch_branch_day(
        self,
        auth_session: PointAuthenticatedSession,
        *,
        branch: PointAttendanceBranch,
        attendance_date: date,
    ) -> tuple[list[PointAttendancePayload], list[dict[str, Any]]]:
        params = {"fecha": self._point_epoch_ms(attendance_date), "sucursal": branch.external_id}
        attendance_response = auth_session.session.get(
            self._request_url(self.ATTENDANCE_PATH),
            params=params,
            timeout=self.settings.timeout_ms / 1000,
        )
        attendance_response.raise_for_status()
        absences_response = auth_session.session.get(
            self._request_url(self.ABSENCES_PATH),
            params=params,
            timeout=self.settings.timeout_ms / 1000,
        )
        absences_response.raise_for_status()
        attendance_rows = self._decode_payload(attendance_response, label="asistencia diaria Point")
        absence_rows = self._decode_payload(absences_response, label="inasistencias Point")
        if not isinstance(attendance_rows, list) or not isinstance(absence_rows, list):
            raise ExtractionError("Point devolvio asistencia diaria en formato invalido.")

        point_branch = self._upsert_branch(branch)
        payloads: list[PointAttendancePayload] = []
        for row in attendance_rows:
            if not isinstance(row, dict):
                continue
            employee_code = str(row.get("Codigo") or row.get("Codigo_Empleado") or "").strip()
            entry_at = self._parse_point_datetime(row.get("Entrada"))
            exit_at = self._parse_point_datetime(row.get("Salida"))
            payloads.append(
                PointAttendancePayload(
                    branch=point_branch,
                    attendance_date=attendance_date,
                    employee_code=employee_code,
                    employee_name=str(row.get("Empleado") or "").strip(),
                    position=str(row.get("Puesto") or "").strip(),
                    entry_at=entry_at,
                    exit_at=exit_at,
                    scheduled_entry=parse_time(str(row.get("H_Entrada") or "")),
                    scheduled_exit=parse_time(str(row.get("H_Salida") or "")),
                    point_row_id=str(row.get("IDX") or "").strip(),
                    point_worked_hours=self._safe_float(row.get("Horas_Trabajo")),
                    is_late=self._coerce_bool(row.get("Retardo")),
                    is_absence=self._coerce_bool(row.get("Falta")),
                    is_out_of_range=self._coerce_bool(row.get("fuera_rango")),
                    raw_payload=row,
                )
            )
        return payloads, absence_rows

    @transaction.atomic
    def persist_payload(self, payload: PointAttendancePayload) -> tuple[AsistenciaEmpleado | None, str]:
        empleado, match_method, unresolved_reason = self._resolve_employee(payload)
        if empleado is None:
            return None, unresolved_reason

        asistencia, created = AsistenciaEmpleado.objects.get_or_create(
            empleado=empleado,
            fecha=payload.attendance_date,
            defaults={
                "fuente": AsistenciaEmpleado.FUENTE_POINT,
                "sucursal": payload.branch.erp_branch,
            },
        )
        asistencia.sucursal = payload.branch.erp_branch or asistencia.sucursal
        asistencia.entrada = payload.entry_at or asistencia.entrada
        asistencia.salida = payload.exit_at or asistencia.salida
        if asistencia.entrada and asistencia.salida:
            delta = asistencia.salida - asistencia.entrada
            if delta < timedelta(0):
                delta += timedelta(days=1)
            asistencia.minutos_trabajados = max(int(delta.total_seconds() / 60), 0)
        asistencia.turno = asistencia.turno or self._resolve_turno(
            payload.scheduled_entry,
            payload.scheduled_exit,
            asistencia.entrada,
        )
        asistencia.fuente = AsistenciaEmpleado.FUENTE_POINT
        asistencia.observacion = self._merge_point_observation(
            asistencia.observacion,
            (
                f"{self.OBSERVATION_PREFIX} IDX={payload.point_row_id or '-'}; "
                f"match={match_method}; "
                f"codigo_point={payload.employee_code or '-'}; "
                f"sucursal={payload.branch.name}; puesto={payload.position or '-'}; "
                f"retardo={payload.is_late}; falta={payload.is_absence}; "
                f"fuera_rango={payload.is_out_of_range}; "
                f"horas_point={payload.point_worked_hours if payload.point_worked_hours is not None else '-'}"
            ),
        )
        asistencia.save()
        if asistencia.salida and asistencia.turno_id:
            generar_horas_extra_automatico(asistencia)
        if match_method in {"name", "name_tokens"}:
            return asistencia, "created_by_name" if created else "updated_by_name"
        return asistencia, "created" if created else "updated"

    def run_sync(
        self,
        *,
        start_date: date,
        end_date: date,
        branch_filter: str | None = None,
        triggered_by=None,
        attempt_count: int = 1,
    ) -> PointSyncJob:
        if end_date < start_date:
            raise ValueError("end_date no puede ser menor que start_date.")
        sync_job = self.create_job(
            start_date=start_date,
            end_date=end_date,
            branch_filter=branch_filter,
            triggered_by=triggered_by,
            attempt_count=attempt_count,
        )
        self.record_log(sync_job, PointExtractionLog.LEVEL_INFO, "Inicio de sincronizacion Point asistencias.")

        try:
            auth_session = self.http_session_service.create()
            branches = self._filter_branches(self.fetch_branches(auth_session), branch_filter)
            if not branches:
                raise PersistenceError("Point no devolvio sucursales de asistencia para el filtro solicitado.")

            summary = {
                "days_processed": 0,
                "branches_processed": 0,
                "attendance_rows_seen": 0,
                "attendance_created": 0,
                "attendance_updated": 0,
                "attendance_matched_by_code": 0,
                "attendance_matched_by_name": 0,
                "absences_seen": 0,
                "missing_employee": 0,
                "missing_code": 0,
                "ambiguous_employee": 0,
                "unresolved_samples": [],
            }
            current = start_date
            seen_days: set[date] = set()
            seen_branches: set[str] = set()
            while current <= end_date:
                seen_days.add(current)
                for branch in branches:
                    payloads, absences = self.fetch_branch_day(auth_session, branch=branch, attendance_date=current)
                    seen_branches.add(branch.external_id)
                    summary["attendance_rows_seen"] += len(payloads)
                    summary["absences_seen"] += len(absences)
                    branch_summary = {
                        "branch": branch.name,
                        "date": current.isoformat(),
                        "rows": len(payloads),
                        "absences": len(absences),
                    }
                    for payload in payloads:
                        _, result = self.persist_payload(payload)
                        if result == "created":
                            summary["attendance_created"] += 1
                            summary["attendance_matched_by_code"] += 1
                        elif result == "updated":
                            summary["attendance_updated"] += 1
                            summary["attendance_matched_by_code"] += 1
                        elif result == "created_by_name":
                            summary["attendance_created"] += 1
                            summary["attendance_matched_by_name"] += 1
                        elif result == "updated_by_name":
                            summary["attendance_updated"] += 1
                            summary["attendance_matched_by_name"] += 1
                        elif result in {"missing_employee", "missing_code", "ambiguous_employee"}:
                            summary[result] += 1
                            if len(summary["unresolved_samples"]) < 15:
                                summary["unresolved_samples"].append(
                                    {
                                        "date": current.isoformat(),
                                        "branch": branch.name,
                                        "employee_code": payload.employee_code,
                                        "employee_name": payload.employee_name,
                                        "reason": result,
                                    }
                                )
                    self.record_log(
                        sync_job,
                        PointExtractionLog.LEVEL_INFO,
                        "Asistencias Point procesadas por sucursal.",
                        context=branch_summary,
                    )
                current += timedelta(days=1)

            summary["days_processed"] = len(seen_days)
            summary["branches_processed"] = len(seen_branches)
            unresolved = summary["missing_employee"] + summary["missing_code"] + summary["ambiguous_employee"]
            if unresolved:
                return self.mark_partial(
                    sync_job,
                    summary,
                    warning_message="Asistencias Point sincronizadas con empleados sin mapear.",
                )
            return self.mark_success(sync_job, summary)
        except Exception as exc:
            return self.mark_failure(sync_job, exc)
