#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from types import SimpleNamespace
from pathlib import Path

APP_ENV_WAS_INJECTED = not bool(os.environ.get("APP_ENV"))
if APP_ENV_WAS_INJECTED:
    os.environ["APP_ENV"] = "local"

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
DEFAULT_TIMEOUT = 120
PRODUCTION_HOST = "root@68.183.165.47"
PRODUCTION_KEY = Path.home() / ".ssh" / "agente_dg_ops"
PRODUCTION_DIR = "/opt/pastelerias-erp"
PRODUCTION_COMPOSE = f"docker compose -f {PRODUCTION_DIR}/docker-compose.yml"
DB_TUNNEL_HINT = (
    "Tunnel: ssh -L 55433:127.0.0.1:55432 root@68.183.165.47 -N. "
    "Luego: DATABASE_URL=postgresql://...:55433/... python scripts/erp_doctor.py --quick"
)
CRITICAL_BEAT_TASKS = [
    {
        "label": "snapshot_historical_costing",
        "names": ["recetas: snapshot costeo historico mensual", "reportes: snapshot costeo historico mensual"],
        "task": "reportes.snapshot_historical_costing_task",
        "expected_cron": {"minute": "0", "hour": "1", "day_of_month": "1"},
    },
    {
        "label": "sync_ventas_autoritativas",
        "names": ["ventas: sync ventas autoritativas mensual", "sync_ventas_autoritativas"],
        "task": "ventas.sync_ventas_autoritativas",
        "expected_cron": {"minute": "0", "hour": "3", "day_of_month": "2"},
    },
    {
        "label": "fleet_document_expiration_alerts",
        "names": ["logistica-alertar-documentos-por-vencer", "fleet_document_expiration_alerts"],
        "task": "logistica.tasks.alertar_documentos_por_vencer",
        "expected_cron": {"minute": "0", "hour": "8"},
    },
    {
        "label": "fleet_upcoming_service_alerts",
        "names": ["logistica-alertar-servicios-proximos", "fleet_upcoming_service_alerts"],
        "task": "logistica.tasks.alertar_servicios_proximos",
        "expected_cron": {"minute": "5", "hour": "8"},
    },
    {
        "label": "fleet_pending_wash_alerts",
        "names": ["logistica-alertar-lavados-pendientes", "fleet_pending_wash_alerts"],
        "task": "logistica.tasks.alertar_lavados_pendientes",
        "expected_cron": {"minute": "10", "hour": "8"},
    },
    {
        "label": "monthly_closure_email",
        "names": ["core: cierre automático mes anterior", "monthly_closure_email"],
        "task": "core.tasks.cerrar_mes_anterior",
        "expected_cron": {"minute": "0", "hour": "6", "day_of_month": "5"},
    },
    {
        "label": "pos_bridge_nightly_sync",
        "names": ["pos_bridge: ventas cerradas diario", "pos_bridge: sync ventas diario", "pos_bridge_nightly_sync"],
        "task": "pos_bridge.daily_sales_sync",
        "expected_cron": {"minute": "30", "hour": "1"},
    },
]
INVESTMENT_COGS_RATIO_THRESHOLD = Decimal("2.0")
INVESTMENT_COGS_OFFENDER_LIMIT = 20
PERCENT_FIELD_MIN = Decimal("-9999.9999")
PERCENT_FIELD_MAX = Decimal("9999.9999")
GUAMUCHIL_PROJECT_ID = 1
GUAMUCHIL_PROJECT_NAME = "Apertura Guamuchil 2026"
GUAMUCHIL_EXPECTED_INVESTMENT = Decimal("1121753.85")
GUAMUCHIL_RECON_PREFIX = "GML_RECON_2026_05_18"
GUAMUCHIL_EXPECTED_RECON_ROWS = 69
GUAMUCHIL_OLD_PLACEHOLDER_DESCRIPTION = "Inversion apertura importado presupuesto 2026"
GUAMUCHIL_OLD_PLACEHOLDER_AMOUNT = Decimal("492343.00")
PAGE_LOAD_WARN_MS = 2500
PAGE_LOAD_FAIL_MS = 8000
PAGE_LOAD_ROUTES = [
    ("Dashboard", "/dashboard/"),
    ("Maestros", "/maestros/"),
    ("Recetas", "/recetas/"),
    ("Plan produccion", "/recetas/plan-produccion/"),
    ("Compras", "/compras/"),
    ("Inventario", "/inventario/"),
    ("Activos", "/activos/"),
    ("Control", "/control/"),
    ("CRM", "/crm/"),
    ("Ventas", "/ventas/"),
    ("RRHH", "/rrhh/"),
    ("Bonos produccion", "/bonos-produccion/dashboard/"),
    ("Bonos ventas", "/bonos-ventas/dashboard/"),
    ("Logistica", "/logistica/"),
    ("Fallas", "/fallas/"),
    ("Mermas", "/mermas/"),
    ("Mantenimiento", "/mantenimiento/app/"),
    ("Reportes", "/reportes/"),
    ("Inversiones", "/inversiones/"),
    ("Rentabilidad", "/rentabilidad/"),
    ("Horarios especiales", "/horarios-especiales/"),
    ("Orquestacion", "/orquestacion/"),
]


@dataclass
class CheckResult:
    name: str
    severity: str
    status: str
    command: str | None = None
    exit_code: int | None = None
    duration_ms: int = 0
    summary: str = ""
    details: list[object] = field(default_factory=list)
    fixed: bool = False
    fix_action: str | None = None


def python_bin() -> str:
    venv_python = ROOT / ".venv" / "bin" / "python"
    if venv_python.exists() and os.access(venv_python, os.X_OK):
        return str(venv_python)
    return sys.executable


def trim_output(value: str, limit: int = 6000) -> list[str]:
    value = value.strip()
    if not value:
        return []
    if len(value) <= limit:
        return value.splitlines()
    return (value[:limit] + "\n... [truncated]").splitlines()


def run_command(
    name: str,
    command: list[str] | str,
    *,
    fail_severity: str = "FAIL",
    warn_exit_codes: set[int] | None = None,
    timeout: int = DEFAULT_TIMEOUT,
    env: dict[str, str] | None = None,
) -> CheckResult:
    started = time.monotonic()
    shell = isinstance(command, str)
    command_text = command if isinstance(command, str) else shlex.join(command)
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            env={**os.environ, **(env or {})},
            shell=shell,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        duration_ms = int((time.monotonic() - started) * 1000)
    except subprocess.TimeoutExpired as exc:
        duration_ms = int((time.monotonic() - started) * 1000)
        details = trim_output((exc.stdout or "") + "\n" + (exc.stderr or ""))
        return CheckResult(
            name=name,
            severity=fail_severity,
            status=fail_severity,
            command=command_text,
            exit_code=None,
            duration_ms=duration_ms,
            summary=f"Timeout despues de {timeout}s.",
            details=details,
        )

    output = "\n".join(part for part in [completed.stdout, completed.stderr] if part)
    details = trim_output(output)
    warn_exit_codes = warn_exit_codes or set()
    if completed.returncode == 0:
        status = "OK"
        severity = "OK"
        summary = "OK"
    elif completed.returncode in warn_exit_codes:
        status = "WARN"
        severity = "WARN"
        summary = f"Termino con exit code {completed.returncode}."
    else:
        status = fail_severity
        severity = fail_severity
        summary = f"Fallo con exit code {completed.returncode}."

    return CheckResult(
        name=name,
        severity=severity,
        status=status,
        command=command_text,
        exit_code=completed.returncode,
        duration_ms=duration_ms,
        summary=summary,
        details=details,
    )


def skipped(name: str, summary: str, command: str | None = None) -> CheckResult:
    return CheckResult(
        name=name,
        severity="SKIPPED",
        status="SKIPPED",
        command=command,
        summary=summary,
    )


def read_dotenv_values(keys: set[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    env_path = ROOT / ".env"
    if not env_path.exists():
        return values
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in keys:
            continue
        value = value.strip().strip("'\"")
        values[key] = value
    return values


def env_bool_value(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def check_environment_sanity() -> CheckResult:
    dotenv = read_dotenv_values({"APP_ENV", "DEBUG"})
    app_env = os.getenv("APP_ENV", dotenv.get("APP_ENV", "production")).strip().lower()
    is_development = app_env in {"development", "dev", "local", "test"}
    debug_raw = os.getenv("DEBUG", dotenv.get("DEBUG"))
    debug = env_bool_value(debug_raw, default=is_development)
    if APP_ENV_WAS_INJECTED:
        return CheckResult(
            name="APP_ENV",
            severity="WARN",
            status="WARN",
            summary=(
                'APP_ENV no estaba definido; se asumio "local" para esta ejecucion. '
                "En produccion define APP_ENV=production explicitamente en .env."
            ),
            details=[
                "APP_ENV=local",
                f"DEBUG={debug_raw if debug_raw is not None else debug}",
            ],
        )
    if app_env in {"production", "staging"} and debug:
        return CheckResult(
            name="APP_ENV",
            severity="FAIL",
            status="FAIL",
            summary=(
                "APP_ENV resuelve como production/staging con DEBUG activo; "
                "settings.py aborta antes de cargar Django."
            ),
            details=[
                f"APP_ENV={app_env}",
                f"DEBUG={debug_raw if debug_raw is not None else debug}",
                "Para local, define APP_ENV=local/development o desactiva DEBUG en entornos production/staging.",
            ],
        )
    return CheckResult(
        name="APP_ENV",
        severity="OK",
        status="OK",
        summary=f"APP_ENV definido como {app_env}.",
        details=[f"DEBUG={debug}"],
    )


def db_is_reachable(host: str, port: int = 5432, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def get_default_db_connection_info() -> tuple[str | None, int | None, CheckResult | None]:
    py = python_bin()
    script = """
import json
import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
from django.conf import settings
db = settings.DATABASES["default"]
print(json.dumps({"HOST": db.get("HOST") or "127.0.0.1", "PORT": db.get("PORT") or 5432}))
"""
    result = run_command("DB config", [py, "-c", script], timeout=30)
    if result.status != "OK":
        result.name = "Migrations check"
        result.summary = "No fue posible leer settings.DATABASES['default'] antes de migrate --check."
        return None, None, result
    try:
        data = json.loads("\n".join(str(line) for line in result.details))
        return str(data["HOST"]), int(data["PORT"]), None
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return (
            None,
            None,
            CheckResult(
                name="Migrations check",
                severity="FAIL",
                status="FAIL",
                command=result.command,
                exit_code=result.exit_code,
                duration_ms=result.duration_ms,
                summary="No fue posible interpretar HOST/PORT de la DB configurada.",
                details=[str(exc), *result.details],
            ),
        )


def check_migrations() -> CheckResult:
    host, port, error = get_default_db_connection_info()
    if error:
        return error
    assert host is not None
    assert port is not None
    if not db_is_reachable(host, port, timeout=2.0):
        return skipped(
            "Migrations check",
            (
                f"DB no alcanzable (host: {host}:{port}). "
                "Ejecuta con DB local o via SSH tunnel para validar migraciones. "
                f"{DB_TUNNEL_HINT}"
            ),
            f"{python_bin()} manage.py migrate --check",
        )
    return run_command("Migrations check", [python_bin(), "manage.py", "migrate", "--check"], timeout=180)


def check_django() -> list[CheckResult]:
    py = python_bin()
    return [
        run_command("Django check", [py, "manage.py", "check"], timeout=180),
        check_migrations(),
    ]


def check_quality_guards() -> list[CheckResult]:
    py = python_bin()
    checks: list[CheckResult] = []
    for script in ["scripts/check_pointdailysale_usage.py", "scripts/check_protected_sales_readers.py"]:
        path = ROOT / script
        if not path.exists():
            checks.append(skipped(f"Quality guard: {script}", "No existe el script esperado."))
            continue
        checks.append(run_command(f"Quality guard: {script}", [py, script], timeout=120))
    return checks


def check_optional_tool(name: str, command: str, install_hint: str, *, fail_severity: str = "WARN") -> CheckResult:
    executable = shlex.split(command)[0]
    if shutil.which(executable) is None:
        return skipped(name, f"Herramienta no instalada. Sugerencia: {install_hint}", command)
    return run_command(name, command, fail_severity=fail_severity, timeout=300)


def check_python_static(full: bool) -> list[CheckResult]:
    checks = [
        check_optional_tool(
            "Python lint: ruff",
            "ruff check .",
            "instalar ruff como dependencia de desarrollo o usar pipx run ruff check .",
            fail_severity="WARN",
        )
    ]
    if full:
        checks.append(
            check_optional_tool(
                "Security deps: pip-audit",
                "pip-audit -r requirements.txt",
                "instalar pip-audit como herramienta de desarrollo o correr pipx run pip-audit -r requirements.txt",
                fail_severity="WARN",
            )
        )
        checks.append(
            check_optional_tool(
                "Security code: semgrep",
                "semgrep --config p/python --config p/django --config p/security-audit --error",
                "instalar semgrep opcionalmente; no se agrega al runtime del ERP",
                fail_severity="WARN",
            )
        )
    return checks


def check_templates(full: bool) -> list[CheckResult]:
    if not full:
        templates = set(ROOT.glob("**/templates/**/*.html")) | set((ROOT / "templates").glob("**/*.html"))
        template_count = len(templates)
        return [
            CheckResult(
                name="Templates inventory",
                severity="OK",
                status="OK",
                summary=f"{template_count} templates HTML detectados; djlint se ejecuta en --full si esta instalado.",
            )
        ]
    return [
        check_optional_tool(
            "Templates: djlint",
            "djlint templates */templates --check --extension html",
            "instalar djlint como dependencia opcional de desarrollo; no usar --reformat en esta fase",
            fail_severity="WARN",
        )
    ]


def project_js_files() -> list[Path]:
    candidates: list[Path] = []
    for pattern in ["static/**/*.js", "*/static/**/*.js"]:
        candidates.extend(ROOT.glob(pattern))
    excluded_parts = {"staticfiles", "node_modules", ".venv"}
    return sorted({path for path in candidates if not excluded_parts.intersection(path.parts)})


def check_js() -> list[CheckResult]:
    files = project_js_files()
    if not files:
        return [skipped("JS/PWA", "No se detectaron archivos JS propios fuera de staticfiles.")]
    if shutil.which("node") is None:
        return [
            skipped(
                "JS/PWA syntax",
                f"Node no esta instalado; {len(files)} archivos JS propios detectados.",
                "node --check <archivo>",
            )
        ]
    checks: list[CheckResult] = []
    for path in files:
        checks.append(
            run_command(
                f"JS syntax: {path.relative_to(ROOT)}",
                ["node", "--check", str(path)],
                fail_severity="WARN",
                timeout=30,
            )
        )
    return checks


def check_celery() -> list[CheckResult]:
    py = python_bin()
    script = """
import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
from config.celery import app
app.loader.import_default_modules()
tasks = sorted(name for name in app.tasks if not name.startswith("celery."))
critical = [
    "pos_bridge.daily_sales_sync",
    "pos_bridge.inventory_sync",
    "recetas.consolidado_nocturno_cedis",
    "recetas.inventario_final_cierre_email",
    "reportes.operations_automation_cycle",
]
missing = [name for name in critical if name not in tasks]
print(f"registered_tasks={len(tasks)}")
for name in critical:
    print(f"{name}={'OK' if name in tasks else 'MISSING'}")
if missing:
    raise SystemExit("missing critical celery tasks: " + ", ".join(missing))
"""
    checks = [
        run_command(
            "Celery registry",
            [py, "-c", script],
            timeout=180,
        )
    ]
    if (ROOT / "pos_bridge" / "management" / "commands" / "setup_celery_schedules.py").exists():
        checks.append(
            CheckResult(
                name="Celery beat schedules",
                severity="OK",
                status="OK",
                command="python manage.py setup_celery_schedules",
                summary="Comando canonico detectado; no se ejecuta en doctor local para evitar escrituras en DB.",
            )
        )
    else:
        checks.append(skipped("Celery beat schedules", "No se encontro setup_celery_schedules."))
    return checks


def _setup_django() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django
    from django.apps import apps

    if not apps.ready:
        django.setup()


def _format_schedule_detail(task) -> tuple[dict[str, object] | None, dict[str, object] | None]:
    cron = None
    interval = None
    if task.crontab_id:
        cron_obj = task.crontab
        cron = {
            "minute": cron_obj.minute,
            "hour": cron_obj.hour,
            "day_of_week": cron_obj.day_of_week,
            "day_of_month": cron_obj.day_of_month,
            "month_of_year": cron_obj.month_of_year,
            "timezone": str(cron_obj.timezone),
        }
    if task.interval_id:
        interval = {
            "every": task.interval.every,
            "period": task.interval.period,
        }
    return cron, interval


def _select_periodic_task(item: dict[str, object]):
    from django.db.models import Q
    from django_celery_beat.models import PeriodicTask

    names = list(item["names"])
    matches = list(
        PeriodicTask.objects.select_related("crontab", "interval").filter(
            Q(name__in=names) | Q(task=item["task"])
        )
    )
    selected = next((task for name in names for task in matches if task.name == name), None)
    if selected is None and matches:
        selected = matches[0]
    return selected


def check_celery_beat_schedules(fix: bool = False) -> CheckResult:
    started = time.monotonic()
    host, port, db_config_error = get_default_db_connection_info()
    if db_config_error:
        db_config_error.name = "Celery Beat - schedules criticos"
        db_config_error.summary = "No fue posible leer la configuracion de DB antes de consultar Beat."
        return db_config_error
    assert host is not None
    assert port is not None
    if not db_is_reachable(host, port, timeout=2.0):
        return skipped(
            "Celery Beat - schedules criticos",
            f"DB no alcanzable (host: {host}:{port}); no se puede consultar ni corregir PeriodicTask.",
            "django_celery_beat PeriodicTask ORM",
        )
    try:
        _setup_django()
        from django_celery_beat.models import PeriodicTask
    except Exception as exc:  # noqa: BLE001
        return CheckResult(
            name="Celery Beat - schedules criticos",
            severity="FAIL",
            status="FAIL",
            command="django_celery_beat PeriodicTask ORM",
            duration_ms=int((time.monotonic() - started) * 1000),
            summary="Error al inicializar Django para consultar Beat.",
            details=[str(exc)],
        )

    details: list[dict[str, object]] = []
    fixed_actions: list[str] = []
    global_status = "OK"

    try:
        for item in CRITICAL_BEAT_TASKS:
            names = list(item["names"])
            expected_cron = dict(item.get("expected_cron") or {})
            selected = _select_periodic_task(item)
            if selected is None:
                details.append({
                    "label": item["label"],
                    "status": "WARN",
                    "summary": "no encontrada en Beat",
                    "expected_names": names,
                    "task": item["task"],
                    "fixed": False,
                })
                global_status = "WARN"
                continue

            item_fixed = False
            item_fix_action = None
            if fix and not selected.enabled:
                item_fix_action = f"Beat task '{selected.name}' estaba disabled -> reactivada."
                print(f"[FIX] {item_fix_action}", file=sys.stderr)
                PeriodicTask.objects.filter(pk=selected.pk).update(enabled=True)
                selected = _select_periodic_task(item)
                item_fixed = True
                fixed_actions.append(item_fix_action)

            cron, interval = _format_schedule_detail(selected)
            mismatches = []
            if cron is None and expected_cron:
                mismatches.append("sin crontab")
            elif cron:
                for key, expected in expected_cron.items():
                    if str(cron.get(key)) != str(expected):
                        mismatches.append(f"{key}={cron.get(key)} esperado={expected}")

            if not selected.enabled:
                item_status = "WARN"
                summary = "enabled=False - revisar"
            elif mismatches:
                item_status = "WARN"
                summary = "frecuencia distinta: " + ", ".join(mismatches)
            else:
                item_status = "OK"
                summary = "enabled"

            if item_status == "WARN":
                global_status = "WARN"

            details.append({
                "label": item["label"],
                "status": item_status,
                "summary": summary,
                "name": selected.name,
                "task": selected.task,
                "enabled": selected.enabled,
                "cron": cron,
                "interval": interval,
                "expected_names": names,
                "expected_cron": expected_cron,
                "fixed": item_fixed,
                "fix_action": item_fix_action,
            })
    except Exception as exc:  # noqa: BLE001
        return CheckResult(
            name="Celery Beat - schedules criticos",
            severity="FAIL",
            status="FAIL",
            command="django_celery_beat PeriodicTask ORM",
            duration_ms=int((time.monotonic() - started) * 1000),
            summary="Error al consultar django_celery_beat_periodictask.",
            details=[str(exc)],
        )

    warn_count = sum(1 for item in details if item["status"] == "WARN")
    ok_count = sum(1 for item in details if item["status"] == "OK")
    return CheckResult(
        name="Celery Beat - schedules criticos",
        severity=global_status,
        status=global_status,
        command="django_celery_beat PeriodicTask ORM",
        exit_code=0,
        duration_ms=int((time.monotonic() - started) * 1000),
        summary=f"{ok_count} OK, {warn_count} WARN sobre {len(details)} schedules criticos.",
        details=details,
        fixed=bool(fixed_actions),
        fix_action="; ".join(fixed_actions) if fixed_actions else None,
    )


def _as_decimal(value: object) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _money(value: object) -> str:
    return str(_as_decimal(value).quantize(Decimal("0.01")))


def _ratio(numerator: object, denominator: object) -> Decimal | None:
    denominator_decimal = _as_decimal(denominator)
    if denominator_decimal == 0:
        return None
    return (_as_decimal(numerator) / denominator_decimal).quantize(Decimal("0.0001"))


def _percent(numerator: object, denominator: object) -> Decimal | None:
    denominator_decimal = _as_decimal(denominator)
    if denominator_decimal == 0:
        return None
    return ((_as_decimal(numerator) / denominator_decimal) * Decimal("100")).quantize(Decimal("0.0001"))


def _period_label(value: object) -> str:
    if hasattr(value, "date"):
        value = value.date()
    if hasattr(value, "isoformat"):
        return value.isoformat()[:7]
    return str(value)[:7]


def _period_start(value: object):
    if hasattr(value, "date"):
        value = value.date()
    return value.replace(day=1)


def _next_month_start(value: object):
    period = _period_start(value)
    if period.month == 12:
        return period.replace(year=period.year + 1, month=1, day=1)
    return period.replace(month=period.month + 1, day=1)


def _branch_label(row: dict[str, object]) -> str:
    code = row.get("sucursal__codigo") or row.get("sucursal_codigo") or ""
    name = row.get("sucursal__nombre") or row.get("sucursal_nombre") or ""
    if code and name:
        return f"{code} - {name}"
    return str(code or name or "Sin sucursal")


def _finance_db_ready(check_name: str) -> CheckResult | None:
    host, port, error = get_default_db_connection_info()
    if error:
        return CheckResult(
            name=check_name,
            severity="SKIPPED",
            status="SKIPPED",
            command=error.command,
            exit_code=error.exit_code,
            duration_ms=error.duration_ms,
            summary="DB no configurada/disponible; auditoria read-only omitida.",
            details=error.details,
        )
    assert host is not None
    assert port is not None
    if not db_is_reachable(host, port, timeout=2.0):
        return skipped(
            check_name,
            f"DB no alcanzable (host: {host}:{port}); auditoria read-only omitida.",
            "Django ORM SELECT agregados",
        )
    return None


def _cost_sources_for_recipes(recipe_ids: set[int], period_start: object) -> dict[int, dict[str, object]]:
    from reportes.models import ProductoCostoOperativoMensual

    if not recipe_ids:
        return {}

    latest_rows = (
        ProductoCostoOperativoMensual.objects.filter(receta_id__in=recipe_ids)
        .order_by("receta_id", "-periodo")
        .distinct("receta_id")
    )
    aligned_rows = (
        ProductoCostoOperativoMensual.objects.filter(
            receta_id__in=recipe_ids,
            periodo__lte=period_start,
        )
        .order_by("receta_id", "-periodo")
        .distinct("receta_id")
    )
    sources: dict[int, dict[str, object]] = {
        recipe_id: {
            "latest_period": None,
            "latest_unit_cost": None,
            "period_aligned_period": None,
            "period_aligned_unit_cost": None,
            "latest_differs_from_period": False,
        }
        for recipe_id in recipe_ids
    }
    for row in latest_rows:
        payload = sources.setdefault(int(row.receta_id), {})
        payload["latest_period"] = _period_label(row.periodo)
        payload["latest_unit_cost"] = _money(row.costo_fabricacion_unit)
    for row in aligned_rows:
        payload = sources.setdefault(int(row.receta_id), {})
        payload["period_aligned_period"] = _period_label(row.periodo)
        payload["period_aligned_unit_cost"] = _money(row.costo_fabricacion_unit)
    for payload in sources.values():
        payload["latest_differs_from_period"] = bool(
            payload.get("latest_period")
            and payload.get("period_aligned_period")
            and payload["latest_period"] != payload["period_aligned_period"]
        )
    return sources


def _top_cogs_offenders(FactVentaDiaria, branch_id: int | None, period: object) -> list[dict[str, object]]:
    from django.db.models import Sum

    start = _period_start(period)
    end = _next_month_start(period)
    rows = (
        FactVentaDiaria.objects.filter(
            sucursal_id=branch_id,
            fecha__gte=start,
            fecha__lt=end,
        )
        .values("receta_id", "producto_clave", "producto_nombre", "categoria", "source_kind")
        .annotate(
            cantidad=Sum("cantidad"),
            ventas=Sum("venta_total"),
            cogs=Sum("costo_estimado"),
        )
    )
    materialized = list(rows)
    recipe_ids = {int(item["receta_id"]) for item in materialized if item.get("receta_id")}
    cost_sources = _cost_sources_for_recipes(recipe_ids, start)
    offenders: list[dict[str, object]] = []
    for item in materialized:
        item_ratio = _ratio(item["cogs"], item["ventas"])
        implied_unit_cost = _ratio(item["cogs"], item["cantidad"])
        receta_id = int(item["receta_id"]) if item.get("receta_id") else None
        offenders.append({
            "receta_id": receta_id,
            "producto_clave": item["producto_clave"],
            "producto_nombre": item["producto_nombre"],
            "categoria": item["categoria"],
            "source_kind": item["source_kind"],
            "cantidad": _money(item["cantidad"]),
            "ventas": _money(item["ventas"]),
            "costo": _money(item["cogs"]),
            "ratio_cogs_ventas": str(item_ratio) if item_ratio is not None else None,
            "costo_unitario_usado_implicito": _money(implied_unit_cost),
            "cost_source": cost_sources.get(receta_id, {}) if receta_id else {},
        })
    offenders.sort(key=lambda item: (_as_decimal(item["costo"]), _as_decimal(item["ratio_cogs_ventas"])), reverse=True)
    return offenders[:INVESTMENT_COGS_OFFENDER_LIMIT]


def check_investment_cogs_sanity() -> CheckResult:
    started = time.monotonic()
    try:
        _setup_django()
        from django.db.models import Sum
        from django.db.models.functions import TruncMonth
        from reportes.models import FactVentaDiaria
    except Exception as exc:  # noqa: BLE001
        return CheckResult(
            name="Investment COGS sanity",
            severity="FAIL",
            status="FAIL",
            command="FactVentaDiaria ORM SELECT",
            duration_ms=int((time.monotonic() - started) * 1000),
            summary="Error al inicializar Django para revisar COGS.",
            details=[str(exc)],
        )

    details: list[dict[str, object]] = []
    rows = (
        FactVentaDiaria.objects.annotate(periodo=TruncMonth("fecha"))
        .values("sucursal_id", "sucursal__codigo", "sucursal__nombre", "periodo")
        .annotate(ventas=Sum("venta_total"), cogs=Sum("costo_estimado"))
        .order_by("periodo", "sucursal__codigo")
    )
    for row in rows:
        ratio = _ratio(row["cogs"], row["ventas"])
        if ratio is None or ratio <= INVESTMENT_COGS_RATIO_THRESHOLD:
            continue
        period = _period_label(row["periodo"])
        interpretation = "costo no confiable; no tratar como utilidad negativa real"
        details.append({
            "label": f"{_branch_label(row)} {period}",
            "status": "WARN",
            "summary": f"COGS/ventas={ratio} supera {INVESTMENT_COGS_RATIO_THRESHOLD}",
            "sucursal": _branch_label(row),
            "periodo": period,
            "ventas": _money(row["ventas"]),
            "costo": _money(row["cogs"]),
            "ratio": str(ratio),
            "interpretation": interpretation,
            "top_offenders": _top_cogs_offenders(FactVentaDiaria, row["sucursal_id"], row["periodo"]),
        })

    status = "WARN" if details else "OK"
    summary = (
        f"{len(details)} sucursal/mes con COGS/ventas > {INVESTMENT_COGS_RATIO_THRESHOLD}; "
        "reportar como costo no confiable, no utilidad negativa real."
        if details
        else "Sin ratios COGS/ventas absurdos en FactVentaDiaria."
    )
    return CheckResult(
        name="Investment COGS sanity",
        severity=status,
        status=status,
        command="FactVentaDiaria GROUP BY sucursal, mes",
        exit_code=0,
        duration_ms=int((time.monotonic() - started) * 1000),
        summary=summary,
        details=details,
    )


def check_investment_project_consistency() -> CheckResult:
    started = time.monotonic()
    try:
        _setup_django()
        from django.db.models import Count, Sum
        from reportes.models import ProyectoInversion, ProyectoInversionGasto
    except Exception as exc:  # noqa: BLE001
        return CheckResult(
            name="Investment project consistency",
            severity="FAIL",
            status="FAIL",
            command="ProyectoInversion ORM SELECT",
            duration_ms=int((time.monotonic() - started) * 1000),
            summary="Error al inicializar Django para revisar proyectos de inversion.",
            details=[str(exc)],
        )

    details: list[dict[str, object]] = []
    global_status = "OK"
    projects = ProyectoInversion.objects.all().order_by("id").annotate(detail_total=Sum("gastos_inversion__monto_total"))
    for project in projects:
        real = _as_decimal(project.monto_inversion_real)
        detail_total = _as_decimal(project.detail_total)
        if abs(real - detail_total) > Decimal("0.01"):
            global_status = "FAIL"
            details.append({
                "label": f"{project.id} - {project.nombre_proyecto}",
                "status": "FAIL",
                "summary": "monto_inversion_real no cuadra contra detalle CAPEX",
                "project_id": project.id,
                "monto_inversion_real": _money(real),
                "detalle_gastos_total": _money(detail_total),
                "diferencia": _money(real - detail_total),
            })

        reconciled_count = ProyectoInversionGasto.objects.filter(
            proyecto=project,
        ).exclude(referencia_contable="").count()
        placeholder_rows = ProyectoInversionGasto.objects.filter(
            proyecto=project,
            categoria=ProyectoInversionGasto.CATEGORIA_OTROS,
            descripcion__icontains=GUAMUCHIL_OLD_PLACEHOLDER_DESCRIPTION,
            monto_total__gt=0,
        )
        if reconciled_count and placeholder_rows.exists():
            if global_status == "OK":
                global_status = "WARN"
            details.append({
                "label": f"{project.id} - {project.nombre_proyecto}",
                "status": "WARN",
                "summary": "placeholder OTROS activo junto con partidas reconciliadas",
                "project_id": project.id,
                "placeholder_count": placeholder_rows.count(),
                "placeholder_total": _money(placeholder_rows.aggregate(total=Sum("monto_total"))["total"]),
                "reconciled_count": reconciled_count,
            })

    duplicates = (
        ProyectoInversionGasto.objects.exclude(referencia_contable="")
        .values("proyecto_id", "proyecto__nombre_proyecto", "referencia_contable")
        .annotate(count=Count("id"), total=Sum("monto_total"))
        .filter(count__gt=1)
        .order_by("proyecto_id", "referencia_contable")
    )
    for duplicate in duplicates:
        if global_status == "OK":
            global_status = "WARN"
        details.append({
            "label": f"{duplicate['proyecto_id']} - {duplicate['referencia_contable']}",
            "status": "WARN",
            "summary": "referencia_contable duplicada dentro del proyecto",
            "project_id": duplicate["proyecto_id"],
            "project_name": duplicate["proyecto__nombre_proyecto"],
            "referencia_contable": duplicate["referencia_contable"],
            "count": duplicate["count"],
            "total": _money(duplicate["total"]),
        })

    try:
        guamuchil = ProyectoInversion.objects.get(pk=GUAMUCHIL_PROJECT_ID)
    except ProyectoInversion.DoesNotExist:
        if global_status == "OK":
            global_status = "WARN"
        details.append({
            "label": GUAMUCHIL_PROJECT_NAME,
            "status": "WARN",
            "summary": "proyecto especifico id=1 no encontrado",
            "project_id": GUAMUCHIL_PROJECT_ID,
        })
    else:
        if abs(_as_decimal(guamuchil.monto_inversion_real) - GUAMUCHIL_EXPECTED_INVESTMENT) > Decimal("0.01"):
            global_status = "FAIL"
            details.append({
                "label": f"{guamuchil.id} - {guamuchil.nombre_proyecto}",
                "status": "FAIL",
                "summary": "inversion real esperada de Guamuchil no coincide",
                "expected": _money(GUAMUCHIL_EXPECTED_INVESTMENT),
                "actual": _money(guamuchil.monto_inversion_real),
            })
        recon_count = ProyectoInversionGasto.objects.filter(
            proyecto=guamuchil,
            referencia_contable__startswith=GUAMUCHIL_RECON_PREFIX,
        ).count()
        if recon_count != GUAMUCHIL_EXPECTED_RECON_ROWS:
            global_status = "FAIL"
            details.append({
                "label": f"{guamuchil.id} - {guamuchil.nombre_proyecto}",
                "status": "FAIL",
                "summary": "conteo de partidas reconciliadas GML no coincide",
                "expected": GUAMUCHIL_EXPECTED_RECON_ROWS,
                "actual": recon_count,
                "referencia_prefix": GUAMUCHIL_RECON_PREFIX,
            })
        old_placeholder_total = ProyectoInversionGasto.objects.filter(
            proyecto=guamuchil,
            categoria=ProyectoInversionGasto.CATEGORIA_OTROS,
            descripcion__icontains=GUAMUCHIL_OLD_PLACEHOLDER_DESCRIPTION,
        ).aggregate(total=Sum("monto_total"))["total"]
        if _as_decimal(old_placeholder_total) != 0:
            global_status = "FAIL"
            details.append({
                "label": f"{guamuchil.id} - {guamuchil.nombre_proyecto}",
                "status": "FAIL",
                "summary": "placeholder viejo OTROS debe estar en 0",
                "expected": "0.00",
                "actual": _money(old_placeholder_total),
                "placeholder_description": GUAMUCHIL_OLD_PLACEHOLDER_DESCRIPTION,
                "placeholder_reference_amount": _money(GUAMUCHIL_OLD_PLACEHOLDER_AMOUNT),
            })

    summary = "Proyectos de inversion cuadran contra detalle CAPEX."
    if details:
        fail_count = sum(1 for item in details if item["status"] == "FAIL")
        warn_count = sum(1 for item in details if item["status"] == "WARN")
        summary = f"{fail_count} FAIL, {warn_count} WARN en consistencia de proyectos de inversion."
    return CheckResult(
        name="Investment project consistency",
        severity=global_status,
        status=global_status,
        command="ProyectoInversion/ProyectoInversionGasto ORM SELECT",
        exit_code=0,
        duration_ms=int((time.monotonic() - started) * 1000),
        summary=summary,
        details=details,
    )


def check_investment_snapshot_readiness() -> CheckResult:
    started = time.monotonic()
    try:
        _setup_django()
        from django.db.models import Max, Sum
        from django.db.models.functions import TruncMonth
        from reportes.models import FactVentaDiaria, ProyectoInversion
    except Exception as exc:  # noqa: BLE001
        return CheckResult(
            name="Investment snapshot readiness",
            severity="FAIL",
            status="FAIL",
            command="ProyectoInversion snapshot ORM SELECT",
            duration_ms=int((time.monotonic() - started) * 1000),
            summary="Error al inicializar Django para revisar snapshots.",
            details=[str(exc)],
        )

    details: list[dict[str, object]] = []
    active_statuses = [ProyectoInversion.ESTATUS_ACTIVO, ProyectoInversion.ESTATUS_EN_RECUPERACION]
    projects = ProyectoInversion.objects.filter(estatus__in=active_statuses).select_related("sucursal_relacionada")
    for project in projects:
        snapshots = project.snapshots_mensuales.order_by("-periodo")
        latest_snapshot = snapshots.first()
        if project.fecha_apertura and latest_snapshot is None:
            details.append({
                "label": f"{project.id} - {project.nombre_proyecto}",
                "status": "WARN",
                "summary": "tiene fecha_apertura pero no tiene snapshots mensuales",
                "project_id": project.id,
                "fecha_apertura": project.fecha_apertura.isoformat(),
            })

        if not project.sucursal_relacionada_id:
            continue

        last_sale_month = (
            FactVentaDiaria.objects.filter(sucursal_id=project.sucursal_relacionada_id)
            .annotate(periodo=TruncMonth("fecha"))
            .aggregate(last_period=Max("periodo"))["last_period"]
        )
        if latest_snapshot and last_sale_month and _period_start(latest_snapshot.periodo) < _period_start(last_sale_month):
            details.append({
                "label": f"{project.id} - {project.nombre_proyecto}",
                "status": "WARN",
                "summary": "latest snapshot es anterior al ultimo mes con ventas",
                "project_id": project.id,
                "latest_snapshot": _period_label(latest_snapshot.periodo),
                "last_sales_month": _period_label(last_sale_month),
            })

        sales_rows = (
            FactVentaDiaria.objects.filter(sucursal_id=project.sucursal_relacionada_id)
            .annotate(periodo=TruncMonth("fecha"))
            .values("periodo")
            .annotate(ventas=Sum("venta_total"), cogs=Sum("costo_estimado"))
            .order_by("periodo")
        )
        for row in sales_rows:
            ratio = _ratio(row["cogs"], row["ventas"])
            if ratio is None or ratio <= INVESTMENT_COGS_RATIO_THRESHOLD:
                continue
            details.append({
                "label": f"{project.id} - {project.nombre_proyecto} {_period_label(row['periodo'])}",
                "status": "WARN",
                "summary": "costo_venta_mensual potencial supera 200% de ventas",
                "project_id": project.id,
                "periodo": _period_label(row["periodo"]),
                "ventas": _money(row["ventas"]),
                "costo": _money(row["cogs"]),
                "ratio": str(ratio),
                "interpretation": "costo no confiable; snapshot debe guardar costo nulo/guardrail",
            })

    status = "WARN" if details else "OK"
    summary = f"{len(details)} riesgos de snapshots de inversion detectados." if details else "Snapshots de inversion listos segun ventas disponibles."
    return CheckResult(
        name="Investment snapshot readiness",
        severity=status,
        status=status,
        command="ProyectoInversionSnapshotMensual ORM SELECT",
        exit_code=0,
        duration_ms=int((time.monotonic() - started) * 1000),
        summary=summary,
        details=details,
    )


def check_decimal_overflow_guard() -> CheckResult:
    started = time.monotonic()
    pct_fields = [
        "porcentaje_recuperado",
        "cash_on_cash",
        "roi_mensual",
        "roi_acumulado",
        "roi_anualizado",
        "tir",
    ]
    try:
        _setup_django()
        from reportes.models import ProyectoInversion, ProyectoInversionSnapshotMensual
    except Exception as exc:  # noqa: BLE001
        return CheckResult(
            name="Decimal overflow guard",
            severity="FAIL",
            status="FAIL",
            command="ProyectoInversionSnapshotMensual ORM SELECT",
            duration_ms=int((time.monotonic() - started) * 1000),
            summary="Error al inicializar Django para revisar overflow decimal.",
            details=[str(exc)],
        )

    details: list[dict[str, object]] = []
    for snapshot in ProyectoInversionSnapshotMensual.objects.select_related("proyecto").order_by("proyecto_id", "periodo"):
        for field_name in pct_fields:
            value = getattr(snapshot, field_name, None)
            if value is None:
                continue
            decimal_value = _as_decimal(value)
            if decimal_value < PERCENT_FIELD_MIN or decimal_value > PERCENT_FIELD_MAX:
                details.append({
                    "label": f"{snapshot.proyecto_id} {_period_label(snapshot.periodo)} {field_name}",
                    "status": "FAIL",
                    "summary": "valor decimal almacenado excede DecimalField(max_digits=8, decimal_places=4)",
                    "project_id": snapshot.proyecto_id,
                    "periodo": _period_label(snapshot.periodo),
                    "field": field_name,
                    "value": str(decimal_value),
                    "min": str(PERCENT_FIELD_MIN),
                    "max": str(PERCENT_FIELD_MAX),
                })

    for project in ProyectoInversion.objects.prefetch_related("snapshots_mensuales").order_by("id"):
        investment = _as_decimal(project.monto_inversion_real)
        running_free_cashflow = Decimal("0")
        for snapshot in project.snapshots_mensuales.order_by("periodo"):
            running_free_cashflow += _as_decimal(snapshot.flujo_libre)
            potential_values = {
                "porcentaje_recuperado": _percent(snapshot.recuperacion_acumulada, investment),
                "roi_mensual": _percent(snapshot.flujo_libre, investment),
                "roi_acumulado": _percent(running_free_cashflow, investment),
            }
            if investment == 0 and any(
                _as_decimal(getattr(snapshot, field_name, None)) != 0
                for field_name in ["flujo_libre", "recuperacion_acumulada", "monto_recuperacion_mes"]
            ):
                details.append({
                    "label": f"{project.id} - {project.nombre_proyecto} {_period_label(snapshot.periodo)}",
                    "status": "FAIL",
                    "summary": "snapshot tiene flujo/recuperacion pero inversion real es 0; porcentaje potencial dividiria entre cero",
                    "project_id": project.id,
                    "periodo": _period_label(snapshot.periodo),
                    "monto_inversion_real": _money(investment),
                })
                continue
            for field_name, value in potential_values.items():
                if value is None:
                    continue
                if value < PERCENT_FIELD_MIN or value > PERCENT_FIELD_MAX:
                    details.append({
                        "label": f"{project.id} - {project.nombre_proyecto} {_period_label(snapshot.periodo)} {field_name}",
                        "status": "FAIL",
                        "summary": "porcentaje potencial excede rango seguro antes de guardar snapshot",
                        "project_id": project.id,
                        "periodo": _period_label(snapshot.periodo),
                        "field": field_name,
                        "value": str(value),
                        "min": str(PERCENT_FIELD_MIN),
                        "max": str(PERCENT_FIELD_MAX),
                    })

    status = "FAIL" if details else "OK"
    summary = f"{len(details)} riesgos de overflow decimal detectados." if details else "Sin riesgo de overflow decimal en porcentajes de snapshots."
    return CheckResult(
        name="Decimal overflow guard",
        severity=status,
        status=status,
        command="ProyectoInversionSnapshotMensual percentage range SELECT",
        exit_code=0,
        duration_ms=int((time.monotonic() - started) * 1000),
        summary=summary,
        details=details,
    )


def _run_finance_check_safely(name: str, check_func) -> CheckResult:
    try:
        return check_func()
    except Exception as exc:  # noqa: BLE001
        try:
            from django.db.utils import OperationalError
        except Exception:  # noqa: BLE001
            OperationalError = ()  # type: ignore[assignment]
        if isinstance(exc, OperationalError):
            return skipped(
                name,
                f"DB configurada pero no usable para auditoria financiera read-only: {exc}",
                "Django ORM SELECT agregados",
            )
        return CheckResult(
            name=name,
            severity="FAIL",
            status="FAIL",
            command="Django ORM SELECT agregados",
            summary="Error ejecutando auditoria financiera read-only.",
            details=[str(exc)],
        )


def check_investment_finance_sanity() -> list[CheckResult]:
    db_error = _finance_db_ready("Investment finance sanity")
    if db_error:
        return [db_error]
    return [
        _run_finance_check_safely("Investment COGS sanity", check_investment_cogs_sanity),
        _run_finance_check_safely("Investment project consistency", check_investment_project_consistency),
        _run_finance_check_safely("Investment snapshot readiness", check_investment_snapshot_readiness),
        _run_finance_check_safely("Decimal overflow guard", check_decimal_overflow_guard),
    ]


def production_readonly_finance_checks() -> list[CheckResult]:
    if not PRODUCTION_KEY.exists():
        return [skipped("Production investment finance sanity", f"No existe llave SSH esperada: {PRODUCTION_KEY}")]
    ssh_base = [
        "ssh",
        "-i",
        str(PRODUCTION_KEY),
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        PRODUCTION_HOST,
    ]
    remote = (
        f"cd {PRODUCTION_DIR} && {PRODUCTION_COMPOSE} exec -T web "
        "python scripts/erp_doctor.py --finance-only-json"
    )
    command = [*ssh_base, remote]
    command_text = shlex.join(command)
    started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            env=os.environ,
            text=True,
            capture_output=True,
            timeout=300,
        )
    except subprocess.TimeoutExpired as exc:
        return [
            CheckResult(
                name="Production investment finance sanity",
                severity="FAIL",
                status="FAIL",
                command=command_text,
                duration_ms=int((time.monotonic() - started) * 1000),
                summary="Timeout despues de 300s.",
                details=trim_output((exc.stdout or "") + "\n" + (exc.stderr or "")),
            )
        ]
    raw_output = completed.stdout.strip()
    try:
        payload = json.loads(raw_output)
        return [CheckResult(**item) for item in payload]
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        output = "\n".join(part for part in [completed.stdout, completed.stderr] if part)
        return [
            CheckResult(
                name="Production investment finance sanity",
                severity="FAIL",
                status="FAIL",
                command=command_text,
                exit_code=completed.returncode,
                duration_ms=int((time.monotonic() - started) * 1000),
                summary="No fue posible interpretar JSON de auditoria financiera en produccion.",
                details=[str(exc), *trim_output(output)],
            )
        ]


def check_page_load_performance() -> CheckResult:
    started = time.monotonic()
    db_error = _finance_db_ready("Page load performance")
    if db_error:
        return db_error
    try:
        _setup_django()
        from django.contrib.auth import get_user_model
    except Exception as exc:  # noqa: BLE001
        return CheckResult(
            name="Page load performance",
            severity="FAIL",
            status="FAIL",
            command="Django test client GET critical module routes",
            duration_ms=int((time.monotonic() - started) * 1000),
            summary="Error al inicializar Django para auditoria de carga de paginas.",
            details=[str(exc)],
        )

    User = get_user_model()
    try:
        user = User.objects.filter(is_active=True, is_superuser=True).order_by("id").first()
    except Exception as exc:  # noqa: BLE001
        try:
            from django.db.utils import OperationalError
        except Exception:  # noqa: BLE001
            OperationalError = ()  # type: ignore[assignment]
        if isinstance(exc, OperationalError):
            return skipped(
                "Page load performance",
                f"DB configurada pero no usable para smoke de paginas: {exc}",
                "Django test client GET critical module routes",
            )
        raise
    if user is None:
        return skipped(
            "Page load performance",
            "No existe superusuario activo para smoke autenticado de modulos.",
            "Django test client force_login",
        )

    details: list[dict[str, object]] = []
    fail_count = 0
    warn_count = 0
    route_script = """
import json
import os
import time

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
import django
django.setup()

from django.contrib.auth import get_user_model
from django.test import Client

path = os.environ["ERP_DOCTOR_ROUTE_PATH"]
user_id = os.environ["ERP_DOCTOR_USER_ID"]
user = get_user_model().objects.get(pk=user_id)
client = Client(HTTP_HOST="erp.pollyanasdolce.com")
client.force_login(user)
client.get("/health/", secure=True)
started = time.monotonic()
response = client.get(path, follow=True, secure=True)
duration_ms = int((time.monotonic() - started) * 1000)
print(json.dumps({
    "status_code": int(response.status_code),
    "duration_ms": duration_ms,
    "content_bytes": len(getattr(response, "content", b"") or b""),
    "redirect_chain": [
        {"url": url, "status_code": code}
        for url, code in getattr(response, "redirect_chain", [])
    ],
}))
"""

    for label, path in PAGE_LOAD_ROUTES:
        route_started = time.monotonic()
        route_env = {
            **os.environ,
            "ERP_DOCTOR_ROUTE_PATH": path,
            "ERP_DOCTOR_USER_ID": str(user.pk),
        }
        try:
            completed = subprocess.run(
                [python_bin(), "-c", route_script],
                cwd=ROOT,
                env=route_env,
                text=True,
                capture_output=True,
                timeout=15,
            )
        except subprocess.TimeoutExpired as exc:
            duration_ms = int((time.monotonic() - route_started) * 1000)
            fail_count += 1
            details.append({
                "label": label,
                "status": "FAIL",
                "summary": "timeout cargando pagina",
                "path": path,
                "duration_ms": duration_ms,
                "stdout": trim_output(exc.stdout or "", limit=1000),
                "stderr": trim_output(exc.stderr or "", limit=1000),
            })
            continue
        duration_ms = int((time.monotonic() - route_started) * 1000)
        if completed.returncode != 0:
            fail_count += 1
            details.append({
                "label": label,
                "status": "FAIL",
                "summary": f"subproceso fallo con exit code {completed.returncode}",
                "path": path,
                "duration_ms": duration_ms,
                "stdout": trim_output(completed.stdout, limit=1000),
                "stderr": trim_output(completed.stderr, limit=2000),
            })
            continue
        try:
            payload = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            fail_count += 1
            details.append({
                "label": label,
                "status": "FAIL",
                "summary": f"respuesta JSON invalida del smoke de pagina: {exc}",
                "path": path,
                "duration_ms": duration_ms,
                "stdout": trim_output(completed.stdout, limit=2000),
                "stderr": trim_output(completed.stderr, limit=2000),
            })
            continue

        route_duration_ms = int(payload["duration_ms"])
        status_code = int(payload["status_code"])
        if status_code >= 500 or route_duration_ms >= PAGE_LOAD_FAIL_MS:
            route_status = "FAIL"
            fail_count += 1
        elif status_code >= 400 or route_duration_ms >= PAGE_LOAD_WARN_MS:
            route_status = "WARN"
            warn_count += 1
        else:
            route_status = "OK"
        if status_code >= 400:
            summary = f"HTTP {status_code} en {route_duration_ms}ms"
        elif route_duration_ms >= PAGE_LOAD_WARN_MS:
            summary = f"carga lenta: {route_duration_ms}ms"
        else:
            summary = f"{status_code} en {route_duration_ms}ms"
        details.append({
            "label": label,
            "status": route_status,
            "summary": summary,
            "path": path,
            "status_code": status_code,
            "duration_ms": route_duration_ms,
            "content_bytes": payload.get("content_bytes"),
            "redirect_chain": payload.get("redirect_chain") or [],
        })

    if fail_count:
        status = "FAIL"
    elif warn_count:
        status = "WARN"
    else:
        status = "OK"
    summary = (
        f"{len(PAGE_LOAD_ROUTES) - fail_count - warn_count} OK, {warn_count} WARN, "
        f"{fail_count} FAIL en rutas criticas; umbrales WARN>{PAGE_LOAD_WARN_MS}ms FAIL>{PAGE_LOAD_FAIL_MS}ms."
    )
    return CheckResult(
        name="Page load performance",
        severity=status,
        status=status,
        command="Django test client GET critical module routes",
        exit_code=0,
        duration_ms=int((time.monotonic() - started) * 1000),
        summary=summary,
        details=details,
    )


def production_readonly_page_performance_checks() -> list[CheckResult]:
    if not PRODUCTION_KEY.exists():
        return [skipped("Production page load performance", f"No existe llave SSH esperada: {PRODUCTION_KEY}")]
    ssh_base = [
        "ssh",
        "-i",
        str(PRODUCTION_KEY),
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        PRODUCTION_HOST,
    ]
    remote = (
        f"cd {PRODUCTION_DIR} && {PRODUCTION_COMPOSE} exec -T web "
        "python scripts/erp_doctor.py --page-performance-only-json"
    )
    command = [*ssh_base, remote]
    command_text = shlex.join(command)
    started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            env=os.environ,
            text=True,
            capture_output=True,
            timeout=300,
        )
    except subprocess.TimeoutExpired as exc:
        return [
            CheckResult(
                name="Production page load performance",
                severity="FAIL",
                status="FAIL",
                command=command_text,
                duration_ms=int((time.monotonic() - started) * 1000),
                summary="Timeout despues de 300s.",
                details=trim_output((exc.stdout or "") + "\n" + (exc.stderr or "")),
            )
        ]
    raw_output = completed.stdout.strip()
    try:
        payload = json.loads(raw_output)
        return [CheckResult(**item) for item in payload]
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        output = "\n".join(part for part in [completed.stdout, completed.stderr] if part)
        return [
            CheckResult(
                name="Production page load performance",
                severity="FAIL",
                status="FAIL",
                command=command_text,
                exit_code=completed.returncode,
                duration_ms=int((time.monotonic() - started) * 1000),
                summary="No fue posible interpretar JSON de auditoria de paginas en produccion.",
                details=[str(exc), *trim_output(output)],
            )
        ]


def check_docker() -> list[CheckResult]:
    if not (ROOT / "docker-compose.yml").exists():
        return [skipped("Docker compose", "No existe docker-compose.yml.")]
    if shutil.which("docker") is None:
        return [skipped("Docker compose", "Docker no esta instalado o no esta en PATH.", "docker compose config -q")]
    return [run_command("Docker compose config", ["docker", "compose", "-f", "docker-compose.yml", "config", "-q"], timeout=120)]


def check_tests() -> list[CheckResult]:
    runner = ROOT / "scripts" / "run_tests_local.sh"
    if not runner.exists():
        return [skipped("Smoke tests", "No existe scripts/run_tests_local.sh.")]
    labels = [
        "api.tests_ai_gateway.AIGatewayApiTests.test_manifest_exposes_safe_gateway_contract",
        "pos_bridge.tests.test_celery_schedule_setup",
        "orquestacion.tests_quality_loop",
    ]
    return [
        run_command(
            "Smoke tests criticos",
            ["./scripts/run_tests_local.sh", *labels],
            timeout=600,
        )
    ]


def check_browser_route() -> CheckResult:
    return skipped(
        "Browser smoke",
        "Preparado para validacion real con Chrome DevTools/MCP o scripts/ui_check_safe.sh en flujos UI especificos.",
        "UI_CHECK_USERNAME=... UI_CHECK_PASSWORD=... ./scripts/ui_check_safe.sh --route /ruta --expect-text Texto",
    )


def production_readonly_checks() -> list[CheckResult]:
    if not PRODUCTION_KEY.exists():
        return [skipped("Production readonly", f"No existe llave SSH esperada: {PRODUCTION_KEY}")]
    ssh_base = [
        "ssh",
        "-i",
        str(PRODUCTION_KEY),
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        PRODUCTION_HOST,
    ]
    remote_checks = [
        (
            "Production services",
            f"cd {PRODUCTION_DIR} && {PRODUCTION_COMPOSE} ps",
        ),
        (
            "Production Django check",
            f"cd {PRODUCTION_DIR} && {PRODUCTION_COMPOSE} exec -T web python manage.py check",
        ),
        (
            "Production migrations check",
            f"cd {PRODUCTION_DIR} && {PRODUCTION_COMPOSE} exec -T web python manage.py migrate --check",
        ),
        (
            "Production Celery registry",
            "cd {dir} && {compose} exec -T web python - <<'PY'\n"
            "import os\n"
            "os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')\n"
            "from config.celery import app\n"
            "app.loader.import_default_modules()\n"
            "tasks=sorted(name for name in app.tasks if not name.startswith('celery.'))\n"
            "print('registered_tasks=' + str(len(tasks)))\n"
            "PY".format(dir=PRODUCTION_DIR, compose=PRODUCTION_COMPOSE),
        ),
    ]
    checks = [
        run_command(name, [*ssh_base, remote], timeout=240)
        for name, remote in remote_checks
    ]
    checks.extend(production_readonly_finance_checks())
    checks.extend(production_readonly_page_performance_checks())
    return checks


def build_report(args: argparse.Namespace | SimpleNamespace) -> dict:
    checks: list[CheckResult] = []
    if args.production_readonly:
        checks.extend(production_readonly_checks())
    else:
        checks.append(check_environment_sanity())
        checks.extend(check_django())
        checks.extend(check_quality_guards())
        checks.extend(check_python_static(full=args.full))
        checks.extend(check_templates(full=args.full))
        checks.extend(check_js())
        checks.extend(check_celery())
        checks.extend(check_docker())
        checks.extend(check_investment_finance_sanity())
        checks.append(check_page_load_performance())
        if args.full or args.fix:
            beat_check = check_celery_beat_schedules(fix=args.fix)
            if args.fix:
                final_beat_check = check_celery_beat_schedules(fix=False)
                final_beat_check.fixed = beat_check.fixed
                final_beat_check.fix_action = beat_check.fix_action
                checks.append(final_beat_check)
            else:
                checks.append(beat_check)
        if args.full:
            checks.extend(check_tests())
        checks.append(check_browser_route())

    if any(check.status == "FAIL" for check in checks):
        status = "FAIL"
        ok = False
    elif any(check.status == "WARN" for check in checks):
        status = "WARN"
        ok = False
    else:
        status = "OK"
        ok = True

    return {
        "ok": ok,
        "status": status,
        "checks": [asdict(check) for check in checks],
    }


def report_findings(report: dict) -> tuple[list[str], list[str]]:
    findings = [
        f"{check['name']}: {check['status']} - {check['summary']}"
        for check in report["checks"]
        if check["status"] in {"WARN", "FAIL"}
    ]
    clear = [
        check["name"]
        for check in report["checks"]
        if check["status"] in {"OK", "SKIPPED"}
    ]
    return findings, clear


def build_email_body(report: dict) -> str:
    findings, clear = report_findings(report)
    lines = [
        f"Estado global: {report['status']}",
        "",
        "Checks con hallazgos:",
    ]
    if findings:
        lines.extend(f"- {item}" for item in findings)
    else:
        lines.append("- Ninguno")
    lines.extend([
        "",
        "Checks sin hallazgos (OK/SKIPPED): " + (", ".join(clear) if clear else "Ninguno"),
        "",
        "-- ERP Pollyana's Dolce",
    ])
    return "\n".join(lines)


def send_email_report(report: dict) -> bool:
    if report["status"] == "OK":
        return False
    _setup_django()
    from django.conf import settings
    from django.core.mail import send_mail

    today = datetime.now().date().isoformat()
    recipient = os.getenv("ERP_DOCTOR_EMAIL", "maburgos12@pollyanasdolce.com")
    send_mail(
        subject=f"ERP Doctor - {report['status']} {today}",
        message=build_email_body(report),
        from_email=getattr(settings, "DEFAULT_FROM_EMAIL", "erp@pollyanasdolce.com"),
        recipient_list=[recipient],
        fail_silently=False,
    )
    return True


def run_doctor(
    *,
    quick: bool = True,
    full: bool = False,
    fix: bool = False,
    production_readonly: bool = False,
    email: bool = False,
) -> dict:
    args = SimpleNamespace(
        quick=quick,
        full=full,
        fix=fix,
        production_readonly=production_readonly,
    )
    report = build_report(args)
    email_sent = False
    if email:
        email_sent = send_email_report(report)
    report["email_sent"] = email_sent
    return report


def print_human(report: dict) -> None:
    print("ERP Doctor")
    print(f"Status: {report['status']}")
    print()
    for check in report["checks"]:
        print(f"{check['name']}: {check['status']} - {check['summary']}")
        if check.get("command"):
            print(f"  command: {check['command']}")
        if check.get("exit_code") is not None:
            print(f"  exit_code: {check['exit_code']} duration_ms: {check['duration_ms']}")
        details = check.get("details") or []
        if details:
            preview = details[:12]
            for line in preview:
                if isinstance(line, dict):
                    label = line.get("label") or line.get("name") or "detail"
                    status = line.get("status", "")
                    summary = line.get("summary", "")
                    schedule = line.get("cron") or line.get("interval")
                    schedule_text = f" ({schedule})" if schedule else ""
                    print(f"  {label}: {status} - {summary}{schedule_text}")
                else:
                    print(f"  {line}")
            if len(details) > len(preview):
                print(f"  ... {len(details) - len(preview)} more lines")
        print()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auditoria local/readonly del ERP de Pollyana's Dolce.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--quick", action="store_true", help="Checks rapidos para pre-commit/pre-deploy local.")
    mode.add_argument("--full", action="store_true", help="Auditoria mas completa, incluyendo herramientas opcionales y smoke tests.")
    parser.add_argument("--json", action="store_true", help="Salida estructurada JSON.")
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Corrige solo hallazgos seguros y reversibles, como reactivar PeriodicTask criticas deshabilitadas.",
    )
    parser.add_argument(
        "--email",
        action="store_true",
        help="Envia reporte por email si el estado global es WARN o FAIL.",
    )
    parser.add_argument(
        "--production-readonly",
        action="store_true",
        help="Ejecuta diagnosticos seguros de solo lectura contra el VPS de produccion.",
    )
    parser.add_argument("--finance-only-json", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--page-performance-only-json", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.finance_only_json or args.page_performance_only_json:
        return args
    if not args.quick and not args.full:
        args.quick = True
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    if args.finance_only_json:
        checks = check_investment_finance_sanity()
        print(json.dumps([asdict(check) for check in checks], ensure_ascii=False, indent=2))
        return 0
    if args.page_performance_only_json:
        print(json.dumps([asdict(check_page_load_performance())], ensure_ascii=False, indent=2))
        return 0
    report = run_doctor(
        quick=args.quick,
        full=args.full,
        fix=args.fix,
        production_readonly=args.production_readonly,
        email=args.email,
    )
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print_human(report)
    return 0 if report["status"] == "OK" else 1


if __name__ == "__main__":
    raise SystemExit(main())
