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
from pathlib import Path

APP_ENV_WAS_INJECTED = not bool(os.environ.get("APP_ENV"))
if APP_ENV_WAS_INJECTED:
    os.environ["APP_ENV"] = "local"

ROOT = Path(__file__).resolve().parents[1]
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
        "expected_cron": {"minute": "0", "hour": "2", "day_of_month": "1"},
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


def check_celery_beat_schedules() -> CheckResult:
    py = python_bin()
    started = time.monotonic()
    payload = json.dumps(CRITICAL_BEAT_TASKS, ensure_ascii=False)
    script = f"""
import json
import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
import django
django.setup()
from django.db.models import Q
from django_celery_beat.models import PeriodicTask

critical = json.loads({payload!r})
details = []
global_status = "OK"

def cron_dict(task):
    if not task.crontab_id:
        return None
    cron = task.crontab
    return {{
        "minute": cron.minute,
        "hour": cron.hour,
        "day_of_week": cron.day_of_week,
        "day_of_month": cron.day_of_month,
        "month_of_year": cron.month_of_year,
        "timezone": str(cron.timezone),
    }}

def interval_dict(task):
    if not task.interval_id:
        return None
    return {{"every": task.interval.every, "period": task.interval.period}}

for item in critical:
    names = item["names"]
    expected_cron = item.get("expected_cron") or {{}}
    qs = PeriodicTask.objects.select_related("crontab", "interval").filter(
        Q(name__in=names) | Q(task=item["task"])
    )
    matches = list(qs)
    selected = next((task for name in names for task in matches if task.name == name), None)
    if selected is None and matches:
        selected = matches[0]
    if selected is None:
        details.append({{
            "label": item["label"],
            "status": "WARN",
            "summary": "no encontrada en Beat",
            "expected_names": names,
            "task": item["task"],
        }})
        global_status = "WARN"
        continue

    cron = cron_dict(selected)
    interval = interval_dict(selected)
    mismatches = []
    if cron is None and expected_cron:
        mismatches.append("sin crontab")
    elif cron:
        for key, expected in expected_cron.items():
            if str(cron.get(key)) != str(expected):
                mismatches.append(f"{{key}}={{cron.get(key)}} esperado={{expected}}")

    if not selected.enabled:
        status = "WARN"
        summary = "enabled=False - revisar"
    elif mismatches:
        status = "WARN"
        summary = "frecuencia distinta: " + ", ".join(mismatches)
    else:
        status = "OK"
        summary = "enabled"

    if status == "WARN":
        global_status = "WARN"

    details.append({{
        "label": item["label"],
        "status": status,
        "summary": summary,
        "name": selected.name,
        "task": selected.task,
        "enabled": selected.enabled,
        "cron": cron,
        "interval": interval,
        "expected_names": names,
        "expected_cron": expected_cron,
    }})

print(json.dumps({{"status": global_status, "details": details}}, ensure_ascii=False))
"""
    command = [py, "-c", script]
    command_text = shlex.join(command)
    try:
        completed = subprocess.run(
            command,
            cwd=ROOT,
            env=os.environ.copy(),
            text=True,
            capture_output=True,
            timeout=120,
        )
    except subprocess.TimeoutExpired as exc:
        return CheckResult(
            name="Celery Beat - schedules criticos",
            severity="FAIL",
            status="FAIL",
            command=command_text,
            duration_ms=int((time.monotonic() - started) * 1000),
            summary="Timeout consultando django_celery_beat_periodictask.",
            details=trim_output((exc.stdout or "") + "\n" + (exc.stderr or "")),
        )

    duration_ms = int((time.monotonic() - started) * 1000)
    if completed.returncode != 0:
        return CheckResult(
            name="Celery Beat - schedules criticos",
            severity="FAIL",
            status="FAIL",
            command=command_text,
            exit_code=completed.returncode,
            duration_ms=duration_ms,
            summary="Error al consultar django_celery_beat_periodictask.",
            details=trim_output(completed.stdout + "\n" + completed.stderr),
        )

    try:
        payload_result = json.loads(completed.stdout.strip())
    except json.JSONDecodeError as exc:
        return CheckResult(
            name="Celery Beat - schedules criticos",
            severity="FAIL",
            status="FAIL",
            command=command_text,
            exit_code=completed.returncode,
            duration_ms=duration_ms,
            summary="No fue posible interpretar la consulta de Beat como JSON.",
            details=[str(exc), *trim_output(completed.stdout + "\n" + completed.stderr)],
        )

    status = payload_result["status"]
    details = payload_result["details"]
    warn_count = sum(1 for item in details if item["status"] == "WARN")
    ok_count = sum(1 for item in details if item["status"] == "OK")
    return CheckResult(
        name="Celery Beat - schedules criticos",
        severity=status,
        status=status,
        command=command_text,
        exit_code=completed.returncode,
        duration_ms=duration_ms,
        summary=f"{ok_count} OK, {warn_count} WARN sobre {len(details)} schedules criticos.",
        details=details,
    )


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
    return [
        run_command(name, [*ssh_base, remote], timeout=240)
        for name, remote in remote_checks
    ]


def build_report(args: argparse.Namespace) -> dict:
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
        if args.full:
            checks.append(check_celery_beat_schedules())
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
        "--production-readonly",
        action="store_true",
        help="Ejecuta diagnosticos seguros de solo lectura contra el VPS de produccion.",
    )
    args = parser.parse_args(argv)
    if not args.quick and not args.full:
        args.quick = True
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    report = build_report(args)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print_human(report)
    return 0 if report["status"] == "OK" else 1


if __name__ == "__main__":
    raise SystemExit(main())
