"""Linux integration proof. Run only with isolated PostgreSQL/Redis and APP_ENV=development.

Uses real prefork workers, hard kill, broker delivery and PostgreSQL rollback.
Only the Point HTTP source is replaced; time limits/recovery age are accelerated.
"""

import os
import signal
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

if (
    os.environ.get("APP_ENV") != "development"
    or os.environ.get("CATALOG_WORKER_PROOF") != "1"
):
    raise SystemExit(
        "Requires APP_ENV=development CATALOG_WORKER_PROOF=1 and isolated DB/Redis"
    )
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
import django

django.setup()

from django.core.management import call_command
from django.db import connections
from django.utils import timezone
from config.celery import app
from maestros.models import CostoInsumo, Insumo, UnidadMedida
from pos_bridge.models import PointSyncJob
from pos_bridge.services.catalog_recipe_execution import execution_lock
from pos_bridge.tasks import task_catalog_recipe_sync
from recetas.models import Receta, LineaReceta

MODE = Path("/tmp/recipe-worker-proof-mode")
ENTERED = Path("/tmp/recipe-worker-proof-entered")
OTHER = Path("/tmp/recipe-worker-proof-other")
workers = []
logs = []


def wait_for(predicate, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    raise AssertionError("Condition timed out; inspect /tmp/recipe-proof-*.log")


def start_worker(queue, pool):
    log = open(f"/tmp/recipe-proof-{queue}.log", "a")
    logs.append(log)
    worker = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "celery",
            "-A",
            "config",
            "worker",
            "-l",
            "info",
            "--pool",
            pool,
            "--concurrency=1",
            "--prefetch-multiplier=1",
            "-Q",
            queue,
            "--hostname",
            f"proof-{queue}@%h",
            "-I",
            "pos_bridge.tests.catalog_worker_probe",
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    workers.append(worker)
    return worker


def stop(worker):
    if worker.poll() is None:
        os.killpg(worker.pid, signal.SIGKILL)
        worker.wait(timeout=10)


try:
    assert connections["default"].vendor == "postgresql"
    unit, _ = UnidadMedida.objects.get_or_create(
        codigo="pza", defaults={"nombre": "Pieza", "tipo": UnidadMedida.TIPO_PIEZA}
    )
    ingredient, _ = Insumo.objects.get_or_create(
        codigo_point="PROBEINPUT",
        defaults={"nombre": "Worker probe input", "unidad_base": unit},
    )
    CostoInsumo.objects.get_or_create(
        insumo=ingredient, fecha=timezone.localdate(), defaults={"costo_unitario": 10}
    )
    recipe, _ = Receta.objects.get_or_create(
        codigo_point="WORKERPROBE", defaults={"nombre": "Worker probe"}
    )
    MODE.write_text("ok")
    general = start_worker("celery", "solo")
    worker = start_worker("recipes", "prefork")
    for mode in ["soft", "hard", "crash", "restart"]:
        recipe.lineas.all().delete()
        previous = LineaReceta.objects.create(
            receta=recipe, insumo=ingredient, cantidad=1, unidad=unit
        )
        ENTERED.unlink(missing_ok=True)
        OTHER.unlink(missing_ok=True)
        MODE.write_text(mode)
        job = PointSyncJob.objects.create(
            job_type="recipes",
            parameters={
                "action": "SYNC_ONLY_NEW_PRODUCTS",
                "resume_codes": ["WORKERPROBE"],
            },
        )
        task_catalog_recipe_sync.apply_async(
            kwargs={"job_id": job.id},
            soft_time_limit=3,
            time_limit=5,
            ignore_result=True,
        )
        wait_for(ENTERED.exists)
        app.send_task("recipe_probe.other_job", queue="celery", ignore_result=True)
        wait_for(OTHER.exists, timeout=10)
        if mode in {"hard", "restart"}:
            with execution_lock(job.id) as acquired:
                assert not acquired, (
                    "General queue must finish while recipe executor is still blocked"
                )
        if mode == "restart":
            stop(worker)
            MODE.write_text("ok")
            worker = start_worker("recipes", "prefork")
        else:
            # Let the real soft/hard limit or child crash complete.
            time.sleep(6)
        assert LineaReceta.objects.filter(pk=previous.pk).exists(), (
            f"{mode}: lost previous recipe"
        )
        MODE.write_text("ok")
        PointSyncJob.objects.filter(pk=job.pk).update(
            updated_at=timezone.now() - timedelta(minutes=6)
        )
        call_command("watch_catalog_recipes", once=True)
        wait_for(lambda: PointSyncJob.objects.get(pk=job.pk).status == "SUCCESS")
        job.refresh_from_db()
        assert job.attempt_count == 2, (mode, job.attempt_count)
        assert job.parameters["auto_recoveries"] == 1
        assert recipe.lineas.count() == 1
        assert recipe.lineas.get().cantidad == 2
        assert Receta.objects.filter(codigo_point="WORKERPROBE").count() == 1
        line_ids = list(recipe.lineas.values_list("id", flat=True))
        task_catalog_recipe_sync.apply_async(
            kwargs={"job_id": job.pk}, ignore_result=True
        )
        time.sleep(2)
        job.refresh_from_db()
        assert job.attempt_count == 2
        assert list(recipe.lineas.values_list("id", flat=True)) == line_ids
        print(
            f"PASS {mode}: rollback, automatic recovery, duplicate delivery, independent general queue",
            flush=True,
        )
    legacy = PointSyncJob.objects.create(
        job_type="recipes",
        parameters={
            "action": "SYNC_ONLY_NEW_PRODUCTS",
            "resume_codes": ["WORKERPROBE"],
        },
    )
    task_catalog_recipe_sync.apply_async(
        kwargs={"job_id": legacy.pk}, queue="celery", ignore_result=True
    )
    wait_for(lambda: PointSyncJob.objects.get(pk=legacy.pk).status == "SUCCESS")
    legacy.refresh_from_db()
    assert legacy.attempt_count == 1
    print("PASS legacy queue: forwarded to recipe worker and executed once", flush=True)
finally:
    for worker in workers:
        stop(worker)
    for log in logs:
        log.close()
    connections.close_all()
