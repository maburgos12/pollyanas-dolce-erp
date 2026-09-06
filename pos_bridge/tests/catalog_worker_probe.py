"""Loaded explicitly by scripts/verify_catalog_worker_recovery.py, never by production."""

import os
import signal
import time
from pathlib import Path

if (
    os.environ.get("CATALOG_WORKER_PROOF") != "1"
    or os.environ.get("APP_ENV") != "development"
):
    raise RuntimeError(
        "Recipe worker probe requires an isolated development environment"
    )

from celery import shared_task
from pos_bridge.services.product_recipe_sync_service import (
    PointProductRecipeSyncService,
)
from pos_bridge.tests.test_product_recipe_sync_service import FakePointHttpClient
from pos_bridge.tasks import celery_tasks

MODE = Path("/tmp/recipe-worker-proof-mode")
ENTERED = Path("/tmp/recipe-worker-proof-entered")
OTHER = Path("/tmp/recipe-worker-proof-other")
CODE = "WORKERPROBE"
PAYLOAD = {
    "products": [
        {
            "PK_Producto": 99001,
            "Codigo": CODE,
            "Nombre": "Worker probe",
            "hasReceta": True,
        }
    ],
    "details": {
        99001: {"PK_Producto": 99001, "Codigo": CODE, "Nombre": "Worker probe"}
    },
    "boms": {
        99001: [
            {
                "PK_Articulo": 99002,
                "Codigo_Articulo": "PROBEINPUT",
                "Articulo": "Worker probe input",
                "Cantidad": 2,
                "Unidad_corto": "PZA",
            }
        ]
    },
    "articulos": [],
    "articulo_details": {},
}


class ProbeService(PointProductRecipeSyncService):
    def __init__(self):
        super().__init__(http_client_factory=lambda: FakePointHttpClient(PAYLOAD))

    def discover_new_product_codes(self, **kwargs):
        return {"new_codes": []}

    def _materialize_node_lines(self, **kwargs):
        mode = MODE.read_text().strip()
        if mode != "ok":
            # Deliberately interrupt the real outer product transaction after
            # a destructive write; PostgreSQL must preserve the prior recipe.
            kwargs["receta"].lineas.all().delete()
            ENTERED.write_text(mode)
            if mode == "crash":
                os._exit(23)
            if mode in {"hard", "restart"}:
                signal.signal(signal.SIGUSR1, signal.SIG_IGN)
            time.sleep(60)
        return super()._materialize_node_lines(**kwargs)


celery_tasks.PointProductRecipeSyncService = ProbeService


@shared_task(name="recipe_probe.other_job", ignore_result=True)
def other_job():
    OTHER.touch()
