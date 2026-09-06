"""Independent supervisor: recovery must not wait behind a blocked Celery task."""

import logging
import time
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db import close_old_connections, connection

from pos_bridge.services.catalog_recipe_execution import recover_abandoned_catalog_jobs

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        "Recupera trabajos de recetas interrumpidos, con un máximo de dos reintentos."
    )

    def add_arguments(self, parser):
        parser.add_argument("--once", action="store_true")

    def handle(self, *args, **options):
        while True:
            close_old_connections()
            try:
                # This process owns its DB session; do not affect web/workers.
                with connection.cursor() as cursor:
                    cursor.execute("SET statement_timeout = '5s'")
                    cursor.execute("SET lock_timeout = '2s'")
                recover_abandoned_catalog_jobs()
                Path("/tmp/recipes-watchdog-heartbeat").touch()
            except Exception:
                logger.exception(
                    "Recipe supervisor scan failed; retrying in 30 seconds"
                )
                if options["once"]:
                    raise
            finally:
                connection.close()
            if options["once"]:
                return
            time.sleep(30)
