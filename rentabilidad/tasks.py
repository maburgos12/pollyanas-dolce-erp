"""
Celery autodiscovery entrypoint for rentabilidad.

The concrete tasks live in tasks_rentabilidad.py for historical reasons, but
Celery autodiscover_tasks() only imports app-level tasks.py modules by default.
Importing them here registers their original task names with the worker.
"""

from .tasks_rentabilidad import (  # noqa: F401
    analizar_sucursal_con_ia,
    recalcular_rentabilidad_mensual,
    recalcular_rentabilidad_periodo_actual,
)

