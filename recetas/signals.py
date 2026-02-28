from __future__ import annotations

from django.db.models.signals import post_save, pre_delete
from django.dispatch import receiver
from django.db import OperationalError, ProgrammingError

from recetas.models import Receta, RecetaPresentacion
from recetas.utils.derived_insumos import sync_presentacion_insumo, sync_receta_derivados


@receiver(post_save, sender=Receta, dispatch_uid="recetas_sync_derivados_on_receta_save")
def sync_derivados_on_receta_save(sender, instance: Receta, **kwargs):
    try:
        sync_receta_derivados(instance)
    except (OperationalError, ProgrammingError):
        # Puede ocurrir durante migraciones/entornos parciales.
        return
    except Exception:
        # No bloquea guardado de receta por errores de sincronización.
        return


@receiver(post_save, sender=RecetaPresentacion, dispatch_uid="recetas_sync_derivado_on_presentacion_save")
def sync_derivado_on_presentacion_save(sender, instance: RecetaPresentacion, **kwargs):
    try:
        sync_presentacion_insumo(instance)
    except (OperationalError, ProgrammingError):
        return
    except Exception:
        return


@receiver(pre_delete, sender=RecetaPresentacion, dispatch_uid="recetas_deactivate_derivado_on_presentacion_delete")
def deactivate_derivado_on_presentacion_delete(sender, instance: RecetaPresentacion, **kwargs):
    try:
        sync_presentacion_insumo(instance, deactivate=True)
    except (OperationalError, ProgrammingError):
        return
    except Exception:
        return
