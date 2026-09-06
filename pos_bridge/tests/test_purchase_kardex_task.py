"""Cobertura de la tarea diaria que lleva compras de Point al kardex."""
from __future__ import annotations

import json
from datetime import date
from unittest.mock import patch

from django.test import TestCase
from django_celery_beat.models import PeriodicTask

from pos_bridge.tasks.celery_tasks import task_purchase_kardex_sync


class PurchaseKardexTaskTests(TestCase):
    def test_encadena_extraccion_y_puente_escribiendo_en_ambos(self):
        llamadas = []

        def fake_call_command(name, **kwargs):
            llamadas.append((name, kwargs))

        with patch("pos_bridge.tasks.celery_tasks.call_command", fake_call_command):
            with patch("pos_bridge.tasks.celery_tasks.timezone.localdate", return_value=date(2026, 9, 10)):
                resultado = task_purchase_kardex_sync(dias=7)

        self.assertEqual([nombre for nombre, _ in llamadas], ["extraer_compras_point", "importar_compras_point_a_kardex"])

        extraccion = llamadas[0][1]
        self.assertTrue(extraccion["apply"])
        self.assertEqual(extraccion["hasta"], "2026-09-10")
        # Ventana hacia atrás: Point registra compras con fecha posterior a la compra.
        self.assertEqual(extraccion["desde"], "2026-09-03")

        self.assertTrue(llamadas[1][1]["apply"])
        self.assertEqual(resultado["desde"], "2026-09-03")

    def test_la_ventana_nunca_es_menor_a_un_dia(self):
        llamadas = []

        with patch("pos_bridge.tasks.celery_tasks.call_command", lambda name, **kw: llamadas.append((name, kw))):
            with patch("pos_bridge.tasks.celery_tasks.timezone.localdate", return_value=date(2026, 9, 10)):
                task_purchase_kardex_sync(dias=0)

        self.assertEqual(llamadas[0][1]["desde"], "2026-09-09")


class PurchaseKardexScheduleTests(TestCase):
    def test_setup_registra_la_tarea_diaria(self):
        from django.core.management import call_command

        call_command("setup_celery_schedules")

        tarea = PeriodicTask.objects.get(name="pos_bridge: compras Point al kardex")
        self.assertEqual(tarea.task, "pos_bridge.purchase_kardex_sync")
        self.assertTrue(tarea.enabled)
        self.assertEqual(json.loads(tarea.kwargs), {"dias": 7})
        # 3:30: asistencias corre 6-23h y ventas 8-22h, así que a esa hora la única
        # competencia por la sesión Point es domicilios, que sí toma el candado.
        self.assertEqual((tarea.crontab.minute, tarea.crontab.hour), ("30", "3"))
