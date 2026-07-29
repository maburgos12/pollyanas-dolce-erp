from __future__ import annotations

import json
from uuid import uuid4

from django.apps import apps
from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone
from unittest.mock import patch

from .models import AsistenciaEmpleado, Empleado, EmpleadoIdentidadPendiente
from .services_identidad import vincular_identidad_pendiente


@override_settings(ERP_PUBLIC_API_KEY="hik-v2-test-key")
class HikIngestaV2Tests(TestCase):
    endpoint = "/rrhh/api/asistencia-hik/v2/"
    source = "hikconnect_cloud"
    max_batch_size = 100

    def setUp(self) -> None:
        self.empleado = Empleado.objects.create(
            nombre="Empleado Hik V2",
            codigo="328",
            salario_diario="400.00",
        )

    def _event(
        self,
        *,
        event_id: str | None = None,
        employee_external_id: str = "328",
        occurred_at: str = "2026-07-28T08:00:00-07:00",
        source: str | None = None,
        kind: str = "check_in",
    ) -> dict[str, str]:
        return {
            "event_id": event_id or str(uuid4()),
            "source": source or self.source,
            "employee_external_id": employee_external_id,
            "occurred_at": occurred_at,
            "kind": kind,
            "device_id": "hik-device-01",
        }

    def _post(
        self,
        events: list[dict[str, str]],
        *,
        batch_id: str | None = None,
    ):
        return self.client.post(
            self.endpoint,
            data=json.dumps(
                {
                    "contract_version": 2,
                    "batch_id": batch_id or str(uuid4()),
                    "events": events,
                }
            ),
            content_type="application/json",
            HTTP_X_API_KEY="hik-v2-test-key",
        )

    def _single_result(self, response) -> dict:
        self.assertEqual(response.status_code, 200, response.content)
        body = response.json()
        self.assertEqual(body["contract_version"], 2)
        self.assertEqual(len(body["results"]), 1)
        return body["results"][0]

    def _ledger(self):
        return apps.get_model("rrhh", "EventoHikCloud")

    def test_mismo_guid_y_mismo_payload_es_duplicate_y_conserva_un_solo_recibo(self):
        event = self._event(event_id="guid-same-payload")

        first = self._single_result(self._post([event]))
        reordered_event = {key: event[key] for key in reversed(event)}
        second = self._single_result(self._post([reordered_event]))

        self.assertEqual(first["event_id"], event["event_id"])
        self.assertEqual(first["outcome"], "accepted")
        self.assertEqual(second["event_id"], event["event_id"])
        self.assertEqual(second["outcome"], "duplicate")
        self.assertFalse(second["retryable"])
        self.assertEqual(
            self._ledger().objects.filter(
                fuente=self.source,
                event_id=event["event_id"],
            ).count(),
            1,
        )

    def test_mismo_guid_y_payload_distinto_es_payload_conflict_sin_mutar_recibo(self):
        event = self._event(event_id="guid-conflicting-payload")
        conflict = {**event, "kind": "check_out"}

        first = self._single_result(self._post([event]))
        second = self._single_result(self._post([conflict]))

        self.assertEqual(first["outcome"], "accepted")
        self.assertEqual(second["event_id"], event["event_id"])
        self.assertEqual(second["outcome"], "payload_conflict")
        self.assertFalse(second["retryable"])
        self.assertEqual(
            self._ledger().objects.filter(
                fuente=self.source,
                event_id=event["event_id"],
            ).count(),
            1,
        )

    def test_identidad_desconocida_queda_deferred_sin_crear_asistencia(self):
        event = self._event(
            event_id="guid-unknown-identity",
            employee_external_id="HIK-NO-VINCULADO",
        )

        result = self._single_result(self._post([event]))

        self.assertEqual(result["event_id"], event["event_id"])
        self.assertEqual(result["outcome"], "deferred")
        self.assertEqual(result["reason_code"], "identity_unresolved")
        self.assertTrue(result["retryable"])
        self.assertEqual(
            self._ledger().objects.filter(
                fuente=self.source,
                event_id=event["event_id"],
            ).count(),
            1,
        )
        self.assertFalse(
            AsistenciaEmpleado.objects.filter(
                empleado__codigo="HIK-NO-VINCULADO",
            ).exists()
        )
        self.assertEqual(AsistenciaEmpleado.objects.count(), 0)

    def test_lote_mixto_devuelve_un_resultado_correlacionado_por_event_id(self):
        duplicate = self._event(event_id="guid-already-received")
        self.assertEqual(
            self._single_result(self._post([duplicate]))["outcome"],
            "accepted",
        )
        accepted = self._event(
            event_id="guid-new-known-employee",
            occurred_at="2026-07-28T17:00:00-07:00",
            kind="check_out",
        )
        deferred = self._event(
            event_id="guid-new-unknown-employee",
            employee_external_id="HIK-PENDIENTE-2",
        )
        batch_id = str(uuid4())

        response = self._post(
            [accepted, deferred, duplicate],
            batch_id=batch_id,
        )

        self.assertEqual(response.status_code, 200, response.content)
        body = response.json()
        self.assertEqual(body["contract_version"], 2)
        self.assertEqual(body["batch_id"], batch_id)
        self.assertEqual(
            [result["event_id"] for result in body["results"]],
            [
                accepted["event_id"],
                deferred["event_id"],
                duplicate["event_id"],
            ],
        )
        self.assertEqual(
            {
                result["event_id"]: result["outcome"]
                for result in body["results"]
            },
            {
                accepted["event_id"]: "accepted",
                deferred["event_id"]: "deferred",
                duplicate["event_id"]: "duplicate",
            },
        )

    def test_timestamp_sin_zona_y_fuente_no_permitida_son_rejected(self):
        naive_timestamp = self._event(
            event_id="guid-naive-timestamp",
            occurred_at="2026-07-28T08:00:00",
        )
        invalid_source = self._event(
            event_id="guid-invalid-source",
            source="hikconnect_browser_guess",
        )

        response = self._post([naive_timestamp, invalid_source])

        self.assertEqual(response.status_code, 200, response.content)
        results = response.json()["results"]
        self.assertEqual(
            [result["event_id"] for result in results],
            [naive_timestamp["event_id"], invalid_source["event_id"]],
        )
        self.assertEqual(
            [result["outcome"] for result in results],
            ["rejected", "rejected"],
        )
        self.assertTrue(all(not result["retryable"] for result in results))
        self.assertEqual(AsistenciaEmpleado.objects.count(), 0)

    def test_lote_sobredimensionado_se_rechaza_sin_persistir_eventos(self):
        events = [
            self._event(event_id=f"guid-oversized-{index}")
            for index in range(self.max_batch_size + 1)
        ]

        response = self._post(events)

        self.assertEqual(response.status_code, 400, response.content)
        self.assertEqual(response.json()["error_code"], "batch_too_large")
        self.assertEqual(self._ledger().objects.count(), 0)
        self.assertEqual(AsistenciaEmpleado.objects.count(), 0)

    def test_dos_marcajes_legitimos_cercanos_se_conservan_y_proyectan(self):
        entrada = self._event(
            event_id="guid-close-check-in",
            occurred_at="2026-07-28T08:00:00-07:00",
            kind="check_in",
        )
        salida = self._event(
            event_id="guid-close-check-out",
            occurred_at="2026-07-28T08:03:00-07:00",
            kind="check_out",
        )

        response = self._post([entrada, salida])

        self.assertEqual(response.status_code, 200, response.content)
        self.assertEqual(
            [item["outcome"] for item in response.json()["results"]],
            ["accepted", "accepted"],
        )
        asistencia = AsistenciaEmpleado.objects.get(
            empleado=self.empleado,
            fecha="2026-07-28",
        )
        self.assertEqual(
            asistencia.entrada.isoformat(),
            "2026-07-28T15:00:00+00:00",
        )
        self.assertEqual(
            asistencia.salida.isoformat(),
            "2026-07-28T15:03:00+00:00",
        )
        self.assertEqual(
            self._ledger().objects.filter(projection_status="applied").count(),
            2,
        )

    def test_reenvio_del_guid_no_reproyecta_ni_repite_efectos(self):
        event = self._event(event_id="guid-effects-once")

        with patch(
            "rrhh.services_hik_ingesta._run_post_projection_effects"
        ) as effects:
            first = self._single_result(self._post([event]))
            second = self._single_result(self._post([event]))

        self.assertEqual(first["outcome"], "accepted")
        self.assertEqual(second["outcome"], "duplicate")
        self.assertEqual(effects.call_count, 1)
        self.assertEqual(AsistenciaEmpleado.objects.count(), 1)

    def test_vincular_identidad_reprocesa_el_payload_durable(self):
        event = self._event(
            event_id="guid-replay-after-link",
            employee_external_id="HIK-PENDIENTE-REPLAY",
        )
        result = self._single_result(self._post([event]))
        self.assertEqual(result["outcome"], "deferred")
        pendiente = EmpleadoIdentidadPendiente.objects.get(
            fuente=EmpleadoIdentidadPendiente.FUENTE_HIKVISION,
            codigo_externo="HIK-PENDIENTE-REPLAY",
        )
        empleado = Empleado.objects.create(
            nombre="Empleado para replay",
            codigo="TEMP-REPLAY",
            salario_diario="400.00",
        )
        user = get_user_model().objects.create_user(username="rrhh-replay")

        with self.captureOnCommitCallbacks(execute=True):
            vincular_identidad_pendiente(pendiente, empleado, user=user)

        receipt = self._ledger().objects.get(event_id=event["event_id"])
        self.assertEqual(receipt.empleado_id, empleado.id)
        self.assertEqual(receipt.estado, "accepted")
        self.assertEqual(receipt.projection_status, "applied")
        self.assertTrue(
            AsistenciaEmpleado.objects.filter(
                empleado=empleado,
                fecha="2026-07-28",
            ).exists()
        )

    def test_reporte_salud_distingue_recuperacion_de_accion_humana(self):
        endpoint = "/rrhh/api/asistencia-hik/v2/health/"
        payload = {
            "status": "recovering",
            "last_cycle_at": timezone.now().isoformat(),
            "last_success_at": timezone.now().isoformat(),
            "outbox_pending": 4,
            "identity_deferred": 2,
            "failure_count": 1,
            "incident_key": "",
            "last_error": "timeout temporal ERP",
        }

        response = self.client.post(
            endpoint,
            data=json.dumps(payload),
            content_type="application/json",
            HTTP_X_API_KEY="hik-v2-test-key",
        )

        self.assertEqual(response.status_code, 200, response.content)
        state = apps.get_model("rrhh", "EstadoIntegracionHik").objects.get(
            nombre="hikconnect_cloud"
        )
        self.assertEqual(state.estado, "recovering")
        self.assertEqual(state.outbox_pending, 4)
        self.assertEqual(state.identity_deferred, 2)

        payload.update(
            {
                "status": "action_required",
                "failure_count": 5,
                "incident_key": "hik-auth-expired",
            }
        )
        second = self.client.post(
            endpoint,
            data=json.dumps(payload),
            content_type="application/json",
            HTTP_X_API_KEY="hik-v2-test-key",
        )
        self.assertEqual(second.status_code, 200, second.content)
        state.refresh_from_db()
        self.assertEqual(state.estado, "action_required")
        self.assertEqual(state.incident_key, "hik-auth-expired")

    def test_caida_entre_recibo_y_proyeccion_se_recupera_en_reintento(self):
        event = self._event(event_id="guid-recover-pending-projection")

        with patch(
            "rrhh.services_hik_ingesta.project_receipt",
            side_effect=RuntimeError("caida simulada"),
        ):
            with self.assertRaises(RuntimeError):
                self._post([event])

        receipt = self._ledger().objects.get(event_id=event["event_id"])
        self.assertEqual(receipt.projection_status, "pending")
        self.assertEqual(AsistenciaEmpleado.objects.count(), 0)

        recovered = self._single_result(self._post([event]))

        self.assertEqual(recovered["outcome"], "accepted")
        self.assertEqual(recovered["projection"], "applied")
        self.assertEqual(AsistenciaEmpleado.objects.count(), 1)

    def test_eventos_fuera_de_orden_se_reconstruyen_segun_hora_y_kind(self):
        salida = self._event(
            event_id="guid-out-of-order-out",
            occurred_at="2026-07-28T17:00:00-07:00",
            kind="check_out",
        )
        entrada = self._event(
            event_id="guid-out-of-order-in",
            occurred_at="2026-07-28T08:00:00-07:00",
            kind="check_in",
        )

        self.assertEqual(self._single_result(self._post([salida]))["outcome"], "accepted")
        self.assertEqual(self._single_result(self._post([entrada]))["outcome"], "accepted")

        asistencia = AsistenciaEmpleado.objects.get(empleado=self.empleado, fecha="2026-07-28")
        self.assertEqual(asistencia.entrada.isoformat(), "2026-07-28T15:00:00+00:00")
        self.assertEqual(asistencia.salida.isoformat(), "2026-07-29T00:00:00+00:00")
        self.assertIsNone(asistencia.salida_comida)

    def test_kind_desconocido_no_crea_entrada_falsa(self):
        event = self._event(event_id="guid-unknown-kind", kind="unknown")

        result = self._single_result(self._post([event]))

        self.assertEqual(result["outcome"], "rejected")
        self.assertEqual(result["reason_code"], "invalid_kind")
        self.assertEqual(AsistenciaEmpleado.objects.count(), 0)

    def test_salida_aislada_no_se_disfraza_como_entrada(self):
        event = self._event(
            event_id="guid-isolated-check-out",
            occurred_at="2026-07-28T17:00:00-07:00",
            kind="check_out",
        )

        self.assertEqual(self._single_result(self._post([event]))["outcome"], "accepted")

        asistencia = AsistenciaEmpleado.objects.get(empleado=self.empleado, fecha="2026-07-28")
        self.assertIsNone(asistencia.entrada)
        self.assertEqual(asistencia.salida.isoformat(), "2026-07-29T00:00:00+00:00")
        receipt = self._ledger().objects.get(event_id=event["event_id"])
        self.assertEqual(receipt.effects_status, "completed")
        self.assertEqual(receipt.effects_version, receipt.projection_version)

    def test_punches_derivados_fuera_de_orden_se_reconstruyen_por_cronologia(self):
        tarde = self._event(
            event_id="guid-derived-late-first",
            occurred_at="2026-07-28T17:00:00-07:00",
            kind="punch",
        )
        temprano = self._event(
            event_id="guid-derived-early-later",
            occurred_at="2026-07-28T08:00:00-07:00",
            kind="punch",
        )

        self.assertEqual(self._single_result(self._post([tarde]))["outcome"], "accepted")
        self.assertEqual(self._single_result(self._post([temprano]))["outcome"], "accepted")

        asistencia = AsistenciaEmpleado.objects.get(empleado=self.empleado, fecha="2026-07-28")
        self.assertEqual(asistencia.entrada.isoformat(), "2026-07-28T15:00:00+00:00")
        self.assertEqual(asistencia.salida.isoformat(), "2026-07-29T00:00:00+00:00")
