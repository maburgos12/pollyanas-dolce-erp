"""A historical automatic round is not a second physical measurement."""
import copy
from types import SimpleNamespace as N
from django.test import SimpleTestCase
from reportes.services_production_physical_evidence import _automatic_history_flags as automatic_history_flags

class AutomaticHistoryTests(SimpleTestCase):

    def setUp(self):
        self.counts = {8: N(estado='RECONTEO', ronda=2, iniciado_en=None)}
        self.events = [N(pk=1, conteo_id=8, action='enviar', actor_id=1, payload={'snapshot': [{'ronda': 1, 'cantidad': '0'}]}), N(pk=2, conteo_id=8, action='conciliacion_nocturna', actor_id=None, payload={'ronda': 1, 'automatizacion': {'accion': 'RECONTEO', 'linea_ids': [11]}})]
        self.rows = [{'linea__conteo_id': 8, 'linea_id': 11, 'ronda': 2, 'cantidad': None, 'contado_en': None, 'incidencia': ''}]

    def flags(self):
        return automatic_history_flags(self.counts, self.events, self.rows)

    def test_automatic_legacy_preserves_explicit_zero_first_submission(self):
        self.assertEqual(self.flags(), {8: {'event_id': 2, 'previous_round': 1}})

    def test_manual_recount_after_auto_not_legacy(self):
        self.events.append(N(pk=3, conteo_id=8, action='reconteo', actor_id=1, payload={}))
        self.assertEqual(self.flags(), {})

    def test_new_round_started_not_legacy(self):
        self.events.append(N(pk=3, conteo_id=8, action='iniciar', actor_id=1, payload={}))
        self.assertEqual(self.flags(), {})

    def test_second_submission_not_legacy(self):
        self.events.append(N(pk=3, conteo_id=8, action='enviar', actor_id=1, payload={}))
        self.assertEqual(self.flags(), {})

    def test_any_actual_new_measurement_disqualifies_even_zero(self):
        for field, value in [('cantidad', '0'), ('cantidad', '2'), ('contado_en', '2026-01-01'), ('incidencia', 'Sin producto')]:
            with self.subTest(field=field, value=value):
                rows = copy.deepcopy(self.rows)
                rows[0][field] = value
                self.assertEqual(automatic_history_flags(self.counts, self.events, rows), {})

    def test_unknown_or_inconsistent_provenance_fails_closed(self):
        for mutation in ['actor', 'bool-round', 'bool-id', 'duplicate-id', 'other-id', 'wrong-action', 'missing-row', 'current-round', 'started-count']:
            with self.subTest(mutation=mutation):
                counts, events, rows = copy.deepcopy((self.counts, self.events, self.rows))
                auto = events[1]
                if mutation == 'actor':
                    auto.actor_id = 2
                elif mutation == 'bool-round':
                    auto.payload['ronda'] = True
                elif mutation == 'bool-id':
                    auto.payload['automatizacion']['linea_ids'] = [True]
                elif mutation == 'duplicate-id':
                    auto.payload['automatizacion']['linea_ids'] = [11, 11]
                elif mutation == 'other-id':
                    auto.payload['automatizacion']['linea_ids'] = [12]
                elif mutation == 'wrong-action':
                    auto.payload['automatizacion']['accion'] = 'ACEPTADO'
                elif mutation == 'missing-row':
                    rows = []
                elif mutation == 'current-round':
                    counts[8].ronda = 3
                else:
                    counts[8].iniciado_en = '2026-01-01'
                self.assertEqual(automatic_history_flags(counts, events, rows), {})

    def test_no_auto_event_keeps_current_manual_presentation(self):
        self.assertEqual(automatic_history_flags(self.counts, self.events[:1], self.rows), {})

    def test_existing_unrelated_count_state_not_legacy(self):
        self.counts[8].estado = 'ENVIADO'
        self.assertEqual(self.flags(), {})

    def test_historical_import_without_real_send_is_not_a_submission(self):
        self.events[0].action = 'importacion_historica'
        self.assertEqual(self.flags(), {})

    def test_invalid_submission_and_automatic_payloads_fail_closed(self):
        for payload in [None, [], {}, {'snapshot': [None]}, {'snapshot': [{'ronda': True, 'cantidad': '0'}]}, {'lecturas': {}, 'ronda': False}]:
            with self.subTest(payload=payload):
                events = copy.deepcopy(self.events)
                events[0].payload = payload
                self.assertEqual(automatic_history_flags(self.counts, events, self.rows), {})

    def test_input_quantities_dates_and_states_are_not_modified(self):
        before = copy.deepcopy((self.counts, self.events, self.rows))
        self.flags()
        self.assertEqual((self.counts, self.events, self.rows), before)
