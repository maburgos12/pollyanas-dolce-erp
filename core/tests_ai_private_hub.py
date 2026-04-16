from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from unittest.mock import patch
from django.test import TestCase
from django.urls import reverse

from core.access import ROLE_DG, ROLE_PRODUCCION
from orquestacion.models import ChatMessage
from orquestacion.services.chat_service import create_chat_conversation


class AIPrivateHubViewTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.dg = user_model.objects.create_user(
            username="dg_ai_private_hub",
            email="dg_ai_private_hub@example.com",
            password="test12345",
        )
        dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
        self.dg.groups.add(dg_group)

        self.produccion = user_model.objects.create_user(
            username="prod_ai_private_hub",
            email="prod_ai_private_hub@example.com",
            password="test12345",
        )
        produccion_group, _ = Group.objects.get_or_create(name=ROLE_PRODUCCION)
        self.produccion.groups.add(produccion_group)

    def test_ai_private_hub_requires_login(self):
        response = self.client.get(reverse("ai_private_hub"))
        self.assertEqual(response.status_code, 302)
        self.assertIn("/login/", response.url)

    def test_ai_private_hub_visible_for_dg(self):
        self.client.force_login(self.dg)
        response = self.client.get(reverse("ai_private_hub"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Chat ERP nativo")
        self.assertContains(response, "Pollyana's Dolce AI")
        self.assertContains(response, "Nueva conversación")
        self.assertContains(response, "¿Qué tanto gasto en insumos haremos en lo que resta del mes de abril?")

    @patch("orquestacion.chat_views.get_chat_runtime_status")
    def test_ai_private_hub_shows_runtime_warning_when_model_not_ready(self, mock_runtime_status):
        mock_runtime_status.return_value = {
            "ready": False,
            "model_name": "gpt-4o-mini",
            "issue": "Falta OPENAI_API_KEY en la configuración del ERP.",
        }
        self.client.force_login(self.dg)

        response = self.client.get(reverse("ai_private_hub"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Configuración pendiente del modelo")
        self.assertContains(response, "falta completar la configuración del modelo")

    def test_ai_private_hub_forbidden_for_non_governance_role(self):
        self.client.force_login(self.produccion)
        response = self.client.get(reverse("ai_private_hub"))
        self.assertEqual(response.status_code, 403)

    def test_ai_private_hub_loads_existing_conversation_detail(self):
        conversation = create_chat_conversation(user=self.dg, session_key="test-session")
        ChatMessage.objects.create(
            conversation=conversation,
            sequence=1,
            role=ChatMessage.ROLE_USER,
            status=ChatMessage.STATUS_COMPLETE,
            content="¿Qué riesgos de stock bajo tengo esta semana?",
            created_by=self.dg,
        )
        ChatMessage.objects.create(
            conversation=conversation,
            sequence=2,
            role=ChatMessage.ROLE_ASSISTANT,
            status=ChatMessage.STATUS_COMPLETE,
            content="Hay 3 insumos con riesgo alto de quiebre en 48 horas.",
        )

        self.client.force_login(self.dg)
        response = self.client.get(reverse("ai_private_hub"), {"conversation": str(conversation.public_id)})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Hay 3 insumos con riesgo alto de quiebre en 48 horas.")
        self.assertContains(response, str(conversation.public_id))

    def test_ai_private_conversation_api_allows_create_and_list_for_dg(self):
        self.client.force_login(self.dg)

        create_response = self.client.post(
            reverse("ai_private_conversation_create_api"),
            data="{}",
            content_type="application/json",
        )

        self.assertEqual(create_response.status_code, 201)
        create_payload = create_response.json()
        conversation_id = create_payload["conversation"]["id"]
        self.assertEqual(create_payload["conversation"]["title"], "Nueva conversación")

        list_response = self.client.get(reverse("ai_private_conversations_api"))
        self.assertEqual(list_response.status_code, 200)
        list_payload = list_response.json()
        self.assertEqual(len(list_payload["items"]), 1)
        self.assertEqual(list_payload["items"][0]["id"], conversation_id)

        detail_response = self.client.get(
            reverse("ai_private_conversation_detail_api", kwargs={"conversation_id": conversation_id})
        )
        self.assertEqual(detail_response.status_code, 200)
        detail_payload = detail_response.json()
        self.assertEqual(detail_payload["conversation"]["id"], conversation_id)
        self.assertEqual(detail_payload["messages"], [])

    @patch("orquestacion.chat_views.execute_chat_turn")
    def test_ai_private_stream_api_returns_assistant_error_payload_on_failure(self, mock_execute_chat_turn):
        mock_execute_chat_turn.side_effect = RuntimeError("Falla controlada para prueba.")
        conversation = create_chat_conversation(user=self.dg, session_key="test-session")

        self.client.force_login(self.dg)
        response = self.client.post(
            reverse("ai_private_message_stream_api", kwargs={"conversation_id": str(conversation.public_id)}),
            data='{"content":"¿Qué tanto gasto en insumos haremos en lo que resta del mes de abril?"}',
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 500)
        payload = response.json()
        self.assertEqual(payload["error_code"], "chat_execution_error")
        self.assertTrue(payload["assistant_content"])
        self.assertIn("La conversación quedó guardada", payload["assistant_content"])

        assistant_message = ChatMessage.objects.get(public_id=payload["assistant_message_id"])
        self.assertEqual(assistant_message.status, ChatMessage.STATUS_ERROR)
        self.assertEqual(assistant_message.content, payload["assistant_content"])
