from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import TestCase

from core.access import ROLE_DG
from orquestacion.models import (
    ChatConversationState,
    ChatToolCall,
    ChatToolResult,
)
from orquestacion.services.chat_service import (
    ChatConfigurationError,
    create_chat_conversation,
    create_user_turn,
    execute_chat_turn,
)


def _fake_response(*, content: str = "", tool_calls=None):
    message = SimpleNamespace(content=content, tool_calls=tool_calls or [])
    choice = SimpleNamespace(message=message)
    return SimpleNamespace(choices=[choice])


def _fake_tool_call(name: str, arguments: dict, tool_call_id: str = "call_erp_tool"):
    return SimpleNamespace(
        id=tool_call_id,
        function=SimpleNamespace(name=name, arguments=json.dumps(arguments)),
    )


class NativeERPChatServiceTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create_user(
            username="dg_chat_service",
            email="dg_chat_service@example.com",
            password="test12345",
        )
        dg_group, _ = Group.objects.get_or_create(name=ROLE_DG)
        self.user.groups.add(dg_group)

    def test_create_chat_conversation_creates_state(self):
        conversation = create_chat_conversation(user=self.user, session_key="dg-session")

        self.assertEqual(conversation.title, "Nueva conversación")
        self.assertEqual(conversation.session_key, "dg-session")
        self.assertTrue(
            ChatConversationState.objects.filter(conversation=conversation).exists()
        )

    @patch("orquestacion.services.chat_service._model_client")
    @patch("orquestacion.services.chat_service._build_tool_definitions")
    def test_execute_chat_turn_without_tools_persists_answer(self, mock_build_tools, mock_model_client):
        mock_build_tools.return_value = ([], {})
        mock_client = SimpleNamespace(
            chat=SimpleNamespace(
                completions=SimpleNamespace(
                    create=lambda **kwargs: _fake_response(
                        content="Resumen ejecutivo:\n- No hay compras comprometidas hoy."
                    )
                )
            )
        )
        mock_model_client.return_value = mock_client

        conversation = create_chat_conversation(user=self.user, session_key="dg-session")
        user_message, assistant_message = create_user_turn(
            user=self.user,
            conversation=conversation,
            content="¿Qué gasto de insumos tenemos comprometido hoy?",
            session_key="dg-session",
        )

        result = execute_chat_turn(
            user=self.user,
            conversation=conversation,
            user_message=user_message,
            assistant_message=assistant_message,
        )

        assistant_message.refresh_from_db()
        conversation.refresh_from_db()
        self.assertIn("No hay compras comprometidas hoy", result.assistant_text)
        self.assertEqual(assistant_message.content, result.assistant_text)
        self.assertEqual(assistant_message.status, assistant_message.STATUS_COMPLETE)
        self.assertEqual(conversation.title, "¿Qué gasto de insumos tenemos comprometido hoy?")
        self.assertEqual(conversation.state.metadata_json["last_tool_count"], 0)

    @patch("orquestacion.services.chat_service.invoke_tool")
    @patch("orquestacion.services.chat_service._model_client")
    @patch("orquestacion.services.chat_service._build_tool_definitions")
    def test_execute_chat_turn_records_tool_call_and_result(
        self,
        mock_build_tools,
        mock_model_client,
        mock_invoke_tool,
    ):
        tool_name = "erp_get_discrepancies"
        mock_build_tools.return_value = (
            [
                {
                    "type": "function",
                    "function": {
                        "name": tool_name,
                        "description": "Discrepancias operativas",
                        "parameters": {"type": "object", "properties": {}},
                    },
                }
            ],
            {
                tool_name: {
                    "key": "erp.get_discrepancies",
                    "name": tool_name,
                    "display_name": "Discrepancias operativas",
                    "requires_approval": False,
                }
            },
        )
        first_response = _fake_response(
            tool_calls=[_fake_tool_call(tool_name, {"severity": "alta"})]
        )
        second_response = _fake_response(
            content="Resumen ejecutivo:\n- Hay 2 discrepancias críticas abiertas."
        )
        responses = [first_response, second_response]
        mock_client = SimpleNamespace(
            chat=SimpleNamespace(
                completions=SimpleNamespace(
                    create=None
                )
            )
        )

        def _create(**kwargs):
            return responses.pop(0)

        mock_client.chat.completions.create = _create
        mock_model_client.return_value = mock_client
        mock_invoke_tool.return_value = {
            "status": "ok",
            "result": {"status": "ok", "summary": "2 discrepancias abiertas"},
        }

        conversation = create_chat_conversation(user=self.user, session_key="dg-session")
        user_message, assistant_message = create_user_turn(
            user=self.user,
            conversation=conversation,
            content="¿Qué discrepancias operativas críticas siguen abiertas hoy?",
            session_key="dg-session",
        )

        result = execute_chat_turn(
            user=self.user,
            conversation=conversation,
            user_message=user_message,
            assistant_message=assistant_message,
        )

        tool_call = ChatToolCall.objects.get(conversation=conversation)
        tool_result = ChatToolResult.objects.get(tool_call=tool_call)
        assistant_message.refresh_from_db()

        self.assertEqual(tool_call.tool_name, tool_name)
        self.assertEqual(tool_call.status, ChatToolCall.STATUS_COMPLETE)
        self.assertEqual(tool_result.summary, "ok")
        self.assertIn("2 discrepancias críticas abiertas", result.assistant_text)
        self.assertEqual(assistant_message.metadata_json["tool_events"][0]["tool_name"], tool_name)

    @patch("orquestacion.services.chat_service.settings")
    def test_model_client_requires_openai_key(self, mock_settings):
        mock_settings.OPENAI_API_KEY = ""

        conversation = create_chat_conversation(user=self.user, session_key="dg-session")
        user_message, assistant_message = create_user_turn(
            user=self.user,
            conversation=conversation,
            content="¿Qué tanto gasto en insumos haremos en lo que resta del mes?",
            session_key="dg-session",
        )

        with self.assertRaises(ChatConfigurationError):
            execute_chat_turn(
                user=self.user,
                conversation=conversation,
                user_message=user_message,
                assistant_message=assistant_message,
            )
