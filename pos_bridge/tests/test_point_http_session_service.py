from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from pos_bridge.services.point_http_session_service import PointHttpSessionService


class PointHttpSessionServiceTests(SimpleTestCase):
    def _settings(self):
        return SimpleNamespace(
            base_url="https://app.pointmeup.com",
            username="user@example.com",
            password="secret",
            timeout_ms=30000,
        )

    @patch("pos_bridge.services.point_http_session_service.requests.Session")
    def test_create_selects_account_matching_branch(self, session_cls):
        session = Mock()
        session_cls.return_value = session
        session.post.side_effect = [
            Mock(status_code=200, json=Mock(return_value={"redirectToUrl": "/Account/workSpaces"}), raise_for_status=Mock()),
            Mock(
                status_code=200,
                json=Mock(
                    return_value={
                        "json": json.dumps(
                            [
                                {
                                    "ACC_ID": "acc-1",
                                    "JSON_WORKSPACES": json.dumps(
                                        [
                                            {"id_suc": 1, "wsName": "Matriz"},
                                            {"id_suc": 2, "wsName": "Crucero"},
                                        ]
                                    ),
                                }
                            ]
                        )
                    }
                ),
                raise_for_status=Mock(),
            ),
            Mock(status_code=200, json=Mock(return_value={"redirectToUrl": "/Home/Index"}), raise_for_status=Mock()),
        ]
        session.get.return_value = Mock(status_code=200, raise_for_status=Mock())

        service = PointHttpSessionService(self._settings())
        result = service.create(branch_external_id="1", branch_display_name="Matriz")

        self.assertIs(result.session, session)
        self.assertEqual(session.post.call_args_list[2].kwargs["data"]["acid"], "acc-1")
        self.assertEqual(session.post.call_args_list[2].kwargs["data"]["sucid"], "1")
        self.assertEqual(session.post.call_args_list[2].kwargs["data"]["sucname"], "Matriz")

    @patch("pos_bridge.services.point_http_session_service.requests.Session")
    def test_create_falls_back_to_first_account_for_all_branches(self, session_cls):
        session = Mock()
        session_cls.return_value = session
        session.post.side_effect = [
            Mock(status_code=200, json=Mock(return_value={"redirectToUrl": "/Account/workSpaces"}), raise_for_status=Mock()),
            Mock(
                status_code=200,
                json=Mock(
                    return_value={
                        "json": json.dumps(
                            [
                                {"ACC_ID": "acc-1", "JSON_WORKSPACES": "[]"},
                                {"ACC_ID": "acc-2", "JSON_WORKSPACES": "[]"},
                            ]
                        )
                    }
                ),
                raise_for_status=Mock(),
            ),
            Mock(status_code=200, json=Mock(return_value={"redirectToUrl": "/Home/Index"}), raise_for_status=Mock()),
        ]
        session.get.return_value = Mock(status_code=200, raise_for_status=Mock())

        service = PointHttpSessionService(self._settings())
        service.create()

        self.assertEqual(session.post.call_args_list[2].kwargs["data"], {"acid": "acc-1"})
