from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.test import SimpleTestCase

from pos_bridge.services.point_http_session_service import PointHttpSessionService
from pos_bridge.utils.exceptions import ConfigurationError


class PointHttpSessionServiceTests(SimpleTestCase):
    def _settings(self):
        return SimpleNamespace(
            base_url="https://app.pointmeup.com",
            username="user@example.com",
            password="secret",
            timeout_ms=30000,
        )

    def test_create_requires_point_base_url(self):
        settings = self._settings()
        settings.base_url = ""

        service = PointHttpSessionService(settings)

        with self.assertRaises(ConfigurationError):
            service.create()

    @patch("pos_bridge.services.point_http_session_service.requests.Session")
    def test_create_selects_account_matching_branch(self, session_cls):
        session = Mock()
        session_cls.return_value = session
        session.get.side_effect = [
            Mock(status_code=200, raise_for_status=Mock()),
            Mock(status_code=200, raise_for_status=Mock()),
            Mock(status_code=200, text="accIdActual = 'acc-13';", raise_for_status=Mock()),
            Mock(status_code=200, raise_for_status=Mock()),
            Mock(status_code=200, raise_for_status=Mock()),
        ]
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
                                    "JSON_WORKSPACES": json.dumps([{"id_suc": "1", "wsName": "Matriz"}]),
                                },
                                {
                                    "ACC_ID": "acc-13",
                                    "JSON_WORKSPACES": json.dumps([{"id_suc": "13", "wsName": "Guamuchil"}]),
                                },
                            ]
                        )
                    }
                ),
                raise_for_status=Mock(),
            ),
            Mock(status_code=200, json=Mock(return_value={"success": True}), raise_for_status=Mock()),
            Mock(status_code=200, json=Mock(return_value={"redirectToUrl": "/Home/Index"}), raise_for_status=Mock()),
        ]

        service = PointHttpSessionService(self._settings())
        result = service.create(branch_external_id="1", branch_display_name="Matriz")

        self.assertIs(result.session, session)
        self.assertEqual(result.account_id, "acc-1")
        self.assertEqual(result.workspace_name, "Matriz")
        self.assertGreaterEqual(session.get.call_count, 5)
        self.assertEqual(session.get.call_args_list[0].args[0], "https://app.pointmeup.com/")
        self.assertEqual(session.post.call_args_list[0].kwargs["data"]["timeZone"], "0")
        self.assertEqual(session.post.call_args_list[2].kwargs["data"]["accId"], "acc-1")
        self.assertEqual(
            session.post.call_args_list[3].kwargs["data"],
            {"acid": "acc-1", "sucid": "1", "sucname": "Matriz"},
        )

    @patch("pos_bridge.services.point_http_session_service.requests.Session")
    def test_create_falls_back_to_first_account_for_all_branches(self, session_cls):
        session = Mock()
        session_cls.return_value = session
        session.get.side_effect = [
            Mock(status_code=200, raise_for_status=Mock()),
            Mock(status_code=200, raise_for_status=Mock()),
            Mock(status_code=200, text="accIdActual = 'acc-1';", raise_for_status=Mock()),
            Mock(status_code=200, raise_for_status=Mock()),
            Mock(status_code=200, raise_for_status=Mock()),
        ]
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
                                    "JSON_WORKSPACES": json.dumps([{"id_suc": "1", "wsName": "Matriz"}]),
                                }
                            ]
                        )
                    }
                ),
                raise_for_status=Mock(),
            ),
            Mock(status_code=200, json=Mock(return_value={"success": True}), raise_for_status=Mock()),
            Mock(status_code=200, json=Mock(return_value={"redirectToUrl": "/Home/Index"}), raise_for_status=Mock()),
        ]

        service = PointHttpSessionService(self._settings())
        service.create()

        self.assertGreaterEqual(session.get.call_count, 5)
        self.assertEqual(session.get.call_args_list[0].args[0], "https://app.pointmeup.com/")
        self.assertEqual(session.post.call_args_list[0].kwargs["data"]["timeZone"], "0")
        self.assertEqual(session.post.call_args_list[2].kwargs["data"], {"accId": "acc-1"})
        self.assertEqual(session.post.call_args_list[3].kwargs["data"], {"acid": "acc-1"})
