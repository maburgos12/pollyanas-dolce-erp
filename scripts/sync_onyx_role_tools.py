#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from typing import Any

import requests
import urllib3


@dataclass(frozen=True)
class RoleSyncSpec:
    profile: str
    tool_name: str
    tool_display_name: str
    persona_name: str
    erp_token_env: str
    preserve_tool_id: int | None = None


ROLE_SPECS = [
    RoleSyncSpec(
        profile="dg",
        tool_name="pollyana_erp_dg",
        tool_display_name="Pollyana ERP DG",
        persona_name="Direccion General",
        erp_token_env="ERP_DG_TOKEN",
        preserve_tool_id=11,
    ),
    RoleSyncSpec(
        profile="compras",
        tool_name="pollyana_erp_compras",
        tool_display_name="Pollyana ERP Compras",
        persona_name="Compras",
        erp_token_env="ERP_COMPRAS_TOKEN",
    ),
    RoleSyncSpec(
        profile="produccion",
        tool_name="pollyana_erp_produccion",
        tool_display_name="Pollyana ERP Produccion",
        persona_name="Produccion",
        erp_token_env="ERP_PRODUCCION_TOKEN",
    ),
    RoleSyncSpec(
        profile="auditoria",
        tool_name="pollyana_erp_auditoria",
        tool_display_name="Pollyana ERP Auditoria",
        persona_name="Auditoria y Conciliacion",
        erp_token_env="ERP_AUDITORIA_TOKEN",
    ),
]


PERSONA_ALLOWED_FIELDS = {
    "name",
    "description",
    "document_set_ids",
    "is_public",
    "llm_model_provider_override",
    "llm_model_version_override",
    "starter_messages",
    "users",
    "groups",
    "remove_image",
    "uploaded_image_id",
    "icon_name",
    "search_start_date",
    "label_ids",
    "is_featured",
    "display_priority",
    "user_file_ids",
    "hierarchy_node_ids",
    "document_ids",
    "system_prompt",
    "replace_base_system_prompt",
    "task_prompt",
    "datetime_aware",
}


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


VERIFY_TLS = os.getenv("ONYX_VERIFY_TLS", "").strip().lower() not in {"0", "false", "no"}


if not VERIFY_TLS:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _json_or_raise(response: requests.Response) -> Any:
    try:
        response.raise_for_status()
    except Exception as exc:
        raise SystemExit(f"Request failed {response.request.method} {response.url}: {response.status_code} {response.text[:500]}") from exc
    try:
        return response.json()
    except Exception as exc:
        raise SystemExit(f"Invalid JSON response from {response.url}: {response.text[:500]}") from exc


def login_session(base_url: str, email: str, password: str) -> requests.Session:
    session = requests.Session()
    login_url = f"{base_url.rstrip('/')}/api/auth/login"
    response = session.post(
        login_url,
        data={"username": email, "password": password},
        timeout=30,
        verify=VERIFY_TLS,
    )
    if response.status_code != 204:
        raise SystemExit(f"Onyx login failed: {response.status_code} {response.text[:500]}")
    if "fastapiusersauth" not in session.cookies:
        raise SystemExit("Onyx login succeeded but auth cookie was not set.")
    return session


def fetch_erp_openapi(erp_openapi_base_url: str, profile: str, token: str) -> dict[str, Any]:
    url = f"{erp_openapi_base_url.rstrip('/')}/openapi/?profile={profile}"
    response = requests.get(
        url,
        headers={"Authorization": f"Token {token}"},
        timeout=30,
    )
    spec = _json_or_raise(response)
    if not spec.get("paths"):
        raise SystemExit(f"ERP OpenAPI profile '{profile}' returned no paths.")
    return spec


def get_tools(session: requests.Session, base_url: str) -> list[dict[str, Any]]:
    response = session.get(f"{base_url.rstrip('/')}/api/tool/openapi", timeout=30, verify=VERIFY_TLS)
    return _json_or_raise(response)


def get_personas(session: requests.Session, base_url: str) -> list[dict[str, Any]]:
    response = session.get(f"{base_url.rstrip('/')}/api/persona", timeout=30, verify=VERIFY_TLS)
    return _json_or_raise(response)


def get_persona_detail(session: requests.Session, base_url: str, persona_id: int) -> dict[str, Any]:
    response = session.get(f"{base_url.rstrip('/')}/api/persona/{persona_id}", timeout=30, verify=VERIFY_TLS)
    return _json_or_raise(response)


def upsert_tool(
    session: requests.Session,
    base_url: str,
    tool_id: int | None,
    tool_name: str,
    tool_display_name: str,
    description: str,
    definition: dict[str, Any],
    erp_token: str,
) -> dict[str, Any]:
    payload = {
        "name": tool_name,
        "display_name": tool_display_name,
        "description": description,
        "definition": definition,
        "custom_headers": [{"key": "Authorization", "value": f"Token {erp_token}"}],
        "passthrough_auth": False,
        "oauth_config_id": None,
    }
    if tool_id is None:
        response = session.post(
            f"{base_url.rstrip('/')}/api/admin/tool/custom",
            json=payload,
            timeout=60,
            verify=VERIFY_TLS,
        )
    else:
        response = session.put(
            f"{base_url.rstrip('/')}/api/admin/tool/custom/{tool_id}",
            json=payload,
            timeout=60,
            verify=VERIFY_TLS,
        )
    return _json_or_raise(response)


def build_persona_payload(snapshot: dict[str, Any], tool_ids: list[int]) -> dict[str, Any]:
    payload = {key: snapshot.get(key) for key in PERSONA_ALLOWED_FIELDS if key in snapshot}
    payload["tool_ids"] = tool_ids
    payload.setdefault("document_set_ids", [])
    payload.setdefault("is_public", True)
    payload.setdefault("description", "")
    payload.setdefault("system_prompt", "")
    payload.setdefault("task_prompt", "")
    payload.setdefault("datetime_aware", True)
    payload.setdefault("starter_messages", [])
    payload.setdefault("users", [])
    payload.setdefault("groups", [])
    payload.setdefault("hierarchy_node_ids", [])
    payload.setdefault("document_ids", [])
    return payload


def patch_persona(
    session: requests.Session,
    base_url: str,
    persona_id: int,
    payload: dict[str, Any],
) -> dict[str, Any]:
    response = session.patch(
        f"{base_url.rstrip('/')}/api/persona/{persona_id}",
        json=payload,
        timeout=60,
        verify=VERIFY_TLS,
    )
    return _json_or_raise(response)


def main() -> int:
    onyx_base_url = require_env("ONYX_BASE_URL")
    onyx_admin_email = require_env("ONYX_ADMIN_EMAIL")
    onyx_admin_password = require_env("ONYX_ADMIN_PASSWORD")
    erp_openapi_base_url = require_env("ERP_OPENAPI_BASE_URL")

    session = login_session(onyx_base_url, onyx_admin_email, onyx_admin_password)
    tools = get_tools(session, onyx_base_url)
    personas = get_personas(session, onyx_base_url)

    tools_by_name = {tool["name"]: tool for tool in tools}
    personas_by_name = {persona["name"]: persona for persona in personas}

    results: list[dict[str, Any]] = []

    for spec in ROLE_SPECS:
        erp_token = require_env(spec.erp_token_env)
        definition = fetch_erp_openapi(erp_openapi_base_url, spec.profile, erp_token)
        existing_tool = None
        if spec.preserve_tool_id is not None:
            existing_tool = next((tool for tool in tools if int(tool["id"]) == spec.preserve_tool_id), None)
        if existing_tool is None:
            existing_tool = tools_by_name.get(spec.tool_name)

        tool_snapshot = upsert_tool(
            session=session,
            base_url=onyx_base_url,
            tool_id=int(existing_tool["id"]) if existing_tool else None,
            tool_name=spec.tool_name,
            tool_display_name=spec.tool_display_name,
            description=f"Superficie ERP filtrada para perfil {spec.profile} de Pollyana's Dolce.",
            definition=definition,
            erp_token=erp_token,
        )

        persona = personas_by_name.get(spec.persona_name)
        if not persona:
            raise SystemExit(f"Persona not found in Onyx: {spec.persona_name}")
        persona_detail = get_persona_detail(session, onyx_base_url, int(persona["id"]))
        existing_tool_ids = [int(tool_id) for tool_id in persona_detail.get("tool_ids", [])]
        preserved_tool_ids = [
            tool_id
            for tool_id in existing_tool_ids
            if tool_id not in {11, int(tool_snapshot["id"])}
        ]
        persona_payload = build_persona_payload(
            persona_detail,
            preserved_tool_ids + [int(tool_snapshot["id"])],
        )
        patched_persona = patch_persona(
            session=session,
            base_url=onyx_base_url,
            persona_id=int(persona["id"]),
            payload=persona_payload,
        )
        results.append(
            {
                "profile": spec.profile,
                "tool_id": tool_snapshot["id"],
                "tool_name": tool_snapshot["name"],
                "persona_id": patched_persona["id"],
                "persona_name": patched_persona["name"],
                "tool_ids": patched_persona.get("tool_ids", []),
            }
        )

    print(json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
