from __future__ import annotations

from syncfy_client.services.base import SyncfyClient, get_syncfy_config


def obtener_token(*, client: SyncfyClient | None = None) -> str:
    config = client.config if client is not None else get_syncfy_config(require_id_user=True)
    client = client or SyncfyClient(config=config)
    response = client.post(
        "/sessions",
        json={"api_key": config.api_key, "id_user": config.id_user},
    )
    token = ""
    if isinstance(response, dict):
        token = str(response.get("token") or "")
    if not token:
        raise ValueError("La respuesta de Syncfy no contiene token")
    return token
