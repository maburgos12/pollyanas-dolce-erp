from __future__ import annotations

from syncfy_client.services.base import SyncfyClient, get_syncfy_config


def crear_usuario_pollyanas(*, name: str = "pollyanas_dolce", client: SyncfyClient | None = None) -> str:
    config = client.config if client is not None else get_syncfy_config(require_id_user=False)
    client = client or SyncfyClient(config=config)
    response = client.post("/users", json={"name": name}, api_key_auth=True)
    id_user = ""
    if isinstance(response, dict):
        id_user = str(response.get("id_user") or "")
    if not id_user:
        raise ValueError("La respuesta de Syncfy no contiene id_user")
    return id_user
