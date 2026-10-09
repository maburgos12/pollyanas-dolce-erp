"""Local review server. Generates short-lived laboratory JWTs, never an OpenAI key."""
import json
import os
import secrets
import time
import jwt
import uvicorn
from runtime import ARTIFACTS, build_app

SCOPES = ["agents:read", "teams:read", "config:read", "sessions:read", "sessions:write",
          "agents:erp-read-lab:run", "agents:maya-read-lab:run"]


if __name__ == "__main__":
    key = secrets.token_urlsafe(48)
    tokens = {user: jwt.encode({"sub": user, "scopes": scopes, "exp": int(time.time()) + 3600}, key, algorithm="HS256")
        for user, scopes in [("lab-manager", SCOPES), ("lab-reader", ["agents:read", "sessions:read"])]}
    token_path = ARTIFACTS / "temporary-review-tokens.json"
    descriptor = os.open(token_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as file:
        json.dump(tokens, file)
    uvicorn.run(build_app(key), host="127.0.0.1", port=7779)
