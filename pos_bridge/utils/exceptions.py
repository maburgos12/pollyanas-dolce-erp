from __future__ import annotations


class PosBridgeError(Exception):
    def __init__(self, message: str, *, context: dict | None = None):
        super().__init__(message)
        self.context = context or {}


class ConfigurationError(PosBridgeError):
    pass


class AuthenticationError(PosBridgeError):
    pass


class NavigationError(PosBridgeError):
    pass


class ExtractionError(PosBridgeError):
    pass


class PersistenceError(PosBridgeError):
    pass
