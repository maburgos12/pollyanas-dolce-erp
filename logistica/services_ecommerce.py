from __future__ import annotations

import time
from typing import Any

import requests
from django.conf import settings


class EcommerceIntegrationError(RuntimeError):
    """La tienda en línea no está configurada o no respondió correctamente."""


def _ensure_configured() -> None:
    if not settings.ECOMMERCE_API_BASE_URL or not settings.ECOMMERCE_SERVICE_EMAIL or not settings.ECOMMERCE_SERVICE_PASSWORD:
        raise EcommerceIntegrationError(
            "Configura ECOMMERCE_API_BASE_URL, ECOMMERCE_SERVICE_EMAIL y ECOMMERCE_SERVICE_PASSWORD."
        )


class EcommerceClient:
    """Cliente HTTP hacia el backend FastAPI de pollyanas-ecommerce.

    Volumen bajo (solo cuando un despachador asigna una entrega): se reloguea en
    cada llamada en vez de cachear el JWT — simplificación deliberada v1.
    """

    def __init__(self) -> None:
        _ensure_configured()
        self.base_url = settings.ECOMMERCE_API_BASE_URL.rstrip("/")
        self.timeout = settings.ECOMMERCE_API_TIMEOUT_SECONDS

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    def _request(self, method: str, path: str, *, token: str | None = None, **kwargs) -> dict[str, Any]:
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        last_error: Exception | None = None
        for attempt in range(1, 3):
            try:
                response = requests.request(
                    method, self._url(path), timeout=self.timeout, headers=headers, **kwargs
                )
                if response.status_code >= 500 and attempt < 2:
                    time.sleep(1)
                    continue
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= 2:
                    break
                time.sleep(1)

        raise EcommerceIntegrationError(f"No se pudo conectar con la tienda en línea: {last_error}")

    def _login(self) -> str:
        payload = self._request(
            "POST",
            "/api/auth/login",
            json={"email": settings.ECOMMERCE_SERVICE_EMAIL, "password": settings.ECOMMERCE_SERVICE_PASSWORD},
        )
        token = payload.get("access_token")
        if not token:
            raise EcommerceIntegrationError("La tienda en línea no devolvió un token de acceso.")
        return token

    def listar_pedidos_pendientes(self) -> list[dict[str, Any]]:
        """Pedidos de domicilio del e-commerce que todavía no tienen repartidor asignado."""
        token = self._login()
        orders = self._request("GET", "/api/orders/admin", token=token, params={"limit": 200})
        return [
            order
            for order in orders
            if order.get("delivery_type") == "delivery"
            and order.get("status") not in ("delivered", "cancelled")
            and (order.get("needs_delivery_assignment") or not (order.get("delivery_task") or {}).get("driver_id"))
        ]

    def asignar(
        self,
        *,
        order_id: int,
        erp_repartidor_id: str,
        repartidor_name: str,
        repartidor_phone: str,
        erp_unidad_id: str,
        unidad_code: str,
        unidad_type: str,
        unidad_plate: str,
    ) -> dict[str, Any]:
        """Devuelve {task_id, driver_access_token, driver_url}."""
        token = self._login()
        return self._request(
            "POST",
            f"/api/tracking/admin/order/{order_id}/assign-external",
            token=token,
            json={
                "erp_repartidor_id": erp_repartidor_id,
                "repartidor_name": repartidor_name,
                "repartidor_phone": repartidor_phone,
                "erp_unidad_id": erp_unidad_id,
                "unidad_code": unidad_code,
                "unidad_type": unidad_type,
                "unidad_plate": unidad_plate,
            },
        )
