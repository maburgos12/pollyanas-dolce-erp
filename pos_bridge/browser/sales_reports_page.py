from __future__ import annotations

from datetime import date, datetime, time

from django.utils import timezone

from pos_bridge.utils.exceptions import NavigationError


class PointSalesReportsPage:
    REPORT_PATH = "/Report/Ventas"
    BRANCHES_ENDPOINT = "/Report/Get_Sucursales"
    SALES_ENDPOINT = "/Report/VentasCategorias"

    def __init__(self, page, bridge_settings):
        self.page = page
        self.settings = bridge_settings

    def open(self) -> None:
        if not self.settings.base_url:
            raise NavigationError("Falta POINT_BASE_URL para abrir reportes de ventas.")
        target_url = f"{self.settings.base_url.rstrip('/')}{self.REPORT_PATH}"
        last_error = None
        self.page.wait_for_timeout(1200)
        for _ in range(3):
            try:
                self.page.goto(target_url, wait_until="domcontentloaded")
                self.page.wait_for_timeout(1200)
                return
            except Exception as exc:
                last_error = exc
                self.page.wait_for_timeout(1200)
        raise NavigationError("No se pudo abrir el reporte de ventas de Point.", context={"error": str(last_error or "")})

    def list_branches(self) -> list[dict]:
        payload = self.page.evaluate(
            """async (endpoint) => {
                const response = await fetch(endpoint, { credentials: 'include' });
                return { status: response.status, text: await response.text() };
            }""",
            self.BRANCHES_ENDPOINT,
        )
        if payload.get("status") != 200:
            raise NavigationError(
                "No se pudieron consultar sucursales de ventas en Point.",
                context={"status": payload.get("status")},
            )
        return self.page.evaluate(
            """(text) => {
                try {
                    return JSON.parse(text || '[]').map((row) => ({
                        external_id: String(row.PK_Sucursal ?? '').trim(),
                        name: String(row.Sucursal ?? '').trim(),
                        short_name: String(row.Sucursal_Corto ?? '').trim(),
                        plaza_id: row.FK_Plaza ?? null,
                    }));
                } catch (error) {
                    return [];
                }
            }""",
            payload.get("text") or "[]",
        )

    def fetch_daily_sales(self, *, branch_external_id: str, sale_date: date) -> list[dict]:
        local_tz = timezone.get_current_timezone()
        start_dt = timezone.make_aware(datetime.combine(sale_date, time.min), local_tz)
        end_dt = timezone.make_aware(datetime.combine(sale_date, time.max), local_tz)
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)
        payload = None
        last_error = None
        for _ in range(3):
            try:
                payload = self.page.evaluate(
                    """async ({ endpoint, startMs, endMs, branchExternalId, timeoutMs }) => {
                        const params = new URLSearchParams({
                            fi: String(startMs),
                            ff: String(endMs),
                            sucursal: branchExternalId,
                            credito: 'null',
                        });
                        const controller = new AbortController();
                        const timer = setTimeout(() => controller.abort(), timeoutMs);
                        try {
                            const response = await fetch(`${endpoint}?${params.toString()}`, {
                                credentials: 'include',
                                signal: controller.signal,
                            });
                            return { status: response.status, text: await response.text(), aborted: false };
                        } catch (error) {
                            return {
                                status: 0,
                                text: '',
                                aborted: error && error.name === 'AbortError',
                                error: String(error || ''),
                            };
                        } finally {
                            clearTimeout(timer);
                        }
                    }""",
                    {
                        "endpoint": self.SALES_ENDPOINT,
                        "startMs": start_ms,
                        "endMs": end_ms,
                        "branchExternalId": str(branch_external_id),
                        "timeoutMs": self.settings.timeout_ms,
                    },
                )
                if payload.get("status") == 200:
                    break
                last_error = payload
                self.page.wait_for_timeout(600)
            except Exception as exc:
                last_error = {"error": str(exc)}
                self.page.wait_for_timeout(600)

        if payload is None or payload.get("status") != 200:
            raise NavigationError(
                "Point rechazó la consulta de ventas históricas.",
                context={
                    "status": payload.get("status") if isinstance(payload, dict) else None,
                    "branch_external_id": branch_external_id,
                    "sale_date": sale_date.isoformat(),
                    "error": last_error,
                },
            )
        return self.page.evaluate(
            """(text) => {
                try {
                    const parsed = JSON.parse(text || '[]');
                    return Array.isArray(parsed) ? parsed : [];
                } catch (error) {
                    return [];
                }
            }""",
            payload.get("text") or "[]",
        )
