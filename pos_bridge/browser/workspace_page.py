from __future__ import annotations

from pos_bridge.browser.waits import wait_for_any
from pos_bridge.selectors.workspace_selectors import WORKSPACE_PAGE_MARKERS
from pos_bridge.utils.exceptions import NavigationError
from pos_bridge.utils.helpers import normalize_text, select_candidates


class PointWorkspacePage:
    def __init__(self, page, bridge_settings):
        self.page = page
        self.settings = bridge_settings

    def wait_until_loaded(self) -> None:
        wait_for_any(
            self.page,
            select_candidates(self.settings.selector_overrides, "workspace.page_markers", WORKSPACE_PAGE_MARKERS),
            "pantalla de sucursales Point",
            timeout_ms=self.settings.timeout_ms,
        )

    def list_workspaces(self) -> list[dict]:
        return self.page.evaluate(
            """() => Array.from(document.querySelectorAll('[onclick*="selWS"]'))
                .map((node) => {
                    const onclick = node.getAttribute('onclick') || '';
                    const text = (node.textContent || '').trim();
                    const containerText = (node.parentElement?.parentElement?.innerText || '').trim();
                    return {
                        onclick,
                        text,
                        containerText,
                        tag: node.tagName,
                    };
                })
                .filter((item) => item.onclick.includes('selWS(') && !item.onclick.match(/selWS\\(\"[^\"]+\"\\)$/))
            """
        )

    def select_workspace(self, branch_hint: str | None = None) -> dict:
        self.wait_until_loaded()
        workspaces = self.list_workspaces()
        if not workspaces:
            raise NavigationError("No se detectaron tarjetas de sucursal en Point.")

        selected = None
        if branch_hint:
            target = normalize_text(branch_hint)
            for workspace in workspaces:
                if target and target in normalize_text(workspace.get("containerText", "")):
                    selected = workspace
                    break
        if selected is None:
            selected = workspaces[0]

        onclick = selected["onclick"]
        self.page.evaluate(
            """(targetOnclick) => {
                const el = Array.from(document.querySelectorAll('[onclick*="selWS"]'))
                    .find((node) => (node.getAttribute('onclick') || '') === targetOnclick);
                if (!el) {
                    throw new Error(`workspace_not_found:${targetOnclick}`);
                }
                el.click();
            }""",
            onclick,
        )
        try:
            self.page.wait_for_load_state("networkidle", timeout=self.settings.timeout_ms)
        except Exception:
            pass
        return {
            "workspace_onclick": onclick,
            "workspace_label": selected.get("containerText") or selected.get("text") or "",
        }
