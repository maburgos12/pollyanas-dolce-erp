#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Valida una vista del ERP en un contexto aislado de Playwright sin usar el perfil MCP persistente."
    )
    parser.add_argument("--base-url", default="http://localhost:8011", help="Host base oficial del ERP.")
    parser.add_argument("--route", default="/recetas/?vista=productos", help="Ruta a validar después del login.")
    parser.add_argument("--username", default=os.getenv("UI_CHECK_USERNAME", ""), help="Usuario del ERP.")
    parser.add_argument("--password", default=os.getenv("UI_CHECK_PASSWORD", ""), help="Contraseña del ERP.")
    parser.add_argument("--headed", action="store_true", help="Abre el navegador visible.")
    parser.add_argument("--timeout-ms", type=int, default=15000, help="Timeout por paso.")
    parser.add_argument(
        "--output-dir",
        default="output/playwright",
        help="Directorio donde se guardan screenshots y HTML capturado.",
    )
    parser.add_argument(
        "--expect-text",
        default="",
        help="Texto opcional que debe aparecer en la vista final para considerar la validación completa.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.username or not args.password:
        print("Faltan credenciales. Usa --username/--password o UI_CHECK_USERNAME/UI_CHECK_PASSWORD.", file=sys.stderr)
        return 2

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    screenshot_path = output_dir / f"ui_validate_{stamp}.png"
    html_path = output_dir / f"ui_validate_{stamp}.html"

    user_data_dir = tempfile.mkdtemp(prefix="ui-check-profile-")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                headless=not args.headed,
                channel="chrome",
                viewport={"width": 1440, "height": 1200},
            )
            page = browser.pages[0] if browser.pages else browser.new_page()
            page.set_default_timeout(args.timeout_ms)

            login_url = args.base_url.rstrip("/") + "/login/"
            target_url = args.base_url.rstrip("/") + args.route

            page.goto(login_url, wait_until="domcontentloaded")
            page.get_by_role("textbox", name="Usuario").fill(args.username)
            page.get_by_role("textbox", name="Contraseña").fill(args.password)
            page.get_by_role("button", name="Iniciar sesión").click()
            page.wait_for_load_state("networkidle")

            page.goto(target_url, wait_until="networkidle")

            if args.expect_text:
                page.get_by_text(args.expect_text).wait_for(timeout=args.timeout_ms)

            page.screenshot(path=str(screenshot_path), full_page=True)
            html_path.write_text(page.content(), encoding="utf-8")

            print(
                {
                    "url": page.url,
                    "title": page.title(),
                    "screenshot": str(screenshot_path),
                    "html": str(html_path),
                }
            )
            browser.close()
            return 0
    except PlaywrightTimeoutError as exc:
        print(f"Timeout validando UI: {exc}", file=sys.stderr)
        return 3
    finally:
        shutil.rmtree(user_data_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
