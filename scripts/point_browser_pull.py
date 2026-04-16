#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from urllib.parse import urljoin, urlparse

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from pos_bridge.config import load_point_bridge_settings
from pos_bridge.services.point_http_session_service import PointHttpSessionService


def parse_kv_pair(raw: str) -> tuple[str, str]:
    if "=" not in raw:
        raise argparse.ArgumentTypeError(f"Formato inválido: {raw}. Usa selector=valor.")
    key, value = raw.split("=", 1)
    key = key.strip()
    if not key:
        raise argparse.ArgumentTypeError(f"Selector vacío en: {raw}")
    return key, value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Abre Point ya autenticado, permite llenar/clickear y descargar archivos desde la UI real."
    )
    parser.add_argument("--path", default="/Home/Index", help="Ruta interna de Point a abrir después de autenticar.")
    parser.add_argument("--branch", default="", help="ID externo de sucursal Point.")
    parser.add_argument("--branch-name", default="", help="Nombre visible de sucursal Point.")
    parser.add_argument("--headed", action="store_true", help="Abre navegador visible.")
    parser.add_argument("--timeout-ms", type=int, default=20000, help="Timeout por paso.")
    parser.add_argument("--wait-ms", type=int, default=0, help="Espera fija adicional antes de capturar artefactos.")
    parser.add_argument("--wait-selector", default="", help="Selector CSS a esperar tras abrir la pantalla.")
    parser.add_argument("--wait-text", default="", help="Texto a esperar tras abrir o interactuar.")
    parser.add_argument("--expect-text", default="", help="Texto final esperado para validar el flujo.")
    parser.add_argument("--fill", action="append", default=[], type=parse_kv_pair, help="Llena selector=valor.")
    parser.add_argument("--type", action="append", default=[], type=parse_kv_pair, help="Escribe selector=valor sin limpiar.")
    parser.add_argument("--click", action="append", default=[], help="Hace click en el selector CSS indicado.")
    parser.add_argument("--js", action="append", default=[], help="Ejecuta JavaScript en la página autenticada y captura el resultado.")
    parser.add_argument("--js-file", action="append", default=[], help="Ruta a archivo .js con código a evaluar en la página autenticada.")
    parser.add_argument(
        "--js-payload",
        default="{}",
        help="JSON opcional que se pasa como argumento a snippets JS que exportan una función.",
    )
    parser.add_argument("--download-selector", default="", help="Selector CSS que detona la descarga.")
    parser.add_argument("--download-name", default="", help="Nombre opcional del archivo descargado.")
    parser.add_argument("--output-dir", default="output/point_browser", help="Directorio de artefactos.")
    return parser.parse_args()


def build_cookies(cookie_jar, *, base_url: str) -> list[dict]:
    parsed = urlparse(base_url)
    host = parsed.hostname or ""
    cookies: list[dict] = []
    for cookie in cookie_jar:
        payload = {
            "name": cookie.name,
            "value": cookie.value,
            "domain": (cookie.domain or host).lstrip("."),
            "path": cookie.path or "/",
            "secure": bool(cookie.secure),
        }
        if cookie.expires:
            payload["expires"] = int(cookie.expires)
        cookies.append(payload)
    return cookies


def main() -> int:
    args = parse_args()
    settings = load_point_bridge_settings()
    auth_service = PointHttpSessionService(settings)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    auth_session = auth_service.create(
        branch_external_id=args.branch.strip() or None,
        branch_display_name=args.branch_name.strip() or None,
    )

    user_data_dir = tempfile.mkdtemp(prefix="point-browser-")
    downloads_dir = tempfile.mkdtemp(prefix="point-downloads-")
    try:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                headless=not args.headed,
                ignore_https_errors=True,
                accept_downloads=True,
                downloads_path=downloads_dir,
                viewport={"width": 1440, "height": 1200},
            )
            context.set_default_timeout(args.timeout_ms)
            cookies = build_cookies(auth_session.session.cookies, base_url=settings.base_url)
            if cookies:
                context.add_cookies(cookies)

            page = context.pages[0] if context.pages else context.new_page()
            target_url = urljoin(settings.base_url.rstrip("/") + "/", args.path.lstrip("/"))
            page.goto(target_url, wait_until="domcontentloaded")

            if args.wait_selector:
                page.locator(args.wait_selector).wait_for(timeout=args.timeout_ms)
            if args.wait_text:
                page.get_by_text(args.wait_text).wait_for(timeout=args.timeout_ms)

            for selector, value in args.fill:
                page.locator(selector).fill(value)
            for selector, value in args.type:
                page.locator(selector).type(value)
            for selector in args.click:
                page.locator(selector).click()
                page.wait_for_load_state("domcontentloaded")

            if args.wait_ms > 0:
                page.wait_for_timeout(args.wait_ms)

            snippets = list(args.js)
            for js_file in args.js_file:
                snippets.append(Path(js_file).read_text(encoding="utf-8"))

            try:
                js_payload = json.loads(args.js_payload or "{}")
            except json.JSONDecodeError as exc:
                print(f"JSON inválido en --js-payload: {exc}")
                return 2

            js_results: list[object] = []
            for snippet in snippets:
                js_results.append(
                    page.evaluate(
                        """
                        async ({ snippet, payload }) => {
                          const candidate = eval(snippet);
                          if (typeof candidate === "function") {
                            return await candidate(payload);
                          }
                          return await Promise.resolve(candidate);
                        }
                        """,
                        {"snippet": snippet, "payload": js_payload},
                    )
                )

            download_path = ""
            if args.download_selector:
                with page.expect_download(timeout=args.timeout_ms) as pending_download:
                    page.locator(args.download_selector).click()
                download = pending_download.value
                suggested_name = args.download_name.strip() or download.suggested_filename
                final_download_path = output_dir / suggested_name
                download.save_as(str(final_download_path))
                download_path = str(final_download_path)

            if args.expect_text:
                page.get_by_text(args.expect_text).wait_for(timeout=args.timeout_ms)

            screenshot_path = output_dir / "point_browser_last.png"
            html_path = output_dir / "point_browser_last.html"
            page.screenshot(path=str(screenshot_path), full_page=True)
            html_path.write_text(page.content(), encoding="utf-8")

            print(
                json.dumps(
                    {
                        "url": page.url,
                        "title": page.title(),
                        "screenshot": str(screenshot_path),
                        "html": str(html_path),
                        "download": download_path,
                        "js_results": js_results,
                    },
                    ensure_ascii=False,
                )
            )
            context.close()
        return 0
    except PlaywrightTimeoutError as exc:
        print(f"Timeout operando Point en navegador: {exc}")
        return 2
    finally:
        try:
            auth_session.session.close()
        except Exception:  # noqa: BLE001
            pass
        shutil.rmtree(user_data_dir, ignore_errors=True)
        shutil.rmtree(downloads_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
