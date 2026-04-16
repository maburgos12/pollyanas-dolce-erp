#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT_DIR / "orquestacion" / "services" / "pointdailysale_guard.py"
SPEC = importlib.util.spec_from_file_location("pointdailysale_guard_script", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"No se pudo cargar {MODULE_PATH}")
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)
scan_pointdailysale_usage = MODULE.scan_pointdailysale_usage


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Detecta lecturas directas no autorizadas de PointDailySale fuera de la allowlist canónica."
    )
    parser.add_argument(
        "--base-dir",
        default=str(ROOT_DIR),
        help="Ruta raíz del repo a revisar. Por defecto usa el workspace actual.",
    )
    args = parser.parse_args()

    if os.getenv("POINTDAILYSALE_GUARD_ENABLED", "1").strip().lower() in {"0", "false", "no", "off"}:
        print("PointDailySale guard desactivado por POINTDAILYSALE_GUARD_ENABLED=0")
        return 0

    result = scan_pointdailysale_usage(base_dir=args.base_dir)
    if not result.has_violations:
        print(f"PointDailySale guard OK: {result.checked_files} archivos revisados, sin violaciones.")
        return 0

    print(
        "PointDailySale guard BLOCKED: se detectaron lecturas directas fuera de la allowlist canónica.",
        file=sys.stderr,
    )
    for violation in result.violations:
        print(
            f"- {violation.relative_path}:{violation.line_number} | {violation.reason} | "
            f"Sugerencia: {violation.suggestion}",
            file=sys.stderr,
        )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
