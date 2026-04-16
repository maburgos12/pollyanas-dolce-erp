#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from pathlib import Path


def _load_guard_module():
    repo_root = Path(__file__).resolve().parent.parent
    module_path = repo_root / "orquestacion" / "services" / "protected_sales_reader_guard.py"
    spec = importlib.util.spec_from_file_location("protected_sales_reader_guard", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"No se pudo cargar el guard desde {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def main() -> int:
    parser = argparse.ArgumentParser(description="Valida lectores crudos de ventas en rutas protegidas del ERP.")
    parser.add_argument("--base-dir", default=str(Path(__file__).resolve().parent.parent), help="Ruta raíz del repo.")
    args = parser.parse_args()

    if os.environ.get("PROTECTED_SALES_READER_GUARD_ENABLED", "1") == "0":
        print("Protected sales reader guard disabled by PROTECTED_SALES_READER_GUARD_ENABLED=0")
        return 0

    module = _load_guard_module()
    result = module.scan_protected_sales_reader_usage(base_dir=args.base_dir)
    if result.has_violations:
        print(
            f"Protected sales reader guard encontró {len(result.violations)} violación(es) "
            f"en {result.checked_files} archivo(s) protegido(s):"
        )
        for violation in result.violations:
            print(
                f"- {violation.relative_path}:{violation.line_number} [{violation.symbol}] | "
                f"{violation.reason} | {violation.suggestion}"
            )
        return 1

    print(
        f"Protected sales reader guard OK: {result.checked_files} archivos protegidos revisados, "
        "sin violaciones."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
