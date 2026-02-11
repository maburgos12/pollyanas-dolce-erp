import csv
from datetime import datetime
from pathlib import Path

def _write_dicts(path: Path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

def generar_reportes(base_dir: Path, resumen: dict, errores: list, pendientes: list):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    logs_dir = base_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    # resumen
    resumen_path = logs_dir / f"import_summary_{ts}.csv"
    with open(resumen_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Metrica", "Valor"])
        for k, v in resumen.items():
            w.writerow([k, v])

    if errores:
        _write_dicts(logs_dir / f"import_errors_{ts}.csv", errores, ["sheet", "error"])

    if pendientes:
        _write_dicts(logs_dir / f"import_pending_matches_{ts}.csv", pendientes, ["receta", "ingrediente", "score", "method", "status"])
    return {
        "summary": str(resumen_path),
    }
