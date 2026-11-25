#!/usr/bin/env python3
"""Exercise native Python import resolution in serial and parallel-size projects.

Uses static fixtures only; does not import or execute their Python code. Temporary
indexes/config and fixture files are removed on success or failure.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile


def verify(binary):
    results = []
    with tempfile.TemporaryDirectory(prefix="cbm-relative-") as directory:
        lab = Path(directory)
        env = dict(os.environ, CBM_CACHE_DIR=str(lab / "cache"),
                   XDG_CONFIG_HOME=str(lab / "config"))

        def call(tool, arguments):
            proc = subprocess.run([str(binary), "cli", tool, json.dumps(arguments)],
                                  env=env, text=True, capture_output=True, timeout=180)
            if proc.returncode:
                raise RuntimeError(f"{tool}: {proc.stderr[-2000:]}")
            result = json.loads(proc.stdout)
            if "error" in result:
                raise RuntimeError(result)
            return result

        for padding in (0, 150):
            repo = lab / f"fixture-{padding}"
            files = {"pkg/__init__.py": "", "pkg/nested/__init__.py": "",
                     "pkg/services_costos.py": "def asignar_costos_filas(rows):\n    return rows\n",
                     "other/services_costos.py": "def asignar_costos_filas(rows):\n    return []\n",
                     "other/__init__.py": ""}
            for i in range(9):
                if i < 4:
                    path, module = f"pkg/reader{i}.py", "pkg.services_costos"
                elif i < 8:
                    path, module = f"pkg/reader{i}.py", ".services_costos"
                else:
                    path, module = "pkg/nested/reader8.py", "..services_costos"
                files[path] = f"from {module} import asignar_costos_filas\ndef reader{i}(rows):\n    return asignar_costos_filas(rows)\n"
            files["pkg/external.py"] = "from unavailable.services_costos import asignar_costos_filas\ndef external(rows):\n    return asignar_costos_filas(rows)\n"
            for i in range(padding):
                files[f"padding/pad{i}.py"] = f"def irrelevant{i}():\n    return {i}\n"
            for relative, source in files.items():
                path = repo / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(source)
            indexed = call("index_repository", {"repo_path": str(repo), "mode": "fast", "persistence": False})
            project = indexed["project"]
            result = call("query_graph", {"project": project, "query": "MATCH (a)-[:CALLS]->(b) WHERE b.qualified_name = '" + project + ".pkg.services_costos.asignar_costos_filas' RETURN a.qualified_name"})
            actual = {row[0].removeprefix(project + ".") for row in result["rows"]}
            expected = {f"pkg.reader{i}.reader{i}" for i in range(8)} | {"pkg.nested.reader8.reader8"}
            wrong = call("query_graph", {"project": project, "query": "MATCH (a)-[:CALLS]->(b) WHERE b.qualified_name = '" + project + ".other.services_costos.asignar_costos_filas' RETURN a.qualified_name"})
            results.append({"padding": padding, "callers": sorted(actual),
                            "missing": sorted(expected - actual), "unexpected": sorted(actual - expected),
                            "homonym_false_edges": len(wrong["rows"])})
    if any(r["missing"] or r["unexpected"] or r["homonym_false_edges"] for r in results):
        raise AssertionError(results)
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.binary.resolve()), indent=2))
