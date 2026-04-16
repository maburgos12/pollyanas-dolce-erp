#!/usr/bin/env python3
from __future__ import annotations

import ast
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from orquestacion.tool_binding import resolve_gateway_tool_alias

CATALOG_PATH = ROOT / "orquestacion" / "catalog.py"
RUNNERS_PATH = ROOT / "orquestacion" / "services" / "rule_runners.py"
AGENT_RUNTIME_PATH = ROOT / "orquestacion" / "services" / "agent_runtime.py"
GATEWAY_PATH = ROOT / "api" / "ai_gateway_services.py"
SCHEDULES_PATH = ROOT / "pos_bridge" / "management" / "commands" / "setup_celery_schedules.py"


def _read_ast(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _literal_constant(path: Path, constant_name: str) -> Any:
    tree = _read_ast(path)
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == constant_name:
                    return ast.literal_eval(node.value)
    raise RuntimeError(f"No se encontro la constante {constant_name} en {path}.")


def _extract_tools(path: Path) -> dict[str, dict[str, Any]]:
    tree = _read_ast(path)
    for node in tree.body:
        if isinstance(node, ast.Assign):
            is_tools = any(isinstance(target, ast.Name) and target.id == "TOOLS" for target in node.targets)
            value = node.value
        elif isinstance(node, ast.AnnAssign):
            is_tools = isinstance(node.target, ast.Name) and node.target.id == "TOOLS"
            value = node.value
        else:
            continue
        if not is_tools:
            continue
        if not isinstance(value, ast.Dict):
            raise RuntimeError("TOOLS no es un dict literal en ai_gateway_services.py")
        tools: dict[str, dict[str, Any]] = {}
        for key_node, value_node in zip(value.keys, value.values):
            if not isinstance(key_node, ast.Constant) or not isinstance(key_node.value, str):
                continue
            tool_key = key_node.value
            metadata: dict[str, Any] = {"key": tool_key}
            if isinstance(value_node, ast.Call):
                for kw in value_node.keywords:
                    if kw.arg in {"name", "description", "operation_type", "data_domain"}:
                        if isinstance(kw.value, ast.Constant):
                            metadata[kw.arg] = kw.value.value
                    elif kw.arg in {"branch_scoped", "requires_approval"}:
                        if isinstance(kw.value, ast.Constant):
                            metadata[kw.arg] = bool(kw.value.value)
            tools[tool_key] = metadata
        return tools
    raise RuntimeError("No se encontro TOOLS en ai_gateway_services.py")


def _extract_orchestration_schedules(path: Path) -> list[dict[str, Any]]:
    tree = _read_ast(path)
    schedules: list[dict[str, Any]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (
            isinstance(func, ast.Attribute)
            and func.attr == "update_or_create"
            and isinstance(func.value, ast.Attribute)
            and func.value.attr == "objects"
        ):
            continue

        name = None
        defaults: ast.Dict | None = None
        for kw in node.keywords:
            if kw.arg == "name" and isinstance(kw.value, ast.Constant):
                name = kw.value.value
            elif kw.arg == "defaults" and isinstance(kw.value, ast.Dict):
                defaults = kw.value
        if not isinstance(name, str) or not name.startswith("orquestacion:") or defaults is None:
            continue

        details: dict[str, Any] = {"name": name, "task": "", "rule_code": "", "schedule_kind": "unknown"}
        defaults_pairs = {}
        for key_node, value_node in zip(defaults.keys, defaults.values):
            if isinstance(key_node, ast.Constant) and isinstance(key_node.value, str):
                defaults_pairs[key_node.value] = value_node

        task_node = defaults_pairs.get("task")
        if isinstance(task_node, ast.Constant) and isinstance(task_node.value, str):
            details["task"] = task_node.value

        kwargs_node = defaults_pairs.get("kwargs")
        if isinstance(kwargs_node, ast.Call) and isinstance(kwargs_node.func, ast.Attribute):
            if kwargs_node.func.attr == "dumps" and kwargs_node.args and isinstance(kwargs_node.args[0], ast.Dict):
                kwargs_dict = {
                    key.value: value.value
                    for key, value in zip(kwargs_node.args[0].keys, kwargs_node.args[0].values)
                    if isinstance(key, ast.Constant) and isinstance(value, ast.Constant)
                }
                details["rule_code"] = str(kwargs_dict.get("rule_code") or "")

        crontab_node = defaults_pairs.get("crontab")
        interval_node = defaults_pairs.get("interval")
        if isinstance(crontab_node, ast.Constant) and crontab_node.value is None:
            if interval_node is not None:
                details["schedule_kind"] = "interval"
        elif crontab_node is not None:
            details["schedule_kind"] = "crontab"
        schedules.append(details)

    schedules.sort(key=lambda row: row["name"])
    return schedules


def _extract_goal_handlers(path: Path) -> list[dict[str, Any]]:
    tree = _read_ast(path)
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef) or node.name != "_goal_handlers":
            continue
        for inner in node.body:
            if not isinstance(inner, ast.Return) or not isinstance(inner.value, ast.Dict):
                continue
            handlers: list[dict[str, Any]] = []
            for key_node, value_node in zip(inner.value.keys, inner.value.values):
                if not isinstance(key_node, ast.Constant) or not isinstance(key_node.value, str):
                    continue
                goal_type = key_node.value
                if not isinstance(value_node, ast.Call):
                    continue
                handler: dict[str, Any] = {
                    "goal_type": goal_type,
                    "agent_code": "",
                    "has_executor": False,
                    "handoff_targets": [],
                    "tool_hints": [],
                }
                for kw in value_node.keywords:
                    if kw.arg == "agent_code" and isinstance(kw.value, ast.Constant):
                        handler["agent_code"] = str(kw.value.value)
                    elif kw.arg == "executor":
                        handler["has_executor"] = not (
                            isinstance(kw.value, ast.Constant) and kw.value.value is None
                        )
                    elif kw.arg == "handoff_targets" and isinstance(kw.value, (ast.List, ast.Tuple)):
                        handler["handoff_targets"] = [
                            elt.value for elt in kw.value.elts if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
                        ]
                    elif kw.arg == "tool_hints" and isinstance(kw.value, ast.List):
                        for entry in kw.value.elts:
                            if not isinstance(entry, ast.Dict):
                                continue
                            hint = {
                                key.value: value.value
                                for key, value in zip(entry.keys, entry.values)
                                if isinstance(key, ast.Constant) and isinstance(key.value, str)
                                and isinstance(value, ast.Constant)
                            }
                            if hint:
                                handler["tool_hints"].append(hint)
                handlers.append(handler)
            return handlers
    raise RuntimeError("No se encontro _goal_handlers en agent_runtime.py")


def build_snapshot() -> dict[str, Any]:
    agents = _literal_constant(CATALOG_PATH, "AGENTS")
    capabilities = _literal_constant(CATALOG_PATH, "CAPABILITIES")
    rules = _literal_constant(CATALOG_PATH, "RULES")
    supported_rules = set(_literal_constant(RUNNERS_PATH, "SUPPORTED_RULE_CODES"))
    goal_handlers = _extract_goal_handlers(AGENT_RUNTIME_PATH)
    tools = _extract_tools(GATEWAY_PATH)
    schedules = _extract_orchestration_schedules(SCHEDULES_PATH)

    capability_count_by_agent = defaultdict(int)
    for capability in capabilities:
        capability_count_by_agent[capability["agent_code"]] += 1

    rules_by_primary = defaultdict(list)
    rules_by_secondary = defaultdict(list)
    for rule in rules:
        rules_by_primary[rule["primary_agent_code"]].append(rule)
        if rule.get("secondary_agent_code"):
            rules_by_secondary[rule["secondary_agent_code"]].append(rule)

    schedules_by_rule = {row["rule_code"]: row for row in schedules if row["rule_code"]}
    goal_handlers_by_agent = defaultdict(list)
    for handler in goal_handlers:
        goal_handlers_by_agent[handler["agent_code"]].append(handler)

    agent_rows = []
    for agent in agents:
        primary_rules = rules_by_primary[agent["code"]]
        secondary_rules = rules_by_secondary[agent["code"]]
        primary_rule_codes = [rule["code"] for rule in primary_rules]
        secondary_rule_codes = [rule["code"] for rule in secondary_rules]
        runnable_primary_rules = sorted(code for code in primary_rule_codes if code in supported_rules)
        runnable_secondary_rules = sorted(code for code in secondary_rule_codes if code in supported_rules)
        scheduled_primary_rules = sorted(code for code in primary_rule_codes if code in schedules_by_rule)
        exact_gateway_tool_matches = sorted(set(agent.get("allowed_tools_json", [])) & set(tools.keys()))
        resolved_gateway_tool_matches = sorted(
            {
                resolved
                for declared_tool_key in agent.get("allowed_tools_json", [])
                if (resolved := resolve_gateway_tool_alias(declared_tool_key, available_keys=tools.keys()))
            }
        )
        runtime_goal_handlers = goal_handlers_by_agent[agent["code"]]
        runtime_goal_types = sorted(handler["goal_type"] for handler in runtime_goal_handlers)
        runtime_goal_types_declared = sorted(agent.get("supported_goal_types_json", []))
        if runnable_primary_rules or runtime_goal_handlers:
            runtime_state = "runtime_parcial"
        elif runnable_secondary_rules:
            runtime_state = "participacion_indirecta"
        else:
            runtime_state = "solo_catalogo"
        agent_rows.append(
            {
                "code": agent["code"],
                "name": agent["name"],
                "domain": agent["domain"],
                "system_prompt_version": agent.get("system_prompt_version") or "",
                "declared_capabilities_count": capability_count_by_agent[agent["code"]],
                "primary_rules": sorted(primary_rule_codes),
                "secondary_rules": sorted(secondary_rule_codes),
                "runnable_primary_rules": runnable_primary_rules,
                "runnable_secondary_rules": runnable_secondary_rules,
                "scheduled_primary_rules": scheduled_primary_rules,
                "runtime_goal_types_declared": runtime_goal_types_declared,
                "runtime_goal_handlers": runtime_goal_types,
                "allowed_tools_declared": agent.get("allowed_tools_json", []),
                "exact_gateway_tool_matches": exact_gateway_tool_matches,
                "resolved_gateway_tool_matches": resolved_gateway_tool_matches,
                "allowed_actions_declared": agent.get("allowed_actions_json", []),
                "runtime_state": runtime_state,
            }
        )

    rule_rows = []
    for rule in rules:
        rule_rows.append(
            {
                "code": rule["code"],
                "primary_agent_code": rule["primary_agent_code"],
                "secondary_agent_code": rule.get("secondary_agent_code") or "",
                "trigger_type": rule["trigger_type"],
                "action_mode": rule["action_mode"],
                "runner_implemented": rule["code"] in supported_rules,
                "scheduled": rule["code"] in schedules_by_rule,
                "schedule_name": schedules_by_rule.get(rule["code"], {}).get("name", ""),
            }
        )

    operation_types = defaultdict(int)
    approval_count = 0
    for tool in tools.values():
        operation_types[str(tool.get("operation_type") or "unknown")] += 1
        if tool.get("requires_approval"):
            approval_count += 1

    return {
        "generated_at": datetime.now().astimezone().isoformat(),
        "sources": {
            "catalog": str(CATALOG_PATH.relative_to(ROOT)),
            "runners": str(RUNNERS_PATH.relative_to(ROOT)),
            "agent_runtime": str(AGENT_RUNTIME_PATH.relative_to(ROOT)),
            "gateway": str(GATEWAY_PATH.relative_to(ROOT)),
            "schedules": str(SCHEDULES_PATH.relative_to(ROOT)),
        },
        "counts": {
            "agents_declared": len(agents),
            "capabilities_declared": len(capabilities),
            "rules_declared": len(rules),
            "supported_rules": len(supported_rules),
            "runtime_goal_handlers": len(goal_handlers),
            "gateway_tools": len(tools),
            "orchestration_schedules": len(schedules),
            "gateway_tools_requiring_approval": approval_count,
        },
        "gateway_operation_types": dict(sorted(operation_types.items())),
        "agents": sorted(agent_rows, key=lambda row: row["code"]),
        "rules": sorted(rule_rows, key=lambda row: row["code"]),
        "runtime_goal_handlers": sorted(goal_handlers, key=lambda row: row["goal_type"]),
        "gateway_tools": [tools[key] for key in sorted(tools)],
        "orchestration_schedules": schedules,
        "derived_findings": {
            "agents_with_exact_gateway_tool_match": sorted(
                [row["code"] for row in agent_rows if row["exact_gateway_tool_matches"]]
            ),
            "agents_with_resolved_gateway_tool_match": sorted(
                [row["code"] for row in agent_rows if row["resolved_gateway_tool_matches"]]
            ),
            "rules_declared_without_runner": sorted(
                [row["code"] for row in rule_rows if not row["runner_implemented"]]
            ),
            "rules_with_runner_without_schedule": sorted(
                [row["code"] for row in rule_rows if row["runner_implemented"] and not row["scheduled"]]
            ),
            "agents_without_runnable_primary_rules": sorted(
                [row["code"] for row in agent_rows if not row["runnable_primary_rules"] and not row["runtime_goal_handlers"]]
            ),
            "agents_with_runtime_goal_handlers": sorted(
                [row["code"] for row in agent_rows if row["runtime_goal_handlers"]]
            ),
        },
    }


def main() -> int:
    snapshot = build_snapshot()
    print(json.dumps(snapshot, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
