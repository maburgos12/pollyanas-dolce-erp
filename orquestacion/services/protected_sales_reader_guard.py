from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProtectedSalesReaderRule:
    relative_path: str
    forbidden_symbols: tuple[str, ...]
    reason: str
    suggestion: str


PROTECTED_SALES_READER_RULES = (
    ProtectedSalesReaderRule(
        relative_path="api/ai_gateway_services.py",
        forbidden_symbols=("PointDailySale", "VentaHistorica", "FactVentaDiaria"),
        reason=(
            "Gateway ERP de ventas no debe leer modelos crudos; debe consumir la capa "
            "canonica compartida del ERP."
        ),
        suggestion=(
            "Usa ventas/services/sales_read_service.py o ventas/services/sales_canonical_source.py "
            "para responder ventas en el gateway."
        ),
    ),
    ProtectedSalesReaderRule(
        relative_path="reportes/dashboard_sales_dataset.py",
        forbidden_symbols=("PointDailySale", "VentaHistorica", "FactVentaDiaria"),
        reason=(
            "El dataset visible del dashboard no debe depender de modelos crudos; debe "
            "resolver ventas desde la capa compartida canonica."
        ),
        suggestion=(
            "Usa ventas/services/sales_read_service.py o ventas/services/sales_canonical_source.py "
            "en lugar de lectores crudos en datasets visibles."
        ),
    ),
    ProtectedSalesReaderRule(
        relative_path="pos_bridge/services/agent_query_service.py",
        forbidden_symbols=("PointDailySale", "FactVentaDiaria"),
        reason=(
            "Las consultas operativas de agentes no deben leer PointDailySale ni FactVentaDiaria "
            "directamente; deben consumir agregados canonicos compartidos."
        ),
        suggestion=(
            "Usa ventas/services/sales_read_service.py para agregados canonicos; "
            "VentaHistorica solo permanece valida para reconciliacion explicita."
        ),
    ),
)


@dataclass(frozen=True)
class ProtectedSalesReaderViolation:
    relative_path: str
    line_number: int
    symbol: str
    reason: str
    suggestion: str


@dataclass(frozen=True)
class ProtectedSalesReaderScanResult:
    checked_files: int
    violations: tuple[ProtectedSalesReaderViolation, ...]

    @property
    def has_violations(self) -> bool:
        return bool(self.violations)


def scan_protected_sales_reader_usage(*, base_dir: str | Path) -> ProtectedSalesReaderScanResult:
    root = Path(base_dir).resolve()
    violations: list[ProtectedSalesReaderViolation] = []
    checked_files = 0

    for rule in PROTECTED_SALES_READER_RULES:
        path = root / rule.relative_path
        if not path.exists():
            continue
        checked_files += 1
        source = path.read_text(encoding="utf-8")
        relevant_symbols = [symbol for symbol in rule.forbidden_symbols if symbol in source]
        if not relevant_symbols:
            continue
        symbol_lines = _detect_symbol_lines(source, symbols=tuple(relevant_symbols))
        for symbol, lines in symbol_lines.items():
            for line_number in lines:
                violations.append(
                    ProtectedSalesReaderViolation(
                        relative_path=rule.relative_path,
                        line_number=line_number,
                        symbol=symbol,
                        reason=rule.reason,
                        suggestion=rule.suggestion,
                    )
                )

    return ProtectedSalesReaderScanResult(checked_files=checked_files, violations=tuple(violations))


def _detect_symbol_lines(source: str, *, symbols: tuple[str, ...]) -> dict[str, list[int]]:
    tree = ast.parse(source)
    line_map: dict[str, set[int]] = {symbol: set() for symbol in symbols}
    symbol_set = set(symbols)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name in symbol_set:
                    line_map[alias.name].add(node.lineno)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                tail = alias.name.rsplit(".", 1)[-1]
                if tail in symbol_set:
                    line_map[tail].add(node.lineno)
        elif isinstance(node, ast.Name) and node.id in symbol_set:
            line_map[node.id].add(node.lineno)
    return {symbol: sorted(lines) for symbol, lines in line_map.items() if lines}
