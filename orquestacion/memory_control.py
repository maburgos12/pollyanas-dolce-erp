from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from core.models import AuditLog


SECTION_TITLE_BY_KEY = {
    "fact": "Hechos estables confirmados",
    "stable_fact": "Hechos estables confirmados",
    "error": "Errores recurrentes a evitar",
    "recurrent_error": "Errores recurrentes a evitar",
    "gap": "Gaps estables confirmados",
    "known_gap": "Gaps estables confirmados",
}

SECTION_KEY_BY_TITLE = {
    title.lower(): key for key, title in SECTION_TITLE_BY_KEY.items() if key in {"fact", "error", "gap"}
}


@dataclass(frozen=True)
class MemoryWriteResult:
    path: str
    section_key: str
    section_title: str
    text: str
    written: bool
    reason: str

    def as_dict(self) -> dict[str, object]:
        return {
            "path": self.path,
            "section_key": self.section_key,
            "section_title": self.section_title,
            "text": self.text,
            "written": self.written,
            "reason": self.reason,
        }


def append_controlled_memory_entry(
    *,
    section: str,
    text: str,
    evidence_refs: Iterable[str],
    source: str,
    actor=None,
    base_dir: str | Path | None = None,
    relative_path: str = "memory.md",
) -> MemoryWriteResult:
    section_key = _normalize_section_key(section)
    section_title = SECTION_TITLE_BY_KEY[section_key]
    normalized_text = _normalize_text(text)
    normalized_source = str(source or "").strip()
    normalized_evidence = [str(item).strip() for item in evidence_refs if str(item).strip()]

    if not normalized_text:
        raise ValueError("El texto de memoria no puede estar vacío.")
    if not normalized_source:
        raise ValueError("La escritura controlada de memoria requiere source explícito.")
    if not normalized_evidence:
        raise ValueError("La escritura controlada de memoria requiere al menos una evidencia.")

    root = Path(base_dir or Path.cwd())
    memory_path = root / relative_path
    if not memory_path.exists():
        raise ValueError(f"No existe {relative_path} en {root}.")

    lines = memory_path.read_text(encoding="utf-8").splitlines()
    header = f"## {section_title}"
    section_start = _find_section_start(lines, header)
    if section_start is None:
        raise ValueError(f"No se encontró la sección '{section_title}' en {memory_path}.")

    section_end = _find_section_end(lines, section_start + 1)
    existing_entries = {
        _normalize_text(line[2:])
        for line in lines[section_start + 1 : section_end]
        if line.strip().startswith("- ")
    }
    if normalized_text in existing_entries:
        return MemoryWriteResult(
            path=str(memory_path),
            section_key=section_key,
            section_title=section_title,
            text=str(text).strip(),
            written=False,
            reason="duplicate_entry",
        )

    insert_at = section_end
    while insert_at > section_start + 1 and not lines[insert_at - 1].strip():
        insert_at -= 1

    insertion = [f"- {str(text).strip()}"]
    if section_end < len(lines):
        insertion.append("")
    lines[insert_at:insert_at] = insertion
    memory_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

    AuditLog.objects.create(
        user=actor,
        action="UPDATE",
        model="memory.md",
        object_id=section_title,
        payload={
            "section_key": section_key,
            "section_title": section_title,
            "text": str(text).strip(),
            "source": normalized_source,
            "evidence_refs": normalized_evidence,
            "path": str(memory_path),
        },
    )

    return MemoryWriteResult(
        path=str(memory_path),
        section_key=section_key,
        section_title=section_title,
        text=str(text).strip(),
        written=True,
        reason="appended",
    )


def _normalize_section_key(value: str) -> str:
    candidate = str(value or "").strip().lower()
    if candidate in SECTION_TITLE_BY_KEY:
        return candidate
    if candidate in SECTION_KEY_BY_TITLE:
        return SECTION_KEY_BY_TITLE[candidate]
    raise ValueError(
        "Sección de memoria inválida. Usa una de: fact, error, gap."
    )


def _normalize_text(value: str) -> str:
    return " ".join(str(value or "").strip().split())


def _find_section_start(lines: list[str], header: str) -> int | None:
    for index, line in enumerate(lines):
        if line.strip() == header:
            return index
    return None


def _find_section_end(lines: list[str], start_index: int) -> int:
    for index in range(start_index, len(lines)):
        if lines[index].startswith("## "):
            return index
    return len(lines)
