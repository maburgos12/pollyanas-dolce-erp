from __future__ import annotations

import base64
import io
import os
import re
from dataclasses import dataclass
from datetime import date
from collections import deque
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from recetas.utils.normalizacion import normalizar_nombre

from .almacen_import import ENTRADAS_FILE, INVENTARIO_FILE, MERMA_FILE, SALIDAS_FILE, import_folder


DRIVE_SCOPE = ["https://www.googleapis.com/auth/drive.readonly"]
FOLDER_MIME = "application/vnd.google-apps.folder"
SHEET_MIME = "application/vnd.google-apps.spreadsheet"
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


MONTH_WORDS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


@dataclass
class DriveSyncResult:
    folder_id: str
    folder_name: str
    target_month: str
    used_fallback_month: bool
    downloaded_sources: list[str]
    skipped_files: list[str]
    summary: Any


@dataclass
class DriveFolderCandidate:
    folder_id: str
    name: str
    month: int | None
    year: int | None


def _month_back(target: date) -> date:
    if target.month == 1:
        return date(target.year - 1, 12, 1)
    return date(target.year, target.month - 1, 1)


def _parse_yyyy_mm(value: str | None) -> date | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    m = re.fullmatch(r"(\d{4})-(\d{2})", text)
    if not m:
        raise ValueError("El formato de mes debe ser YYYY-MM (ejemplo: 2026-02).")
    year = int(m.group(1))
    month = int(m.group(2))
    if month < 1 or month > 12:
        raise ValueError("Mes inválido en --month. Debe estar entre 01 y 12.")
    return date(year, month, 1)


def _extract_month_year(name: str) -> tuple[int | None, int | None]:
    norm = normalizar_nombre(name)

    m = re.search(r"(?<!\d)(20\d{2})[-_/\s](0?[1-9]|1[0-2])(?!\d)", norm)
    if m:
        return int(m.group(2)), int(m.group(1))

    m = re.search(r"(?<!\d)(0?[1-9]|1[0-2])[-_/\s](20\d{2})(?!\d)", norm)
    if m:
        return int(m.group(1)), int(m.group(2))

    year_match = re.search(r"(?<!\d)(20\d{2})(?!\d)", norm)
    year = int(year_match.group(1)) if year_match else None
    for word, month in MONTH_WORDS.items():
        if re.search(rf"(?<![a-z0-9]){re.escape(word)}(?![a-z0-9])", norm):
            return month, year

    return None, year


def _classify_filename(filename: str) -> tuple[str | None, str | None]:
    if not filename:
        return None, None
    normalized = normalizar_nombre(Path(filename).name)

    expected = {
        INVENTARIO_FILE: "inventario",
        ENTRADAS_FILE: "entradas",
        SALIDAS_FILE: "salidas",
        MERMA_FILE: "merma",
    }
    for expected_name, source in expected.items():
        if normalized == normalizar_nombre(expected_name):
            return source, expected_name

    if "inventario" in normalized and "almacen" in normalized:
        return "inventario", INVENTARIO_FILE
    if "entradas" in normalized and "almacen" in normalized:
        return "entradas", ENTRADAS_FILE
    if "salidas" in normalized and "almacen" in normalized:
        return "salidas", SALIDAS_FILE
    if "merma" in normalized and "almacen" in normalized:
        return "merma", MERMA_FILE
    return None, None


def _build_drive_service():
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Faltan dependencias de Google Drive. Instala: google-api-python-client, google-auth, google-auth-httplib2"
        ) from exc

    raw_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    raw_b64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_B64", "").strip()
    file_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "").strip()

    creds = None
    if raw_json:
        import json

        creds = service_account.Credentials.from_service_account_info(json.loads(raw_json), scopes=DRIVE_SCOPE)
    elif raw_b64:
        import json

        decoded = base64.b64decode(raw_b64).decode("utf-8")
        creds = service_account.Credentials.from_service_account_info(json.loads(decoded), scopes=DRIVE_SCOPE)
    elif file_path:
        creds = service_account.Credentials.from_service_account_file(file_path, scopes=DRIVE_SCOPE)

    if creds is None:
        raise RuntimeError(
            "Faltan credenciales de Google. Define GOOGLE_SERVICE_ACCOUNT_JSON o GOOGLE_SERVICE_ACCOUNT_B64 o GOOGLE_SERVICE_ACCOUNT_FILE."
        )

    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _list_child_folders(service, root_folder_id: str) -> list[DriveFolderCandidate]:
    candidates: list[DriveFolderCandidate] = []
    token = None
    query = f"'{root_folder_id}' in parents and trashed=false and mimeType='{FOLDER_MIME}'"

    while True:
        resp = (
            service.files()
            .list(
                q=query,
                fields="nextPageToken, files(id,name)",
                pageSize=1000,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageToken=token,
            )
            .execute()
        )
        for item in resp.get("files", []):
            month, year = _extract_month_year(item.get("name", ""))
            candidates.append(
                DriveFolderCandidate(
                    folder_id=item["id"],
                    name=item.get("name", item["id"]),
                    month=month,
                    year=year,
                )
            )

        token = resp.get("nextPageToken")
        if not token:
            break

    return candidates


def _pick_month_folder(
    folders: list[DriveFolderCandidate],
    target: date,
    fallback_previous: bool,
) -> tuple[DriveFolderCandidate | None, bool]:
    for folder in folders:
        if folder.month == target.month and folder.year == target.year:
            return folder, False

    if fallback_previous:
        prev = _month_back(target)
        for folder in folders:
            if folder.month == prev.month and folder.year == prev.year:
                return folder, True

    dated = [f for f in folders if f.month and f.year]
    if dated:
        dated.sort(key=lambda x: (int(x.year or 0), int(x.month or 0)), reverse=True)
        return dated[0], True

    return None, False


def _download_file(service, file_id: str, mime_type: str | None = None) -> bytes:
    from googleapiclient.http import MediaIoBaseDownload

    if mime_type:
        request = service.files().export_media(fileId=file_id, mimeType=mime_type)
    else:
        request = service.files().get_media(fileId=file_id)

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return fh.getvalue()


def _iter_files(service, folder_id: str):
    token = None
    query = f"'{folder_id}' in parents and trashed=false"

    while True:
        resp = (
            service.files()
            .list(
                q=query,
                fields="nextPageToken, files(id,name,mimeType,fileExtension)",
                pageSize=1000,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageToken=token,
            )
            .execute()
        )
        for item in resp.get("files", []):
            yield item

        token = resp.get("nextPageToken")
        if not token:
            break


def _list_children(service, folder_id: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    token = None
    query = f"'{folder_id}' in parents and trashed=false"
    while True:
        resp = (
            service.files()
            .list(
                q=query,
                fields="nextPageToken, files(id,name,mimeType,fileExtension)",
                pageSize=1000,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageToken=token,
            )
            .execute()
        )
        items.extend(resp.get("files", []))
        token = resp.get("nextPageToken")
        if not token:
            break
    return items


def _find_folder_with_expected_files(
    service,
    start_folder_id: str,
    include_sources: set[str],
    max_depth: int = 4,
    max_folders_scanned: int = 120,
) -> tuple[str, str | None]:
    queue: deque[tuple[str, int]] = deque([(start_folder_id, 0)])
    visited: set[str] = set()
    scanned = 0

    while queue and scanned < max_folders_scanned:
        folder_id, depth = queue.popleft()
        if folder_id in visited:
            continue
        visited.add(folder_id)
        scanned += 1

        children = _list_children(service, folder_id)
        matched_sources = set()
        for item in children:
            source, _ = _classify_filename(item.get("name", ""))
            if source and source in include_sources:
                matched_sources.add(source)

        if matched_sources:
            return folder_id, None

        if depth >= max_depth:
            continue

        for item in children:
            if item.get("mimeType") == FOLDER_MIME:
                queue.append((item["id"], depth + 1))

    return start_folder_id, (
        f"No se encontró carpeta con archivos válidos tras escanear {scanned} carpeta(s). "
        "Revisa nombres esperados y permisos del service account."
    )


def sync_almacen_from_drive(
    include_sources: set[str] | None = None,
    month_override: str | None = None,
    fallback_previous: bool = True,
    fuzzy_threshold: int = 96,
    create_aliases: bool = False,
    alias_threshold: int = 95,
    create_missing_insumos: bool = True,
    dry_run: bool = False,
) -> DriveSyncResult:
    include_sources = include_sources or {"inventario", "entradas", "salidas", "merma"}

    root_folder_id = os.getenv("GOOGLE_DRIVE_ROOT_FOLDER_ID", "").strip()
    if not root_folder_id:
        raise RuntimeError("Falta GOOGLE_DRIVE_ROOT_FOLDER_ID en variables de entorno.")

    target = _parse_yyyy_mm(month_override) or date.today().replace(day=1)
    target_label = f"{target.year:04d}-{target.month:02d}"

    service = _build_drive_service()
    folders = _list_child_folders(service, root_folder_id)
    if folders:
        folder, fallback_used = _pick_month_folder(folders, target, fallback_previous=fallback_previous)
    else:
        folder = DriveFolderCandidate(folder_id=root_folder_id, name="ROOT", month=None, year=None)
        fallback_used = False

    if folder is None:
        raise RuntimeError(
            "No encontré carpetas mensuales en Google Drive. Revisa GOOGLE_DRIVE_ROOT_FOLDER_ID y el naming (ej. ALMACEN ENERO 2026)."
        )

    downloaded_sources: set[str] = set()
    skipped_files: list[str] = []

    target_folder_id, scan_warning = _find_folder_with_expected_files(
        service=service,
        start_folder_id=folder.folder_id,
        include_sources=include_sources,
    )
    if scan_warning:
        skipped_files.append(scan_warning)
    if target_folder_id != folder.folder_id:
        meta = (
            service.files()
            .get(fileId=target_folder_id, fields="id,name", supportsAllDrives=True)
            .execute()
        )
        folder = DriveFolderCandidate(
            folder_id=target_folder_id,
            name=meta.get("name", folder.name),
            month=folder.month,
            year=folder.year,
        )

    with TemporaryDirectory(prefix="inv-drive-") as tmpdir:
        tmp = Path(tmpdir)
        for file_item in _iter_files(service, folder.folder_id):
            name = file_item.get("name", "")
            source, target_name = _classify_filename(name)
            if not source or not target_name:
                skipped_files.append(f"No reconocido: {name}")
                continue
            if source not in include_sources:
                skipped_files.append(f"Omitido por filtro ({source}): {name}")
                continue

            mime_type = file_item.get("mimeType", "")
            file_id = file_item["id"]
            if mime_type == SHEET_MIME:
                blob = _download_file(service, file_id, mime_type=XLSX_MIME)
            elif name.lower().endswith((".xlsx", ".xlsm", ".xls")):
                blob = _download_file(service, file_id)
            else:
                skipped_files.append(f"Formato no soportado: {name}")
                continue

            (tmp / target_name).write_bytes(blob)
            downloaded_sources.add(source)

        run_sources = include_sources.intersection(downloaded_sources)
        if not run_sources:
            raise RuntimeError(
                f"No encontré archivos válidos para importar dentro de '{folder.name}'. Revisa nombres esperados de Inventario/Entradas/Salidas/Merma."
            )

        summary = import_folder(
            folderpath=str(tmp),
            include_sources=run_sources,
            fuzzy_threshold=fuzzy_threshold,
            create_aliases=create_aliases,
            alias_threshold=alias_threshold,
            create_missing_insumos=create_missing_insumos,
            dry_run=dry_run,
        )

    return DriveSyncResult(
        folder_id=folder.folder_id,
        folder_name=folder.name,
        target_month=target_label,
        used_fallback_month=fallback_used,
        downloaded_sources=sorted(downloaded_sources),
        skipped_files=skipped_files,
        summary=summary,
    )
