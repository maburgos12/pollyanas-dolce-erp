from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse

from pos_bridge.config import PointBridgeSettings, load_point_bridge_settings
from pos_bridge.services.point_http_session_service import PointHttpSessionService
from pos_bridge.utils.dates import timestamp_token
from pos_bridge.utils.exceptions import ExtractionError
from pos_bridge.utils.helpers import safe_slug


@dataclass
class PointFileDownloadResult:
    request_url: str
    output_path: str
    content_type: str
    size_bytes: int
    branch_external_id: str | None
    resource_path: str


class PointFileDownloadService:
    def __init__(
        self,
        bridge_settings: PointBridgeSettings | None = None,
        http_session_service: PointHttpSessionService | None = None,
    ):
        self.settings = bridge_settings or load_point_bridge_settings()
        self.http_session_service = http_session_service or PointHttpSessionService(self.settings)

    def _base_url(self) -> str:
        return self.settings.base_url.rstrip("/") + "/"

    def _resolve_target(
        self,
        *,
        path_or_url: str,
        params: dict[str, str] | None,
    ) -> tuple[str, str, dict[str, str]]:
        raw_target = str(path_or_url or "").strip()
        if not raw_target:
            raise ExtractionError("Falta la ruta o URL del archivo Point a descargar.")

        extra_params = {str(key): str(value) for key, value in (params or {}).items()}
        parsed = urlparse(raw_target)
        base_parsed = urlparse(self._base_url())

        if parsed.scheme and parsed.netloc:
            if parsed.netloc != base_parsed.netloc:
                raise ExtractionError(
                    "La descarga autenticada solo permite URLs del mismo host Point configurado.",
                    context={"target_host": parsed.netloc, "base_host": base_parsed.netloc},
                )
            merged_params = dict(parse_qsl(parsed.query, keep_blank_values=True))
            merged_params.update(extra_params)
            resource_path = parsed.path or "/"
            request_url = f"{base_parsed.scheme}://{base_parsed.netloc}{resource_path}"
            return request_url, resource_path, merged_params

        resource_path = "/" + raw_target.lstrip("/")
        request_url = urljoin(self._base_url(), resource_path.lstrip("/"))
        return request_url, resource_path, extra_params

    def _infer_filename(
        self,
        *,
        resource_path: str,
        output_name: str | None,
        content_type: str,
    ) -> str:
        if output_name:
            candidate = Path(str(output_name).strip()).name
            if candidate:
                return candidate

        path_name = Path(resource_path).name
        if "." in path_name:
            return path_name

        ext = ".bin"
        lowered = content_type.lower()
        if "excel" in lowered or "spreadsheet" in lowered:
            ext = ".xls"
        elif "csv" in lowered:
            ext = ".csv"
        elif "json" in lowered:
            ext = ".json"
        elif "pdf" in lowered:
            ext = ".pdf"

        return f"{safe_slug(resource_path)}{ext}"

    def _build_output_path(
        self,
        *,
        resource_path: str,
        output_name: str | None,
        content_type: str,
    ) -> Path:
        filename = self._infer_filename(
            resource_path=resource_path,
            output_name=output_name,
            content_type=content_type,
        )
        timestamp = timestamp_token()
        output_dir = self.settings.raw_exports_dir / "point_files"
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir / f"{timestamp}_{safe_slug(Path(filename).stem)}{Path(filename).suffix or ''}"

    def download(
        self,
        *,
        path_or_url: str,
        params: dict[str, str] | None = None,
        branch_external_id: str | None = None,
        branch_display_name: str | None = None,
        output_name: str | None = None,
    ) -> PointFileDownloadResult:
        request_url, resource_path, merged_params = self._resolve_target(path_or_url=path_or_url, params=params)
        auth_session = self.http_session_service.create(
            branch_external_id=branch_external_id,
            branch_display_name=branch_display_name,
        )
        try:
            response = auth_session.session.get(
                request_url,
                params=merged_params,
                timeout=self.settings.timeout_ms / 1000,
            )
            response.raise_for_status()
            content_type = str(response.headers.get("Content-Type") or "application/octet-stream")
            output_path = self._build_output_path(
                resource_path=resource_path,
                output_name=output_name,
                content_type=content_type,
            )
            output_path.write_bytes(response.content)
            full_request_url = request_url
            if merged_params:
                full_request_url = f"{request_url}?{urlencode(merged_params)}"
            return PointFileDownloadResult(
                request_url=full_request_url,
                output_path=str(output_path),
                content_type=content_type,
                size_bytes=len(response.content),
                branch_external_id=branch_external_id,
                resource_path=resource_path,
            )
        finally:
            try:
                auth_session.session.close()
            except Exception:  # noqa: BLE001
                pass
