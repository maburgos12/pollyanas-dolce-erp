from pathlib import Path

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.core.files.storage import FileSystemStorage
from django.utils.deconstruct import deconstructible


@deconstructible
class InventoryAuditEvidenceStorage(FileSystemStorage):
    """Private storage whose location follows settings overrides at runtime."""

    @property
    def base_location(self) -> str:
        configured = getattr(settings, "INVENTORY_AUDIT_PRIVATE_ROOT", None)
        private_root = Path(
            configured
            or Path(settings.BASE_DIR) / "storage" / "inventory_audit_evidence"
        ).resolve(strict=False)
        media_root = Path(settings.MEDIA_ROOT).resolve(strict=False)
        if private_root == media_root or private_root.is_relative_to(media_root):
            raise ImproperlyConfigured(
                "INVENTORY_AUDIT_PRIVATE_ROOT debe estar fuera de MEDIA_ROOT."
            )
        return str(private_root)

    @property
    def location(self) -> str:
        return self.base_location

    def url(self, name: str) -> str:
        raise NotImplementedError(
            "Las evidencias de auditoría no tienen una URL pública."
        )


inventory_audit_evidence_storage = InventoryAuditEvidenceStorage()
