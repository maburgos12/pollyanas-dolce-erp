#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${1:-$(mktemp -d "${TMPDIR:-/tmp}/native-chat-release.XXXXXX")}"

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "Missing required command: $1"
}

need_cmd git
need_cmd rsync
need_cmd python3

rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}"

echo "== Exporting clean git HEAD to ${OUTPUT_DIR} =="
git -C "${ROOT_DIR}" archive --format=tar HEAD | tar -xf - -C "${OUTPUT_DIR}"

mkdir -p "${OUTPUT_DIR}/config"
rsync -a \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  "${ROOT_DIR}/config/middleware.py" "${OUTPUT_DIR}/config/middleware.py"

echo "== Overlaying native chat sources =="
rsync -a \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  "${ROOT_DIR}/orquestacion/" "${OUTPUT_DIR}/orquestacion/"

echo "== Overlaying required untracked app modules =="
rsync -a \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  "${ROOT_DIR}/ventas/" "${OUTPUT_DIR}/ventas/"

mkdir -p "${OUTPUT_DIR}/core"
rsync -a \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  "${ROOT_DIR}/core/branch_catalog.py" "${OUTPUT_DIR}/core/branch_catalog.py"
rsync -a \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  "${ROOT_DIR}/core/cache_versions.py" "${OUTPUT_DIR}/core/cache_versions.py"
rsync -a \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  "${ROOT_DIR}/core/middleware.py" "${OUTPUT_DIR}/core/middleware.py"

mkdir -p "${OUTPUT_DIR}/recetas/utils"
rsync -a \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  "${ROOT_DIR}/recetas/utils/commercial_composition.py" "${OUTPUT_DIR}/recetas/utils/commercial_composition.py"
rsync -a \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  "${ROOT_DIR}/recetas/utils/derived_product_presentations.py" "${OUTPUT_DIR}/recetas/utils/derived_product_presentations.py"

mkdir -p "${OUTPUT_DIR}/reportes"
rsync -a \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  "${ROOT_DIR}/reportes/models.py" "${OUTPUT_DIR}/reportes/models.py"
rsync -a \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  "${ROOT_DIR}/reportes/dashboard_sales_dataset.py" "${OUTPUT_DIR}/reportes/dashboard_sales_dataset.py"

mkdir -p "${OUTPUT_DIR}/pos_bridge/models"
rsync -a \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  "${ROOT_DIR}/pos_bridge/models/__init__.py" "${OUTPUT_DIR}/pos_bridge/models/__init__.py"
rsync -a \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  "${ROOT_DIR}/pos_bridge/models/product_history.py" "${OUTPUT_DIR}/pos_bridge/models/product_history.py"
rsync -a \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  "${ROOT_DIR}/pos_bridge/models/sales_pipeline.py" "${OUTPUT_DIR}/pos_bridge/models/sales_pipeline.py"

mkdir -p "${OUTPUT_DIR}/maestros/migrations"
rsync -a \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  "${ROOT_DIR}/maestros/migrations/0009_fix_colorante_rojo_volume_unit.py" "${OUTPUT_DIR}/maestros/migrations/0009_fix_colorante_rojo_volume_unit.py"

mkdir -p "${OUTPUT_DIR}/recetas/migrations"
rsync -a \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  "${ROOT_DIR}/recetas/migrations/0025_productomonthclosure_and_more.py" "${OUTPUT_DIR}/recetas/migrations/0025_productomonthclosure_and_more.py"
rsync -a \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  "${ROOT_DIR}/recetas/migrations/0026_alter_productomonthclosure_opening_source.py" "${OUTPUT_DIR}/recetas/migrations/0026_alter_productomonthclosure_opening_source.py"
rsync -a \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  "${ROOT_DIR}/recetas/migrations/0027_receta_modo_costeo.py" "${OUTPUT_DIR}/recetas/migrations/0027_receta_modo_costeo.py"

mkdir -p "${OUTPUT_DIR}/templates/orquestacion"
rsync -a \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  "${ROOT_DIR}/templates/orquestacion/" "${OUTPUT_DIR}/templates/orquestacion/"

python3 - <<'PY' "${OUTPUT_DIR}"
from pathlib import Path
import sys

root = Path(sys.argv[1])


def replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    if old in text:
        path.write_text(text.replace(old, new, 1), encoding="utf-8")


def ensure_contains(path: Path, needle: str, insertion_anchor: str, insertion: str) -> None:
    text = path.read_text(encoding="utf-8")
    if needle in text:
        return
    if insertion_anchor not in text:
        raise SystemExit(f"Anchor not found in {path}: {insertion_anchor!r}")
    path.write_text(text.replace(insertion_anchor, insertion_anchor + insertion, 1), encoding="utf-8")


# config/urls.py
urls_path = root / "config" / "urls.py"
ensure_contains(
    urls_path,
    "from orquestacion import chat_views as ai_chat_views\n",
    "from core import views as core_views\n",
    "from orquestacion import chat_views as ai_chat_views\n",
)
replace_once(
    urls_path,
    '    path("auditoria/", core_views.audit_log_view, name="audit_log"),\n',
    '    path("auditoria/", core_views.audit_log_view, name="audit_log"),\n'
    '    path("ia-privada/", ai_chat_views.chat_home, name="ai_private_hub"),\n'
    '    path("ia-privada/api/conversations/", ai_chat_views.conversations_api, name="ai_private_conversations_api"),\n'
    '    path("ia-privada/api/conversations/new/", ai_chat_views.create_conversation_api, name="ai_private_conversation_create_api"),\n'
    '    path("ia-privada/api/conversations/<uuid:conversation_id>/", ai_chat_views.conversation_detail_api, name="ai_private_conversation_detail_api"),\n'
    '    path("ia-privada/api/conversations/<uuid:conversation_id>/stream/", ai_chat_views.stream_message_api, name="ai_private_message_stream_api"),\n',
)
replace_once(
    urls_path,
    '    path("integraciones/", include(("integraciones.urls", "integraciones"), namespace="integraciones")),\n',
    '    path("ventas/", include(("ventas.urls", "ventas"), namespace="ventas")),\n'
    '    path("integraciones/", include(("integraciones.urls", "integraciones"), namespace="integraciones")),\n'
    '    path("orquestacion/", include(("orquestacion.urls", "orquestacion"), namespace="orquestacion")),\n',
)
ensure_contains(
    urls_path,
    '    path("ventas/", include(("ventas.urls", "ventas"), namespace="ventas")),\n',
    '    path("crm/", include(("crm.urls", "crm"), namespace="crm")),\n',
    '    path("ventas/", include(("ventas.urls", "ventas"), namespace="ventas")),\n',
)
replace_once(
    urls_path,
    '    path("integraciones/", include(("integraciones.urls", "integraciones"), namespace="integraciones")),\n'
    '    path("orquestacion/", include(("orquestacion.urls", "orquestacion"), namespace="orquestacion")),\n',
    '    path("integraciones/", include(("integraciones.urls", "integraciones"), namespace="integraciones")),\n'
    '    path("orquestacion/", include(("orquestacion.urls", "orquestacion"), namespace="orquestacion")),\n',
)

# core/access.py
access_path = root / "core" / "access.py"
ensure_contains(
    access_path,
    "def can_view_orquestacion(user: AbstractBaseUser) -> bool:\n",
    "def can_manage_users(user: AbstractBaseUser) -> bool:\n    return has_any_role(user, ROLE_DG, ROLE_ADMIN)\n\n\n",
    "def can_view_orquestacion(user: AbstractBaseUser) -> bool:\n"
    '    return has_any_role(user, ROLE_DG, ROLE_ADMIN) and not _is_locked(user, "lock_auditoria")\n\n\n'
    "def can_manage_orquestacion(user: AbstractBaseUser) -> bool:\n"
    '    return has_any_role(user, ROLE_DG, ROLE_ADMIN) and not _is_locked(user, "lock_auditoria")\n\n\n',
)

# core/context_processors.py
cp_path = root / "core" / "context_processors.py"
ensure_contains(
    cp_path,
    "    can_manage_orquestacion,\n",
    "    can_capture_piso,\n",
    "    can_manage_orquestacion,\n",
)
ensure_contains(
    cp_path,
    "    can_view_orquestacion,\n",
    "    can_manage_users,\n",
    "    can_view_orquestacion,\n",
)
ensure_contains(
    cp_path,
    '            "can_view_orquestacion": can_view_orquestacion(user),\n',
    '            "can_view_audit": can_view_audit(user),\n',
    '            "can_view_orquestacion": can_view_orquestacion(user),\n'
    '            "can_manage_orquestacion": can_manage_orquestacion(user),\n',
)

# templates/base.html
base_path = root / "templates" / "base.html"
ensure_contains(
    base_path,
    '<span>IA privada</span>',
    '                <ul class="sidebar-nav sidebar-nav-group">\n',
    '                    {% if ui_access.can_view_orquestacion %}\n'
    '                    <li>\n'
    '                        <a href="{% url \'ai_private_hub\' %}" class="{% if \'/ia-privada/\' in request.path %}active{% endif %}">\n'
    '                            <span class="nav-icon" aria-hidden="true">\n'
    '                                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3 4 7.5v9L12 21l8-4.5v-9L12 3Z"/><path d="M8.5 11.5 12 9l3.5 2.5L12 14l-3.5-2.5Z"/><path d="M8.5 15.5 12 18l3.5-2.5"/></svg>\n'
    '                            </span>\n'
    '                            <span>IA privada</span>\n'
    '                        </a>\n'
    '                    </li>\n'
    '                    {% endif %}\n',
)

print(f"Prepared native chat release at {root}")
PY

echo "== Release ready =="
echo "${OUTPUT_DIR}"
