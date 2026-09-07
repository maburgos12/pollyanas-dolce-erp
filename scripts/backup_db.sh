#!/bin/bash
set -euo pipefail
umask 077
export LC_ALL=C

BACKUP_DIR="${BACKUP_DIR:-/opt/backups/erp}"
LOG_FILE="${LOG_FILE:-/var/log/erp_backup.log}"
CONTAINER="${CONTAINER:-pastelerias-erp-db-1}"
DB_NAME="${DB_NAME:-pastelerias_erp}"
DB_USER="${DB_USER:-postgres}"
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CONTEOS_EVIDENCE_DIR="${CONTEOS_EVIDENCE_DIR:-$SCRIPT_DIR/../storage/conteos_evidencias}"
KEEP_LAST=7

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

mkdir -p "$BACKUP_DIR"
# Linux production uses a kernel lock: SIGKILL/reboot releases it automatically.
# Keep its inode; unlinking a flock file could admit a second lock holder.
LOCK_DIR=""
if command -v flock >/dev/null 2>&1; then
    exec 9> "$BACKUP_DIR/.backup.flock"
    if ! flock -n 9; then
        log "ERROR: otro respaldo está activo"
        exit 1
    fi
else
    # Portable fallback (e.g. macOS): TERM/INT/EXIT release the directory.
    # After SIGKILL/reboot, inspect its pid and remove the stale directory manually;
    # automatic PID-based stealing can race another owner and is deliberately avoided.
    LOCK_DIR="$BACKUP_DIR/.backup.lock"
    if ! mkdir "$LOCK_DIR" 2>/dev/null; then
        log "ERROR: otro respaldo está activo o requiere revisar $LOCK_DIR/pid"
        exit 1
    fi
    echo "$$" > "$LOCK_DIR/pid"
fi
STAGING=""
PREFIX=""
PUBLISHED=0
cleanup() {
    local status=$?
    if [ "$PUBLISHED" -eq 0 ] && [ -n "$PREFIX" ]; then
        rm -f "$BACKUP_DIR/$PREFIX.sql.gz" "$BACKUP_DIR/$PREFIX.conteos.tar.gz" "$BACKUP_DIR/$PREFIX.incomplete"
    fi
    if [ -n "$STAGING" ]; then rm -rf "$STAGING"; fi
    if [ -n "$LOCK_DIR" ]; then rm -f "$LOCK_DIR/pid"; rmdir "$LOCK_DIR"; fi
    return "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
CANDIDATE="backup_${TIMESTAMP}"
for suffix in sql.gz conteos.tar.gz manifest incomplete; do
    if [ -e "$BACKUP_DIR/$CANDIDATE.$suffix" ]; then
        log "ERROR: el identificador $CANDIDATE ya existe; no se sobrescribe"
        exit 1
    fi
done
PREFIX="$CANDIDATE"
STAGING=$(mktemp -d "$BACKUP_DIR/.partial.XXXXXX")
log "Iniciando backup de $DB_NAME y evidencias de conteos..."

if ! docker exec "$CONTAINER" pg_dump -U "$DB_USER" "$DB_NAME" | gzip > "$STAGING/$PREFIX.sql.gz"; then
    log "ERROR: falló pg_dump"
    exit 1
fi
# SQL first: committed evidence already exists and is immutable. Additional files
# newer than the SQL snapshot are harmless; never omit an archived file on error.
if [ -d "$CONTEOS_EVIDENCE_DIR" ]; then
    if ! tar -czf "$STAGING/$PREFIX.conteos.tar.gz" -C "$CONTEOS_EVIDENCE_DIR" .; then
        log "ERROR: falló el respaldo de evidencias"
        exit 1
    fi
elif [ ! -e "$CONTEOS_EVIDENCE_DIR" ]; then
    if ! tar -czf "$STAGING/$PREFIX.conteos.tar.gz" -T /dev/null; then
        log "ERROR: falló el respaldo vacío de evidencias"
        exit 1
    fi
else
    log "ERROR: la fuente de evidencias no es un directorio"
    exit 1
fi

# Standard SHA-256 check-file format. Verify from BACKUP_DIR during restore using
# sha256sum -c backup_<timestamp>.manifest (or shasum -a 256 -c ...).
checksum_files() (
    cd "$1"
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$2.sql.gz" "$2.conteos.tar.gz"
    else
        shasum -a 256 "$2.sql.gz" "$2.conteos.tar.gz"
    fi
)
complete_set() {
    local directory="$1" base="$2" actual
    [ -f "$directory/$base.sql.gz" ] && [ -f "$directory/$base.conteos.tar.gz" ] &&
        [ -f "$directory/$base.manifest" ] || return 1
    # Compare the exact two expected entries, never follow paths from a manifest.
    actual=$(checksum_files "$directory" "$base") || return 1
    [ "$actual" = "$(cat "$directory/$base.manifest")" ]
}
checksum_files "$STAGING" "$PREFIX" > "$STAGING/$PREFIX.manifest"
if ! complete_set "$STAGING" "$PREFIX"; then
    log "ERROR: falló la verificación del nuevo respaldo"
    exit 1
fi
# A crash after publishing SQL must not make it look like a legacy SQL-only dump.
: > "$BACKUP_DIR/$PREFIX.incomplete"
mv "$STAGING/$PREFIX.sql.gz" "$BACKUP_DIR/$PREFIX.sql.gz"
mv "$STAGING/$PREFIX.conteos.tar.gz" "$BACKUP_DIR/$PREFIX.conteos.tar.gz"
# The manifest is the completion marker, always published last.
mv "$STAGING/$PREFIX.manifest" "$BACKUP_DIR/$PREFIX.manifest"
PUBLISHED=1
rm -f "$BACKUP_DIR/$PREFIX.incomplete"

# Preserve the original policy: seven usable restore points TOTAL, including
# legacy SQL-only backups. Incomplete/corrupt sets are neither counted nor erased.
shopt -s nullglob
COMPLETE=()
for sql in "$BACKUP_DIR"/backup_*.sql.gz; do
    base=${sql##*/}
    base=${base%.sql.gz}
    [[ "$base" =~ ^backup_[0-9]{8}_[0-9]{6}$ ]] || continue
    if complete_set "$BACKUP_DIR" "$base"; then
        # A valid manifest is authoritative even if its crash marker remains.
        COMPLETE+=("$base")
    elif [ ! -e "$BACKUP_DIR/$base.manifest" ] &&
        [ ! -e "$BACKUP_DIR/$base.conteos.tar.gz" ] &&
        [ ! -e "$BACKUP_DIR/$base.incomplete" ] && gzip -t "$sql" 2>/dev/null; then
        COMPLETE+=("$base")
    fi
done
DELETE_COUNT=$(( ${#COMPLETE[@]} - KEEP_LAST ))
for ((i=0; i<DELETE_COUNT; i++)); do
    old=${COMPLETE[$i]}
    rm -f "$BACKUP_DIR/$old.manifest" "$BACKUP_DIR/$old.sql.gz" "$BACKUP_DIR/$old.conteos.tar.gz" "$BACKUP_DIR/$old.incomplete"
    log "Rotado conjunto: $old"
done
log "Backup completado: $BACKUP_DIR/$PREFIX.manifest (SQL y evidencias con manifiesto SHA-256)"
