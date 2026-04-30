#!/bin/bash
set -euo pipefail

BACKUP_DIR="/opt/backups/erp"
LOG_FILE="/var/log/erp_backup.log"
CONTAINER="pastelerias-erp-db-1"
DB_NAME="${DB_NAME:-pastelerias_erp}"
DB_USER="${DB_USER:-postgres}"
KEEP_LAST=7

mkdir -p "$BACKUP_DIR"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_FILE="$BACKUP_DIR/backup_${TIMESTAMP}.sql.gz"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

log "Iniciando backup de $DB_NAME..."

if docker exec "$CONTAINER" pg_dump -U "$DB_USER" "$DB_NAME" | gzip > "$BACKUP_FILE"; then
    SIZE=$(du -sh "$BACKUP_FILE" | cut -f1)
    log "Backup completado: $BACKUP_FILE ($SIZE)"
else
    log "ERROR: falló pg_dump"
    rm -f "$BACKUP_FILE"
    exit 1
fi

BACKUP_COUNT=$(ls -1 "$BACKUP_DIR"/backup_*.sql.gz 2>/dev/null | wc -l)
if [ "$BACKUP_COUNT" -gt "$KEEP_LAST" ]; then
    DELETE_COUNT=$(( BACKUP_COUNT - KEEP_LAST ))
    ls -1t "$BACKUP_DIR"/backup_*.sql.gz | tail -n "$DELETE_COUNT" | while read -r OLD_FILE; do
        rm -f "$OLD_FILE"
        log "Rotado (eliminado): $OLD_FILE"
    done
fi

log "Backup finalizado. Archivos en $BACKUP_DIR: $(ls -1 "$BACKUP_DIR"/backup_*.sql.gz 2>/dev/null | wc -l)"
