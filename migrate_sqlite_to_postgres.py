#!/usr/bin/env python3
"""
migrate_sqlite_to_postgres.py

Migra datos del backup SQLite → PostgreSQL local.
Uso:  python migrate_sqlite_to_postgres.py
"""

import json
import sqlite3
import sys
from pathlib import Path

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 no instalado. Corre: pip install psycopg2-binary")
    sys.exit(1)

# ── Colores ANSI ──────────────────────────────────────────────────────────────
RED    = "\033[91m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RESET  = "\033[0m"

# ── Paths y config ────────────────────────────────────────────────────────────
SQLITE_PATH = Path("tmp/db_recovered_20260324.sqlite3")
BATCH_SIZE  = 1_000   # filas por INSERT batch

# ── Tablas a NO migrar ────────────────────────────────────────────────────────
SKIP = {
    # Indicado explícitamente por el usuario
    "django_migrations",
    "django_session",
    "django_content_type",
    "auth_permission",
    "django_admin_log",
    # Dependen de auth_permission (que no se migra) → FK violation segura
    "auth_group_permissions",
    "auth_user_user_permissions",
    # Celery beat — no son datos de negocio
    "django_celery_beat_clockedschedule",
    "django_celery_beat_crontabschedule",
    "django_celery_beat_intervalschedule",
    "django_celery_beat_periodictask",
    "django_celery_beat_periodictasks",
    "django_celery_beat_solarschedule",
    # Interna de SQLite
    "sqlite_sequence",
}

# ── Orden de migración (de menos a más dependencias FK) ───────────────────────
ORDER = [
    # ── Auth & Core ──────────────────────────────────────────────────────────
    "auth_group",
    "auth_user",
    "auth_user_groups",           # FK → auth_group + auth_user
    "authtoken_token",            # FK → auth_user
    "core_sucursal",
    "core_departamento",
    "core_userprofile",           # FK → auth_user + core_sucursal
    # ── Maestros ─────────────────────────────────────────────────────────────
    "maestros_unidadmedida",
    "maestros_proveedor",
    "maestros_insumo",            # FK → maestros_unidadmedida
    "maestros_insumoalias",       # FK → maestros_insumo
    "maestros_costoinsumo",       # FK → maestros_insumo + maestros_proveedor
    "maestros_pointpendingmatch",
    # ── Recetas (árbol de dependencias) ──────────────────────────────────────
    "recetas_receta",
    "recetas_recetapresentacion",          # FK → recetas_receta
    "recetas_recetapresentacionderivada",  # FK → recetas_receta + recetas_recetapresentacion
    "recetas_lineareceta",                 # FK → recetas_receta + maestros_insumo
    "recetas_recetacodigopointalias",      # FK → recetas_receta
    "recetas_recetaagrupacionaddon",       # FK → recetas_receta
    "recetas_costodriver",
    "recetas_recetacostoversion",          # FK → recetas_receta + recetas_costodriver
    "recetas_recetacostosemanal",          # FK → recetas_receta
    "recetas_ventahistorica",              # FK → recetas_recetapresentacion
    "recetas_solicitudventa",              # FK → recetas_recetapresentacion
    "recetas_pronosticoventa",             # FK → recetas_recetapresentacion
    "recetas_planproduccion",              # FK → core_sucursal + auth_user
    "recetas_planproduccionitem",          # FK → recetas_planproduccion + recetas_recetapresentacion
    "recetas_inventariocedisproducto",     # FK → recetas_recetapresentacion + core_sucursal
    "recetas_politicastocksucursalproducto",
    "recetas_solicitudreabastocedis",      # FK → core_sucursal + auth_user
    "recetas_solicitudreabastocedislinea", # FK → recetas_solicitudreabastocedis
    "recetas_movimientoproductocedis",     # FK → recetas_recetapresentacion + core_sucursal
    # ── Inventario ───────────────────────────────────────────────────────────
    "inventario_inventarioconfig",         # singleton, sin FK
    "inventario_existenciainsumo",         # FK → maestros_insumo
    "inventario_movimientoinventario",     # FK → maestros_insumo
    "inventario_ajusteinventario",         # FK → maestros_insumo + auth_user
    "inventario_almacensyncrun",           # FK → auth_user
    # ── Control ──────────────────────────────────────────────────────────────
    "control_ventapos",
    "control_mermapos",
    # ── POS Bridge ───────────────────────────────────────────────────────────
    "pos_bridge_branches",                 # FK → core_sucursal
    "pos_bridge_products",
    "pos_bridge_sync_jobs",
    "pos_bridge_extraction_logs",          # FK → pos_bridge_sync_jobs
    "pos_bridge_inventory_snapshots",
    "pos_bridge_daily_sales",              # FK → pos_bridge_branches + pos_bridge_products
    "pos_bridge_transfer_lines",
    "pos_bridge_production_lines",
    "pos_bridge_waste_lines",
    "pos_bridge_daily_branch_indicators",  # FK → pos_bridge_branches
    "pos_bridge_monthly_sales_official",
    "pos_bridge_recipe_runs",
    "pos_bridge_recipe_nodes",             # FK → pos_bridge_recipe_runs + pos_bridge_products
    "pos_bridge_recipe_node_lines",        # FK → pos_bridge_recipe_nodes
    # ── Core Audit ───────────────────────────────────────────────────────────
    "core_auditlog",                       # FK → auth_user
    # ── Integraciones ────────────────────────────────────────────────────────
    "integraciones_publicapiclient",
    "integraciones_publicapiaccesslog",    # FK → integraciones_publicapiclient
    # ── CRM ──────────────────────────────────────────────────────────────────
    "crm_cliente",
    "crm_pickupreservation",               # FK → crm_cliente + core_sucursal
    "crm_pedidocliente",                   # FK → crm_cliente + core_sucursal
    "crm_seguimientopedido",               # FK → crm_pedidocliente
    # ── Compras ──────────────────────────────────────────────────────────────
    "compras_presupuestocompraperiodo",
    "compras_presupuestocompraproveedor",  # FK → maestros_proveedor + compras_presupuestocompraperiodo
    "compras_presupuestocompracategoria",  # FK → compras_presupuestocompraperiodo
    "compras_solicitudcompra",             # FK → maestros_insumo + auth_user + core_sucursal
    "compras_ordencompra",                 # FK → maestros_proveedor + compras_solicitudcompra
    "compras_recepcioncompra",             # FK → compras_ordencompra + auth_user
    # ── Activos ──────────────────────────────────────────────────────────────
    "activos_activo",                      # FK → core_sucursal
    "activos_planmantenimiento",           # FK → activos_activo
    "activos_ordenmantenimiento",          # FK → activos_activo + auth_user
    "activos_bitacoramantenimiento",       # FK → activos_ordenmantenimiento + auth_user
    # ── Logística ────────────────────────────────────────────────────────────
    "logistica_rutaentrega",               # FK → core_sucursal
    "logistica_entregaruta",               # FK → logistica_rutaentrega
    # ── RRHH ─────────────────────────────────────────────────────────────────
    "rrhh_empleado",                       # FK → core_sucursal
    "rrhh_nominaperiodo",
    "rrhh_nominalinea",                    # FK → rrhh_nominaperiodo + rrhh_empleado
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def load_env() -> dict:
    env: dict = {}
    env_file = Path(".env")
    if not env_file.exists():
        print(f"{RED}ERROR: .env no encontrado en el directorio actual.{RESET}")
        sys.exit(1)
    for raw in env_file.read_text().splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip()
    return env


def pg_connect(env: dict):
    url = env.get("DATABASE_URL")
    if url:
        return psycopg2.connect(url)
    return psycopg2.connect(
        dbname=env.get("DB_NAME", "pollyanas_erp_dev"),
        user=env.get("DB_USER", "postgres"),
        password=env.get("DB_PASSWORD", ""),
        host=env.get("DB_HOST", "localhost"),
        port=int(env.get("DB_PORT", 5432)),
    )


def get_pg_col_info(pg_cur, table: str) -> dict[str, dict]:
    """
    Devuelve {column_name: {type, nullable, default}} para la tabla en PostgreSQL.
    nullable: True si acepta NULL, False si es NOT NULL.
    default: valor Python a usar cuando el valor SQLite es NULL y la columna es NOT NULL.
    """
    pg_cur.execute(
        """
        SELECT column_name, data_type, is_nullable, column_default,
               character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table,),
    )
    info: dict[str, dict] = {}
    for col, dtype, nullable, default, max_len in pg_cur.fetchall():
        info[col] = {
            "type":     dtype,
            "nullable": (nullable == "YES"),
            "default":  default,        # string SQL expression or None
            "max_len":  max_len,        # for character varying; None for others
        }
    return info


# Defaults por tipo cuando una columna es NOT NULL pero el valor SQLite es NULL
_NOT_NULL_FALLBACKS: dict[str, object] = {
    "boolean":                  False,
    "integer":                  0,
    "bigint":                   0,
    "smallint":                 0,
    "numeric":                  "0",
    "double precision":         0.0,
    "real":                     0.0,
    "character varying":        "",
    "text":                     "",
    "json":                     psycopg2.extras.Json({}),
    "jsonb":                    psycopg2.extras.Json({}),
    "timestamp with time zone": None,   # dejar NULL — Postgres puede permitir en bulk
    "date":                     None,
}


def coerce(value, col_info: dict):
    """
    Convierte un valor SQLite al tipo/restricción esperado por PostgreSQL.
    - Envuelve json/jsonb en psycopg2.extras.Json() para que psycopg2 lo adapte.
    - Si value es None en columna NOT NULL, aplica fallback por tipo.
    """
    dtype    = col_info["type"]
    nullable = col_info["nullable"]

    # Columna NOT NULL con valor NULL → aplicar fallback
    if value is None:
        if not nullable:
            return _NOT_NULL_FALLBACKS.get(dtype, None)
        return None

    if dtype == "boolean":
        return bool(value)

    if dtype in ("json", "jsonb"):
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except (ValueError, TypeError):
                parsed = {} if not nullable else None
            return psycopg2.extras.Json(parsed) if parsed is not None else None
        if isinstance(value, (dict, list)):
            return psycopg2.extras.Json(value)
        return value

    # Truncar strings que exceden character_maximum_length (esquema evolucionó)
    max_len = col_info.get("max_len")
    if max_len and isinstance(value, str) and len(value) > max_len:
        return value[:max_len]

    return value


def bar(done: int, total: int, width: int = 22) -> str:
    pct  = done * 100 // total if total else 100
    fill = pct * width // 100
    return f"[{'█' * fill}{'░' * (width - fill)}] {pct:3d}%"


def migrate_table(sq_conn, pg_conn, table: str) -> tuple[int | None, str | None]:
    """
    Migra una tabla de SQLite a PostgreSQL.
    Devuelve (filas_insertadas, mensaje_error).
    None en filas_insertadas significa que la tabla fue saltada.
    """
    sq_cur = sq_conn.cursor()
    pg_cur = pg_conn.cursor()

    # ── Columnas en SQLite ────────────────────────────────────────────────────
    sq_cur.execute(f'PRAGMA table_info("{table}")')
    sq_cols = [r[1] for r in sq_cur.fetchall()]
    if not sq_cols:
        return None, "tabla inaccesible en SQLite"

    # ── Columnas en PostgreSQL (con tipo + nullable + default) ────────────────
    pg_info = get_pg_col_info(pg_cur, table)
    if not pg_info:
        return None, "tabla no existe en PostgreSQL (esquema desactualizado)"

    # Columnas presentes en ambas (base de datos y SQLite)
    cols = [c for c in sq_cols if c in pg_info]
    if not cols:
        return None, "ninguna columna en común entre SQLite y PostgreSQL"

    # Columnas que existen en PostgreSQL (NOT NULL, sin DB default) pero NO en SQLite
    # → fueron agregadas en migraciones posteriores al backup; hay que inyectar un fallback
    extra_cols: list[str]  = []
    extra_vals: list       = []
    for col, info in pg_info.items():
        if col in sq_cols:
            continue                       # ya está en cols, se maneja normalmente
        if info["nullable"]:
            continue                       # es nullable; PostgreSQL pondrá NULL
        if info["default"] is not None:
            continue                       # PostgreSQL tiene un DEFAULT (sequence, etc.)
        fallback = _NOT_NULL_FALLBACKS.get(info["type"])
        if fallback is not None:
            extra_cols.append(col)
            extra_vals.append(fallback)
        # Si no hay fallback para ese tipo, lo dejamos fuera (FK ids, etc.)

    # ── Conteo rápido ─────────────────────────────────────────────────────────
    sq_cur.execute(f'SELECT COUNT(*) FROM "{table}"')
    total: int = sq_cur.fetchone()[0]
    if total == 0:
        return 0, None

    # ── Construir SQL de inserción ────────────────────────────────────────────
    all_cols   = cols + extra_cols
    col_select = ", ".join(f'"{c}"' for c in cols)
    col_insert = ", ".join(f'"{c}"' for c in all_cols)
    insert_sql = (
        f'INSERT INTO "{table}" ({col_insert}) VALUES %s ON CONFLICT DO NOTHING'
    )

    sq_cur.execute(f'SELECT {col_select} FROM "{table}"')

    inserted = 0
    extra_tuple = tuple(extra_vals)

    try:
        while True:
            rows = sq_cur.fetchmany(BATCH_SIZE)
            if not rows:
                break

            data = [
                tuple(coerce(v, pg_info[c]) for v, c in zip(row, cols)) + extra_tuple
                for row in rows
            ]
            psycopg2.extras.execute_values(pg_cur, insert_sql, data, page_size=BATCH_SIZE)
            pg_conn.commit()
            inserted += len(rows)

            print(
                f"\r  {bar(inserted, total)}  {inserted:>9,} / {total:,}   ",
                end="",
                flush=True,
            )

    except Exception as exc:
        pg_conn.rollback()
        return inserted or None, str(exc)

    return inserted, None


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if not SQLITE_PATH.exists():
        print(f"{RED}ERROR: No se encontró {SQLITE_PATH}{RESET}")
        sys.exit(1)

    print(f"\n{BOLD}{'═' * 60}{RESET}")
    print(f"{BOLD}  Migración SQLite → PostgreSQL{RESET}")
    print(f"{BOLD}{'═' * 60}{RESET}\n")

    env    = load_env()
    sq     = sqlite3.connect(str(SQLITE_PATH))
    pg     = pg_connect(env)

    db_name = env.get("DB_NAME", "pollyanas_erp_dev")
    print(f"  SQLite  : {CYAN}{SQLITE_PATH}{RESET}")
    print(f"  Postgres: {CYAN}{db_name}@{env.get('DB_HOST','localhost')}{RESET}\n")

    # Tablas realmente presentes en el SQLite
    sq_cur = sq.cursor()
    sq_cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    sqlite_tables = {r[0] for r in sq_cur.fetchall()}

    # Verificar si hay tablas en ORDER que no están en SQLite (solo advertir)
    missing = [t for t in ORDER if t not in sqlite_tables and t not in SKIP]
    if missing:
        print(f"{YELLOW}  Tablas en ORDER no encontradas en SQLite (se saltarán):{RESET}")
        for t in missing:
            print(f"    · {t}")
        print()

    # Resultados
    ok_tables:    list[tuple[str, int]] = []
    skip_tables:  list[tuple[str, str]] = []
    error_tables: list[tuple[str, str]] = []
    total_rows   = 0

    tables_to_run = [t for t in ORDER if t not in SKIP]

    print(f"  Tablas a migrar: {BOLD}{len(tables_to_run)}{RESET}\n")
    print(f"{'─' * 60}")

    for idx, table in enumerate(tables_to_run, 1):
        prefix = f"  [{idx:>2}/{len(tables_to_run)}]  {table}"

        if table not in sqlite_tables:
            print(f"{DIM}{prefix}{RESET}")
            skip_tables.append((table, "no en SQLite"))
            continue

        print(f"{prefix}")

        inserted, err = migrate_table(sq, pg, table)

        if err and inserted is None:
            # Tabla saltada por razón técnica
            print(f"\r  {YELLOW}⚠  {err}{RESET}")
            skip_tables.append((table, err))
        elif err:
            # Migración parcial con error
            print(f"\r  {RED}✗  error tras {inserted:,} filas: {err}{RESET}")
            error_tables.append((table, err))
            total_rows += inserted
        elif inserted == 0:
            print(f"\r  {DIM}─  0 filas (tabla vacía){RESET}")
            ok_tables.append((table, 0))
        else:
            print(f"\r  {GREEN}✓  {inserted:,} filas insertadas{RESET}          ")
            ok_tables.append((table, inserted))
            total_rows += inserted

    sq.close()
    pg.close()

    # ── Resumen final ─────────────────────────────────────────────────────────
    print(f"\n{BOLD}{'═' * 60}{RESET}")
    print(f"{BOLD}  RESUMEN FINAL{RESET}")
    print(f"{BOLD}{'═' * 60}{RESET}")
    print(f"  {GREEN}✓ Tablas OK     : {len(ok_tables)}{RESET}")
    print(f"  {YELLOW}⚠ Tablas saltadas: {len(skip_tables)}{RESET}")
    print(f"  {RED}✗ Tablas con error: {len(error_tables)}{RESET}")
    print(f"  {BOLD}Total filas migradas: {total_rows:,}{RESET}\n")

    if ok_tables:
        print(f"  {GREEN}── Tablas OK ────────────────────────────────────────{RESET}")
        for t, n in ok_tables:
            tag = f"{n:>10,} filas" if n else "     vacía    "
            print(f"    {GREEN}✓{RESET}  {t:<50} {tag}")

    if skip_tables:
        print(f"\n  {YELLOW}── Saltadas ─────────────────────────────────────────{RESET}")
        for t, reason in skip_tables:
            print(f"    {YELLOW}⚠{RESET}  {t:<50} {DIM}{reason}{RESET}")

    if error_tables:
        print(f"\n  {RED}── Errores ──────────────────────────────────────────{RESET}")
        for t, reason in error_tables:
            print(f"    {RED}✗{RESET}  {t:<50} {reason}")

    print()


if __name__ == "__main__":
    main()
