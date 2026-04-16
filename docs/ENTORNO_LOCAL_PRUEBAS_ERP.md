# Entorno Local de Pruebas del ERP

Fecha de corte: 2026-04-09

## Resumen

La ruta oficial de pruebas del ERP debe usar PostgreSQL, no SQLite.

Motivo:

- el proyecto incluye migraciones y SQL de PostgreSQL en `reportes`, incluyendo `CREATE MATERIALIZED VIEW`
- `config.settings_test` forzando SQLite hacia fallar la suite durante migraciones
- la venv local del repo ya existe y debe usarse como interprete de pruebas

## Problemas reales detectados

### 1. Interprete equivocado

`python3 manage.py test` puede fallar con `ImportError: Couldn't import Django` si no usa la venv del repo.

Ruta correcta:

- `./.venv/bin/python`

### 2. Backend de pruebas incorrecto

El override anterior de `config.settings_test` forzaba SQLite.

Eso no es compatible con:

- [reportes/migrations/0016_mv_dashboard_daily_ops.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/reportes/migrations/0016_mv_dashboard_daily_ops.py)
- [reportes/migrations/0017_dashboardfullsnapshot_mv_dashboard_full.py](/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/reportes/migrations/0017_dashboardfullsnapshot_mv_dashboard_full.py)

Porque ambas usan `MATERIALIZED VIEW`, que es SQL no portable a SQLite.

## Decision tomada

Ruta elegida: **PostgreSQL obligatorio**

- usar PostgreSQL para tests como camino oficial
- no ofrecer SQLite como fallback operativo ni de laboratorio para la suite del ERP
- no prometer compatibilidad general de la suite sobre SQLite

## Comando oficial

Para correr pruebas locales:

```bash
./scripts/run_tests_local.sh orquestacion.tests.SeedOrquestacionCatalogTest \
  api.tests_ai_gateway.AIGatewayApiTests.test_manifest_exposes_safe_gateway_contract
```

Tambien funciona:

```bash
./.venv/bin/python manage.py test --settings=config.settings_test --noinput <test_labels>
```

## Variables esperadas

Se toma PostgreSQL desde:

- `TEST_DATABASE_URL`, o si no existe
- `DATABASE_URL`, o si no existe
- `DB_HOST` + `DB_NAME` + `DB_USER` + `DB_PASSWORD` + `DB_PORT`

Para evitar colisiones entre corridas:

- `scripts/run_tests_local.sh` asigna un `TEST_DB_NAME` unico por proceso si no se define uno manualmente
- si se quiere fijar una base de prueba concreta, puede exportarse `TEST_DB_NAME` antes de correr el script

## Validacion confirmada

Con PostgreSQL y `.venv`, esta bateria paso:

- `orquestacion.tests.SeedOrquestacionCatalogTest`
- `api.tests_ai_gateway.AIGatewayApiTests.test_manifest_exposes_safe_gateway_contract` (verifica el contrato seguro del manifest del gateway)

## Politica actual

SQLite queda retirado como backend de prueba del ERP.  
Las pruebas locales deben correr sobre PostgreSQL.

## Riesgos

- si se fija manualmente el mismo `TEST_DB_NAME` en varias corridas simultaneas, puede haber colisiones
- si PostgreSQL local no esta disponible, la suite oficial no podra correr

## Rollback

- eliminar `scripts/run_tests_local.sh`
- volver a una estrategia manual con `./.venv/bin/python manage.py test`
- no recomendado: volver a forzar SQLite para toda la suite
