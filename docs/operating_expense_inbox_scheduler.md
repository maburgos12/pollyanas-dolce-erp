# Operating Expense Inbox Scheduler

## Comando

Ejecutar desde la raíz del ERP:

```bash
.venv/bin/python manage.py process_operating_expense_inbox --dir /ruta/al/inbox/gastos --year 2026 --settings=config.settings
```

Sin refresh de proyectos:

```bash
.venv/bin/python manage.py process_operating_expense_inbox --dir /ruta/al/inbox/gastos --year 2026 --no-refresh-projects --settings=config.settings
```

## Ruta esperada de archivos

- Inbox operativo sugerido: `/ruta/al/inbox/gastos`
- Archivos aceptados: `.xlsx`
- El comando procesa archivos del nivel superior del inbox
- Subcarpetas usadas por el pipeline:
  - `processed/`
  - `failed/`
  - `duplicate/`

## Comportamiento esperado

- Archivo válido:
  - se valida
  - se hace upsert en `GastoOperativoMensual`
  - se refrescan proyectos si el comando no lleva `--no-refresh-projects`
  - se mueve a `processed/`
- Archivo duplicado por hash:
  - no se valida
  - no hace upsert
  - no refresca proyectos
  - se mueve a `duplicate/`
- Archivo inválido, corrupto, vacío o con error de negocio:
  - se registra en `CargaGastoOperativoArchivo`
  - se mueve a `failed/`
  - el batch continúa con los demás archivos

## Cron en macOS

Editar `crontab -e` y agregar:

```cron
*/10 * * * * cd /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1 && /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/.venv/bin/python manage.py process_operating_expense_inbox --dir /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/storage/uploads/gastos/inbox --year 2026 --settings=config.settings >> /Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/storage/logs/opex_inbox.log 2>&1
```

## Launchd en macOS

Archivo sugerido: `~/Library/LaunchAgents/com.pollyana.erp.opex-inbox.plist`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>com.pollyana.erp.opex-inbox</string>
    <key>ProgramArguments</key>
    <array>
      <string>/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/.venv/bin/python</string>
      <string>/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/manage.py</string>
      <string>process_operating_expense_inbox</string>
      <string>--dir</string>
      <string>/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/storage/uploads/gastos/inbox</string>
      <string>--year</string>
      <string>2026</string>
      <string>--settings=config.settings</string>
    </array>
    <key>WorkingDirectory</key>
    <string>/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1</string>
    <key>StartInterval</key>
    <integer>600</integer>
    <key>RunAtLoad</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/storage/logs/opex_inbox.stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/storage/logs/opex_inbox.stderr.log</string>
  </dict>
</plist>
```

Cargar el job:

```bash
launchctl load ~/Library/LaunchAgents/com.pollyana.erp.opex-inbox.plist
launchctl start com.pollyana.erp.opex-inbox
```
