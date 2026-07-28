# agente-hikconnect

Agente para sincronizar checadas desde Hik-Connect Cloud hacia el ERP de Pollyana's Dolce.

Este agente no usa la IP local ni la contraseña local del dispositivo. Inicia sesion en el portal web de Hik-Connect, consulta la API cloud interna que alimenta `Attendance` y manda los eventos al receptor existente del ERP:

```text
POST /rrhh/api/asistencia-hik/
```

## Fuente

Hik-Connect Cloud:

```text
https://ius-team.hikcentralconnect.com/hcc/hccacs/v1/event/certificateRecords/search
```

Campos usados:

- `personInfo.baseInfo.personCode` -> `Empleado.codigo`
- `personInfo.baseInfo.firstName` + `lastName`
- `deviceTime`
- `deviceName`
- `recordGuid`

Como la tabla cloud de acceso no siempre marca entrada/salida, el agente ordena los eventos por empleado y dia:

- primer evento del dia -> `checkIn`
- siguientes eventos del dia -> `checkOut`

El receptor del ERP conserva la primera entrada y va actualizando la salida al ultimo evento del dia.

## Instalacion

```bash
cd /opt/agente-hikconnect
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
cp .env.example .env
```

Editar `.env` con:

```text
HIKCONNECT_EMAIL=
HIKCONNECT_PASSWORD=
ERP_API_KEY=
```

Si Hik-Connect trae un ID distinto al codigo maestro de nomina, usar alias:

```text
EMPLOYEE_CODE_ALIASES=234=337
```

Ejemplo actual: Hik-Connect muestra `234 ANIA FELIX LARA`, pero el ERP la conserva como `337 FELIX LARA ANIA YURIEL` por nomina.

## Pruebas

```bash
python main.py --test
python main.py --sync-once --dry-run
python main.py --backfill-hours 48 --dry-run
```

Cuando el dry-run se vea correcto:

```bash
python main.py --backfill-hours 48
python main.py
```

## Importacion manual de export de Attendance

Si se descarga CSV/XLSX desde el portal:

```bash
python main.py --import-file /ruta/attendance.csv --dry-run
python main.py --import-file /ruta/attendance.csv
```

Columnas esperadas:

```text
First Name, Last Name, ID, Department, Date, Time, Device Name, Device Serial No.
```

## Operacion persistente en el VPS

El agente corre en el VPS porque usa Hik-Connect Cloud: **no necesita estar en la misma red que el
checador**. Por eso siguio funcionando cuando se retiro el NAS de la sucursal. El camino viejo por
ISAPI/tunel (`scripts/checador_tunnel_proxy.py`, PeriodicTask `rrhh-sync-checador-hikvision-isapi`)
esta retirado y deshabilitado: no intentar revivirlo.

Ruta de despliegue: `/opt/agente-hikconnect`. Unidades en `systemd/` de esta carpeta.

```bash
# Publicar cambios de esta carpeta al VPS (el .env y la sesion NO se tocan):
rsync -av --exclude .env --exclude .venv --exclude storage_state.json --exclude '*.db' \
  ops/agente-hikconnect/ root@68.183.165.47:/opt/agente-hikconnect/
ssh root@68.183.165.47 'systemctl restart agente-hikconnect'
```

### Catch-up diario

`hik-catchup.timer` corre `catchup.py --horas 72` cada dia a las 11:00 UTC (04:00 America/Mazatlan).
Es la red de seguridad: si el sync de 5 minutos deja fuera algun marcaje, el barrido lo recupera esa
madrugada. El ERP deduplica, asi que correrlo de mas es inocuo.

El job para y rearranca el agente porque ambos comparten `storage_state.json` y `state.db`. Envia en
lotes de 25 porque `erp_client.send_events` manda todo en un POST con `timeout=20` y revienta con
volumen.

Ojo al operar: **toda ingesta de checadas dispara el motor de reglas de asistencia** — el receptor del
ERP llama `evaluar_dia_empleado` por cada empleado-dia y sincroniza bonos en BORRADOR. No es algo que
introduzca el catch-up: el sync normal de 5 minutos hace lo mismo con cada marcaje. Lo que cambia con
un catch-up ancho es el volumen de dias que se re-evaluan de golpe.

```bash
# Ver que falta sin escribir nada:
.venv/bin/python catchup.py --horas 72 --dry-run
# Hueco largo (recupera hacia atras lo que la nube conserve, ~31 dias):
systemctl stop agente-hikconnect
.venv/bin/python catchup.py --horas 744 --max-pages 80
systemctl start agente-hikconnect
```

Despues de un catch-up grande puede hacer falta recalcular reglas y bonos, **pero eso NO se corre por
iniciativa propia**: desde el 27-jul-2026 el motor de asistencia esta bajo revision de Mauricio y
cualquier recalculo en produccion requiere su autorizacion explicita. Si el backfill deja incidencias
que se ven mal, reportarlas y esperar.

Con autorizacion, el recalculo es:

```python
from rrhh.services_asistencia_reglas import evaluar_rango_asistencia
from rrhh.tasks import reconciliar_bonos_asistencia_periodo_actual
evaluar_rango_asistencia(desde, hasta)          # resuelve faltas/avisos/bajas falsos
reconciliar_bonos_asistencia_periodo_actual()   # solo BORRADOR; no pisa bono_extra/ajustes
```

### Paginacion: por que no se corta en la primera pagina

La nube pagina por **momento de subida**, no por `deviceTime`. Un marcaje viejo que el checador subio
con retraso aparece en las primeras paginas, y uno reciente puede quedar mas atras. La version
original de `fetch_records_since` cortaba en la primera pagina cuyo registro mas viejo caia fuera de
la ventana, y con eso perdia para siempre los marcajes buenos de las paginas siguientes (jun-jul 2026:
368 marcajes perdidos en el hueco 29-jun a 3-jul, con faltas falsas que cancelaron bonos).

Hoy se corta tras `PAGINAS_SECAS_PARA_CORTAR` paginas seguidas sin nada en ventana, y se deduplica por
`record_guid`. El check de regresion falla contra el codigo viejo:

```bash
.venv/bin/python test_pagination.py
```

### Fuera de git

`.env` (credenciales Hik-Connect y API key del ERP), `storage_state.json` (sesion del portal),
`state.db` y los logs viven solo en el servidor.
