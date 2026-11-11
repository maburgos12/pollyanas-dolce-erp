# Publicación ERP autorizada desde Telegram

Único consumidor Telegram: `erp-point-decision.service`. El instalador agrega un
hook `d:` antes del parser original p/t/g, solamente si SHA256 del consumidor
coincide con el baseline inspeccionado. No instala un segundo `getUpdates`.

El timer nuevo consulta el ledger existente `/var/lib/erp-autofix/ledger.json`.
Solo propone candidatos `awaiting_approval` / `draft_complete` con PR y SHA válidos;
excluye `operator-*`, conserva decisiones anteriores y no reenvía el mismo PR/SHA.
La solicitud manual root permite revisar nuevamente un candidato expresamente.
Ninguna ruta, comando o argumento de ejecución se toma del diagnóstico generado.

## Contrato de aprobación

- Botón **Aprobar y desplegar**: repo fijo, PR, SHA, base y producción iguales,
  nonce aleatorio, expiración de una hora, chat privado autorizado y message_id.
- HMAC incluye todos esos datos de revisión. El actor Telegram debe coincidir con
  el chat privado configurado. Una pulsación repetida no ejecuta dos publicaciones.
- **Rechazar / revocar** funciona antes de que el worker reclame la aprobación.
  Después del claim no se puede prometer cancelación de una operación remota.
- Ledger root, directorio0700, archivos0600, esquema cerrado, flock y escritura
  atómica con fsync. `erp-codex` y procesos generadores no tienen permiso de escritura.
- La GitHub App registra la decisión como comentario de bot, con SHA/nonce/fecha;
  no suplanta una revisión humana GitHub ni publica identificadores privados Telegram.
- Antes de solicitar o publicar, exige protección vigente de main: strict=true,
  enforce_admins=true, check test de GitHub Actions (app15368), sin bypass. La
  credencial debe poder LEER esa política; un403 bloquea y no se interpreta como
  permiso. No modifica protecciones ni credenciales automáticamente.
- El publicador verifica CI paginado completo y `test` exitoso de GitHub Actions,
  repositorio/base/head vigentes y base ancestro del candidato. Exige merge normal
  con SHA esperado; comprueba padres `[base, head]` y árbol idéntico al candidato.
- La protección strict de GitHub rechaza la integración de una base atrasada.
  La API de merge no ofrece CAS sobre base_sha: la comprobación posterior de
  padres/árbol sigue siendo defensiva si un administrador cambia la política
  durante la operación, y bloquea desplegar una revisión distinta.
- Deploy oficial recibe `--expected-sha RELEASE --expected-base PRODUCTION`;
  no se hace git pull por adelantado. El lock oficial se comparte con deploy manual.

`published` significa que el deploy oficial terminó y HEAD de producción coincide
con el release comprobado. No certifica automáticamente la aceptación funcional
particular de cada reparación; esa evidencia sigue perteneciendo a la fábrica.

## Requisitos pendientes de habilitación

Este código no está operativo por estar escrito o probado localmente. Antes de
habilitarlo, Mauricio debe autorizar y un administrador debe configurar:

- `main`: required status checks strict=true, enforce_admins=true y check `test`
  vinculado a GitHub Actions app15368, sin bypass de revisiones.
- La GitHub App existente: permiso **Administration: Read-only** concedido en la
  instalación del repositorio ERP. No se solicita Administration:write.

El publicador importa una instancia aislada del adaptador existente y añade solo
`administration: read` a los permisos pedidos al emitir SU token. No modifica el
archivo del adaptador ni los permisos pedidos por el generador. La ruta de
protección está limitada a GET; nunca configura protecciones desde el worker.
Sin el grant, GitHub rechaza la emisión o lectura y el flujo permanece bloqueado.
Las comprobaciones realizadas con permisos existentes devolvieron REST403 y
GraphQL FORBIDDEN; no se trató esa falta de acceso como protección válida.

## Instalación y operación

1. Integrar mediante PR, checks requeridos y deploy oficial este código revisado.
2. Ejecutar desde el checkout limpio de producción:
   `python3 /opt/pastelerias-erp/ops/erp-telegram-release/install.py`.
3. El instalador respalda el consumidor original por hash, verifica bytes, instala
   módulo/unidades, reinicia el consumidor único y comprueba servicio activo.
4. Habilita `erp-telegram-release.timer` y deshabilita exclusivamente
   `erp-mobile-approval.timer` para no enviar la vieja solicitud ambigua en paralelo.
   No toca timers Point, Doctor, disco, ni archivos de datos del ERP.
5. Verificar timers, logs, ausencia de un segundo getUpdates, botones en el chat
   autorizado, comentario real GitHub y revisión exacta desplegada.

Solicitud manual sin mostrar credenciales:

```sh
CREDENTIALS_DIRECTORY=/etc/erp-mac-ia-watchdog \
  python3 -B /usr/local/libexec/erp-point-decision/erp_telegram_release.py request --pr NUMERO
```

El worker y la solicitud CLI comparten un flock exclusivo no bloqueante; se
procesan decisiones y notificaciones aunque el ledger de descubrimiento falte.
Telegram prohíbe redirecciones y sanea errores para no revelar la URL con token.

El instalador toma el mismo `.worker.lock` no bloqueante durante toda la instalación;
rechaza servicios active/activating/reloading/deactivating antes de mutar archivos.
No intenta detener el timer en su primera instalación si LoadState=not-found.
Comprueba credencial y política live antes de reemplazar archivos o desactivar
el timer anterior; un403 o strict=false conserva el flujo existente.
Valida sintaxis/unidades antes de mutar, admite repetir exactamente
el hook instalado y restaura el consumidor/archivos anteriores y estados de
timers si falla. Conserva archivos nuevos inactivos para diagnóstico; no limpia.

La credencial GitHub App se carga usando el adaptador ya instalado de autofix.
El generador conserva intacta su lista de endpoints autorizados y no recibe
permisos adicionales. El publicador root usa llamadas fijas al repositorio ERP.
No se instalan dependencias, puertos ni servicios pagados.

## Incertidumbre y desactivación

`notifying`, `executing` y `uncertain` requieren conciliación humana ante caída o
respuesta perdida. No se repiten automáticamente la solicitud inicial, el merge ni el deploy.
El outbox reintenta únicamente notificaciones informativas finales pendientes;
una respuesta Telegram perdida puede duplicar el aviso, nunca la publicación. Conservar
ledger, comparar GitHub, SHA de producción y logs del deploy antes de decidir.
No editar un estado para volver a ejecutar a ciegas. Una solicitud vencida se
invalida antes de una nueva solicitud manual. Rechazados no se reabren por timer.

Para detener publicaciones nuevas, detener y deshabilitar únicamente
`erp-telegram-release.timer`, revisar si hay servicio ejecutándose y conservar la
sesión/ledger. Para retirar el hook, comparar el consumidor instalado con el
baseline más hook; restaurar solo el respaldo verificado y reiniciar su servicio.
Volver al timer viejo requiere una decisión explícita; no cambia ni revierte
código ERP ya publicado. Nunca hay rollback de datos ni limpieza automática.

## Pruebas

`python3 -m unittest discover -s scripts/tests -p test_telegram_release.py`

Incluye suplantación, chat/grupo/mensaje/firma, expiración/replay/revocación,
head/base diferentes, CI ausente/fallido/no confiable, paginación, pérdida de
respuesta, caída tras claim, árbol/padres incorrectos, permisos/esquema/symlinks,
discovery/deduplicación y conservación de rutas p/t/g. Estas pruebas usan mocks
para servicios externos: no sustituyen un ciclo real de Telegram/GitHub/VPS.
