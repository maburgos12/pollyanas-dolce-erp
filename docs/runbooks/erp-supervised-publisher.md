# Publicador recurrente con supervisión de tareas

El timer existente y el único receptor Telegram se conservan. Una aprobación
autentica una revisión, base y vencimiento; no es permiso para reiniciar procesos
sin comprobar su trabajo. `publish()` ya no dispone de un deployer predeterminado.
La CLI `worker` usa `erp_release_worker` y el callback durable `Supervisor`.

## Alcance automático y límites

El perfil inicial procesa cambios de Python de aplicación sin dependencias,
migraciones, Compose, configuración, herramientas de operación ni cambios del
propio publicador. Esos alcances exigen el operador supervisado explícito: no se
integran parcialmente, ni se utiliza el procedimiento antiguo como alternativa.
El texto de bloqueo permanece en Telegram. El horario de nuevas solicitudes
07:00–20:00 de Mazatlán, la revalidación diaria, las 12 horas y la revocación se
conservan; no se fabrican decisiones para continuar.

## Exclusión y publicación

Orden de candados no bloqueantes: despliegue, router, publicador, archivado Point,
preparación de limpieza y ejecución de limpieza. Los archivos son los mismos que
usan esos consumidores. Se comparan unidades y fuentes instaladas con Git antes
de confiar en ese contrato. No se detienen timers ajenos ni se cambia el receptor.
El descriptor del candado de despliegue se hereda al script oficial, que verifica
su inode: no existe un intervalo desbloqueado entre las comprobaciones y el deploy.

Una sesión en el contenedor PostgreSQL existente mantiene la exclusión Point real;
una consulta que sólo observa `pg_locks` no basta. Usa la base y el usuario ya
configurados, sin contraseñas en argumentos ni cambios de autenticación. El
protocolo verifica sesión, PID, inicio del backend y candado propios. Sobrevive al
reinicio de la web y exige confirmación de cierre y transporte recogido.

Antes del merge se verifican base limpia, CI, revisión, propietario/modo0600 y
lectura de `.env`, diez roles y cuatro respuestas dirigidas de los siete workers.
Si están ocupados, la decisión sigue pendiente de ejecución y se revalida en el
siguiente ciclo. La contención conocida no consume el límite de tres fallos de
código por revisión. Las incidencias y aprobaciones anteriores se conservan.

El callback captura nuevamente las colas, solicita TERM una sola vez y espera
la salida natural. No envía KILL a trabajadores. Se comprueban mensajes pendientes,
identidades, bytes, rutas y restauraciones nativas únicas antes del script oficial.
Una desaparición no equivale a éxito aunque el backend diga SUCCESS. Las capturas
tienen límites de cantidad, bytes, memoria y tiempo; excederlos requiere revisión.

El script oficial recibe SHA/base exactos. La comprobación posterior incluye
procesos nuevos, fuentes de la revisión dentro de los consumidores y `.env`
intacto. El script oficial conserva los checks Django y HTTP. La aceptación
funcional específica de cada cambio sigue perteneciendo a su tarea Factory.

## Incertidumbre y recuperación

Cada intento tiene un directorio exclusivo0700 y eventos0600 con fsync en
`/var/lib/erp-point-decision/releases/supervision/`. Se registra la intención antes
de cada efecto. No se reutilizan pines, cohortes, GO ni certificados históricos.

Una caída tras merge, TERM o inicio del deploy queda HOLD. No hay remerge, nueva
señal, rollback ni repetición automática del comando. El plazo del despliegue es
una alarma: se notifica y se mantienen candados mientras el comando o sus hijos
siguen trabajando; no se mata sólo al cliente para terminar la espera. Por eso la
unidad no aplica un timeout que interrumpa automáticamente la publicación.
No detener el servicio en curso para desbloquearlo: revisar su expediente primero.
La fila canónica `executing/uncertain` impide otra publicación hasta
que el operador reconcilie la evidencia y el estado real. No borrar registros ni
reiniciar contadores para desbloquearla. Las confirmaciones distinguen espera,
fallo previo y publicación sin confirmar; no presentan un HOLD como limpieza.

## Instalación y activación

Tras CI, revisión y aprobación Telegram exacta, publicar esta primera instalación
con el operador supervisado vigente, no mediante el publicador antiguo pausado.
Desde el checkout oficialmente publicado y limpio, fuera de los candados del
operador:

```sh
python3 ops/erp-telegram-release/install.py \
  --release-nonce NONCE_PUBLICADO --activate-supervised
```

El refresh exige la decisión aprobada y publicación exacta del checkout, verifica
los módulos/unidades, conserva el consumidor y activa el timer explícitamente.
Sin esa opción respeta su estado previo. Si falla la activación, conserva los
módulos nuevos compatibles y deja el timer detenido para revisión. Comprobar la
unidad realmente cargada, receptor, primer ciclo y registros; un archivo copiado
o timer `active` por sí solo no acredita el flujo.

Los registros antiguos `uncertain` no se eliminan ni se convierten artificialmente
en `published`. Si requieren conciliación, el operador añade explícitamente
`--reconcile-uncertain NONCE_HISTORICO` por registro al comando anterior. Exige
la publicación nueva aprobada y comprobada, el estado real del PR histórico y,
si hubo merge, padres, árbol y ascendencia. Guarda un certificado privado separado:
`no_merge_verified` o `superseded_verified`. Esto no acredita retrospectivamente
un despliegue antiguo ni permite volver a ejecutarlo. Una fila `executing` nunca
se exime con este mecanismo. Cada ciclo valida que el certificado siga vinculado
al historial intacto y a una publicación aprobada que sea ancestro del runtime.

Pruebas afectadas: `test_release_*.py`, `test_telegram*.py` y
`scripts/test_deploy_web_safe.sh`; integración aislada con PostgreSQL16,
Redis/Celery reales, restauración nativa y descriptor de flock heredado. Registrar
sus resultados por SHA y retirar exclusivamente los recursos sintéticos propios.

Procedencia: contrato de preservación usado por el operador supervisado1831,
convertido en módulos sin UUID, fechas, pines ni excepciones de aquella ejecución.
El prototipo `factory-telegram-supervised-20261120` se conserva como antecedente;
no se instala su perfil experimental de Nginx ni sus cambios ajenos a este alcance.
