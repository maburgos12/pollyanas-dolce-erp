# Archivo recuperable Point: piloto aprobado por lote

El servicio de VPS funciona sin Codex, Mac-IA ni API de IA. Sólo prepara fuentes
XLS de categorías cerradas, copia al NAS, lee de regreso y compara bytes/parser.
Sobre 75% solicita un lote exacto por Telegram; el ejecutor requiere aprobación
vigente. Instalar este código no autoriza ningún lote de históricos.

## Alcance efectivo

Piloto global: máximo 10 fuentes registradas, cada propuesta ≤100 MB; mínimo
30 días desde cierre de todas sus tareas, jobs exitosos, hashes/conteos coherentes,
sin alertas relevantes ni referencias mixtas. Ventas autoritativas, cierres
mensuales e históricos de producto referidos se excluyen del piloto. No se cambia
ningún registro operativo. La edad sola no autoriza eliminar.

Los cuatro lectores conservan ruta e identidad lógica. Si falta una fuente
registrada, encolan su ID y esperan como máximo dos segundos. El servicio recupera
la fuente en su ruta original; mientras tanto se informa recuperación pendiente,
nunca ventas vacías ni SUCCESS. La invocación que falló no se repite modificando
datos automáticamente: el siguiente intento usa el archivo recuperado.

El NAS guarda objetos por SHA y un índice inmutable por ruta/hash. Antes de ampliar
el piloto debe verificarse una copia independiente del catálogo y objetos.
No hay expulsión automática de archivos recuperados: volver a retirarlos requiere
otro lote permitido. Ninguna cuota de ahorro justifica ampliar una autorización.

APT/logs y Docker siguen sus procedimientos separados. El ejecutor residente no
los acepta: sus productores no comparten los leases de Point. Se conservan los
monitores existentes, respaldos, servicios y toda fuente fuera del lote.

## Instalación, después de aprobación exacta de PR/SHA

1. Publicar por el flujo Telegram/GitHub existente. Verificar CI, decisión y SHA
   publicado en el ledger. No copiar código por SCP ni hacer pull manual.
2. Desde `/opt/pastelerias-erp`, ejecutar como root:
   `python3 -B ops/erp-telegram-release/install_lifecycle.py --release-nonce NONCE`.
   Conserva byte a byte el receptor Point/Mac-IA y sus timers; instala nuevas
   unidades inactivas. Rechaza otro hash de receptor o trabajos residentes activos.
3. `python3 -B ops/erp-telegram-release/verify_lifecycle.py`: dos ciclos reales de
   NAS sobre una copia privada de un XLS, cola, publicación atómica y parser del
   worker. Nunca retira la fuente operativa. Conserva prueba privada ligada al SHA.
4. En menos de una hora, ejecutar el instalador con el mismo nonce y `--activate`.
   Exige dos ciclos exitosos del SHA actual. Activa sólo los dos timers nuevos.
5. Verificar unidades y una solicitud real con botones. Aprobar/rechazar un lote
   es una decisión separada de Mauricio. Registrar nonce/message_id y resultados.

El montaje esperado es `/opt/pastelerias-erp` → `/app`, UID worker 1000; la raíz
predeterminada `storage/pos_bridge`. Una instalación con raíz distinta requiere
adaptar y revisar la correspondencia host/contenedor antes de activar.

## Estados, fallas y recuperación

- Una decisión humana no se reemplaza por el resultado de ejecución.
- El claim bloquea revocaciones posteriores, como indica el mensaje. Vencer durante
  ejecución detiene retiros restantes. Cada archivo lleva intención fsync y resultado.
- Tras caída, el journal se reconcilia con existencia/hash; nunca se reinicia el lote
  ignorando retiros previos. Tres fallas previas bloquean y conservan el journal.
- Recuperación: tres intentos (1/5/15 min), luego intervención; originales de fuentes
  nuevas se conservan si falla preparación. Preparación falla como máximo tres veces
  por estado persistente, sin reiniciar el contador creando propuestas distintas.
- Notificación durable: rechazo explícito de Telegram permite reintento acotado;
  entrega ambigua no se reenvía ciegamente. Repetir botón informa estado sin repetir
  borrados. Los resultados se consultan en `cleanups/executions` y `cleanups/receipts`.
- Nunca desbloquear editando decisiones ni borrar registros. Diagnosticar NAS, espacio
  y permisos; una recuperación bloqueada necesita revisión explícita.
- Las unidades despiertan cada 30 s; la preparación inspecciona páginas de 100 tareas
  cada cinco minutos. Recuperación tiene prioridad, una transferencia a la vez.

## Reversión

Detener sólo timers nuevos, esperar servicios activos, conservar lector compatible
y ledger. Si hubo retiros, recuperar todas las fuentes afectadas y comprobar bytes
antes de retirar el lector. No revertir a lectores locales antiguos con fuentes
archivadas. Fallar una activación conserva módulos compatibles; no revierte código
mientras un job recién iniciado podría estar usándolo.

## Coordinación con horario Telegram

Depende del horario permanente del PR #1729. El broker prepara solicitudes
solo entre 07:00 y 20:00, America/Mazatlan; la recuperación pedida por lectores
continúa fuera de ese horario. El programador compartido revalida a las 07:00
el mismo lote pendiente vencido, sin generar una propuesta distinta. Rechazos,
aprobaciones y ejecuciones no se renuevan. La instalación conserva los módulos
y el timer del horario; no debe desplegarse una versión anterior encima.
