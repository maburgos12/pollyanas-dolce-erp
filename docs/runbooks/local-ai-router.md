# Asignación de solicitudes a IA local

Propietario: Pastelerías / Mauricio Burgos. Tarea: ia-local-router-20261119.
Alta19/11/2026. Revisión de recursos permanentes26/11/2026.

## Qué asigna

| Contrato | Entrada JSON mínima | Destino | Validación / límite |
| --- | --- | --- | --- |
| summary | text | <=1000 bytes UTF-8: Debian Qwen4B; hasta2500: Mac16 Qwen8B | JSON/longitud; revisión humana del contenido |
| extract | text y fields (1–6 nombres) | Mac16 Nemotron | Citas literales exactas o null; revisar correspondencia semántica |
| verify | rows con expected/actual como strings numéricos | Debian Python | Hasta100 pares; Decimal recalculado en ERP; sin inferencia |
| summary_cases | casos y referencias del lector existente | <=2 Debian Qwen4B; 3–4 Mac16 Qwen8B | Esquema, claves, fuentes y referencias vigentes; revisión humana |

La solicitud declara su contrato; reglas deterministas eligen equipo/modelo.
Si Debian ya tiene trabajo, los resúmenes breves compatibles pueden derivarse
a Qwen8B. La referencia de reparto es21s frente a43s del recibo de calibración:
tras usar ambos nodos, se permite aproximadamente el doble de cola breve en4B
antes de sumar otro trabajo a8B. Es una heurística acotada, no una promesa de
velocidad. Antes de reservar, el ejecutor comprueba disponibilidad. Los cambios periódicos se agrupan de dos en dos para poder repartir
trabajo entre ambos nodos; el piloto anterior conserva lotes de cuatro.
No se gasta otra inferencia para decidir el destino. La identidad del agente se
registra y debe estar activa. No se interpreta una frase ambigua como permiso
para pagos, SQL, archivos, comandos o despliegues. Los contratos no compatibles
quedan escalados, sin llamar una API de pago. El chat READ mantiene sus propios
permisos/herramientas; no se cambian proveedores silenciosamente.

## Uso real por agentes

Primero obtener evidencia autorizada mínima, sanearla y elegir contrato. Desde
la Mac de gestión, dentro del checkout del proyecto:

```bash
# El archivo contiene sólo: {"text":"evidencia breve ya localizada"}
python3 scripts/local_ai_request.py --agent local_ai_router --kind summary < evidencia.json
# Cotejo: {"rows":[{"expected":"0.30","actual":"0.3"}]}
python3 scripts/local_ai_request.py --agent local_ai_router --kind verify < cotejo.json
# Extracción: {"text":"Estado PENDIENTE","fields":["estado","importe"]}
python3 scripts/local_ai_request.py --agent local_ai_router --kind extract < campos.json
python3 scripts/local_ai_request.py --status
```

El cliente utiliza el acceso administrativo SSH existente, en modo BatchMode y
con huella estricta; no transmite esa llave a los nodos. El UUID predeterminado
es estable para el mismo agente/contrato/evidencia; repetir la solicitud devuelve
su recibo y no genera otra inferencia. Para datos de una nueva revisión, enviar
la nueva evidencia. `--request-id` permite UUID explícito; contenido diferente
con el mismo UUID se rechaza. Los demás agentes usan su código activo, sus fuentes
y permisos actuales. Este mecanismo no intercepta conversaciones de Codex fuera
del proyecto ni convierte tareas complejas en trabajo local sin validar alcance.

La entrada interna equivalente es `python manage.py local_ai_route submit`,
con JSON por stdin: `agent`, `kind`, `data`, `request_id`. Acceso únicamente por
el canal administrativo; no se abrió API pública. `status` devuelve recibos,
asignación y estado de contacto sin entradas completas, credenciales ni leases.
No consultar repetitivamente: revisar cuando termine el intervalo esperado o
cuando Orquestación muestre un cambio. Un trabajo bloqueado se revisa; no se
repiten automáticamente solicitudes con UUID distintos para eludir límites.

## Cola y trabajo periódico

Activar con `local_ai_route enable`; pausar nuevas reservas con `disable`.
El encolador horario `orquestacion.local_ai_enqueue` reutiliza las mismas cinco
fuentes y deduplicación del piloto. Si el router está activo entrega sus nuevos
lotes a esta cola; lo ya aceptado por `mac_ia_jobs` se termina antes de competir
por la Mac. Desactivar sólo el router devuelve los nuevos lotes al piloto previo.
`mac_ia_jobs disable` conserva su semántica: detiene generación periódica del
piloto; las solicitudes explícitas del router se controlan por separado.

Máximo16 trabajos pendientes/en ejecución, una reserva por nodo, vencimiento12min,
3 intentos con espera5/10min. Datos o referencias cambiados cancelan el resultado.
Fuentes idénticas no crean otra inferencia. Si un nodo está desconectado se conserva
la cola, se registra el contacto fallido y no se usa proveedor pagado. No se
llenan equipos con trabajo redundante para aparentar ocupación.

Pantalla: `/orquestacion/#mac-ia-02`, fragmento existente autenticado con el mismo
refresco15s mientras esté visible. Muestra agente, tipo, equipo, modelo, motivo,
estado, resultado y tiempo. Resolver cómputo técnico no resuelve el expediente
operativo. `exact` valida cotejos; la extracción sólo acredita citas presentes;
los resúmenes requieren revisión humana. Tokens locales no equivalen a cuota
Codex ahorrada ni dinero ahorrado.

## Transporte y recursos permanentes

VPS: `/etc/pastelerias-local-router/` (root0700), dos llaves de ejecución propias,
known_hosts fijado, ssh_config privado; ninguna credencial en Git/DB/panel.
El trabajador corre desde el commit desplegado, no desde copias sueltas.
`pastelerias-local-router.timer` activa un oneshot cada60s después de terminar.
Sin trabajo elegible no consulta modelos/nodos. Comparte
`/run/pastelerias-mac-ia/worker.lock` con el piloto y puede usar ambos nodos en
paralelo. Atiende hasta cuatro trabajos por nodo sin pausa intermedia; deja de
iniciar nuevos al alcanzar120s, conservando el timeout propio de cada corrida.
Cede si el nodo está ocupado o un intento falla; no insiste durante el backoff. `systemctl status pastelerias-local-router.service` y journal contienen
sólo IDs, estados y modelos, sin prompts ni errores SSH completos.

Cada llave de ejecución permite exclusivamente el comando forzado
`router_node.py` y opciones `restrict`, sin shell, TTY ni forwarding. Mac16 usa
su túnel privado22066 existente, conserva las huellas y el acceso administrativo.
Debian mantiene un SSH saliente con llave propia hacia `erp_mac_ia_router`, cuenta
sin sesiones con PermitListen sólo127.0.0.1:22067. Configuración VPS versionada:
`ops/erp-mac-ia/82-pastelerias-local-router.conf`; probar `sshd -t` antes de reload.
No se abren puertos públicos ni se modifican túneles NAS/Mac existentes.

Debian sin sudo/linger disponible: un único cron de transporte marcado
`# pastelerias-local-router-transport` llama `debian-router-transport.sh` cada
minuto. `flock` conserva una conexión y evita procesos duplicados; tras cortar
la conexión se reintenta al siguiente minuto. Se conserva literalmente el cron
operativo anterior y se registra su hash; no se toca el consumidor Telegram.
La llave/config/known_hosts de transporte viven en `~/.ssh/` con modo0600.

En ambos nodos, ejecutor publicado en `~/.local/share/pastelerias/local-router/`,
bloqueo propio y directorios efímeros0700 por trabajo que se retiran al terminar.
Debian cede `cron.lock`, ventana con >210s hasta la próxima ronda de15min y memoria
para Qwen4B. Usa nice19, CPU2, ctx2048, salida192, máximo150s. No modifica los
scripts de ERP/Point ya instalados.

Mac16 usa Qwen8B residente en18124. Nemotron se carga bajo demanda en loopback18126,
CPU4, ctx4096, salida192, prioridad baja; se termina solamente su proceso propio.
Si Qwen comienza trabajo o memoria libre<15%/swap crece>128MiB, cede su turno.
No se detiene/reinicia Qwen ni se ejecutan ambas inferencias deliberadamente.
Los archivos temporales y la llave efímera se retiran; pesos verificados ya
instalados se conservan sin descargar otra copia. FileVault/inicio de sesión
siguen pudiendo requerir presencia tras reiniciar la Mac16.

## Reversión

1. `local_ai_route disable` y detener/deshabilitar únicamente
   `pastelerias-local-router.timer`. No borrar historial ni tareas de negocio.
2. Conservar `pastelerias-mac-ia.timer`, modelos y transportes anteriores. El
   encolador existente vuelve al piloto Qwen cuando el router está desactivado.
3. Para retirar acceso nuevo, borrar sólo líneas authorized_keys marcadas
   `pastelerias-router-exec-*` y el cron de transporte con el marcador propio,
   revocar la llave del usuario `erp_mac_ia_router`, retirar configuración82
   tras `sshd -t` y reload. No retirar22066/18125/22065 ni llaves de otros frentes.
4. Antes de retirar fuentes/artefactos con evidencia única, respaldar y verificar.

## Calibración del ejecutor candidato

19/11/2026: evidencia sanitizada real del lote Proyección2027, sin recalcularlo.
Debian Python0.000s interno; Qwen4B21.303s; Mac16 Nemotron34.992s con extracción
94,576/4,633/null; Qwen8B43.169s. Tiempos individuales, transporte aparte y un
solo caso por ruta, no comparación general ni ahorro end-to-end. La aceptación
productiva debe acreditar cola→nodo→validación→Orquestación sobre el commit
desplegado y registrar conservación del cron/PID originales.
