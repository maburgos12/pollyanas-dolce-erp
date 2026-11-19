# Mac-IA-02 — nodo local de inferencia de Pastelerías

Responsable: Mauricio Burgos. Alta: 2026-11-19. Propietario técnico: proyecto
Pastelerías ERP; tarea `mac-ia-02-20261119`. No reemplaza Mac-IA Debian.

## Equipo y acceso

- MacBook Pro de 15 pulgadas (2018), Intel Core i7 2.2 GHz de seis núcleos,
  16 GiB de RAM, macOS 15.1 (24B83), arquitectura x86_64.
- Disco observado al alta: 233 GiB, 62 GiB disponibles antes de instalar el modelo.
- Host observado en casa: `macbookpro.lan`; usuario `mauricioburgos`.
- GPU adicional detectada: Radeon Pro 555X, 4 GiB de VRAM. El binario instalado
  lista sólo BLAS/Accelerate: esta configuración usa CPU, sin prometer aceleración AMD.
- Alias del cliente de gestión: `pastelerias-mac-ia-02`, en `~/.ssh/config`.
- Autenticación por llave exclusiva `~/.ssh/pastelerias_mac_ia_02_ed25519`.
  La clave privada permanece en la Mac de gestión y NO va a Git ni al ERP.
- Host fijado en `~/.ssh/pastelerias_mac_ia_02_known_hosts`, con comprobación estricta.
  Huella ED25519 verificada por Mauricio en la propia MacBook:
  `SHA256:DGD1nZgCmubgvat4nPg/HINMbCJmNitF9vyVonymbOc`.
- No confundir este equipo con Mac-IA Debian. No reutilizar las llaves de Debian,
  NAS o producción. No conceder acceso completo al disco para inferencia.

Desde la Mac de gestión, prueba sin contraseña:

```bash
ssh pastelerias-mac-ia-02 'hostname'
```

El alias principal usa el VPS como salto y no necesita resolver `.lan`. Mauricio
autorizó expresamente el acceso entre redes en este chat, después de revisar la
propuesta de cuenta restringida y puerto privado. La alternativa directa para la
red de casa es `pastelerias-mac-ia-02-lan`. Un nuevo cliente necesita su propia
llave y verificación de identidad; el repositorio no contiene accesos.

### Acceso desde casa, Dolce u otra red

La MacBook mantiene un túnel SSH saliente al VPS existente `68.183.165.47`:

- Cuenta de transporte: `erp_mac_ia_02`, sin contraseña, TTY ni sesiones.
- Configuración exclusiva: `/etc/ssh/sshd_config.d/80-pastelerias-mac-ia-02.conf`.
  `AllowTcpForwarding remote`, `GatewayPorts no`, `MaxSessions 0`,
  `PermitListen 127.0.0.1:22066`. La llave añade `restrict,port-forwarding` y el
  mismo `permitlisten`; no habilita acceso shell o forwarding local.
- En el VPS sólo escucha `127.0.0.1:22066`; destino `127.0.0.1:22` de MacBook.
  No se abrieron puertos en routers ni se publicaron endpoints de inferencia.
- Registro del recurso: `/var/lib/pastelerias-mac-ia-02/ownership.txt`.
- Llave de transporte en MacBook: `~/.ssh/pastelerias_mac_ia_02_vps_ed25519`.
  Host VPS fijado en `~/.ssh/pastelerias_mac_ia_02_vps_known_hosts`; huella
  `SHA256:vl6ULl+3tzY1C9Y6dp5wO5GHoSF/5xnBemtDZ0WfgCM`, contrastada mediante
  la sesión administrativa existente.
- LaunchAgent MacBook: `com.pollyanasdolce.mac-ia-02.vps-tunnel`, arranque al
  iniciar sesión, keepalive 30 segundos y relanzamiento mínimo cada 30 segundos.
- El alias de la Mac principal usa su acceso SSH administrativo existente al VPS
  (`~/.ssh/agente_dg_ops`) como salto y la llave exclusiva de MacBook para el
  destino final; no se copió la llave administrativa al nodo.

Conservar la huella original del destino incluso a través del salto. Para
comprobar el servicio de transporte desde la red local:

```bash
ssh pastelerias-mac-ia-02-lan 'launchctl print gui/502/com.pollyanasdolce.mac-ia-02.vps-tunnel'
```

El cambio de red física no se simula desconectando a otros usuarios. Validar la
ruta por VPS y su reconexión no equivale a haber probado cada red: necesita
Internet y salida SSH al VPS. FileVault y el inicio de sesión siguen siendo límites.

## Runtime local

Directorio propio en la MacBook:
`~/.local/share/pastelerias/mac-ia-02/`.

- `runtime/llama-b12211/llama-server`: binario oficial macOS x64 de llama.cpp,
  build 12211, commit reportado `7d7f02875`.
- Archivo de distribución: `llama-b12211-bin-macos-x64.tar.gz`, SHA256
  `6055cc672cc5c744481bb88ac1a7739ff476966c8a190ddaa20676034693a358`.
- `models/Qwen3-8B-Q4_K_M.gguf`: modelo oficial de `Qwen/Qwen3-8B-GGUF`,
  revisión `7c41481f57cb95916b40956ab2f0b139b296d974`, 5,027,783,488 bytes.
  SHA256 `d98cdcbd03e17ce47681435b5150e34c1417f50b5c0019dd560e4882c5745785`.
  Identidad en metadatos: `Qwen3 8B Awq Compatible Instruct`, arquitectura qwen3,
  variante `awq-compatible-Instruct`. Verificar el contenido; no inferir identidad
  solamente por la etiqueta del proveedor o el nombre del archivo.
- `api.key` y `client.curl`: credencial local y configuración de cliente, modo
  0600. No imprimir, copiar al repositorio ni pasar la credencial como argumento.
- `logs/`: salida del proceso; `evidence/`: comprobaciones sin información privada.
- Una solicitud a la vez, contexto de 4096 tokens, cuatro hilos, CPU, prioridad
  reducida y salida predeterminada de 512 tokens. Razonamiento extendido desactivado
  como punto de partida para tareas breves; se usa la plantilla incluida en el GGUF.
- API: `http://127.0.0.1:18124/v1`, con autenticación. No escuchar en `0.0.0.0`.

Fuentes del runtime: [release oficial](https://github.com/ggml-org/llama.cpp/releases/tag/b12211)
y [servidor llama.cpp](https://github.com/ggml-org/llama.cpp/tree/master/tools/server).
Fuente del modelo: [Qwen3 8B GGUF, revisión fijada](https://huggingface.co/Qwen/Qwen3-8B-GGUF/tree/7c41481f57cb95916b40956ab2f0b139b296d974).
Los hashes se verifican antes de usar el binario y el modelo. Un `.part` no es
un modelo instalado ni un resultado de validación.

## Servicio y recuperación

LaunchAgent de la cuenta `mauricioburgos`:
`~/Library/LaunchAgents/com.pollyanasdolce.mac-ia-02.llama.plist`.

`RunAtLoad` y `KeepAlive` arrancan el runtime al iniciar la sesión y recuperan
salidas inesperadas, con intervalo mínimo de 30 segundos entre relanzamientos.
La configuración usa `nice 10`, E/S de baja prioridad y rutas absolutas.

```bash
ssh pastelerias-mac-ia-02 'launchctl print gui/502/com.pollyanasdolce.mac-ia-02.llama'
ssh pastelerias-mac-ia-02 'curl -fsS http://127.0.0.1:18124/health'
```

Para reiniciar únicamente este servicio:

```bash
ssh pastelerias-mac-ia-02 'launchctl kickstart -k gui/502/com.pollyanasdolce.mac-ia-02.llama'
```

Para detenerlo de forma persistente hasta una nueva carga, ejecutar en la MacBook:

```bash
launchctl bootout gui/502 "$HOME/Library/LaunchAgents/com.pollyanasdolce.mac-ia-02.llama.plist"
```

Para cargarlo nuevamente en la misma sesión:

```bash
launchctl bootstrap gui/502 "$HOME/Library/LaunchAgents/com.pollyanasdolce.mac-ia-02.llama.plist"
```

No confundir recuperación de proceso con recuperación después de reiniciar la
computadora. FileVault está activo: puede requerir desbloqueo local del disco
antes de disponer de SSH y la sesión. Mantener cargador y tapa abierta. No se
modificó suspensión: al alta `sleep=0` con corriente y `sleep=1` con batería.
No desactivar FileVault ni configurar inicio de sesión automático para eludirlo.

## Interfaz de navegador

Usar la interfaz nativa del servidor mediante un túnel SSH limitado a localhost
por ambos extremos. En la Mac de gestión queda instalado el LaunchAgent
`~/Library/LaunchAgents/com.pollyanasdolce.mac-ia-02.tunnel.plist`, con arranque
al iniciar sesión, reconexión y espera de 30 segundos entre intentos. Logs en
`~/Library/Logs/PasteleriasMacIA02/`. No depende de mantener Terminal abierta.
Comprobarlo con `launchctl print gui/502/com.pollyanasdolce.mac-ia-02.tunnel`.

Si se detiene ese túnel y se necesita una conexión manual desde la Mac de gestión:

```bash
ssh -N -o ExitOnForwardFailure=yes -L 127.0.0.1:18124:127.0.0.1:18124 pastelerias-mac-ia-02
```

Abrir `http://127.0.0.1:18124` mientras el túnel esté activo. Desde la propia
MacBook se puede abrir esa dirección sin túnel. La interfaz permite conversaciones
y respuestas en vivo; no es un tablero de trabajos del ERP. La credencial de la
interfaz se usa localmente, sin publicarla en documentación ni en el chat.
Para tareas breves, seleccionar `Reasoning effort → Off` en la interfaz; el
cliente puede enviar su propia preferencia por encima del valor del servidor.
Las conversaciones del navegador no sustituyen la bitácora de un trabajo.

Las métricas de generación están disponibles en `/metrics`. La visibilidad de
actividad y tiempos no demuestra corrección del resultado ni expone un supuesto
proceso interno completo de razonamiento.

## Uso por agentes y reducción de tokens de pago

Antes de delegar, comprobar SSH y salud. Usar este nodo para tareas pequeñas y
acotadas: resumir alertas, clasificar o extraer campos de evidencia ya localizada.
Conservar cálculos y validaciones exactas en código/SQL. No delegar decisiones de
nómina, permisos, pagos, cambios operativos o despliegues.

Enviar al nodo sólo las fuentes mínimas necesarias, con límite de salida. No
mandar todo el repositorio ni claves, tokens, cookies o `.env`. La respuesta del
modelo es dato no confiable: validar estructura, identidades e importes contra
las fuentes. Nunca ejecutar texto generado como comandos o SQL.

Ejemplo desde la Mac de gestión, con `solicitud.json` preparado localmente:

```bash
ssh pastelerias-mac-ia-02 'curl -fsS --max-time 180 --config "$HOME/.local/share/pastelerias/mac-ia-02/client.curl" -H "Content-Type: application/json" --data-binary @- http://127.0.0.1:18124/v1/chat/completions' < solicitud.json
```

El JSON debe seleccionar `pastelerias-qwen3-8b`, contener mensajes acotados,
`max_tokens` limitado y `stream: false`. En extracción, exigir un JSON con
campos y tipos conocidos y conservar evidencia de origen.

Registrar por trabajo: identificador, tipo, modelo, duración, tokens locales,
resultado de validación y si sustituyó una consulta que se habría hecho a un
modelo de pago. Los tokens locales no son por sí mismos ahorro facturado.
No mostrar a Codex toda la entrada/salida si basta un resultado compacto validado.

El ERP mantiene su orquestador. Este nodo no incorpora una segunda cola, no tiene
credenciales de base operativa y no aplica cambios en el ERP. El servicio del
modelo puede estar encendido sin que haya un agente trabajando. Las tareas
programadas nuevas requieren su integración y prueba explícitas. El transporte
SSH desde el VPS no concede al ERP permiso para aplicar cambios ni ejecuta tareas
por sí mismo.

## Modelos mayores y verificación

Qwen3 8B Q4 es el modelo elegido para el chat y las tareas acotadas. No cargar
varios modelos a la vez. Antes de sustituirlo, comparar las mismas fuentes y
respuestas esperadas, medir tiempo, presión de memoria y errores. Un 14B puede
dejar poco margen en 16 GiB: no adoptarlo sólo porque el archivo cabe.

La etiqueta Ollama `qwen3:4b` que existía en la Mac de gestión apuntaba realmente
a `Qwen3-4B-Thinking-2507`, según `general.name` y `general.finetune`. La extracción
con esquema pasó, pero el chat libre mantuvo respuestas largas de razonamiento.
No tratar esa variante como un modelo de chat con razonamiento desactivable. El
archivo fuente de la Mac de gestión se conserva; la copia de prueba en este nodo
se retira después de validar el sustituto. No afirmar mejora de calidad por tener
más parámetros ni extrapolar una extracción breve a una conciliación integral.

Criterios antes de declarar disponible el nodo:

1. SSH sin contraseña con huella verificada.
2. Hash de binario y modelo comprobados.
3. Servicio y puerto exclusivamente local; rechazo de inferencia sin credencial.
4. Tarea de extracción con salida validada y tiempos/tokens registrados.
5. Interfaz comprobada; recuperación del proceso probada por separado del reinicio.
6. Documentación integrada y recursos propios registrados.

## Recursos y cierre

El runtime, modelo, credencial, logs y LaunchAgent son infraestructura operativa
local persistente propiedad de Pastelerías, conservada para este objetivo.
No son contenedores ni una base operativa. Esta tarea no necesita Docker ni una
base local: no levantar PostgreSQL para validar documentación o el modelo.

El túnel persistente de la Mac de gestión y el transporte saliente de la MacBook
al VPS se conservan como recursos operativos;
los túneles adicionales de prueba se cierran al terminar. No confundirlo con
acceso desde otra red. El worktree se retira por el ciclo de vida oficial después de integrar
y verificar. No borrar recursos de Mac-IA Debian, NAS, otras tareas o producción.
