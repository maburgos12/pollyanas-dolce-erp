# Mac-IA-02 — nodo local de inferencia de Pastelerías

Responsable: Mauricio Burgos. Alta: 2026-11-19. Propietario técnico: proyecto
Pastelerías ERP; tarea `mac-ia-02-20261119`. No reemplaza Mac-IA Debian.

## Equipo y acceso

- MacBook Pro de 15 pulgadas (2018), Intel Core i7 2.2 GHz de seis núcleos,
  16 GiB de RAM, macOS 15.1 (24B83), arquitectura x86_64.
- Disco observado al alta: 233 GiB, 62 GiB disponibles antes de instalar el modelo.
- Host observado: `macbookpro.lan`; usuario `mauricioburgos`.
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

El alias depende de la red donde se resuelve `macbookpro.lan`. Una IP fija o una
ruta desde otra red NO quedan probadas por este alias. Un nuevo cliente necesita
su propia llave y verificación de identidad; el repositorio no contiene accesos.

## Runtime local

Directorio propio en la MacBook:
`~/.local/share/pastelerias/mac-ia-02/`.

- `runtime/llama-b12211/llama-server`: binario oficial macOS x64 de llama.cpp,
  build 12211, commit reportado `7d7f02875`.
- Archivo de distribución: `llama-b12211-bin-macos-x64.tar.gz`, SHA256
  `6055cc672cc5c744481bb88ac1a7739ff476966c8a190ddaa20676034693a358`.
- `models/Qwen3-4B-Q4_K_M.gguf`: modelo existente reutilizado del almacén local de
  Ollama `qwen3:4b`; no implica que Ollama esté instalado en la MacBook.
  2,497,280,480 bytes; SHA256
  `3e4cb14174460404e7a233e531675303b2fbf7749c02f91864fe311ab6344e4f`.
- `api.key` y `client.curl`: credencial local y configuración de cliente, modo
  0600. No imprimir, copiar al repositorio ni pasar la credencial como argumento.
- `logs/`: salida del proceso; `evidence/`: comprobaciones sin información privada.
- Una solicitud a la vez, contexto de 4096 tokens, cuatro hilos, CPU, prioridad
  reducida. Thinking desactivado como punto de partida para extracción breve.
- API: `http://127.0.0.1:18124/v1`, con autenticación. No escuchar en `0.0.0.0`.

Fuentes del runtime: [release oficial](https://github.com/ggml-org/llama.cpp/releases/tag/b12211)
y [servidor llama.cpp](https://github.com/ggml-org/llama.cpp/tree/master/tools/server).
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
por ambos extremos. Desde la Mac de gestión:

```bash
ssh -N -o ExitOnForwardFailure=yes -L 127.0.0.1:18124:127.0.0.1:18124 pastelerias-mac-ia-02
```

Abrir `http://127.0.0.1:18124` mientras el túnel esté activo. Desde la propia
MacBook se puede abrir esa dirección sin túnel. La interfaz permite conversaciones
y respuestas en vivo; no es un tablero de trabajos del ERP. La credencial de la
interfaz se entrega por un medio local privado, sin publicarla en documentación.
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

El JSON debe seleccionar `pastelerias-qwen3-4b`, contener mensajes acotados,
`max_tokens` limitado y `stream: false`. En extracción, exigir un JSON con
campos y tipos conocidos y conservar evidencia de origen.

Registrar por trabajo: identificador, tipo, modelo, duración, tokens locales,
resultado de validación y si sustituyó una consulta que se habría hecho a un
modelo de pago. Los tokens locales no son por sí mismos ahorro facturado.
No mostrar a Codex toda la entrada/salida si basta un resultado compacto validado.

El ERP mantiene su orquestador. Este nodo no incorpora una segunda cola, no tiene
credenciales de base operativa y no aplica cambios en el ERP. El servicio del
modelo puede estar encendido sin que haya un agente trabajando. Las tareas
programadas nuevas y el acceso desde el VPS requieren su integración y prueba
explícitas; no quedan activados por instalar este nodo.

## Modelos mayores y verificación

Qwen3 4B es la referencia inicial. Qwen3 8B Q4 es un candidato para evaluación,
no una mejora demostrada ni un modelo instalado por esta documentación. No
cargar dos modelos a la vez. Antes de cambiar el predeterminado, comparar las
mismas fuentes y respuestas esperadas, medir tiempo, presión de memoria y errores.
Un 14B puede dejar poco margen en 16 GiB: no adoptarlo sólo porque el archivo cabe.

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

Los túneles de prueba se cierran al terminar; no confundirlos con acceso desde
otra red. El worktree se retira por el ciclo de vida oficial después de integrar
y verificar. No borrar recursos de Mac-IA Debian, NAS, otras tareas o producción.
