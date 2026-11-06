# Reparaciones técnicas en VPS hasta PR borrador

Autorización Mauricio, chat01a1b1ef, 2026-11-06: flujo autónomo con suscripción
ChatGPT hasta PR borrador. Publicar requiere su aprobación del SHA exacto en
GitHub. El monitor Mac de cinco minutos y los monitores de disco/inventario se
conservan. No se instala código que ejecute merge o deploy.

## Flujo y límites

Un timer exclusivo consulta disponibilidad HTTP básica y los resultados recientes
del observador existente. Dos fallos HTTP consecutivos generan una causa; no
ejecuta Doctor ni cambia Beat. Un resultado anterior al arranque del worker,
vencido o insuficiente no se convierte en una reparación. `/health/` solo prueba
respuesta web, no salud de BD ni cobertura completa del ERP.

`worker.py` es un despachador confiable separado del modelo. Usa flock, ledger
atómico y registros/worktrees Factory existentes. Una reparación simultánea;
máximo tres intentos por causa, incluso cambia el SHA. Una llamada interrumpida
se concilia sin repetir efectos ambiguos. Al arrancar, valida intención, IDs,
labels, imágenes y montajes antes de retirar solo recursos de ensayo propios;
preserva evidencia y bloquea el caso para revisión. No elimina por antigüedad.

Diagnóstico → propuesta JSON/diff → reproducción RED por aserción, no por error
de importación → corrección → GREEN sin reducir descubrimiento → check Django y
migraciones PG16 → commit → revisión independiente ligada al SHA → push de rama
`codex/autofix-*` → PR borrador → CI completo → espera de aprobación.

Los paths automáticos están definidos en `policy.py`: solo código técnico y
regresiones permitidos. Datos, nómina, RRHH, integraciones, modelos/migraciones,
autenticación/permisos, configuración, cachés, SW, workflows, despliegues y el
propio controlador quedan fuera. Los tests existentes solo admiten adiciones.
Los casos fuera de alcance se entregan para decisión; no se cambia la regla de
negocio para ocultar un fallo.

Se omiten causas ya visibles en PRs abiertos del mismo repositorio y de autores
confiables. El monitor anterior no participa en el nuevo lock: esta consulta
reduce duplicados, pero no demuestra exclusión atómica entre Mac y VPS. Ambos
pueden preparar propuestas; este servicio nunca publica ninguna.

## Separación de privilegios

- Modelo: CLI oficial0.144.1, usuario `erp-codex`, ChatGPT forzado, entorno limpio,
  sin API keys ni fallback. Solo contexto de código permitido y salida estructurada;
  shell/apps/plugins/web deshabilitados, cero herramientas y ninguna ejecución de
  código del repositorio bajo la sesión. Flujos serializados en un CODEX_HOME.
- Pruebas: imagen de herramientas separada que copia solo `/opt/venv`, nunca
  `/app`, `.env` o credenciales de la imagen operativa. Usuario65534, fuente y
  herramientas solo lectura, red deshabilitada, sin capacidades, NoNewPrivileges,
  límites CPU/RAM/PID/tiempo/salida y escritura solo en scratch y PG16 sintético.
- PG16 exclusivo por intento, datos tmpfs, socket Unix propio, sin puertos ni
  volúmenes de datos. Intención antes de crear, inspección real antes de ejecutar
  y retiro por ID/labels exactos; nunca producción, snapshots operativos o prune.
- Publicador de PR: código confiable root, sin ejecutar instrucciones del modelo.
  GitHub App limitada a un repositorio; clave root0600, tokens temporales renovados
  por el adaptador. El token solo entra al proceso Git; ni modelo ni pruebas lo
  reciben. API cerrada a lectura, PR borrador y aviso técnico en el PR de control.
- El repositorio fue verificado público; no se cambió visibilidad. La sesión de
  cuenta se mantiene en ejecutor privado y no se carga en GitHub Actions ni se
  expone a código de PR público. Los avisos contienen IDs/códigos técnicos, nunca
  logs productivos, clientes, nómina o salidas sensibles.

Reservas mínimas para iniciar:12GiB libres y2GiB MemAvailable. Modelo768MiB/1CPU;
PG512MiB/0.5CPU; pruebas1GiB/1CPU. No se contrata infraestructura ni API pagada.
La sesión y la cuota pueden fallar: se conserva el bloqueo y se avisa en GitHub,
sin comprar créditos ni cambiar de proveedor. Si cae el VPS, también este proceso.

## Aprobación exacta, sin Mac

El operador consulta el gate de solo lectura:

```bash
python3 /opt/erp-autofix/current/ops/erp-autofix/worker.py approval --pr NUMERO --sha SHA_COMPLETO
```

Solo cuenta un comentario actual, escrito por la cuenta humana de Mauricio
(GitHub ID260828669), cuyo cuerpo completo sea `APRUEBO PUBLICAR SHA_COMPLETO`.
Un comentario posterior `REVOCO PUBLICAR SHA_COMPLETO`, incluso editado, revoca.
No cuentan citas, bots, otras personas, SHA cortos u otro HEAD. Debe aprobarse
CI completo y un único check `test`; el gate vuelve a leer HEAD antes de aceptar.
Escribir la aprobación desde teléfono/GitHub no requiere Mac.

**Un resultado aprobado no ejecuta publicación.** La integración/despliegue queda
como acción separada del operador autorizado, aplicando gate vigente inmediatamente
antes del flujo oficial y verificando SHA/resultado real después. Este componente
no altera protecciones globales de GitHub ni concede capacidades de publicación
al modelo. Los administradores externos conservan sus permisos existentes.

## Instalación y aceptación

Registro GitHub App5648633, instalación179395508 solo en este repo: Contents y
Pull requests escritura; Checks/Commit statuses/Metadata lectura. La clave inicial
sin uso fue revocada con autorización; la activa solo permanece en VPS, fuera de
Git y de evidencias. El binario, sesión y herramientas se registran como recursos
persistentes del piloto, con responsable Codex/Mauricio y revisión2026-11-09.

Desde un repositorio de control actualizado separado del checkout operativo:

```bash
bash ops/erp-autofix/install.sh CONTROL_REPO SHA_REVISADO TOOLS_IMAGE_ID PG16_IMAGE_ID
```

La configuración protegida solo contiene IDs inmutables de imágenes. El código se
obtiene por Git, archive de revisión exacta y release root no modificable por los
ejecutores. No se usa SCP para publicar aplicación ni se toca el checkout ERP.

El ensayo inocuo usa solicitud root0600 `/var/lib/erp-autofix/drill.json`,
`kind=synthetic`, `task_id` y `checks` técnicos. Se consume una sola vez después de
registrar intención. Inyecta en scratch un fallo de parseo CSS; no envía correo ni
modifica producción. El modelo restaura el baseline y añade una regresión; el PR
se etiqueta explícitamente como sintético. Debe completar con SSH desconectado,
CI vigente, PR realmente creado por la App y gate rechazando aprobación ausente.
No se simula una caída real del ERP.

```bash
python3 -m unittest discover -s scripts/tests -p 'test_erp_autofix_*.py'
systemctl status erp-autofix.timer erp-autofix.service --no-pager
systemctl disable --now erp-autofix.timer
systemctl stop erp-autofix.service
```

La parada no borra evidencia. Una interrupción bloquea el caso hasta conciliarlo;
no se eliminan logs ni se reinicia presupuesto con commits. Worktrees/ramas de PR
borrador conservados requieren responsable, razón y fecha. Retirar únicamente
contenedores exclusivos de pruebas después de verificar descarte e identidad;
no es autorización para limpiar Docker productivo. Factory queda esperando
integración, sin inventar un cierre de entrega mergeada.
