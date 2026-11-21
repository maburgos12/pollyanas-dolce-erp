# Orquestación e IA local

Leer también [AGENTS raíz](../AGENTS.md).

## IA local — Mac-IA-02

Para resúmenes, clasificación y extracción acotada, consultar primero
[runbook Mac-IA-02](../docs/runbooks/mac-ia-02.md) y comprobar disponibilidad del nodo por SSH.
Usar el modelo local cuando la tarea sea adecuada y validar su resultado antes
de consumirlo; devolver sólo evidencia compacta para evitar repetir contexto.
No delegar cálculos exactos ni decisiones operativas; no ejecutar comandos
generados por el modelo. Si el nodo no responde, registrar el motivo y continuar
con el flujo autorizado, sin activar APIs pagadas nuevas ni reintentos indefinidos.
El piloto automático aprobado agrupa evidencia de inventario y planes; consultar
su estado/resúmenes compactos con `python manage.py mac_ia_jobs status` en el VPS
o `/orquestacion/#mac-ia-02` antes de repetir una lectura masiva de esas tareas.
El comentario de IA no prueba cierre operativo; verificar la fuente cuando haga
falta decidir. No enviar prompts extensos si el resumen y las referencias bastan.
La activación es explícita con `mac_ia_jobs enable`; desactivar con `disable`.
El acceso entre redes usa los túneles privados documentados en el runbook.

## Asignación automática a los dos nodos locales

Para solicitudes acotadas de un agente, usar primero `scripts/local_ai_request.py`
según [router local](../docs/runbooks/local-ai-router.md): `verify` dirige cotejos exactos a Python
Debian; `summary` elige Qwen4B Debian para hasta1000 bytes UTF-8 y Qwen8B Mac16
para hasta2500; `extract` dirige citas/campos a Nemotron Mac16. El cliente mantiene
UUID estable para evidencia idéntica. Enviar sólo el JSON mínimo saneado; consultar
el recibo antes de repetir inferencia o lectura de datos. El encolador horario ya
canaliza sus nuevos lotes al activar el router; no crear otro cron lector Point.
La interfaz es privada, usa el SSH administrativo existente y no concede acceso
administrativo a modelos/nodos. Un agente nuevo necesita su registro activo y
conserva sus propios permisos/fuentes antes de enviar evidencia. Revisar tipos,
valores ausentes y estados originales; un resumen local no autoriza negocio ni
prueba ahorro de cuota Codex. Sin contrato compatible, escalar explícitamente;
no ejecutar salidas, activar API pagada ni saturar nodos para aparentar actividad.
