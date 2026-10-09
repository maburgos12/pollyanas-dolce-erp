# Evaluación aislada Agno / AgentOS

Este laboratorio prueba un motor conversacional con tool calling y sesiones persistentes. No está instalado en Django, no reemplaza `/ia-privada/` ni Maya, y no habilita canales ni usuarios de producción.

## Alcance y fuentes

- Agno 3.1.2 + OpenAI Responses, modelo existente `gpt-6.1-sol`; proveedor contratado separado del producto ChatGPT.
- AgentOS con JWT de laboratorio y aislamiento de sesiones por identidad verificada; sin privilegios administrativos, eliminación de sesiones ni registro de herramientas de escritura.
- ERP: tres herramientas READ del Gateway existente sobre dos activos sintéticos en PostgreSQL local. No se prueba aquí el catálogo real de activos ni una sesión de un colaborador real.
- Maya: catálogo, sucursales, stock, precio y datos públicos mediante servicios existentes. No se crean pedidos ni se envían mensajes a clientes.
- Agent UI oficial, commit `6dad9593fca6756e1813e4f4b3b2620be6377691`. Es una interfaz de laboratorio; no el diseño final de Pollyana’s Dolce.

No hay clasificador de intenciones ni secuencia de preguntas obligatoria. El modelo elige herramientas; el Gateway valida permisos y argumentos. Las correcciones del usuario continúan en la misma sesión.

## Reproducir

Usar un worktree registrado, preflight y PostgreSQL 16 aislado. Estos puertos y rutas pertenecen exclusivamente a `ai-agentos-evaluation-20261009`; comprobar su disponibilidad antes de recrear el entorno.

```bash
COMPOSE_PROJECT_NAME=erp_agentos_eval_20261009 DB_HOST_PORT=56673 docker compose up -d db
docker compose -p erp_agentos_eval_20261009 exec -T db pg_isready -U postgres
export APP_ENV=development ALLOW_INSECURE_LOCAL_SECRET_KEY=1
export DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:56673/pastelerias_erp
# Usar el intérprete Django existente, con conectividad PostgreSQL verificada.
/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/.venv/bin/python manage.py migrate --noinput
/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/.venv/bin/python manage.py migrate --check
/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/.venv/bin/python manage.py check
/Users/mauricioburgos/Downloads/pastelerias_erp_sprint1/.venv/bin/python labs/agentos_evaluation/erp_bridge.py <<'JSON'
{"action":"seed"}
JSON
uv venv /Users/mauricioburgos/.codex/task-artifacts/ai-agentos-evaluation-20261009/venv --python 3.13
uv pip install --python /Users/mauricioburgos/.codex/task-artifacts/ai-agentos-evaluation-20261009/venv/bin/python -r labs/agentos_evaluation/requirements.txt
```

Con ese intérprete aislado ejecutar `pytest labs/agentos_evaluation/test_controls.py -q`. Las pruebas de controles no llaman al proveedor. `evaluate.py` sí llama a OpenAI y puede consumir el presupuesto restante; no reiniciar ni borrar el ledger para repetirlas. La clave existente del ERP se lee en memoria; nunca se entrega al navegador ni se copia al repositorio.

`serve.py` inicia AgentOS sólo en `127.0.0.1:7779`, genera tokens de laboratorio de una hora y los deja temporalmente en un archivo 0600.

Clonar el repositorio oficial Agent UI en el directorio de artefactos y fijar el commit indicado. Instalar con `corepack pnpm@10.18.3 install --frozen-lockfile`; pnpm 11 ignora los overrides históricos y no coincide con este lockfile. Ejecutar `corepack pnpm@10.18.3 typecheck` y el servidor Next en `127.0.0.1:7309`, con telemetría desactivada. Configurar endpoint `http://127.0.0.1:7779` y sólo el JWT de laboratorio. Nunca usar la clave OpenAI como token de interfaz.

## Límite de gasto

`budget.py` reserva ANTES de cada petición, con bloqueo de archivo y persistencia fsync. Tope acumulado USD 1, hasta 40 peticiones, 60 KB por petición y 1,800 tokens de salida. Reintentos automáticos desactivados. Entrada conservadora: bytes UTF-8 + 4,096 de encuadre, USD 2.75/M; salida USD 11/M. Incluye cache-write y recargo regional sobre las tarifas oficiales verificadas el 2026-10-09: https://developers.openai.com/api/docs/pricing.

Sólo Responses, modelo indicado, tier estándar y `store=false`, sin cadenas implícitas. Se rechazan herramientas pagadas incorporadas e imágenes/audio/archivos no presupuestados. No se devuelve una reserva después de un timeout. El ledger registra tamaño y hash, no contenido ni secretos. La reserva es un techo conservador de esta evaluación, no una factura ni un límite de gasto para toda la cuenta OpenAI.

## Límites antes de producción

Las rutas y puertos fijos son exclusivamente del laboratorio. El puente SSH de Maya es una vía de inspección y prueba; debe sustituirse por una API de capacidades públicas y credenciales de servicio acotadas. El proceso productivo jamás debe tener SSH root. Los JWT propios no sustituyen el login/RBAC del ERP: esa integración requiere el próximo corte aprobado.

No se han validado ejecución transaccional, idempotencia comercial, aprobaciones, adjuntos, voz, Telegram/WhatsApp, aislamiento de clientes en esos canales, estrés, recuperación NAS o diseño móvil definitivo. Una prueba de inyección de texto no demuestra seguridad completa ante archivos adversarios.

Antes de retirar recursos, exportar y verificar las sesiones de evaluación y conservar la evidencia compacta. Detener sólo los procesos registrados de esta tarea y retirar sólo su proyecto Compose, volúmenes y dependencias regenerables. No limpiar Docker ni archivos ajenos.
