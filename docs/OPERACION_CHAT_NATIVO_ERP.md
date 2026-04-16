# Operacion Oficial del Chat Nativo ERP

## 1. Estado oficial

Desde 2026-04-15 la experiencia oficial de chat del ERP vive en `/ia-privada/` dentro del propio sistema Django.

La ruta oficial ya opera con:

- conversaciones persistentes en PostgreSQL
- mensajes persistidos por conversacion
- streaming de respuesta
- continuidad basica entre turnos
- tool calls reales del ERP dentro del mismo hilo
- prompts y reglas DG integrados al runtime nativo

## 2. Fuente de verdad

El chat oficial depende de:

- PostgreSQL del ERP
- servicios internos Django
- `api/ai_gateway_services.py`
- `orquestacion/services/chat_service.py`
- `orquestacion/services/agent_runtime.py`
- memoria y prompts DG del repositorio

No depende de una plataforma externa de chat para operar el flujo principal.

## 3. Validacion operativa cerrada

Validacion remota mas reciente: 2026-04-15 en Railway.

Se valido que el chat nativo:

- abre `/ia-privada/` detras del login del ERP
- crea conversaciones nuevas
- guarda conversaciones y mensajes en PostgreSQL
- responde con modelo activo
- conserva contexto entre turnos
- ejecuta tools reales del ERP

Evidencia funcional verificada:

- turno simple: `Responde solo OK y no uses herramientas.` -> `OK`
- continuidad conversacional:
  - turno 1 -> `OK`
  - turno 2 -> `Te respondi "OK".`
- consulta operativa real:
  - pregunta sobre discrepancias operativas
  - tool ejecutada: `erp_get_discrepancies`

## 4. Componentes que deben conservarse

- PostgreSQL como base unica
- servicios internos Django del ERP
- RBAC y auditabilidad del ERP
- memoria DG en `memory.md`
- prompts DG en `pos_bridge/prompts/`
- runtime nativo en `orquestacion/`
- gateway y tools reales del ERP

## 5. Politica operativa permanente

- la interfaz oficial de IA es `/ia-privada/`
- no reinstalar runtimes externos retirados como arquitectura principal
- no documentar plataformas externas retiradas como opcion vigente
- no volver a usar SQLite para el chat ni para el ERP
- Railway es el entorno principal para validar y publicar
- local se usa solo cuando haga falta y con stack minimo

## 6. Operacion local minima

Cuando haga falta levantar local:

- `db`
- `redis`
- `web`

No prender `worker`, `beat` ni stacks pesados salvo necesidad real.

## 7. Cierre del retiro legacy

El retiro del runtime externo anterior se considera completo cuando:

- no quedan dependencias operativas activas hacia ese runtime
- el repo ya no lo documenta como arquitectura vigente
- no quedan contenedores, imagenes ni volumenes locales ocupando espacio
- el equipo opera y publica usando solo el chat nativo del ERP
