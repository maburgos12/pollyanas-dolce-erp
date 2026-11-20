# Asignación local de agentes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Asignar automáticamente solicitudes acotadas de agentes a los dos nodos existentes, registrar resultados en Orquestación y conservar trabajos operativos.

**Architecture:** Reutilizar AgentTask, OrchestrationRun y OrchestrationRule. Entrada privada JSON mediante management command; selección por contrato de tarea, tamaño y agente, sin inferencia para elegir ni API pagada. Ejecutor VPS usa SSH con comando forzado y llaves propias; cada nodo valida el contrato y cede ante actividad operativa.

**Tech Stack:** Django/PostgreSQL16, Python stdlib, SSH, llama.cpp existente, systemd en VPS/Debian.

## Contrato y asignación autorizada

`submit(agent, kind, data, request_id)` acepta agentes activos existentes y un UUID de idempotencia. `summary`: texto mínimo sin secretos (máximo 2500 bytes UTF-8); <=1000 bytes UTF-8 a Debian Qwen4B, resto Mac16 Qwen8B. `extract`: texto y 1–6 campos, Mac16 Nemotron; cada valor debe ser una cita literal o null. `verify`: 1–100 pares numéricos esperados/observados, Debian Python con Decimal. `summary_cases`: hasta cuatro casos sanitizados del piloto; Debian Qwen4B para <=2, Mac16 Qwen8B para 3–4. Los casos conservan referencias y se revalidan antes de publicar. Los resúmenes breves pueden derivarse a Mac16 si Debian tiene trabajo asignado y la Mac no tiene cola; los lotes periódicos del router agrupan dos casos para distribuir entre nodos, conservando lotes históricos de cuatro del piloto. Otros tipos se escalan sin ejecutar ni llamar proveedor de pago. El agente debe preparar evidencia mínima antes de enviar; el texto no concede permisos.

## Pasos

- [x] Crear `orquestacion/tests_local_router.py`: probar selección, rechazo de entradas, UUID reutilizado con datos distintos, reserva exclusiva por nodo, vencimiento, resultados falsificados, pausa y fuentes cambiadas. Ejecutar `bash work/ia-local-router-20261119/manage test orquestacion.tests_local_router --noinput` desde el workspace de control; verificar fallo antes de implementar.
- [x] Crear `orquestacion/services/local_router.py` y `orquestacion/management/commands/local_ai_route.py`. Usar transacciones con bloqueo de regla; máximo 16 pendientes, una reserva por nodo, 3 intentos con retroceso y recibos sin llaves. No crear tablas ni modificar operaciones.
- [x] Extender `orquestacion/services/local_ai.py` para que el encolador existente entregue los nuevos resúmenes al router si está activado. Conservar historial, deduplicación y piloto Qwen anterior hasta vaciar su cola. Agregar `router` al panel existente y `templates/orquestacion/_local_ai.html` para mostrar asignación, motivo y resultado validado con el mismo refresco autenticado.
- [x] Crear `ops/erp-mac-ia/router_node.py`, `router_worker.py` y unidades systemd. Debian: respetar cron.lock y ventana de cron, nice19, ctx2048, salida192 y timeout150s. Mac16: Qwen servidor actual; Nemotron sólo bajo demanda con puerto propio y corte si Qwen se ocupa o falta memoria; retirar sólo el proceso temporal propio. Compartir bloqueo VPS con piloto.
- [x] Probar módulo y consumidores en PG16; probar ejecutores con transporte simulado y contratos malformados. Solicitar revisión independiente exacta del diff y corregir hallazgos.
- [ ] PR/CI completos, coordinar base/despliegue con Dirección, merge normal y `deploy_web_safe.sh` con SHA/base esperados. Instalar solamente artefactos del commit integrado, validar huellas y comandos forzados antes de activar servicios.
- [ ] Ejecutar solicitudes reales sanitizadas por cada ruta y observar registros en Orquestación; verificar cron Debian y PID Qwen preservados. Documentar rutas, límites y recepción de resultados. Limpiar PG16 propio con respaldo verificado/helper oficial y cerrar registro de tarea. Guardar configuración confirmada en nota de memoria autorizada.

## Recursos y límites

Tarea/propietario `ia-local-router-20261119` / `codex-01a1f4fc`. Worktree exclusivo homónimo. PG16 local Compose `erp_ia_local_router_20261119`, puerto55487, volumen exclusivo postgres_data, red10.251.236.0/28. No datos operativos. Runtime y túneles permanentes se entregan a Pastelerías/Mauricio con revisión 26/11/2026. No usar capacidades no comprobadas de PAIR o ModelOpt. No reenviar decisiones de negocio, comandos o SQL generados. La calidad de un resumen libre requiere revisión humana; los tokens locales no acreditan ahorro de cuota Codex.
