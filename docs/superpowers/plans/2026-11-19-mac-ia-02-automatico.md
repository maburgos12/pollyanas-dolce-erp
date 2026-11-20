# Piloto automático Mac-IA-02 — ejecución

Autorización: Mauricio aprobó implementar, integrar, desplegar y activar el diseño
`../specs/2026-11-19-mac-ia-02-automatico.md` en este chat el 2026-11-19.
Ejecutar en este hilo, en segundo plano. Reutilizar OrchestrationRun/AgentTask,
Django/Celery, componentes del tablero y stdlib; no nuevas dependencias ni tablas.

- [ ] Pruebas primero: `orquestacion/tests_local_ai.py` (agrupación, deduplicación,
  estados, leases, fuentes obsoletas, RBAC). Ejecutar en PostgreSQL 16 exclusivo.
- [ ] Servicio `orquestacion/services/local_ai.py`, comando fijo `mac_ia_jobs`,
  tarea Celery y alta idempotente del horario; desactivado hasta activación expresa.
- [ ] Ejecutor host `ops/erp-mac-ia/worker.py`, pruebas de HTTP y unidades systemd.
  Sólo localhost:18125; credencial fuera de Git; sin shell del modelo ni API pagada.
- [ ] Fragmento del tablero, polling acotado con permisos existentes, sin secretos.
- [ ] Verificar pruebas afectadas, `check`, `migrate --check`, diff y revisión.
- [ ] PR, CI completo vigente, merge y despliegue oficial. Configurar cuenta/túnel
  exclusivo, secreto privado, servicio y horario. Verificar una ejecución real y UI.
- [ ] Documentar medidas y límites en runbook. Retirar entorno local exclusivo,
  registrar respaldo/verificación y cerrar worktree por ciclo oficial.

Recursos de pruebas: propietario codex-01a1f4fc; proyecto Compose
`erp_mac_ia02_auto_20261119`, PostgreSQL 16, puerto 55486, red
10.251.235.0/28 (pools automáticos agotados). Manifiesto/bitácora fuera de Git:
`/Users/mauricioburgos/Documents/Codex/2026-11-19/kim/work/mac-ia-02-automatico/`.
No se levantan web/Redis locales ni se reutilizan bases de otras tareas.
Contratos afectados: cola técnica JSON, tarea Celery, fragmento con RBAC, daemon
host y transporte SSH privado. Sin cambios de modelos, estados operativos, otros
horarios, Compose de producción ni variables de entorno de la aplicación.
