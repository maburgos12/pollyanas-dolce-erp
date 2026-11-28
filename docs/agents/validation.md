# Contrato de validación y medición

Estándar adoptado: 2026-11-27.1. Clasificar el efecto real de la tarea conforme
al AGENTS raíz y seguir el ciclo de trabajo. Una captura operativa no exige
cambiar la aplicación; documentación no requiere una base local.

Para código de aplicación, el procedimiento local mantiene PostgreSQL 16 aislado,
`python manage.py check`, migraciones consistentes y pruebas del módulo y sus
consumidores. CI es el consumidor de los cambios de herramientas de validación;
no requieren desplegar ni reiniciar la aplicación por sí solos.

## Gates reales

La fuente ejecutable es `.github/workflows/ci.yml`: Python 3.12, PostgreSQL 16 y
Redis 7 en la prueba específica de recuperación. `checks` conserva comprobaciones
Django, migraciones, contratos de respaldos/ciclo de trabajo y service worker real.
`recipe-recovery` prueba recuperación de la cola. Cuatro jobs `suite` descubren
la suite completa, sin etiquetas ni filtros. El gate obligatorio `test` exige que
todos terminen correctamente y coteja los inventarios completos: ni una prueba
omitida, duplicada ni un perfil distinto entre shards permite aprobarlo.

## Reparto medido

`scripts/ci_class_durations.json` contiene duraciones observadas de clases completas,
incluyendo fixtures y limpieza Django, junto con SHA y ejecuciones de origen.
El runner conserva el orden Django y mantiene cada clase dentro de un solo job.
Distribuye primero las clases más costosas al job de menor duración acumulada.
Las clases nuevas se descubren automáticamente y usan su número de casos como
peso provisional; nunca se excluyen por no figurar en el perfil.

El perfil no sustituye pruebas ni es una lista de descubrimiento. El manifiesto
registra su hash, los IDs descubiertos/asignados, resultados y tiempos de cada
clase y caso. Los errores de fixtures conservan el resultado original de unittest.
Un perfil corrupto falla; no activa un bypass. No se comparten bases entre jobs.

El despacho manual permite `partition=count` para repetir el reparto anterior o
`partition=duration` para el medido, sobre el mismo SHA. Ambos ejecutan todos los
gates. Registrar cola, instalación, preparación de base, pruebas y duración total;
comparar varias ejecuciones con igual inventario y condiciones. Actualizar pesos
sólo con evidencia y revisión. No atribuir ahorro a la instrumentación por sí sola.

Los artefactos de tarea guardan comandos, SHA, resultados, tiempos comparables y
recursos temporales. El cierre y respaldo local siguen el procedimiento obligatorio;
los servicios efímeros de GitHub no son los de producción.
