# ADR - Conciliacion Mensual De Producto Point

## Context

Se necesita automatizar dentro del ERP el comparativo mensual que hoy se trabaja manualmente en Excel para producto terminado.

El objetivo inmediato no es conciliar contra inventario fisico, sino construir un cierre teorico mensual basado solo en los movimientos que Point genera y que el ERP ya materializa.

Existe una regla critica de negocio:

- las rebanadas no deben cerrar como inventario independiente
- deben convertirse y regresar a su producto entero padre

## Decision

Se adopta un cierre mensual canonico de producto terminado con estas reglas:

- alcance v1 solo teorico, sin fisico
- fuente primaria: Point materializado en ERP
- unidad canonica: `mes + receta padre`
- las presentaciones derivadas se convierten a entero equivalente por `RecetaPresentacionDerivada`
- el inventario inicial del mes sale del cierre del mes anterior; si no existe, del snapshot Point del cierre previo con bandera de excepcion
- el bloqueo manual del mes queda reservado a `DG` o `ADMIN` y solo cuando el cierre este limpio de incidencias de catalogo
- el build queda habilitado para `DG`, `ADMIN`, `PRODUCCION` y `ALMACEN`; el rebuild queda restringido a `DG` y `ADMIN`
- si falta snapshot exacto de fin de mes, se acepta fallback solo dentro de tolerancia de `3` dias calendario y se deja evidencia en metadata

## Alternatives considered

- Option A:
  mantener el Excel como fuente operativa principal
- Option B:
  construir cierre mensual canonico en ERP basado en movimientos Point
- Option C:
  esperar a tener inventario fisico y hacer cierre completo sistema vs fisico desde el inicio

## Consequences

- Benefits:
  - reduce captura manual
  - deja una base automatica para cierres mensuales
  - corrige el problema de rebanadas vs enteros de forma institucional
  - permite despues agregar capa de auditoria fisica sin rehacer el cierre base
- Tradeoffs:
  - el cierre v1 no detecta diferencias de conteo fisico
  - depende de homologacion correcta entre Point y recetas ERP
  - requiere una politica clara para el opening historico de agosto 2025
- Risks:
  - relaciones derivadas faltantes
  - snapshots fin de mes incompletos
  - cambios retroactivos en Point

## Rollback / Mitigation

No se reemplazan tablas operativas existentes.

La mitigacion es:

- guardar el cierre mensual en tablas nuevas
- dejar el Excel como control paralelo mientras se valida
- permitir rebuild por mes
- no bloquear operacion diaria si el cierre mensual falla

## Date / Owner / Status

- Date: 2026-03-26
- Owner: Codex + Direccion General
- Status: Accepted
