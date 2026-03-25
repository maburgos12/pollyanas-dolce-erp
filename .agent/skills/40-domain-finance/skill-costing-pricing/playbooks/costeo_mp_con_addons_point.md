# Playbook - Costeo MP con Add-ons de Point

## Objetivo
Costear correctamente materia prima del producto final cuando Point separa el decorado/sabor/topping en un código adicional con ingreso cero.

## Principio financiero
El costo de materia prima del artículo vendido debe incluir:
1. receta base del producto final
2. subrecetas internas
3. empaques ligados a la receta
4. add-on Point aprobado, cuando exista

No mezclar por ahora:
- mano de obra
- indirectos

Esos componentes quedan en `0` hasta que se configuren drivers explícitos.

## Fórmula
1. `costo_mp_base = suma(componentes receta base)`
2. `costo_mp_addon = suma(componentes receta addon)`
3. `costo_mp_agrupado = costo_mp_base + costo_mp_addon`

## Reglas de cálculo
- Respetar rendimiento base de preparaciones:
  - `kg`
  - `lt`
  - `pza`
- Respetar conversión de unidad:
  - `kg -> g`
  - `lt -> ml`
  - `pza -> pza`
- Usar `CostoInsumo` canónico más reciente por nombre canónico/alias homologado.

## Fuentes de costo Point aceptadas
Prioridad operativa:
1. `Inventario -> Existencias -> ALMACEN`
2. `Reportes -> Movimientos de inventario -> COMPRA -> ALMACEN`
3. `Inventario -> Compras -> Buscar por proveedor`

## Reglas de aprobación
Solo aprobar costo agrupado si:
- la receta base existe,
- la receta add-on existe,
- ambos tienen costo MP resuelto,
- el patrón semántico de negocio fue validado,
- el `sku` de Point no es ambiguo.

## Ejemplos institucionales aprobados
- `0001 + SFRESAG` -> `234.350290`
- `0001 + SOREOG` -> `186.174480`
- `0002 + SOREOM` -> `106.936724`
- `0101 + SFRESAPC` -> `144.624840`
- `0100 + SFRESAPM` -> `204.859375`
- `0099 + SFRESAPG` -> `319.344416`

## Reglas de auditoría
- Cada recalculo genera versión de costo.
- La agrupación add-on debe guardar:
  - confianza
  - cantidad histórica asociada
  - razón de aprobación
- Si un addon se bloquea por ambigüedad, no debe entrar al costo final.

## Qué debe responder el agente
Para una consulta de costeo:
- costo base
- costo addon
- costo agrupado
- desglose de líneas relevantes
- si aplica, advertencia de:
  - addon ambiguo
  - costo faltante
  - receta no homologada

## Casos que deben bloquearse
- `sku` duplicado en Point
- addon detectado pero no aprobado
- líneas del addon sin costo fuente
- match semántico dudoso entre base y addon
