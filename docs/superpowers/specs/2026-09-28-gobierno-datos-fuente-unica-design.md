# Descubrimiento de datos antes de crear módulos

## Objetivo

Antes de diseñar un módulo nuevo, identificar qué datos ya existen en el ERP, dónde se capturan, cómo se relacionan y cuál es su fuente autorizada. Evitar una segunda captura o una tabla paralela para la misma entidad. La base operativa del ERP sigue siendo PostgreSQL; Point y otras integraciones pueden conservar sus propios sistemas y deben relacionarse por identificadores explícitos.

## Opciones evaluadas

1. **Solo instrucciones para agentes.** Barato, pero no deja una evidencia verificable del esquema consultado.
2. **Inventario local de solo lectura más revisión de identidad.** Recomendado: descubre modelos, columnas, llaves y relaciones, y exige comprobar registros relevantes antes de aprobar el diseño.
3. **Plataforma externa de catálogo y resolución automática.** Añade infraestructura y reglas de supervivencia prematuras; se evaluará si el volumen de dominios y responsables lo justifica.

## Contrato propuesto

Para cada módulo, el agente entrega una ficha con: concepto y unidad de análisis; modelos/tablas candidatos; fuente que crea y actualiza el dato; identificador estable; alias y equivalencias conocidos; relaciones; consumidores; evidencia de existencia de registros; decisión de reutilizar, extender o crear; y casos ambiguos. La ficha acompaña la propuesta técnica antes de cualquier nuevo modelo, importación o captura.

El inventario automatizado consulta metadatos de Django y PostgreSQL sin leer valores de filas ni escribir en la base. Una búsqueda con palabras relacionadas produce candidatos léxicos, no declaraciones de identidad. El análisis de registros se hace después, por dominio y mediante consultas de solo lectura limitadas a las tablas candidatas. Debe indicar ambiente, fecha, consulta o procedimiento, conteos y límites de la muestra. No se hace un barrido indiscriminado de datos personales.

## Equivalencias

- **Confirmada:** identificador estable compartido o tabla de equivalencias ya aprobada, con ámbito y vigencia claros.
- **Candidata:** parecido de nombre, campos o contexto; requiere comprobar claves, unidad de análisis, historia y relaciones.
- **Distinta:** mismo nombre o código en ámbitos diferentes, o conceptos operativos diferentes.
- **No resuelta:** evidencia insuficiente o conflicto entre fuentes; el diseño no debe depender de su unión.

Ninguna coincidencia aproximada crea o modifica alias, fusiona filas ni sustituye una fuente oficial. Antes de aprobar una nueva regla de equivalencia se revisan ejemplos positivos y negativos, colisiones, efecto en consumidores y plan de reversión. Los históricos de Crucero y Bamoa ilustran que compartir un identificador heredado no basta para declarar una misma entidad de negocio.

## Validación y alcance

La primera entrega incluye un comando de inventario de solo lectura, una habilidad del repositorio y el requisito de ficha en las instrucciones de agentes. Se valida contra PostgreSQL local aislado y con ejemplos de términos equivalentes. No se cambia ningún modelo, migración, dato operativo ni flujo de producción. El escaneo real de valores de producción para un dominio específico y la adopción de reglas de equivalencia quedan sujetos a una revisión posterior de la ficha.
