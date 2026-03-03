# Playbook - RBAC Matrix Operativa

## Objetivo
Definir accesos por rol evitando interferencia entre áreas.

## Matriz base (ejemplo)
- DG: lectura total + aprobaciones estratégicas.
- Admin ERP: configuración, usuarios, integraciones.
- Compras: solicitudes/órdenes/recepciones.
- Producción CEDIS: recetas, plan producción, consumo.
- Sucursal: captura stock final y solicitud diaria de reabasto (solo su sucursal).

## Reglas
- Mínimo privilegio por defecto.
- Segmentación por sucursal obligatoria para usuarios de tienda.
- Toda acción sensible se audita (quién, qué, cuándo).

## Checklist de alta de usuario
1. Crear usuario.
2. Asignar rol base.
3. Asignar sucursal (si aplica).
4. Probar permisos esperados y denegados.
5. Registrar evidencia.
