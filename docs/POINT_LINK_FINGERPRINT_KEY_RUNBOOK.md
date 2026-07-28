# Rotación de `POINT_LINK_FINGERPRINT_KEY`

`POINT_LINK_FINGERPRINT_KEY` firma la huella estable usada para reconocer un
intento repetido de vincular una nota Point. No es una contraseña de Point ni
debe copiarse a código, imágenes, tickets o bitácoras.

## Variables

- `POINT_LINK_FINGERPRINT_KEY`: clave primaria vigente. Debe ser un secreto
  aleatorio, no vacío y administrado por el proveedor de secretos del entorno.
- `POINT_LINK_FINGERPRINT_KEY_FALLBACKS`: lista separada por comas de claves
  anteriores que todavía deben validar huellas existentes.

Si no se define una clave dedicada, el ERP usa `SECRET_KEY`. Al adoptar por
primera vez la clave dedicada, el runtime conserva compatibilidad temporal con
`SECRET_KEY`; esto no sustituye una rotación controlada.

## Rotación sin invalidar huellas

1. Generar una clave nueva fuera del repositorio y guardarla en el gestor de
   secretos.
2. Mantener la clave vigente anterior como primer valor de
   `POINT_LINK_FINGERPRINT_KEY_FALLBACKS`; conservar también los fallbacks que
   sigan dentro de la ventana de reintentos operativos.
3. Configurar la clave nueva como `POINT_LINK_FINGERPRINT_KEY` y desplegar todas
   las instancias de forma coordinada.
4. Verificar vinculación nueva y reintento de una operación firmada antes del
   cambio. No imprimir las claves ni sus valores derivados.
5. Retirar la clave anterior de fallbacks solo cuando haya vencido la ventana
   máxima de reintentos y se haya confirmado que no quedan despliegues usando
   la clave anterior.

## Rollback

Restaurar la clave anterior como primaria y conservar la nueva temporalmente en
fallbacks. Reiniciar todas las instancias. Nunca borrar huellas de la base para
resolver una rotación.

Una lista mal formada, una clave vacía o un tipo no textual hacen que el ERP
falle cerrado mediante `ImproperlyConfigured`.
