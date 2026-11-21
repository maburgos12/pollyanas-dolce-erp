# Actualización de e.firma desde SAT fiscal

Autorización: Mauricio, 2026-11-21, habilitar actualización desde ERP, exclusiva de Dirección, validación y prueba de conexión SAT.

Objetivo: Dirección carga .cer, .key y contraseña en /sat/; el ERP valida el par y autentica contra SAT antes de reemplazar las credenciales activas. Un fallo conserva la configuración previa. No solicita descargas ni altera CFDI existentes.

Arquitectura: reutilizar get_sat_credentials, firma SOAP y autenticación existentes. Guardar un único registro de configuración con el paquete cifrado mediante Fernet y clave derivada de SECRET_KEY; soportar SECRET_KEY_FALLBACKS para rotación. Mantener la configuración por entorno hasta que se active una e.firma desde ERP. Procesar secretos en memoria, sin media, logs, sesiones ni archivos temporales de aplicación.

Alcance y consumidores:
- sat_client/models.py y migración nueva: configuración única, contenido cifrado, RFC, huella, vigencia, actor y última prueba.
- sat_client/services/base.py: proveedor común y datos en memoria; conservar rutas de entorno.
- sat_client/services/firma.py: admitir bytes en los lectores existentes.
- sat_client/services/autenticacion.py: aceptar credenciales candidatas y timeout acotado sin activarlas.
- sat_client/services/efirma.py y forms.py: límites de archivos, certificado parseable y vigente, llave RSA coincidente, contraseña y RFC fijo de la empresa; autenticación antes de persistencia y errores seguros.
- sat_client/views.py y templates/sat_client: autorización DG en servidor, CSRF, no-cache, contraseña no repoblada, contexto progresivo y prueba de e.firma activa.
- sat_client/services/fiscal_tools.py: configuración no equivale a conexión validada.
- static/erp-sw.js, templates/base.html y core/templates/core/login.html: bump conjunto de la versión del shell según contrato PWA; se conserva el comportamiento exclusivamente de red.
- docs/ux/action-context-coverage.md y docs/data-reuse/sat-efirma-direccion.md: evidencia y límites.
- Pruebas: sat_client completo, core.tests y core.tests_actions; consumidores usan la misma API sin cambiar imports ni firmas existentes obligatorias.

Plan de ejecución en este chat:
- [x] Preparar PostgreSQL 16 exclusivo, comprobar conectividad y migraciones de la base.
- [x] Escribir y observar prueba roja de formulario ausente.
- [x] Implementar almacenamiento cifrado, proveedor compartido y autenticación de candidato.
- [x] Implementar formulario y permisos DG/superusuario; impedir ADMIN/staff/lectura aunque falsifiquen POST.
- [x] Probar errores locales, rechazo SAT, conservación de credenciales, cifrado, firma real con par sintético y CSRF.
- [x] Migraciones/check/suite afectada; navegador real escritorio y WebKit móvil con datos sintéticos.
- [ ] Revisar diff y secretos; commit exclusivo, PR borrador, CI test del HEAD vigente.
- [ ] Integrar y desplegar por flujo oficial con autorización aplicable; verificar UI servida. La prueba real con nueva e.firma depende de que Mauricio capture sus secretos.
- [ ] Retirar entorno local propio y registrar entrega/cierre del worktree.

Criterios de aceptación: controles sólo para DG/superusuario; RFC no editable; ningún secreto en respuesta, repr o archivos públicos; fallos locales no contactan SAT; fallos remotos no reemplazan credenciales; autenticación no descarga ni prueba cobertura de CFDI. UI distingue configuración, vigencia local y última autenticación.

Operación: respaldos DB y SECRET_KEY/fallbacks deben conservarse juntos para recuperación. No retirar la clave anterior mientras existan registros cifrados con ella. Mantener la descarga nocturna y sus periodos; no cerrar solicitudes fallidas como efecto de actualizar credenciales.
