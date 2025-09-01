# Deploy en Render - Plataforma Climática API

Pasos rápidos:

1. Sube este repo a GitHub.
2. Crea una cuenta en Render y conecta tu repositorio.
3. Crea un nuevo **Web Service** y selecciona Docker (o usará el Procfile).
4. Configura variables de entorno en Render:
   - DATABASE_URL (ej: postgresql://user:pass@host:5432/neondb?sslmode=require)
   - SECRET_KEY
   - DB_POOL_MIN (1)
   - DB_POOL_MAX (6)
   - ACCESS_TOKEN_EXPIRE_MINUTES (60)
5. Despliega y revisa logs. Agrega healthchecks si quieres.

Notas:
- Usa un usuario de BD con permisos mínimos si es posible (no uses superuser).
- Ajusta DB_POOL_MAX según el plan/recursos.
