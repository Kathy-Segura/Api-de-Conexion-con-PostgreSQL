import os
import ssl
from typing import Optional
from contextlib import asynccontextmanager

import asyncpg
from dotenv import load_dotenv

load_dotenv()  # Carga variables desde .env

DATABASE_URL = os.getenv("DATABASE_URL")
DB_POOL_MIN = int(os.getenv("DB_POOL_MIN", "1"))
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", "6"))
DB_TIMEOUT = int(os.getenv("DB_TIMEOUT", "60"))
DB_SSL_MODE = os.getenv("DB_SSL_MODE", "require").lower()

pool: Optional[asyncpg.pool.Pool] = None

def _build_ssl_context():
    """
    Neon requiere TLS. asyncpg no entiende 'sslmode' en el DSN,
    así que construimos un SSLContext explícito.
    """
    if DB_SSL_MODE in ("disable", "off", "false", "0"):
        return False  # Sólo para entornos de desarrollo local
    # verify-ca/verify-full -> validación de certs; default context ya valida CAs del sistema
    ctx = ssl.create_default_context()
    # Si quisieras 'allow' podrías relajar, pero NO recomendado con Neon
    return ctx

async def init_db_pool():
    """Inicializa el pool de conexiones."""
    global pool
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL no está definido en el .env")

    if pool is None:
        ssl_ctx = _build_ssl_context()
        pool = await asyncpg.create_pool(
            dsn=DATABASE_URL,
            min_size=DB_POOL_MIN,
            max_size=DB_POOL_MAX,
            command_timeout=DB_TIMEOUT,
            ssl=ssl_ctx,
        )
        # Smoke test inicial (opcional)
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")

async def close_db_pool():
    """Cierra el pool de conexiones."""
    global pool
    if pool:
        await pool.close()
        pool = None

@asynccontextmanager
async def acquire():
    """Entrega una conexión del pool; crea el pool si no existe."""
    global pool
    if pool is None:
        await init_db_pool()
    async with pool.acquire() as conn:
        yield conn