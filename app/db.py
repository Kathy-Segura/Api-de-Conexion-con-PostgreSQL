import os
import ssl
import asyncio
from typing import Optional
from contextlib import asynccontextmanager
import asyncpg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
DB_POOL_MIN = int(os.getenv("DB_POOL_MIN", "1"))
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", "6"))
DB_TIMEOUT = int(os.getenv("DB_TIMEOUT", "60"))
DB_SSL_MODE = os.getenv("DB_SSL_MODE", "require").lower()

pool: Optional[asyncpg.pool.Pool] = None

def _build_ssl_context():
    if DB_SSL_MODE in ("disable", "off", "false", "0"):
        return False
    ctx = ssl.create_default_context()
    return ctx

async def init_db_pool(retries: int = 3, delay: int = 2):
    global pool
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL no está definido en el .env")
    if pool is None:
        ssl_ctx = _build_ssl_context()
        for attempt in range(retries):
            try:
                pool = await asyncpg.create_pool(
                    dsn=DATABASE_URL,
                    min_size=DB_POOL_MIN,
                    max_size=DB_POOL_MAX,
                    command_timeout=DB_TIMEOUT,
                    timeout=30,
                    ssl=ssl_ctx,
                    init=lambda conn: conn.execute('SET search_path TO sensor')
                )
                async with pool.acquire() as conn:
                    val = await conn.fetchval("SELECT schema_name FROM information_schema.schemata WHERE schema_name='sensor'")
                    print("Schema 'sensor' existe:", val)
                break
            except Exception as e:
                if attempt < retries - 1:
                    await asyncio.sleep(delay)
                else:
                    raise e


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