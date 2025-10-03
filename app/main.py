import os
import uvicorn
import logging
from typing import Any
from datetime import datetime, timedelta
from typing import List, Optional
from app import auth, models, schemas
from asyncpg import exceptions as pg_exc
from app.db import init_db_pool, close_db_pool, acquire
from fastapi import FastAPI, HTTPException, Body, BackgroundTasks
from fastapi.responses import StreamingResponse, RedirectResponse

app = FastAPI(title="Plataforma Climática API")

@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/docs")

@app.on_event("startup")
async def startup():
    await init_db_pool()

@app.on_event("shutdown")
async def shutdown():
    await close_db_pool()

logger = logging.getLogger(__name__)

def _normalize_hash_from_db(h: Any) -> str:
    """
    Normaliza el hash obtenido de la DB a un str (si es posible).
    Si no puede decodificarse correctamente, se registra el error y lanzamos HTTP 500.
    """
    if h is None:
        logger.error("Hash en DB es None")
        raise HTTPException(status_code=500, detail="Error interno leyendo hash de contraseña")

    # memoryview -> bytes
    if isinstance(h, memoryview):
        h = h.tobytes()

    # bytes -> try decode utf-8 (bcrypt/passlib hashes son ASCII)
    if isinstance(h, (bytes, bytearray)):
        try:
            return h.decode("utf-8")
        except Exception as ex:
            logger.exception("No se pudo decodificar PasswordHash desde DB: %s", ex)
            raise HTTPException(status_code=500, detail="Error interno leyendo hash de contraseña")

    # str -> ok
    if isinstance(h, str):
        return h

    # cualquier otro tipo: intentar str() pero registrar
    try:
        s = str(h)
        logger.warning("PasswordHash en DB tiene tipo inesperado %s, convertido a str", type(h))
        return s
    except Exception as ex:
        logger.exception("Tipo inesperado para PasswordHash y no convertible: %s", ex)
        raise HTTPException(status_code=500, detail="Error interno leyendo hash de contraseña")


def _verify_password(plain_password: str, stored_hash_any: Any) -> bool:
    """
    Verifica password con passlib (auth.verify_password). Asegura que stored_hash sea str.
    Si ocurre un fallo inesperado en la verificación, registramos y devolvemos 500.
    """
    stored = _normalize_hash_from_db(stored_hash_any)
    try:
        return auth.verify_password(plain_password, stored)
    except Exception as ex:
        logger.exception("Error verificando contraseña: %s", ex)
        # No mostramos stack al cliente, devolvemos error interno:
        raise HTTPException(status_code=500, detail="Error interno verificando contraseña")
    

# -------- REGISTER corregido (almacena username normalizado en minúsculas) --------
@app.post("/register", status_code=201)
async def register_user(payload: dict = Body(...)):
    username_raw = (payload.get("username") or "").strip()
    username = username_raw.lower()  # guardar en DB en minúsculas para unicidad case-insensitive
    email = (payload.get("email") or "").strip().lower()
    password = payload.get("password")
    try:
        rol_id = int(payload.get("rol_id", 2))
    except Exception:
        rol_id = 2
    activo = True

    if not username or not email or not password:
        raise HTTPException(status_code=400, detail="Usuario, correo y contraseña requeridos")

    hashed_password = auth.hash_password(password)
    # passlib devuelve str; si por alguna razón es bytes, lo convertimos (precaución)
    if isinstance(hashed_password, (bytes, memoryview)):
        try:
            hashed_password = hashed_password.decode("utf-8")
        except Exception:
            logger.warning("hash_password devolvió bytes no decodificables; se guarda raw")

    async with acquire() as conn:
        # validar que rol exista para evitar FK violation
        role_exists = await conn.fetchval("SELECT 1 FROM sensor.Roles WHERE RolID=$1", rol_id)
        if not role_exists:
            raise HTTPException(status_code=400, detail=f"RolID {rol_id} no existe")

        try:
            user_id = await conn.fetchval("""
                INSERT INTO sensor.Usuarios (NombreUsuario, Correo, PasswordHash, RolID, Activo)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING UsuarioID
            """, username, email, hashed_password, rol_id, activo)
            return {"usuarioid": user_id, "username": username_raw, "email": email}
        except pg_exc.UniqueViolationError as e:
            logger.warning("Registro duplicado (unique violation): %s", e)
            raise HTTPException(status_code=409, detail="Nombre de usuario o correo ya registrado")
        except pg_exc.ForeignKeyViolationError as e:
            logger.exception("FK error al registrar usuario: %s", e)
            raise HTTPException(status_code=400, detail="Rol inválido")
        except Exception as e:
            logger.exception("Error registrando usuario: %s", e)
            raise HTTPException(status_code=500, detail="Error registrando usuario")


# -------- LOGIN corregido (busca usando lower() para coincidir con almacenamiento) --------
@app.post("/login")
async def login_user(payload: dict = Body(...)):
    login_input = (payload.get("login") or "").strip()
    password = payload.get("password")
    if not login_input or not password:
        raise HTTPException(status_code=400, detail="Usuario/correo y contraseña requeridos")

    login_norm = login_input.lower()

    async with acquire() as conn:
        user = await conn.fetchrow("""
            SELECT UsuarioID AS usuarioid, NombreUsuario AS username, Correo AS email,
                   PasswordHash AS passwordhash, RolID AS rolid, Activo AS activo
            FROM sensor.Usuarios
            WHERE lower(NombreUsuario)=$1 OR lower(Correo)=$1
        """, login_norm)

        if not user:
            raise HTTPException(status_code=401, detail="Usuario no encontrado")
        if not user["activo"]:
            raise HTTPException(status_code=403, detail="Usuario inactivo")

        stored_hash = user["passwordhash"]
        if not _verify_password(password, stored_hash):
            raise HTTPException(status_code=401, detail="Contraseña incorrecta")

        token = auth.create_access_token({
            "sub": str(user["usuarioid"]),
            "username": user["username"],
            "rol": user["rolid"]
        })
        return {
            "success": True,
            "token": token,
            "user": {
                "id": user["usuarioid"],
                "username": user["username"],
                "email": user["email"],
                "rol": user["rolid"]
            }
        }
    

# -------- HEALTH CHECK --------
@app.get("/health")
async def health():
    try:
        async with acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "ok"}
    except Exception:
        raise HTTPException(status_code=500, detail="DB check failed")

# endpoint: recibe dict en configuracion
@app.post("/devices", status_code=201)
async def create_device(device: schemas.DeviceCreate):
    try:
        device_id = await models.upsert_dispositivo(
            device.serie,
            device.nombre,
            device.ubicacion,
            device.tipo,
            device.firmware,
            device.configuracion  # <-- lo pasamos como dict
        )
        return {"dispositivoid": device_id}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

#-----------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------
# endpoint:
@app.post("/sensors", status_code=201)
async def create_sensor(sensor: schemas.SensorCreate):
    sensor_id = await models.upsert_sensor(
        sensor.dispositivoid, sensor.codigosensor, sensor.nombre,
        sensor.unidad, sensor.factorescala, sensor.desplazamiento,
        sensor.rangomin, sensor.rangomax
    )
    return {"sensorid": sensor_id}


#-----------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------
# endpoint:
@app.post("/lecturas/batch", status_code=201)
async def insert_lecturas_batch(
    lecturas: List[schemas.LecturaCreate],
    background_tasks: BackgroundTasks
):
    async with acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO sensor.Lecturas
            (DispositivoID, SensorID, FechaHora, Valor, Calidad, RawRow)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (DispositivoID, SensorID, FechaHora) DO NOTHING
            """,
            [
                (
                    item.dispositivoid,
                    item.sensorid,
                    item.fechahora,
                    item.valor,
                    item.calidad,
                    None  # RawRow opcional, lo dejamos vacío
                )
                for item in lecturas
            ]
        )

    background_tasks.add_task(
        models.get_chart_data,
        None, None,
        datetime.utcnow() - timedelta(days=1),
        datetime.utcnow(),
        "hour"
    )

    return {"inserted": len(lecturas)}


# -------- CHARTS --------
@app.get("/charts")
async def charts(
    dispositivoid: Optional[int] = None,
    sensornombre: Optional[str] = None,
    desde: Optional[datetime] = None,
    hasta: Optional[datetime] = None,
    bucket: str = "hour"
):
    desde = desde or (datetime.utcnow() - timedelta(days=7))
    hasta = hasta or datetime.utcnow()
    data = await models.get_chart_data(dispositivoid, sensornombre, desde, hasta, bucket)
    return data

# -------- EXPORT --------
@app.get("/export/lecturas")
async def export_lecturas(limit: int = 10000, offset: int = 0):
    rows = await models.export_lecturas(limit=limit, offset=offset)
    import csv, io
    def iter_csv():
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["LecturaID","FechaHora","Valor","Calidad","DispositivoID","SensorID"])
        yield output.getvalue()
        output.seek(0); output.truncate(0)
        for r in rows:
            writer.writerow([r["lecturaid"], r["fechahora"], r["valor"], r["calidad"], r["dispositivoid"], r["sensorid"]])
            yield output.getvalue()
            output.seek(0); output.truncate(0)
    return StreamingResponse(iter_csv(), media_type="text/csv", headers={"Content-Disposition":"attachment; filename=lecturas.csv"})

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=True)