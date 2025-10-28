import os
import uvicorn
import logging
import json
from datetime import datetime, timedelta
from typing import List, Optional, Any
from app import auth, models, schemas
from asyncpg import exceptions as pg_exc
from app.db import init_db_pool, close_db_pool, acquire
from fastapi import FastAPI, HTTPException, Body, BackgroundTasks, APIRouter, Query
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

# ------------------- Helpers -------------------

def _normalize_hash_from_db(h: Any) -> str:
    """
    Normaliza el hash obtenido de la DB a un str legible por passlib.
    """
    if h is None:
        logger.error("PasswordHash en DB es NULL")
        raise HTTPException(status_code=500, detail="Error interno: hash de contraseña inválido")

    if isinstance(h, memoryview):
        h = h.tobytes()

    if isinstance(h, (bytes, bytearray)):
        try:
            return h.decode("utf-8")
        except Exception as ex:
            logger.exception("No se pudo decodificar PasswordHash desde DB: %s", ex)
            raise HTTPException(status_code=500, detail="Error interno decodificando hash de contraseña")

    if isinstance(h, str):
        return h.strip()

    logger.warning("PasswordHash en DB tiene tipo inesperado %s", type(h))
    return str(h).strip()


def _verify_password(plain_password: str, stored_hash_any: Any) -> bool:
    """
    Verifica la contraseña en texto plano contra el hash usando passlib.
    """
    stored = _normalize_hash_from_db(stored_hash_any)
    try:
        return auth.verify_password(plain_password, stored)
    except Exception as ex:
        logger.exception("Error inesperado verificando contraseña: %s", ex)
        raise HTTPException(status_code=500, detail="Error interno verificando contraseña")

# ------------------- ENDPOINTS -------------------

# -------- REGISTER --------
@app.post("/register", status_code=201)
async def register_user(payload: dict = Body(...)):
    username_raw = (payload.get("username") or "").strip()
    username = username_raw.lower()                # guardamos en minúsculas para evitar duplicados case-sensitive
    email = (payload.get("email") or "").strip().lower()
    password = payload.get("password")
    rol_id = int(payload.get("rol_id", 2))
    activo = True

    if not username or not email or not password:
        raise HTTPException(status_code=400, detail="Usuario, correo y contraseña son requeridos")

    hashed_password = auth.hash_password(password)
    if isinstance(hashed_password, (bytes, memoryview)):
        try:
            hashed_password = hashed_password.decode("utf-8")
        except Exception:
            logger.warning("hash_password devolvió bytes no decodificables, se guarda tal cual")

    async with acquire() as conn:
        # validar que el rol exista
        role_exists = await conn.fetchval("SELECT 1 FROM sensor.Roles WHERE RolID=$1", rol_id)
        if not role_exists:
            raise HTTPException(status_code=400, detail=f"El RolID {rol_id} no existe")

        try:
            user_id = await conn.fetchval("""
                INSERT INTO sensor.Usuarios (NombreUsuario, Correo, PasswordHash, RolID, Activo)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING UsuarioID
            """, username, email, hashed_password, rol_id, activo)

            return {
                "usuarioid": user_id,
                "username": username_raw,
                "email": email
            }

        except pg_exc.UniqueViolationError:
            raise HTTPException(status_code=409, detail="El nombre de usuario o correo ya están registrados")
        except pg_exc.ForeignKeyViolationError:
            raise HTTPException(status_code=400, detail="Rol inválido, no cumple con la clave foránea")
        except Exception as ex:
            logger.exception("Error registrando usuario: %s", ex)
            raise HTTPException(status_code=500, detail="Error interno al registrar usuario")

# -------- LOGIN --------
@app.post("/login")
async def login_user(payload: dict = Body(...)):
    login_input = (payload.get("login") or "").strip().lower()
    password = payload.get("password")

    if not login_input or not password:
        raise HTTPException(status_code=400, detail="Usuario/correo y contraseña son requeridos")

    async with acquire() as conn:
        user = await conn.fetchrow("""
            SELECT UsuarioID AS usuarioid, NombreUsuario AS username, Correo AS email,
                   PasswordHash AS passwordhash, RolID AS rolid, Activo AS activo
            FROM sensor.Usuarios
            WHERE lower(NombreUsuario)=$1 OR lower(Correo)=$1
        """, login_input)

        if not user:
            raise HTTPException(status_code=401, detail="Usuario no encontrado")
        if not user["activo"]:
            raise HTTPException(status_code=403, detail="La cuenta está inactiva")

        if not _verify_password(password, user["passwordhash"]):
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

#-----------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------
# endpoint:
# -------- GET DEVICES --------
@app.get("/devices", response_model=List[schemas.DeviceOut])
async def get_devices():
    try:
        async with acquire() as conn:
            rows = await conn.fetch("SELECT * FROM sensor.dispositivos ORDER BY dispositivoid")
            dispositivos = []
            for r in rows:
                dispositivos.append({
                    "dispositivoid": r["dispositivoid"],
                    "serie": r["serie"],
                    "nombre": r["nombre"],
                    "ubicacion": r["ubicacion"],
                    "tipo": r["tipo"],
                    "firmware": r["firmware"],
                    "configuracion": json.loads(r["configuracion"]) if r["configuracion"] else None
                })
            return dispositivos
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")
    
#-----------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------
# endpoint: 
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
# endpoint: recibe dict en configuracion
# -------- GET SENSORS --------
@app.get("/sensors", response_model=List[schemas.SensorOut])
async def get_sensors():
    async with acquire() as conn:
        rows = await conn.fetch("""
            SELECT SensorID AS sensorid, DispositivoID AS dispositivoid, CodigoSensor AS codigosensor,
                   Nombre AS nombre, Unidad AS unidad, FactorEscala AS factorescala, Desplazamiento AS desplazamiento,
                   RangoMin AS rangomin, RangoMax AS rangomax
            FROM sensor.Sensores
            ORDER BY SensorID ASC
        """)
        return [dict(r) for r in rows]
    
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


@app.get("/lecturas", tags=["Lecturas"])
async def get_lecturas(
    dispositivoid: Optional[int] = None,
    sensorid: Optional[int] = None,
    desde: Optional[datetime] = Query(None, description="Fecha inicial del rango"),
    hasta: Optional[datetime] = Query(None, description="Fecha final del rango")
):
    """
    Retorna lecturas con filtros opcionales por dispositivo, sensor y rango de fechas.
    Ejemplo: /lecturas?dispositivoid=1&desde=2025-10-01&hasta=2025-10-27
    """
    desde = desde or (datetime.utcnow() - timedelta(days=7))
    hasta = hasta or datetime.utcnow()

    query = """
        SELECT lecturaid, dispositivoid, sensorid, fechahora, valor, calidad
        FROM sensor.Lecturas
        WHERE fechahora BETWEEN $1 AND $2
    """
    params = [desde, hasta]

    if dispositivoid:
        query += " AND dispositivoid = $3"
        params.append(dispositivoid)
    if sensorid:
        query += f" AND sensorid = ${len(params) + 1}"
        params.append(sensorid)

    async with acquire() as conn:
        rows = await conn.fetch(query, *params)

    return [
        {
            "lecturaid": r["lecturaid"],
            "dispositivoid": r["dispositivoid"],
            "sensorid": r["sensorid"],
            "fechahora": r["fechahora"].isoformat(),
            "valor": r["valor"],
            "calidad": r["calidad"],
        }
        for r in rows
    ]

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