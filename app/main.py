import os
from fastapi import FastAPI, HTTPException, Body, BackgroundTasks
from fastapi.responses import StreamingResponse, RedirectResponse
from datetime import datetime, timedelta
from typing import List, Optional
import uvicorn
from app import auth, models, schemas
from app.db import init_db_pool, close_db_pool, acquire

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

# -------- REGISTER --------
@app.post("/register", status_code=201)
async def register_user(payload: dict = Body(...)):
    username = payload.get("username")
    email = payload.get("email")
    password = payload.get("password")
    rol_id = payload.get("rol_id", 2)
    activo = True

    if not username or not email or not password:
        raise HTTPException(status_code=400, detail="Usuario, correo y contraseña requeridos")

    hashed_password = auth.hash_password(password)

    async with acquire() as conn:
        try:
            user_id = await conn.fetchval("""
                INSERT INTO sensor.Usuarios (NombreUsuario, Correo, PasswordHash, RolID, Activo)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING UsuarioID
            """, username, email, hashed_password, rol_id, activo)
            return {"usuarioid": user_id, "username": username, "email": email}
        except Exception:
            raise HTTPException(status_code=500, detail="Error registrando usuario")

# -------- LOGIN --------
@app.post("/login")
async def login_user(payload: dict = Body(...)):
    login_input = payload.get("login")
    password = payload.get("password")
    if not login_input or not password:
        raise HTTPException(status_code=400, detail="Usuario/correo y contraseña requeridos")

    async with acquire() as conn:
        user = await conn.fetchrow("""
            SELECT UsuarioID AS usuarioid, NombreUsuario AS username, Correo AS email,
                   PasswordHash AS passwordhash, RolID AS rolid, Activo AS activo
            FROM sensor.Usuarios
            WHERE NombreUsuario=$1 OR Correo=$1
        """, login_input)

        if not user:
            raise HTTPException(status_code=401, detail="Usuario no encontrado")
        if not user["activo"]:
            raise HTTPException(status_code=403, detail="Usuario inactivo")
        if not auth.verify_password(password, user["passwordhash"]):
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

# -------- DEVICES --------
@app.post("/devices", status_code=201)
async def create_device(device: schemas.DeviceCreate):
    try:
        device_id = await models.upsert_dispositivo(
            device.serie, device.nombre, device.ubicacion, device.tipo,
            device.firmware, device.configuracion
        )
        return {"dispositivoid": device_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

# -------- SENSORS --------
@app.post("/sensors", status_code=201)
async def create_sensor(sensor: schemas.SensorCreate):
    sensor_id = await models.upsert_sensor(
        sensor.dispositivoid, sensor.codigosensor, sensor.nombre,
        sensor.unidad, sensor.factorescala, sensor.desplazamiento,
        sensor.rangomin, sensor.rangomax
    )
    return {"sensorid": sensor_id}

# -------- LECTURAS BATCH --------
@app.post("/lecturas/batch", status_code=201)
async def insert_lecturas_batch(
    lecturas: List[schemas.LecturaCreate],
    background_tasks: BackgroundTasks
):
    async with acquire() as conn:
        await conn.executemany("""
            INSERT INTO sensor.Lecturas
            (DispositivoID, SensorID, FechaHora, Valor, Calidad, RawRow)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (DispositivoID, SensorID, FechaHora) DO NOTHING
        """, [(l.dispositivoid, l.sensorid, l.fechahora, l.valor, l.calidad, None) for l in lecturas])

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