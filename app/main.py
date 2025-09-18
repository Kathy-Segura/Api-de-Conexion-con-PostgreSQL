import os
from datetime import datetime, timedelta
from typing import Optional, List
import uvicorn, json
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse, RedirectResponse
from app.db import init_db_pool, close_db_pool, acquire
from app import models, schemas, auth
from fastapi import Body
import traceback

app = FastAPI(title="Plataforma Climática API")

# Redirige el home (/) hacia la documentación en Swagger
@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/docs")

# Eventos de ciclo de vida
@app.on_event("startup")
async def startup() -> None:
    await init_db_pool()

@app.on_event("shutdown")
async def shutdown() -> None:
    await close_db_pool()

#-----------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------
# endpoint:

@app.post("/login")
async def login_user(payload: dict = Body(...)):
    login_input = payload.get("login")
    password = payload.get("password")
    
    if not login_input or not password:
        raise HTTPException(status_code=400, detail="Usuario y contraseña requeridos")
    
    try:
        async with acquire() as conn:
            user = await conn.fetchrow("""
                SELECT UsuarioID, NombreUsuario, Correo, PasswordHash, RolID, Activo
                FROM sensor.Usuarios
                WHERE NombreUsuario=$1 OR Correo=$1
            """, login_input)

            print("DEBUG: usuario fetchrow ->", user)  # <--- log temporal
            
            if not user:
                raise HTTPException(status_code=401, detail="Usuario no encontrado")
            if not user["Activo"]:
                raise HTTPException(status_code=403, detail="Usuario inactivo")
            
            # Revisar qué tipo tiene PasswordHash
            stored_hash = user["PasswordHash"]
            print("DEBUG: tipo PasswordHash ->", type(stored_hash))  # <--- log temporal
            
            # Conversión segura
            if isinstance(stored_hash, memoryview):
                stored_hash = stored_hash.tobytes().decode('utf-8')
            elif isinstance(stored_hash, bytes):
                stored_hash = stored_hash.decode('utf-8')
            
            print("DEBUG: stored_hash ->", stored_hash[:20], "...")  # mostrar solo inicio
            
            if not auth.verify_password(password, stored_hash):
                raise HTTPException(status_code=401, detail="Contraseña incorrecta")
            
            token = auth.create_access_token({
                "sub": user["UsuarioID"],
                "username": user["NombreUsuario"],
                "rol": user["RolID"]
            })
            
            return {"success": True, "token": token}
    
    except HTTPException:
        raise
    except Exception as e:
        print("ERROR LOGIN:", traceback.format_exc())  # log completo del error
        raise HTTPException(status_code=500, detail="Error interno del servidor")
#-----------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------
# endpoint:
@app.post("/register")
async def register_user(payload: dict = Body(...)):
    name = payload.get("name")
    email = payload.get("email")
    phone = payload.get("phone")
    password = payload.get("password")

    if not all([name, email, phone, password]):
        raise HTTPException(status_code=400, detail="Todos los campos son obligatorios")

    password_hash = auth.hash_password(password)  # bcrypt seguro

    async with acquire() as conn:
        # Verificamos si ya existe usuario o correo
        exists = await conn.fetchrow("""
            SELECT 1 FROM sensor.Usuarios WHERE NombreUsuario=$1 OR Correo=$2
        """, name, email)
        if exists:
            raise HTTPException(status_code=409, detail="Usuario o correo ya existe")

        user_id = await conn.fetchval("""
            INSERT INTO sensor.Usuarios (NombreUsuario, Correo, PasswordHash, RolID, Activo)
            VALUES ($1, $2, $3, 2, TRUE)
            RETURNING UsuarioID
        """, name, email, password_hash)

        # Opcional: generar token automáticamente tras registro
        token = auth.create_access_token({
            "sub": user_id,
            "username": name,
            "rol": 2
        })

        return {"success": True, "token": token}


#-----------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------
# endpoint:
@app.get("/health")
async def health():
    try:
        async with acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "ok"}
    except Exception as e:
        # No exponemos detalles internos en producción
        raise HTTPException(status_code=500, detail="DB check failed")

#-----------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------
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
#-----------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------
# endpoint: recibe dict en configuracion

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

#-----------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------
# endpoint: recibe dict en configuracion
@app.get("/export/lecturas")
async def export_lecturas(limit: int = 10000, offset: int = 0):
    rows = await models.export_lecturas(limit=limit, offset=offset)

    def iter_csv():
        import csv, io
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["LecturaID", "FechaHora", "Valor", "Calidad", "DispositivoID", "SensorID"])
        yield output.getvalue()
        output.seek(0); output.truncate(0)
        for r in rows:
            writer.writerow([
                r["lecturaid"], r["fechahora"], r["valor"],
                r["calidad"], r["dispositivoid"], r["sensorid"]
            ])
            yield output.getvalue()
            output.seek(0); output.truncate(0)

    return StreamingResponse(
        iter_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=lecturas.csv"}
    )

#-----------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        reload=True
    )