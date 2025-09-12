import os
from datetime import datetime, timedelta
from typing import Optional, List
import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse, RedirectResponse
from app.db import init_db_pool, close_db_pool, acquire
from app import models, schemas, auth

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

# Endpoint de salud
@app.get("/health")
async def health():
    try:
        async with acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "ok"}
    except Exception as e:
        # No exponemos detalles internos en producción
        raise HTTPException(status_code=500, detail="DB check failed")


@app.post("/devices", status_code=201)
async def create_device(device: schemas.DeviceCreate):
    try:
        device_id = await models.upsert_dispositivo(
            device.serie, device.nombre, device.ubicacion,
            device.tipo, device.firmware, device.configuracion
        )
        return {"dispositivoid": device_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")
    
    
@app.post("/sensors", status_code=201)
async def create_sensor(sensor: schemas.SensorCreate):
    sensor_id = await models.upsert_sensor(
        sensor.dispositivoid, sensor.codigosensor, sensor.nombre,
        sensor.unidad, sensor.factorescala, sensor.desplazamiento,
        sensor.rangomin, sensor.rangomax
    )
    return {"sensorid": sensor_id}

@app.post("/lecturas/batch", status_code=201)
async def insert_lecturas_batch(
    lecturas: List[schemas.LecturaCreate],
    background_tasks: BackgroundTasks
):
    # Inserción bulk en staging
    async with acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO sensor.Staging_Lecturas
            (Serie, CodigoSensor, FechaHora, Valor, RawRow)
            VALUES ($1,$2,$3,$4,$5)
            """,
            [
                # Ajusta si tu esquema requiere Serie/CodigoSensor reales
                (None, str(item.sensorid), item.fechahora, str(item.valor), None)
                for item in lecturas
            ]
        )

    # Procesamiento diferido (si lo tienes implementado así)
    background_tasks.add_task(
        models.get_chart_data,
        None, None,
        datetime.utcnow() - timedelta(days=1),
        datetime.utcnow(),
        "hour"
    )
    return {"inserted": len(lecturas)}

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

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        reload=True
    )