from app.db import acquire
from typing import Optional

async def upsert_dispositivo(serie: str, nombre: str, ubicacion: Optional[str], tipo: Optional[str], firmware: Optional[str], configuracion: Optional[dict]) -> int:
    async with acquire() as conn:
        row = await conn.fetchrow("SELECT sensor.sp_upsertdispositivo($1,$2,$3,$4,$5,$6) AS id", serie, nombre, ubicacion, tipo, firmware, configuracion)
        return row["id"]

async def upsert_sensor(p_dispositivoid:int, p_codigosensor:Optional[str], p_nombre:str, p_unidad:str, p_factorescala:float, p_desplazamiento:float, p_rangomin:Optional[float], p_rangomax:Optional[float]) -> int:
    async with acquire() as conn:
        row = await conn.fetchrow("SELECT sensor.sp_upsertsensor($1,$2,$3,$4,$5,$6,$7,$8) AS id", p_dispositivoid, p_codigosensor, p_nombre, p_unidad, p_factorescala, p_desplazamiento, p_rangomin, p_rangomax)
        return row["id"]

async def insertar_lectura(p_dispositivoid:int, p_sensorid:int, p_fechahora, p_valor:float, p_calidad:int, p_rawrow: str|None=None):
    async with acquire() as conn:
        await conn.execute("SELECT sensor.sp_insertarlectura($1,$2,$3,$4,$5,$6)", p_dispositivoid, p_sensorid, p_fechahora, p_valor, p_calidad, p_rawrow)

async def get_chart_data(dispositivoid:Optional[int], sensornombre:Optional[str], desde, hasta, bucket: str='hour'):
    async with acquire() as conn:
        rows = await conn.fetch("SELECT * FROM sensor.sp_getchartdata($1,$2,$3,$4,$5)", dispositivoid, sensornombre, desde, hasta, bucket)
        return [dict(r) for r in rows]

async def export_lecturas(limit:int=10000, offset:int=0):
    async with acquire() as conn:
        rows = await conn.fetch("SELECT LecturaID, FechaHora, Valor, Calidad, DispositivoID, SensorID FROM sensor.Lecturas ORDER BY FechaHora DESC LIMIT $1 OFFSET $2", limit, offset)
        return [dict(r) for r in rows]
