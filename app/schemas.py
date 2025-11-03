from typing import Optional
from datetime import datetime
from pydantic import BaseModel, EmailStr

class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"

class UserCreate(BaseModel):
    nombre_usuario: str
    correo: EmailStr
    password: str

class UserOut(BaseModel):
    usuarioid: int
    nombre_usuario: str
    correo: EmailStr
    rolid: int

class DeviceCreate(BaseModel):
    serie: str
    nombre: str
    ubicacion: Optional[str] = None
    tipo: Optional[str] = None
    firmware: Optional[str] = None
    configuracion: Optional[dict] = None

class SensorCreate(BaseModel):
    dispositivoid: int
    codigosensor: Optional[str] = None
    nombre: str
    unidad: str
    factorescala: float = 1.0
    desplazamiento: float = 0.0
    rangomin: Optional[float] = None
    rangomax: Optional[float] = None

class LecturaCreate(BaseModel):
    dispositivoid: int
    sensorid: int
    fechahora: datetime
    temperatura: Optional[float] = None
    humedad: Optional[float] = None
    calidad: Optional[int] = 1

class DeviceOut(BaseModel):
    dispositivoid: int
    serie: str
    nombre: str
    ubicacion: Optional[str]
    tipo: Optional[str]
    firmware: Optional[str]
    configuracion: Optional[dict]

class SensorOut(BaseModel):
    sensorid: int
    dispositivoid: int
    codigosensor: Optional[str]
    nombre: str
    unidad: str
    factorescala: float
    desplazamiento: float
    rangomin: Optional[float]
    rangomax: Optional[float]    