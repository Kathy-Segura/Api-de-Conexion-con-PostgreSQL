from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime

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
    factorescala: Optional[float] = 1.0
    desplazamiento: Optional[float] = 0.0
    rangomin: Optional[float] = None
    rangomax: Optional[float] = None

class LecturaCreate(BaseModel):
    dispositivoid: int
    sensorid: int
    fechahora: datetime
    valor: float
    calidad: Optional[int] = 1
