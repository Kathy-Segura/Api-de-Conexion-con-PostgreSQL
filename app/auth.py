import os
from datetime import datetime, timedelta
from jose import jwt, JWTError
from passlib.context import CryptContext

# Configuración del contexto de hash
PWD_CTX = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Configuración JWT
SECRET_KEY = os.getenv("SECRET_KEY", "change_this_secret_in_production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))


# 🔹 Genera un hash seguro a partir de la contraseña en texto plano
def hash_password(plain: str) -> str:
    # Devuelve siempre un str (Passlib ya devuelve str, no bytes)
    return PWD_CTX.hash(plain)


# 🔹 Verifica contraseña en texto plano contra hash guardado (TEXT en la BD)
def verify_password(plain: str, hashed: str) -> bool:
    try:
        return PWD_CTX.verify(plain, hashed)
    except Exception:
        return False   # Si el hash está corrupto o en formato inválido


# 🔹 Genera un JWT con expiración
def create_access_token(data: dict, expires_delta: timedelta | None = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta if expires_delta else timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


# 🔹 Decodifica un JWT (devuelve payload o None si no es válido)
def decode_access_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None