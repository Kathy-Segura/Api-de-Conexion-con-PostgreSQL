import os
import hashlib
from datetime import datetime, timedelta
from typing import Optional
from jose import jwt, JWTError
from passlib.context import CryptContext

# Contexto bcrypt
PWD_CTX = CryptContext(schemes=["bcrypt"], deprecated="auto")

# JWT
SECRET_KEY = os.getenv("SECRET_KEY", "change_this_secret_in_production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))

# ---------------------------
# Helpers
# ---------------------------

def _prehash(password: str) -> bytes:
    """
    Pre-hash con SHA256 para evitar el límite de 72 bytes de bcrypt.
    Devuelve digest de 32 bytes (seguro, constante).
    """
    return hashlib.sha256(password.encode("utf-8")).digest()

# ---------------------------
# Password hashing
# ---------------------------

def hash_password(plain: str) -> str:
    """
    Devuelve el hash seguro de la contraseña usando SHA256 + bcrypt.
    """
    return PWD_CTX.hash(_prehash(plain))

def verify_password(plain: str, hashed: str) -> bool:
    """
    Verifica si la contraseña en texto plano coincide con el hash.
    """
    try:
        return PWD_CTX.verify(_prehash(plain), hashed)
    except Exception as ex:
        # Aquí se podría loggear el error si usas logging
        # logger.exception("Error verificando contraseña: %s", ex)
        return False

# ---------------------------
# JWT helpers
# ---------------------------

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """
    Crea un JWT firmado con expiración.
    """
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def decode_access_token(token: str) -> Optional[dict]:
    """
    Decodifica un JWT y devuelve el payload, o None si no es válido/expiró.
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None