# API de Plataforma Climática 🌦️

Esta API permite registrar y consultar dispositivos (sensores) y sus configuraciones.  
Está construida en **Python + FastAPI** y documentada automáticamente con **Swagger/OpenAPI**.

---

## ⚙️ Requisitos

- Python 3.10 o superior
- [FastAPI](https://fastapi.tiangolo.com/)
- [Uvicorn](https://www.uvicorn.org/)
- [asyncpg](https://github.com/MagicStack/asyncpg) (si usas PostgreSQL)
- [python-dotenv](https://pypi.org/project/python-dotenv/) (para variables de entorno)

---

## 🚀 Instalación y ejecución

### 1. Clona el repositorio:
   git clone https://github.com/usuario/api-plataforma-climatica.git
   cd api-plataforma-climatica

### 2. Crea un entorno virtual e instalar dependencias:
   `python -m venv venv`
   
   `venv\Scripts\activate`
   
   `pip install -r requirements.txt`

### 3. Configura las variables de entorno en un archivo .env:
   `DATABASE_URL=postgresql://usuario:password@localhost:5432/mi_bd`

### 4. Ejecuta el servidor:
   `uvicorn main:app --reload`
