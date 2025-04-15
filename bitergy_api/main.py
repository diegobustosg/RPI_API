# main.py
from fastapi import FastAPI
# Usar importaciones relativas DENTRO del paquete bitergy_api
from .routers import electrical, physical
from .core.config import settings
from .core.db_client import close_influxdb_client

app = FastAPI(
    title=settings.PROJECT_NAME,
    description="API for accessing real-time and historical electrical and physical data from BITERGY installations.",
    version="1.1.0",
)

# Incluir ambos routers
app.include_router(electrical.router, prefix=settings.API_V1_STR)
app.include_router(physical.router, prefix=settings.API_V1_STR) # Mismo prefijo base API

@app.get("/", tags=["Root"])
async def read_root():
    """Endpoint raíz para verificar que la API está funcionando."""
    return {"message": f"Welcome to the {settings.PROJECT_NAME}. Visit /docs for documentation."}

@app.on_event("startup")
async def startup_event():
    print(f"Starting {settings.PROJECT_NAME}...")
    # Puedes añadir inicializaciones aquí si es necesario

@app.on_event("shutdown")
async def shutdown_event():
    print(f"Shutting down {settings.PROJECT_NAME}...")
    await close_influxdb_client()
