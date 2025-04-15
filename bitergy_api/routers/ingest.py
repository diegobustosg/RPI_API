# routers/ingest.py
from fastapi import APIRouter, HTTPException, Body, Path, status
from ..models.ingest import IngestData # Importar modelo de ingesta
from ..services.data_provider import write_sensor_data # Importar función de escritura

router = APIRouter(
    # Usamos el mismo prefijo para agrupar por instalación
    prefix="/installations/{installation_id}",
    tags=["Data Ingestion"] # Nueva etiqueta para Swagger
)

@router.post(
    "/ingest",
    status_code=status.HTTP_202_ACCEPTED, # 202 Accepted es apropiado para ingesta asíncrona/rápida
    summary="Ingest Real-time Sensor Data",
    description="Receives sensor data (e.g., from MQTT bridge) and writes it to the time-series database."
)
async def ingest_data_endpoint(
    installation_id: str = Path(..., description="The unique identifier of the installation sending data"),
    data: IngestData = Body(..., description="The sensor data point to ingest")
):
    """
    Endpoint para recibir y almacenar datos de sensores en tiempo real.
    """
    success = await write_sensor_data(installation_id=installation_id, data=data)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to write data to the database."
        )

    # Devolver una respuesta simple de éxito
    # No devolvemos los datos escritos, solo confirmación.
    return {"message": "Data accepted"}