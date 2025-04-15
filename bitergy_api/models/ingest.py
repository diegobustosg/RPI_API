# models/ingest.py
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
from datetime import datetime

class IngestData(BaseModel):
    """
    Modelo para los datos recibidos en el endpoint de ingesta.
    """
    # Es crucial tener un timestamp. Si no se provee, podríamos usar el actual,
    # pero es mejor que el origen (ej. MQTT bridge) lo incluya.
    timestamp: datetime = Field(..., description="Timestamp (ISO 8601 UTC preferible) de la medición.")

    # El measurement de InfluxDB (ej. "voltage", "temperature")
    measurement: str = Field(..., description="Nombre de la 'tabla' o tipo de medición en InfluxDB.")

    # Los campos con los valores medidos. Usamos Any para flexibilidad,
    # pero podrías restringirlo a float, int, bool, str si lo prefieres.
    fields: Dict[str, Any] = Field(..., description="Diccionario de campos y sus valores (ej. {'value': 25.5} o {'phase_a': 220.1}).")

    # Tags opcionales para metadatos adicionales
    tags: Optional[Dict[str, str]] = Field(None, description="Diccionario opcional de tags (ej. {'sensor_id': 'temp001', 'location': 'rack2'}).")

    # Ejemplo para Swagger/OpenAPI
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "timestamp": "2023-10-27T10:30:00Z",
                    "measurement": "temperature",
                    "fields": {"value": 22.5},
                    "tags": {"sensor_id": "temp_sala1", "location": "Sala Servidores"}
                },
                {
                     "timestamp": "2023-10-27T10:30:05Z",
                     "measurement": "voltage",
                     "fields": {"phase_a": 228.1, "phase_b": 229.5, "phase_c": 227.9},
                     "tags": {"device": "PZEM-PanelPrincipal"}
                }
            ]
        }
    }