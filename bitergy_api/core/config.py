# core/config.py
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """Configuración de la aplicación cargada desde .env o variables de entorno."""
    PROJECT_NAME: str = "BITERGY API"
    API_V1_STR: str = "/api/v1"

    # Configuración InfluxDB (requerida)
    INFLUXDB_URL: str
    INFLUXDB_TOKEN: str
    INFLUXDB_ORG: str
    INFLUXDB_BUCKET: str

    # Configuración Opcional
    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
        case_sensitive=False
    )

settings = Settings()
