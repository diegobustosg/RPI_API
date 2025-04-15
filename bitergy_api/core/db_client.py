# core/db_client.py
import influxdb_client
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from ..core.config import settings
from typing import Optional # <-- Importar Optional
_async_influx_client: Optional[InfluxDBClientAsync] = None

def get_influxdb_client() -> InfluxDBClientAsync:
    """Obtiene una instancia Singleton del cliente asíncrono de InfluxDB."""
    global _async_influx_client
    if _async_influx_client is None:
        print("Initializing InfluxDB async client...") # Debug message
        _async_influx_client = InfluxDBClientAsync(
            url=settings.INFLUXDB_URL,
            token=settings.INFLUXDB_TOKEN,
            org=settings.INFLUXDB_ORG
        )
    return _async_influx_client

async def get_query_api():
    """Obtiene la API de consulta del cliente InfluxDB."""
    client = get_influxdb_client()
    return client.query_api()

async def close_influxdb_client():
    """Cierra la conexión del cliente InfluxDB si existe."""
    global _async_influx_client
    if _async_influx_client:
        print("Closing InfluxDB async client...") # Debug message
        await _async_influx_client.close()
        _async_influx_client = None
