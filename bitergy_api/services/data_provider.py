# services/data_provider.py
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional
from influxdb_client import Point # Importar la clase Point
from influxdb_client.client.exceptions import InfluxDBError
from ..models.ingest import IngestData # Importar el nuevo modelo
# Importar cliente y configuración
from ..core.db_client import get_query_api, get_write_api_async
from ..core.config import settings
from ..core.enums import SeverityLevel

# Importar modelos comunes
from ..models.common import (
    HistoricalDataPoint,
    Thresholds,
    InstallationThresholds
)
# Importar modelos específicos eléctricos
from ..models.electrical import (
    RealtimeElectricalData,
    PhaseData,
    ElectricalVariableValue,
    GroupedHistoricalElectricalData # <-- Importar desde aquí
)
# Importar modelos específicos físicos
from ..models.physical import (
    RealtimePhysicalData,
    PhysicalVariableValue,
    GroupedHistoricalPhysicalData # <-- Importar desde aquí
)

# Importar clase para escribir datos
from ..models.ingest import IngestData


# --- Funciones de ayuda (get_unit_for_measurement, _get_thresholds_for_installation, _calculate_severity) ---
# (Estas funciones permanecen igual que en la respuesta anterior)
def get_unit_for_measurement(measurement: str, field: Optional[str] = None) -> str:
    """Devuelve la unidad para una medida/campo dado."""
    units = {
        # Eléctricas
        "voltage": "V", "current": "A", "active_power": "kW",
        "apparent_power": "kVA", "reactive_power": "kVAR", "power_factor": "",
        "frequency": "Hz", "energy": "kWh",
        # Físicas
        "temperature": "°C", "humidity": "%", "level": "m", # Ejemplo level
    }
    # Manejo especial para tipos sin unidad o con unidad fija
    if measurement == "power_factor": return ""
    if measurement == "energy": return "kWh" # Asumiendo que se almacena así
    return units.get(measurement, "unknown_unit") # Devuelve 'unknown_unit' si no se encuentra

def _get_thresholds_for_installation(installation_id: str) -> Optional[InstallationThresholds]:
    """Simula la obtención de umbrales para una instalación."""
    # En una aplicación real, obtendrías esto de Firebase, un archivo de config, etc.
    if installation_id == "siteA-mainpanel":
        return InstallationThresholds(
            voltage=Thresholds(critical_low=207, low=218.5, high=241.5, critical_high=253),
            current=Thresholds(high=80, critical_high=95), # Asume 100A max
            frequency=Thresholds(critical_low=48, low=49.5, high=50.5, critical_high=52), # Asume 50Hz nominal
            temperature=Thresholds(low=5, high=40, critical_high=50),
            humidity=Thresholds(high=85, critical_high=95)
        )
    # Devuelve None si no se encuentran umbrales para la instalación
    return None

def _calculate_severity(value: Optional[float], thresholds: Optional[Thresholds]) -> SeverityLevel:
    """Calcula el nivel de severidad basado en el valor y los umbrales."""
    if value is None or thresholds is None:
        return SeverityLevel.UNKNOWN

    if thresholds.critical_low is not None and value < thresholds.critical_low:
        return SeverityLevel.CRITICAL_LOW
    # Importante el orden: verifica 'low' *después* de 'critical_low'
    if thresholds.low is not None and value < thresholds.low:
        return SeverityLevel.LOW
    if thresholds.critical_high is not None and value > thresholds.critical_high:
        return SeverityLevel.CRITICAL_HIGH
    # Importante el orden: verifica 'high' *después* de 'critical_high'
    if thresholds.high is not None and value > thresholds.high:
        return SeverityLevel.HIGH

    # Si no está fuera de ningún umbral definido, es Normal
    return SeverityLevel.NORMAL

# --- Funciones de recuperación de datos ---

async def get_realtime_electrical_data(installation_id: str) -> Optional[RealtimeElectricalData]:
    """Obtiene los datos eléctricos más recientes y calcula su severidad."""
    try:
        query_api = await get_query_api()
        threshold_config = _get_thresholds_for_installation(installation_id)
        measurements = [
            "voltage", "current", "active_power", "apparent_power",
            "reactive_power", "power_factor", "frequency", "energy"
        ]

        flux_query = f'''
        from(bucket: "{settings.INFLUXDB_BUCKET}")
          |> range(start: -5m) // Ventana de búsqueda razonable para 'last()'
          |> filter(fn: (r) => r["_measurement"] == "{'" or r["_measurement"] == "'.join(measurements)}")
          |> filter(fn: (r) => r["installation_id"] == "{installation_id}")
          |> group() // Desagrupar para obtener el último absoluto
          |> last() // Obtener el último registro por cada _field/_measurement
        '''
        # print(f"DEBUG: Realtime Electrical Query:\n{flux_query}") # Descomentar para depurar
        result = await query_api.query(query=flux_query, org=settings.INFLUXDB_ORG)

        if not result:
            print(f"WARN: No realtime electrical data found for installation '{installation_id}' in bucket '{settings.INFLUXDB_BUCKET}'")
            return None

        latest_data: Dict[str, Dict[str, ElectricalVariableValue]] = {}
        latest_timestamp: Optional[datetime] = None

        for table in result:
            for record in table.records:
                try:
                    measurement = record.get_measurement()
                    field = record.get_field()
                    value = record.get_value()
                    time = record.get_time()

                    if latest_timestamp is None or time > latest_timestamp:
                        latest_timestamp = time

                    if measurement not in latest_data:
                        latest_data[measurement] = {}

                    current_thresholds: Optional[Thresholds] = None
                    if threshold_config:
                        if measurement == "voltage": current_thresholds = threshold_config.voltage
                        elif measurement == "current": current_thresholds = threshold_config.current
                        elif measurement == "frequency": current_thresholds = threshold_config.frequency
                        elif measurement == "power_factor": current_thresholds = threshold_config.power_factor
                        # Añadir lógica para otros (power etc.) si tienen umbrales

                    latest_data[measurement][field] = ElectricalVariableValue(
                        value=value,
                        unit=get_unit_for_measurement(measurement, field),
                        severity=_calculate_severity(value, current_thresholds)
                    )
                except (KeyError, TypeError, AttributeError) as e:
                    print(f"WARN: Skipping record due to parsing error in realtime electrical: {e} - Record: {record.values if record else 'None'}")
                    continue # Saltar al siguiente registro si este tiene problemas

        if not latest_data:
             print(f"WARN: Processed data is empty for realtime electrical installation '{installation_id}'")
             return None

        # Mapeo al objeto Pydantic
        realtime_obj = RealtimeElectricalData(
            asset_id=installation_id,
            timestamp=latest_timestamp or datetime.utcnow(), # Usar la última hora encontrada o la actual
            voltage=PhaseData(a=latest_data.get("voltage", {}).get("phase_a"),b=latest_data.get("voltage", {}).get("phase_b"),c=latest_data.get("voltage", {}).get("phase_c")) if "voltage" in latest_data else None,
            current=PhaseData(a=latest_data.get("current", {}).get("phase_a"),b=latest_data.get("current", {}).get("phase_b"),c=latest_data.get("current", {}).get("phase_c")) if "current" in latest_data else None,
            active_power=PhaseData(a=latest_data.get("active_power", {}).get("phase_a"),b=latest_data.get("active_power", {}).get("phase_b"),c=latest_data.get("active_power", {}).get("phase_c")) if "active_power" in latest_data else None,
            apparent_power=PhaseData(a=latest_data.get("apparent_power", {}).get("phase_a"), b=latest_data.get("apparent_power", {}).get("phase_b"), c=latest_data.get("apparent_power", {}).get("phase_c")) if "apparent_power" in latest_data else None,
            reactive_power=PhaseData(a=latest_data.get("reactive_power", {}).get("phase_a"), b=latest_data.get("reactive_power", {}).get("phase_b"), c=latest_data.get("reactive_power", {}).get("phase_c")) if "reactive_power" in latest_data else None,
            power_factor=PhaseData(a=latest_data.get("power_factor", {}).get("phase_a"), b=latest_data.get("power_factor", {}).get("phase_b"), c=latest_data.get("power_factor", {}).get("phase_c")) if "power_factor" in latest_data else None,
            frequency=latest_data.get("frequency", {}).get("value"), # Asume un campo 'value'
            total_energy_kwh=latest_data.get("energy", {}).get("total_kwh"), # Asume un campo 'total_kwh'
            total_active_power=latest_data.get("active_power", {}).get("total") # Asume un campo 'total'
        )
        return realtime_obj

    except Exception as e:
        print(f"ERROR: Unexpected error in get_realtime_electrical_data for '{installation_id}': {e}")
        # Podrías querer loggear el traceback completo aquí
        # import traceback
        # traceback.print_exc()
        return None

async def get_realtime_physical_data(installation_id: str) -> Optional[RealtimePhysicalData]:
    """Obtiene los datos físicos más recientes y calcula su severidad."""
    try:
        query_api = await get_query_api()
        threshold_config = _get_thresholds_for_installation(installation_id)
        measurements = ["temperature", "humidity", "level"] # Ajusta según tus measurements

        flux_query = f'''
        from(bucket: "{settings.INFLUXDB_BUCKET}")
          |> range(start: -5m)
          |> filter(fn: (r) => r["_measurement"] == "{'" or r["_measurement"] == "'.join(measurements)}")
          |> filter(fn: (r) => r["installation_id"] == "{installation_id}")
          |> group() |> last()
        '''
        # print(f"DEBUG: Realtime Physical Query:\n{flux_query}")
        result = await query_api.query(query=flux_query, org=settings.INFLUXDB_ORG)

        if not result:
            print(f"WARN: No realtime physical data found for installation '{installation_id}' in bucket '{settings.INFLUXDB_BUCKET}'")
            return None

        latest_data: Dict[str, PhysicalVariableValue] = {}
        latest_timestamp: Optional[datetime] = None

        for table in result:
            for record in table.records:
                try:
                    measurement = record.get_measurement()
                    field = record.get_field() # Puede ser 'value' u otro
                    value = record.get_value()
                    time = record.get_time()

                    if latest_timestamp is None or time > latest_timestamp:
                        latest_timestamp = time

                    current_thresholds: Optional[Thresholds] = None
                    if threshold_config:
                        if measurement == "temperature": current_thresholds = threshold_config.temperature
                        elif measurement == "humidity": current_thresholds = threshold_config.humidity
                        elif measurement == "level": current_thresholds = threshold_config.level

                    # Asigna al measurement si el campo es 'value' o si aún no existe entrada
                    # Si tienes múltiples sensores con diferentes fields, necesitarás ajustar el modelo Pydantic
                    if measurement not in latest_data or field == 'value':
                        latest_data[measurement] = PhysicalVariableValue(
                            value=value,
                            unit=get_unit_for_measurement(measurement, field),
                            severity=_calculate_severity(value, current_thresholds),
                            sensor_location=record.values.get("location") # Ejemplo: si tienes tag 'location'
                        )
                except (KeyError, TypeError, AttributeError) as e:
                    print(f"WARN: Skipping record due to parsing error in realtime physical: {e} - Record: {record.values if record else 'None'}")
                    continue

        if not latest_data:
             print(f"WARN: Processed data is empty for realtime physical installation '{installation_id}'")
             return None

        # Mapear al objeto Pydantic
        realtime_obj = RealtimePhysicalData(
            asset_id=installation_id,
            timestamp=latest_timestamp or datetime.utcnow(),
            temperature=latest_data.get("temperature"),
            humidity=latest_data.get("humidity"),
            level=latest_data.get("level") # Alias se maneja en Pydantic si lo usaste
        )
        return realtime_obj

    except Exception as e:
        print(f"ERROR: Unexpected error in get_realtime_physical_data for '{installation_id}': {e}")
        return None

async def get_historical_electrical_data(
    installation_id: str, start: datetime, end: datetime
) -> Optional[GroupedHistoricalElectricalData]:
    """Obtiene datos históricos eléctricos insertando fechas formateadas directamente."""
    try:
        query_api = await get_query_api()
        measurements = ["voltage", "current", "active_power", "energy", "frequency", "power_factor"]

        # Asegurar que los datetimes son timezone-aware (UTC) para formato correcto
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)

        # Formatear fechas a RFC3339 con 'Z'
        start_str = start.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        end_str = end.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        # Construir la query insertando los strings de fecha directamente
        flux_query = f'''
        from(bucket: "{settings.INFLUXDB_BUCKET}")
          |> range(start: {start_str}, stop: {end_str}) // <-- Insertar strings formateados
          |> filter(fn: (r) => contains(value: r._measurement, set: {measurements})) // Insertar lista
          |> filter(fn: (r) => r["installation_id"] == "{installation_id}") // Insertar string
          |> yield(name: "results")
        '''
        # print(f"DEBUG: Historical Electrical Query (Direct Timestamp):\n{flux_query}")

        # No se pasan 'params' para las fechas
        result = await query_api.query(query=flux_query, org=settings.INFLUXDB_ORG)

        if not result:
            print(f"WARN: No historical electrical data found for '{installation_id}' in range.")
            return None

        grouped_data: Dict[str, List[HistoricalDataPoint]] = {}
        for table in result:
            for record in table.records:
                try:
                    measurement = record.get_measurement()
                    field = record.get_field()
                    value = record.get_value()
                    time = record.get_time()
                    unit = get_unit_for_measurement(measurement, field)
                    variable_name = f"{measurement.replace('_', ' ').title()} {field.replace('_', ' ').title()}"
                    if measurement in ["frequency", "energy"]:
                         variable_name = f"{measurement.replace('_', ' ').title()}"
                    if variable_name not in grouped_data:
                        grouped_data[variable_name] = []
                    grouped_data[variable_name].append(HistoricalDataPoint(timestamp=time, value=value, unit=unit))
                except (KeyError, TypeError, AttributeError, ValueError) as e:
                    print(f"WARN: Skipping record due to parsing error in historical electrical: {e} - Record: {record.values if record else 'None'}")
                    continue

        if not grouped_data:
            print(f"WARN: Processed historical electrical data is empty for '{installation_id}'.")
            return None

        return GroupedHistoricalElectricalData(
            asset_id=installation_id, start_time=start, end_time=end, data=grouped_data
        )

    except InfluxDBError as e:
        print(f"ERROR: InfluxDB query error (electrical historical) for '{installation_id}': {e}")
        if e.response is not None and hasattr(e.response, 'data'):
             try: error_body = e.response.data.decode('utf-8'); print(f"InfluxDB Response Body: {error_body}")
             except Exception: print(f"InfluxDB Response Body (raw bytes): {e.response.data}")
        return None
    except Exception as e:
        print(f"ERROR: Unexpected error in get_historical_electrical_data for '{installation_id}': {e}")
        return None

    
    
async def get_historical_physical_data(
    installation_id: str, start: datetime, end: datetime
) -> Optional[GroupedHistoricalPhysicalData]:
    """Obtiene datos históricos físicos insertando fechas formateadas directamente."""
    try:
        query_api = await get_query_api()
        measurements = ["temperature", "humidity", "level"]

        # Formatear fechas a RFC3339 con 'Z'
        start_str = start.isoformat(timespec='microseconds').replace('+00:00', 'Z')
        end_str = end.isoformat(timespec='microseconds').replace('+00:00', 'Z')

        # Construir la query insertando todo directamente
        flux_query = f'''
        from(bucket: "{settings.INFLUXDB_BUCKET}")
          |> range(start: {start_str}, stop: {end_str}) // <-- Insertar strings formateados
          |> filter(fn: (r) => contains(value: r._measurement, set: {measurements})) // Insertar lista
          |> filter(fn: (r) => r["installation_id"] == "{installation_id}") // Insertar string
          |> yield(name: "results")
        '''
        # print(f"DEBUG: Historical Physical Query (Direct Timestamp):\n{flux_query}")

        # Ya no pasamos 'params'
        result = await query_api.query(query=flux_query, org=settings.INFLUXDB_ORG)

        # ... (resto del procesamiento de 'result' como antes) ...
        if not result: return None
        grouped_data: Dict[str, List[HistoricalDataPoint]] = {}
        for table in result:
            for record in table.records:
                try:
                    measurement = record.get_measurement(); field = record.get_field(); value = record.get_value(); time = record.get_time(); unit = get_unit_for_measurement(measurement, field)
                    variable_name = f"{measurement.replace('_', ' ').title()}"
                    if field != 'value' and value is not None: variable_name += f" {field.replace('_', ' ').title()}"
                    if variable_name not in grouped_data: grouped_data[variable_name] = []
                    grouped_data[variable_name].append(HistoricalDataPoint(timestamp=time, value=value, unit=unit))
                except (KeyError, TypeError, AttributeError, ValueError) as e:
                    print(f"WARN: Skipping record due to parsing error in historical physical: {e} - Record: {record.values if record else 'None'}")
                    continue
        if not grouped_data: return None
        return GroupedHistoricalPhysicalData(asset_id=installation_id, start_time=start, end_time=end, data=grouped_data)

    except InfluxDBError as e:
        print(f"ERROR: InfluxDB query error (physical historical) for '{installation_id}': {e}")
        if e.response is not None and hasattr(e.response, 'data'):
             try: error_body = e.response.data.decode('utf-8'); print(f"InfluxDB Response Body: {error_body}")
             except Exception: print(f"InfluxDB Response Body (raw bytes): {e.response.data}")
        return None
    except Exception as e:
        print(f"ERROR: Unexpected error in get_historical_physical_data for '{installation_id}': {e}")
        return None
    
async def get_historical_electrical_data(
    installation_id: str, start: datetime, end: datetime
) -> Optional[GroupedHistoricalElectricalData]:
    """Obtiene datos históricos eléctricos formateando la lista de measurements para Flux."""
    try:
        query_api = await get_query_api()
        measurements = ["voltage", "current", "active_power", "energy", "frequency", "power_factor"]

        # --- Formateo Correcto ---
        start_str = start.isoformat(timespec='microseconds').replace('+00:00', 'Z')
        end_str = end.isoformat(timespec='microseconds').replace('+00:00', 'Z')
        # Convertir lista de Python a string de array Flux ["item1", "item2"]
        measurements_flux_array = '[' + ', '.join(f'"{m}"' for m in measurements) + ']'
        # --------------------------

        flux_query = f'''
        from(bucket: "{settings.INFLUXDB_BUCKET}")
          |> range(start: {start_str}, stop: {end_str})
          |> filter(fn: (r) => contains(value: r._measurement, set: {measurements_flux_array})) // <-- Usar string formateado
          |> filter(fn: (r) => r["installation_id"] == "{installation_id}")
          |> yield(name: "results")
        '''
        # print(f"DEBUG: Historical Electrical Query (Formatted List):\n{flux_query}")

        result = await query_api.query(query=flux_query, org=settings.INFLUXDB_ORG)

        # ... (resto del procesamiento de 'result' sin cambios) ...
        if not result: return None
        grouped_data: Dict[str, List[HistoricalDataPoint]] = {}
        for table in result:
             for record in table.records:
                 # ... (procesamiento del record) ...
                 try:
                     measurement = record.get_measurement(); field = record.get_field(); value = record.get_value(); time = record.get_time(); unit = get_unit_for_measurement(measurement, field)
                     variable_name = f"{measurement.replace('_', ' ').title()} {field.replace('_', ' ').title()}"
                     if measurement in ["frequency", "energy"]: variable_name = f"{measurement.replace('_', ' ').title()}"
                     if variable_name not in grouped_data: grouped_data[variable_name] = []
                     grouped_data[variable_name].append(HistoricalDataPoint(timestamp=time, value=value, unit=unit))
                 except (KeyError, TypeError, AttributeError, ValueError) as e: print(f"WARN: Skipping record due to parsing error in historical electrical: {e} - Record: {record.values if record else 'None'}"); continue
        if not grouped_data: return None
        return GroupedHistoricalElectricalData(asset_id=installation_id, start_time=start, end_time=end, data=grouped_data)


    # ... (Manejo de errores sin cambios) ...
    except InfluxDBError as e: print(f"ERROR: InfluxDB query error (electrical historical) for '{installation_id}': {e}"); ...; return None
    except Exception as e: print(f"ERROR: Unexpected error in get_historical_electrical_data for '{installation_id}': {e}"); return None
   

async def get_historical_physical_data(
    installation_id: str, start: datetime, end: datetime
) -> Optional[GroupedHistoricalPhysicalData]:
    """Obtiene datos históricos físicos formateando la lista de measurements para Flux."""
    try:
        query_api = await get_query_api()
        measurements = ["temperature", "humidity", "level"]

        # --- Formateo Correcto ---
        start_str = start.isoformat(timespec='microseconds').replace('+00:00', 'Z')
        end_str = end.isoformat(timespec='microseconds').replace('+00:00', 'Z')
        # Convertir lista de Python a string de array Flux ["item1", "item2"]
        measurements_flux_array = '[' + ', '.join(f'"{m}"' for m in measurements) + ']'
        # --------------------------

        flux_query = f'''
        from(bucket: "{settings.INFLUXDB_BUCKET}")
          |> range(start: {start_str}, stop: {end_str})
          |> filter(fn: (r) => contains(value: r._measurement, set: {measurements_flux_array})) // <-- Usar string formateado
          |> filter(fn: (r) => r["installation_id"] == "{installation_id}")
          |> yield(name: "results")
        '''
        print(f"DEBUG: Historical Physical Query (Formatted List):\n{flux_query}")

        result = await query_api.query(query=flux_query, org=settings.INFLUXDB_ORG)

        # ... (resto del procesamiento de 'result' sin cambios) ...
        if not result: return None
        grouped_data: Dict[str, List[HistoricalDataPoint]] = {}
        # ... (Bucle for para procesar records) ...
        for table in result:
             for record in table.records:
                  # ... (procesamiento del record) ...
                  try:
                      measurement = record.get_measurement(); field = record.get_field(); value = record.get_value(); time = record.get_time(); unit = get_unit_for_measurement(measurement, field)
                      variable_name = f"{measurement.replace('_', ' ').title()}"
                      if field != 'value' and value is not None: variable_name += f" {field.replace('_', ' ').title()}"
                      if variable_name not in grouped_data: grouped_data[variable_name] = []
                      grouped_data[variable_name].append(HistoricalDataPoint(timestamp=time, value=value, unit=unit))
                  except (KeyError, TypeError, AttributeError, ValueError) as e: print(f"WARN: Skipping record due to parsing error in historical physical: {e} - Record: {record.values if record else 'None'}"); continue
        if not grouped_data: return None
        return GroupedHistoricalPhysicalData(asset_id=installation_id, start_time=start, end_time=end, data=grouped_data)

    # ... (Manejo de errores sin cambios) ...
    except InfluxDBError as e: print(f"ERROR: InfluxDB query error (physical historical) for '{installation_id}': {e}"); ...; return None
    except Exception as e: print(f"ERROR: Unexpected error in get_historical_physical_data for '{installation_id}': {e}"); return None


# --- Nueva Función de Escritura ---

async def write_sensor_data(installation_id: str, data: IngestData) -> bool:
    """
    Escribe un punto de datos de sensor en InfluxDB.
    Devuelve True si tiene éxito, False en caso contrario.
    """
    try:
        write_api = await get_write_api_async()

        # Crear el objeto Point de InfluxDB
        point = Point(data.measurement)
        point.tag("installation_id", installation_id) # Tag obligatorio

        # Añadir tags opcionales
        if data.tags:
            for key, value in data.tags.items():
                point.tag(key, value)

        # Añadir fields
        if not data.fields:
             print(f"WARN: Skipping write for {installation_id} - {data.measurement}: No fields provided.")
             return False # O quizás True si no es un error fatal

        for key, value in data.fields.items():
             # Intentar convertir a float si es posible, si no, mantener el tipo original
             # InfluxDB maneja int, float, bool, str
             try:
                 processed_value = float(value)
             except (ValueError, TypeError):
                 processed_value = value # Mantener como string, bool, etc.
             point.field(key, processed_value)


        # Añadir timestamp (asegúrate de que sea timezone-aware, preferiblemente UTC)
        # Pydantic v2 maneja esto bien si el string de entrada tiene formato ISO con offset
        point.time(data.timestamp)

        # Escribir el punto de forma asíncrona
        print(f"DEBUG: Writing point: {point.to_line_protocol()}") # Descomentar para depurar
        await write_api.write(bucket=settings.INFLUXDB_BUCKET, org=settings.INFLUXDB_ORG, record=point)
        # print(f"DEBUG: Point written successfully for {installation_id} - {data.measurement}")
        return True

    except Exception as e:
        print(f"ERROR: Failed to write data to InfluxDB for '{installation_id}': {e}")
        # Considera loggear el traceback completo
        # import traceback
        # traceback.print_exc()
        return False