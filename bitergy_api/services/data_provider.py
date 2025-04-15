# services/data_provider.py
import os
from datetime import datetime
from typing import Dict, List, Optional

# Importar cliente y configuración
from ..core.db_client import get_query_api
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

# --- Funciones Históricas (Sin cambios significativos respecto a la versión anterior, solo corrección de import) ---
async def get_historical_electrical_data(
    installation_id: str, start: datetime, end: datetime
) -> Optional[GroupedHistoricalElectricalData]:
    """Obtiene datos históricos eléctricos."""
    try:
        query_api = await get_query_api()
        start_str = start.isoformat(timespec='microseconds') + "Z"
        end_str = end.isoformat(timespec='microseconds') + "Z"
        measurements = ["voltage", "current", "active_power", "energy", "frequency", "power_factor"]

        flux_query = f'''
        from(bucket: "{settings.INFLUXDB_BUCKET}")
          |> range(start: {start_str}, stop: {end_str})
          |> filter(fn: (r) => r["_measurement"] == "{'" or r["_measurement"] == "'.join(measurements)}")
          |> filter(fn: (r) => r["installation_id"] == "{installation_id}")
          // Opcional: Agregación si el rango es muy grande
          // |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
          |> yield(name: "results")
        '''
        # print(f"DEBUG: Historical Electrical Query:\n{flux_query}")
        result = await query_api.query(query=flux_query, org=settings.INFLUXDB_ORG)

        if not result: return None

        grouped_data: Dict[str, List[HistoricalDataPoint]] = {}
        for table in result:
            for record in table.records:
                try:
                    measurement = record.get_measurement()
                    field = record.get_field()
                    value = record.get_value()
                    time = record.get_time()
                    unit = get_unit_for_measurement(measurement, field)
                    # Crear nombre de variable (p.ej., "Voltage Phase A")
                    variable_name = f"{measurement.replace('_', ' ').title()} {field.replace('_', ' ').title()}"
                    # Corregir para variables sin 'phase'
                    if measurement in ["frequency", "energy"]:
                        variable_name = f"{measurement.replace('_', ' ').title()}"
                    # Asegurar que la clave existe antes de añadir
                    if variable_name not in grouped_data:
                        grouped_data[variable_name] = []
                    grouped_data[variable_name].append(
                        HistoricalDataPoint(timestamp=time, value=value, unit=unit)
                    )
                except (KeyError, TypeError, AttributeError, ValueError) as e: # Captura más errores posibles
                    print(f"WARN: Skipping record due to parsing error in historical electrical: {e} - Record: {record.values if record else 'None'}")
                    continue

        if not grouped_data: return None

        return GroupedHistoricalElectricalData(
            asset_id=installation_id, start_time=start, end_time=end, data=grouped_data
        )
    except Exception as e:
        print(f"ERROR: Unexpected error in get_historical_electrical_data for '{installation_id}': {e}")
        return None

async def get_historical_physical_data(
    installation_id: str, start: datetime, end: datetime
) -> Optional[GroupedHistoricalPhysicalData]:
    """Obtiene datos históricos físicos."""
    try:
        query_api = await get_query_api()
        start_str = start.isoformat(timespec='microseconds') + "Z"
        end_str = end.isoformat(timespec='microseconds') + "Z"
        measurements = ["temperature", "humidity", "level"] # Ajusta a tus measurements

        flux_query = f'''
        from(bucket: "{settings.INFLUXDB_BUCKET}")
          |> range(start: {start_str}, stop: {end_str})
          |> filter(fn: (r) => r["_measurement"] == "{'" or r["_measurement"] == "'.join(measurements)}")
          |> filter(fn: (r) => r["installation_id"] == "{installation_id}")
          // |> aggregateWindow(every: 1m, fn: mean, createEmpty: false) // Opcional
          |> yield(name: "results")
        '''
        # print(f"DEBUG: Historical Physical Query:\n{flux_query}")
        result = await query_api.query(query=flux_query, org=settings.INFLUXDB_ORG)

        if not result: return None

        grouped_data: Dict[str, List[HistoricalDataPoint]] = {}
        for table in result:
            for record in table.records:
                try:
                    measurement = record.get_measurement()
                    field = record.get_field()
                    value = record.get_value()
                    time = record.get_time()
                    unit = get_unit_for_measurement(measurement, field)
                    # Crear nombre de variable, considerando el 'field' si es relevante
                    variable_name = f"{measurement.replace('_', ' ').title()}"
                    # Añadir nombre del campo si no es el genérico 'value' y hay datos
                    if field != 'value' and value is not None:
                         variable_name += f" {field.replace('_', ' ').title()}"
                         # Opcional: Añadir tag de localización si existe
                         # location = record.values.get("location")
                         # if location: variable_name += f" ({location})"

                    if variable_name not in grouped_data:
                        grouped_data[variable_name] = []
                    grouped_data[variable_name].append(
                        HistoricalDataPoint(timestamp=time, value=value, unit=unit)
                    )
                except (KeyError, TypeError, AttributeError, ValueError) as e:
                    print(f"WARN: Skipping record due to parsing error in historical physical: {e} - Record: {record.values if record else 'None'}")
                    continue

        if not grouped_data: return None

        return GroupedHistoricalPhysicalData(
            asset_id=installation_id, start_time=start, end_time=end, data=grouped_data
        )
    except Exception as e:
        print(f"ERROR: Unexpected error in get_historical_physical_data for '{installation_id}': {e}")
        return None
