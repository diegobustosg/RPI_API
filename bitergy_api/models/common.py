# models/common.py
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from ..core.enums import SeverityLevel

class BaseVariableValue(BaseModel):
    value: Optional[float] = None
    unit: str
    severity: SeverityLevel = SeverityLevel.UNKNOWN

class HistoricalDataPoint(BaseModel):
    timestamp: datetime
    value: float
    unit: str

class Thresholds(BaseModel):
    critical_low: Optional[float] = None
    low: Optional[float] = None
    high: Optional[float] = None
    critical_high: Optional[float] = None

class InstallationThresholds(BaseModel):
    # Eléctricos
    voltage: Optional[Thresholds] = None
    current: Optional[Thresholds] = None
    frequency: Optional[Thresholds] = None
    power_factor: Optional[Thresholds] = None
    # Físicos
    temperature: Optional[Thresholds] = None
    humidity: Optional[Thresholds] = None
    level: Optional[Thresholds] = None
