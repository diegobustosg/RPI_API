# models/physical.py
from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from datetime import datetime
from ..models.common import BaseVariableValue, HistoricalDataPoint

class PhysicalVariableValue(BaseVariableValue):
    sensor_location: Optional[str] = None
    pass

class RealtimePhysicalData(BaseModel):
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    asset_id: str
    temperature: Optional[PhysicalVariableValue] = None
    humidity: Optional[PhysicalVariableValue] = None
    level: Optional[PhysicalVariableValue] = Field(None, alias="Level") # Ejemplo

class GroupedHistoricalPhysicalData(BaseModel):
     asset_id: str
     start_time: datetime
     end_time: datetime
     data: Dict[str, List[HistoricalDataPoint]]
