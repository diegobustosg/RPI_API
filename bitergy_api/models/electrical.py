# models/electrical.py
from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from datetime import datetime
from ..models.common import BaseVariableValue, HistoricalDataPoint

class ElectricalVariableValue(BaseVariableValue):
    pass

class PhaseData(BaseModel):
    a: Optional[ElectricalVariableValue] = Field(None, alias="Phase A")
    b: Optional[ElectricalVariableValue] = Field(None, alias="Phase B")
    c: Optional[ElectricalVariableValue] = Field(None, alias="Phase C")

class RealtimeElectricalData(BaseModel):
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    asset_id: str
    voltage: Optional[PhaseData] = None
    current: Optional[PhaseData] = None
    active_power: Optional[PhaseData] = Field(None, alias="Active Power (kW)")
    apparent_power: Optional[PhaseData] = Field(None, alias="Apparent Power (kVA)")
    reactive_power: Optional[PhaseData] = Field(None, alias="Reactive Power (kVAR)")
    power_factor: Optional[PhaseData] = Field(None, alias="Power Factor")
    frequency: Optional[ElectricalVariableValue] = None
    total_active_power: Optional[ElectricalVariableValue] = Field(None, alias="Total Active Power (kW)")
    total_energy_kwh: Optional[ElectricalVariableValue] = Field(None, alias="Total Energy (kWh)")

class GroupedHistoricalElectricalData(BaseModel):
     asset_id: str
     start_time: datetime
     end_time: datetime
     data: Dict[str, List[HistoricalDataPoint]]
