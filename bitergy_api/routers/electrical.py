# routers/electrical.py
from fastapi import APIRouter, HTTPException, Query, Path
from typing import List, Optional
from datetime import datetime
from ..models.electrical import RealtimeElectricalData, GroupedHistoricalElectricalData # Correcto
from ..models.common import HistoricalDataPoint



from ..services.data_provider import get_realtime_electrical_data, get_historical_electrical_data

router = APIRouter(
    prefix="/installations/{installation_id}/electrical",
    tags=["Electrical Data"]
)

@router.get("/realtime", response_model=RealtimeElectricalData, summary="Get Real-time Electrical Data")
async def read_realtime_electrical(installation_id: str = Path(..., description="Unique ID of the installation")):
    """Fetches the most recent electrical measurements."""
    data = await get_realtime_electrical_data(installation_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Electrical realtime data not found")
    return data

@router.get("/historical", response_model=GroupedHistoricalElectricalData, summary="Get Historical Electrical Data")
async def read_historical_electrical(
    installation_id: str = Path(..., description="Unique ID of the installation"),
    start_time: datetime = Query(..., description="Start timestamp ISO 8601"),
    end_time: datetime = Query(..., description="End timestamp ISO 8601")
):
    """Fetches historical electrical measurements within a time range."""
    if start_time >= end_time:
        raise HTTPException(status_code=400, detail="End time must be after start time")
    data = await get_historical_electrical_data(installation_id, start_time, end_time)
    if data is None or not data.data :
         raise HTTPException(status_code=404, detail="Electrical historical data not found for the specified criteria")
    return data
