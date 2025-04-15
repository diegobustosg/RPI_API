# routers/physical.py
from fastapi import APIRouter, HTTPException, Query, Path
from typing import List, Optional
from datetime import datetime


from ..models.physical import RealtimePhysicalData, GroupedHistoricalPhysicalData # Correcto
from ..models.common import HistoricalDataPoint # Puede que no sea necesario si no lo usas directamente aquí

from ..services.data_provider import get_realtime_physical_data, get_historical_physical_data

router = APIRouter(
    prefix="/installations/{installation_id}/physical",
    tags=["Physical Data"]
)

@router.get("/realtime", response_model=RealtimePhysicalData, summary="Get Real-time Physical Data")
async def read_realtime_physical(installation_id: str = Path(..., description="Unique ID of the installation")):
    """Fetches the most recent physical measurements."""
    data = await get_realtime_physical_data(installation_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Physical realtime data not found")
    return data

@router.get("/historical", response_model=GroupedHistoricalPhysicalData, summary="Get Historical Physical Data")
async def read_historical_physical(
    installation_id: str = Path(..., description="Unique ID of the installation"),
    start_time: datetime = Query(..., description="Start timestamp ISO 8601"),
    end_time: datetime = Query(..., description="End timestamp ISO 8601")
):
    """Fetches historical physical measurements within a time range."""
    if start_time >= end_time:
        raise HTTPException(status_code=400, detail="End time must be after start time")
    data = await get_historical_physical_data(installation_id, start_time, end_time)
    if data is None or not data.data:
         raise HTTPException(status_code=404, detail="Physical historical data not found for the specified criteria")
    return data
