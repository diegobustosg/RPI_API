# core/enums.py
from enum import Enum

class SeverityLevel(str, Enum):
    """Define los posibles niveles de severidad para una medición."""
    NORMAL = "Normal"
    LOW = "Low"
    HIGH = "High"
    CRITICAL_LOW = "Critical Low"
    CRITICAL_HIGH = "Critical High"
    UNKNOWN = "Unknown"
