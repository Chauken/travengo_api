from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime


class Flight(BaseModel):
    id: Optional[str] = None
    airline: Optional[str] = None
    flight_number: Optional[str] = None
    departure_airport: Optional[str] = None
    arrival_airport: Optional[str] = None
    departure_time: Optional[str] = None
    arrival_time: Optional[str] = None
    departure_datetime: Optional[str] = None
    arrival_datetime: Optional[str] = None
    price_economy: Optional[float] = None
    price_business: Optional[float] = None
    
    class Config:
        populate_by_name = True


class Hotel(BaseModel):
    code: Optional[str] = None
    name: Optional[str] = None
    rating: Optional[int] = None  # Changed from str to int
    city: Optional[str] = None
    country: Optional[str] = None
    address: Optional[str] = None
    price_per_night: Optional[float] = None


class Activity(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    category: Optional[str] = None
    description: Optional[str] = None
    price: Optional[float] = None
    duration_hours: Optional[float] = None
    rating: Optional[float] = None


class TravelPackage(BaseModel):
    package_id: Optional[int] = None
    outbound_flight: Optional[Flight] = None
    inbound_flight: Optional[Flight] = None
    hotel: Optional[Hotel] = None
    activities: Optional[List[Activity]] = None
    total_package_price: Optional[float] = None


class TravelPackagesRequest(BaseModel):
    sessionId: Optional[str] = None
    message: Optional[str] = None
    travel_packages: Optional[List[TravelPackage]] = None


class TravelPackageHistory(BaseModel):
    history_id: Optional[str] = Field(default_factory=lambda: datetime.now().strftime("%Y%m%d%H%M%S"))
    timestamp: Optional[str] = Field(default_factory=lambda: datetime.now().isoformat())
    travel_packages: Optional[List[TravelPackage]] = None
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    message: Optional[str] = None


class TravelPackageHistoryResponse(BaseModel):
    history_entries: Optional[List[Dict[str, Any]]] = None
    current_packages: Optional[List[TravelPackage]] = None
