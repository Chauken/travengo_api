from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Optional
import uuid
import sys
import os

# Add the project root to the Python path to fix import issues
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))) 

from api.models import TravelPackage, TravelPackagesRequest, TravelPackageHistory, TravelPackageHistoryResponse
from api.chat_models import ChatHistoryResponse
from api.repository import TravelPackageRepository

# Initialize FastAPI app
app = FastAPI(
    title="Travengo API",
    description="Simple Hello World API for Travengo travel application",
    version="0.1.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Simple Hello World route
@app.get("/")
def read_root() -> Dict[str, str]:
    """
    Root endpoint that returns a simple Hello World message
    """
    return {"message": "Hello World from Travengo API!"}

# Simple endpoint with path parameter
@app.get("/hello/{name}")
def hello_name(name: str) -> Dict[str, str]:
    """
    Greet a user by name
    """
    return {"message": f"Hello, {name}! Welcome to Travengo API"}

# Simple endpoint with query parameter
@app.get("/travel")
def travel_destination(destination: str = "World") -> Dict[str, str]:
    """
    Get a travel greeting for a destination
    """
    return {"message": f"Welcome to {destination}! Travengo helps you explore the best places."}

# Dependency to get repository instance
def get_repository():
    return TravelPackageRepository()

# Travel packages endpoints
@app.post("/travel-packages", response_model=Dict[str, str])
def save_travel_packages(
    request: TravelPackagesRequest,
    user_id: Optional[str] = Query(None),
    session_id: Optional[str] = Query(None),
    repository: TravelPackageRepository = Depends(get_repository)
):
    """
    Save travel packages to history
    
    This endpoint takes a list of travel packages and saves them to the history.
    It returns the ID of the history entry.
    """
    # Use sessionId from request if provided, otherwise use query param or generate new one
    if request.sessionId:
        session_id = request.sessionId
    elif not session_id:
        session_id = str(uuid.uuid4())
    
    # Save packages to history
    history_id = repository.save_travel_packages(
        travel_packages=request.travel_packages,
        user_id=user_id,
        session_id=session_id
    )
    
    return {
        "history_id": history_id,
        "session_id": session_id,
        "message": "Travel packages saved successfully"
    }

@app.get("/travel-packages/history", response_model=TravelPackageHistoryResponse)
def get_travel_packages_history(
    limit: int = Query(10, ge=1, le=100),
    user_id: Optional[str] = Query(None),
    repository: TravelPackageRepository = Depends(get_repository)
):
    """
    Get travel package history
    
    This endpoint returns the history of travel packages.
    It can be filtered by user_id and limited to a specific number of entries.
    """
    history = repository.get_history(limit=limit, user_id=user_id)
    
    # Return None for current_packages if no history is found
    if not history:
        return {
            "history_entries": [],
            "current_packages": None
        }
    
    # Return the response with history entries and current packages
    return {
        "history_entries": history,
        "current_packages": None  # Set to None to avoid validation errors
    }

@app.get("/travel-packages/history/session/{session_id}", response_model=TravelPackageHistoryResponse)
def get_travel_packages_history_by_session(
    session_id: str,
    limit: int = Query(10, ge=1, le=100),
    repository: TravelPackageRepository = Depends(get_repository)
):
    """
    Get travel package history by session ID
    
    This endpoint returns the travel package history for a specific session ID.
    """
    history = repository.get_history(limit=limit, session_id=session_id)
    
    # Return empty history and None for current_packages if no history is found
    if not history:
        return {
            "history_entries": [],
            "current_packages": None
        }
    
    # Return the response with history entries and None for current_packages
    return {
        "history_entries": history,
        "current_packages": None  # Set to None to avoid validation errors
    }

@app.get("/travel-packages/history/{history_id}", response_model=Dict)
def get_travel_packages_by_id(
    history_id: str,
    repository: TravelPackageRepository = Depends(get_repository)
):
    """
    Get a specific travel package history entry by ID
    
    This endpoint returns a specific history entry by ID.
    """
    entry = repository.get_history_by_id(history_id)
    
    if not entry:
        raise HTTPException(status_code=404, detail=f"History entry with ID {history_id} not found")
        
    return entry

@app.delete("/travel-packages/history/{history_id}", response_model=Dict[str, str])
def delete_travel_packages_history(
    history_id: str,
    repository: TravelPackageRepository = Depends(get_repository)
):
    """
    Delete a travel package history entry by ID
    
    This endpoint deletes a specific history entry by ID.
    """
    deleted = repository.delete_history(history_id)
    
    if not deleted:
        raise HTTPException(status_code=404, detail=f"History entry with ID {history_id} not found")
        
    return {"message": f"History entry with ID {history_id} deleted successfully"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
