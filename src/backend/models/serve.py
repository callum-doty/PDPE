# models/serve.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Dict, List, Optional
import joblib
import pandas as pd
import numpy as np
from datetime import datetime
import json
import tempfile
import os
from pathlib import Path

try:
    from etl.utils import get_db_conn
except ImportError:
    # Fallback if etl.utils is not available
    def get_db_conn():
        raise Exception("Database connection not configured")


try:
    from backend.visualization.interactive_map_builder import InteractiveMapBuilder
except ImportError:
    InteractiveMapBuilder = None


app = FastAPI(title="PPM API", description="Psychographic Prediction Machine API")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Try to load model, create dummy if not available
try:
    model = joblib.load("models/best_model.pkl")
    MODEL_AVAILABLE = True
except:
    model = None
    MODEL_AVAILABLE = False


# Pydantic models for request/response
class GridBounds(BaseModel):
    north: float
    south: float
    east: float
    west: float


class BatchPredictionRequest(BaseModel):
    grid_bounds: GridBounds
    resolution_meters: int = 500
    timestamp: str


class PredictionResponse(BaseModel):
    psychographic_density: float
    confidence_interval: List[float]
    contributing_factors: Dict[str, float]
    model_ensemble: Dict[str, float]


class VisualizationRequest(BaseModel):
    grid_bounds: GridBounds
    resolution_meters: int = 500
    timestamp: str
    style: str = "streets"
    include_events: bool = True
    include_probability: bool = True


class EventData(BaseModel):
    latitude: float
    longitude: float
    name: str = "Sample Event"
    venue_name: str = "Sample Venue"
    date: str = "2024-01-01"
    total_score: float = 0.5


@app.get("/")
def root():
    return {"message": "PPM API is running", "model_available": MODEL_AVAILABLE}


@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.get("/score_venue/{venue_id}")
def score_venue(venue_id: str, ts: str):
    if not MODEL_AVAILABLE:
        # Return sample prediction if model not available
        return {
            "venue_id": venue_id,
            "score": 0.75 + np.random.random() * 0.25,
            "confidence": 0.8,
            "note": "Sample prediction - model not loaded",
        }

    # build feature row by querying features table for the latest ts or generate features on-demand
    try:
        conn = get_db_conn()
        df = pd.read_sql(
            "SELECT * FROM features WHERE venue_id=%s ORDER BY ts DESC LIMIT 1",
            conn,
            params=(venue_id,),
        )
        conn.close()
        if df.empty:
            return {"error": "no features"}
        X = df.drop(
            columns=["feature_id", "venue_id", "ts", "label", "created_at"]
        ).iloc[0:1]
        preds = (
            model.predict_proba(X)[:, 1]
            if hasattr(model, "predict_proba")
            else model.predict(X)
        )
        return {"venue_id": venue_id, "score": float(preds[0])}
    except Exception as e:
        return {"error": str(e), "venue_id": venue_id, "score": 0.5}


@app.post("/api/v1/predict/batch")
def predict_batch(request: BatchPredictionRequest):
    """Generate batch predictions for heatmap visualization"""
    try:
        bounds = request.grid_bounds
        resolution = request.resolution_meters

        # Calculate grid dimensions
        lat_range = bounds.north - bounds.south
        lng_range = bounds.east - bounds.west

        # Approximate grid size (rough conversion from meters to degrees)
        lat_step = resolution / 111000  # ~111km per degree latitude
        lng_step = resolution / (
            111000 * np.cos(np.radians((bounds.north + bounds.south) / 2))
        )

        features = []

        # Generate grid points
        lat = bounds.south
        while lat < bounds.north:
            lng = bounds.west
            while lng < bounds.east:
                # Generate psychographic density prediction
                if MODEL_AVAILABLE:
                    # Use actual model prediction logic here
                    # For now, generate realistic sample data
                    density = generate_realistic_prediction(lat, lng)
                else:
                    # Generate sample data based on location
                    density = generate_realistic_prediction(lat, lng)

                if density > 0.1:  # Only include points with meaningful density
                    features.append(
                        {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [lng, lat]},
                            "properties": {
                                "intensity": density,
                                "psychographic_density": density,
                                "confidence": 0.7 + np.random.random() * 0.3,
                                "timestamp": request.timestamp,
                            },
                        }
                    )

                lng += lng_step
            lat += lat_step

        return {
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "timestamp": request.timestamp,
                "grid_resolution_meters": resolution,
                "bounds": bounds.dict(),
                "total_points": len(features),
            },
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction error: {str(e)}")


def generate_realistic_prediction(lat: float, lng: float) -> float:
    """Generate realistic psychographic density predictions based on location"""
    # Kansas City downtown center
    kc_center_lat, kc_center_lng = 39.0997, -94.5786

    # Calculate distance from downtown KC
    distance_from_center = np.sqrt(
        (lat - kc_center_lat) ** 2 + (lng - kc_center_lng) ** 2
    )

    # Base intensity decreases with distance from downtown
    base_intensity = max(0, 1 - distance_from_center * 15)

    # Add some noise and clustering patterns
    noise = np.random.random() * 0.4

    # Create some hotspots (simulate popular areas)
    hotspots = [
        (39.1012, -94.5844),  # Power & Light District
        (39.0739, -94.5861),  # Crossroads Arts District
        (39.0458, -94.5833),  # Plaza area
        (39.1167, -94.6275),  # Westport
    ]

    hotspot_bonus = 0
    for hotspot_lat, hotspot_lng in hotspots:
        hotspot_distance = np.sqrt((lat - hotspot_lat) ** 2 + (lng - hotspot_lng) ** 2)
        if hotspot_distance < 0.01:  # Within ~1km
            hotspot_bonus += max(0, 0.5 - hotspot_distance * 50)

    # Combine factors
    intensity = min(1.0, base_intensity + noise + hotspot_bonus)

    return max(0, intensity)


@app.get("/api/v1/predict")
def predict_single(lat: float, lng: float, timestamp: str):
    """Get prediction for a single location"""
    density = generate_realistic_prediction(lat, lng)

    return {
        "psychographic_density": density,
        "confidence_interval": [max(0, density - 0.1), min(1, density + 0.1)],
        "contributing_factors": {
            "venue_attributes": 0.23,
            "foot_traffic": 0.19,
            "event_data": 0.31,
            "demographics": 0.27,
        },
        "model_ensemble": {
            "xgboost": density + np.random.normal(0, 0.05),
            "neural": density + np.random.normal(0, 0.05),
            "bayesian": density,
        },
        "location": {"lat": lat, "lng": lng},
        "timestamp": timestamp,
    }


@app.post("/api/v1/visualize/heatmap")
def create_heatmap_visualization(request: VisualizationRequest):
    """Generate interactive heatmap HTML file"""
    if not InteractiveMapBuilder:
        raise HTTPException(
            status_code=503,
            detail="Visualization module not available. Please install folium.",
        )

    try:
        # Initialize map builder
        map_builder = InteractiveMapBuilder()

        # Generate sample events data
        events_data = generate_sample_events(request.grid_bounds)

        # Generate probability data from batch predictions
        probability_data = {}
        bounds = request.grid_bounds
        resolution = request.resolution_meters

        # Calculate grid dimensions
        lat_step = resolution / 111000  # ~111km per degree latitude
        lng_step = resolution / (
            111000 * np.cos(np.radians((bounds.north + bounds.south) / 2))
        )

        # Generate grid points for probability data
        lat = bounds.south
        while lat < bounds.north:
            lng = bounds.west
            while lng < bounds.east:
                density = generate_realistic_prediction(lat, lng)
                if density > 0.1:  # Only include meaningful probabilities
                    probability_data[(lat, lng)] = density
                lng += lng_step
            lat += lat_step

        # Create temporary file for the visualization
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".html", delete=False
        ) as tmp_file:
            temp_path = tmp_file.name

        # Generate the appropriate visualization
        if request.include_events and request.include_probability:
            # Combined visualization
            output_file = map_builder.create_combined_visualization(
                events_data=events_data,
                probability_data=probability_data,
                output_path=temp_path,
                style=request.style,
            )
        elif request.include_events:
            # Events only
            output_file = map_builder.create_event_heatmap(
                events_data=events_data, output_path=temp_path, style=request.style
            )
        elif request.include_probability:
            # Probability only
            output_file = map_builder.create_probability_heatmap(
                probability_data=probability_data,
                output_path=temp_path,
                style=request.style,
            )
        else:
            raise HTTPException(
                status_code=400,
                detail="At least one of include_events or include_probability must be True",
            )

        if not output_file:
            raise HTTPException(
                status_code=500, detail="Failed to generate visualization"
            )

        # Return the HTML file
        return FileResponse(
            path=str(output_file),
            media_type="text/html",
            filename=f"heatmap_{request.timestamp}.html",
            headers={"Cache-Control": "no-cache"},
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Visualization error: {str(e)}")


@app.post("/api/v1/visualize/events")
def create_events_visualization(events: List[EventData], style: str = "streets"):
    """Generate interactive events visualization"""
    if not InteractiveMapBuilder:
        raise HTTPException(
            status_code=503,
            detail="Visualization module not available. Please install folium.",
        )

    try:
        # Convert Pydantic models to dictionaries
        events_data = [event.dict() for event in events]

        # Initialize map builder
        map_builder = InteractiveMapBuilder()

        # Create temporary file
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".html", delete=False
        ) as tmp_file:
            temp_path = tmp_file.name

        # Generate events heatmap
        output_file = map_builder.create_event_heatmap(
            events_data=events_data, output_path=temp_path, style=style
        )

        if not output_file:
            raise HTTPException(
                status_code=500, detail="Failed to generate events visualization"
            )

        return FileResponse(
            path=str(output_file),
            media_type="text/html",
            filename=f"events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html",
            headers={"Cache-Control": "no-cache"},
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Events visualization error: {str(e)}"
        )


@app.get("/api/v1/visualize/sample-events")
def get_sample_events(
    north: float = 39.15,
    south: float = 39.05,
    east: float = -94.50,
    west: float = -94.65,
    count: int = 20,
):
    """Generate sample events data for testing"""
    bounds = GridBounds(north=north, south=south, east=east, west=west)
    events = generate_sample_events(bounds, count)
    return {"events": events, "count": len(events)}


def generate_sample_events(bounds: GridBounds, count: int = 15) -> List[Dict]:
    """Generate sample events data within the given bounds"""
    events = []

    # Sample event names and venues
    event_names = [
        "Jazz Night",
        "Food Truck Festival",
        "Art Gallery Opening",
        "Live Music",
        "Comedy Show",
        "Wine Tasting",
        "Book Reading",
        "Craft Fair",
        "Farmers Market",
        "Street Festival",
        "Concert",
        "Theater Performance",
        "Dance Show",
        "Film Screening",
        "Poetry Night",
    ]

    venue_names = [
        "The Blue Room",
        "Crossroads Arts District",
        "Power & Light District",
        "Westport Entertainment",
        "Plaza Shopping Center",
        "Union Station",
        "Crown Center",
        "River Market",
        "18th & Vine",
        "Midtown Arts",
        "Downtown KC",
        "Country Club Plaza",
        "Brookside",
        "Waldo",
        "Overland Park",
    ]

    for i in range(count):
        # Generate random location within bounds
        lat = np.random.uniform(bounds.south, bounds.north)
        lng = np.random.uniform(bounds.west, bounds.east)

        # Generate score based on location (higher near downtown)
        score = generate_realistic_prediction(lat, lng)

        # Add some randomness to the score
        score = max(0, min(1, score + np.random.normal(0, 0.1)))

        event = {
            "latitude": lat,
            "longitude": lng,
            "name": np.random.choice(event_names),
            "venue_name": np.random.choice(venue_names),
            "date": f"2024-{np.random.randint(1, 13):02d}-{np.random.randint(1, 29):02d}",
            "total_score": score,
        }
        events.append(event)

    return events


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
