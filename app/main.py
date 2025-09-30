"""
PPM Personal App - Streamlit Interface
Main entry point for the Psychographic Prediction Machine personal tool.
"""

import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# Add project root to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from features.venues.scrapers.kc_event_scraper import KCEventScraper
    from features.venues.collectors.venue_collector import VenueCollector
    from features.ml.models.inference.predictor import MLPredictor
    from features.visualization.builders.interactive_map_builder import (
        InteractiveMapBuilder,
    )
    from shared.database.connection import get_database_connection
except ImportError as e:
    st.error(f"Import error: {e}")
    st.error("Please ensure you're running from the PPM root directory")
    st.stop()


def main():
    """Main Streamlit app"""
    st.set_page_config(
        page_title="PPM - Psychographic Prediction Machine",
        page_icon="🎯",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("🎯 Psychographic Prediction Machine")
    st.markdown("*Where should I go tonight?*")

    # Sidebar for controls
    with st.sidebar:
        st.header("Controls")

        # Data collection options
        st.subheader("Data Collection")
        collect_venues = st.button("🏢 Collect Venue Data")
        collect_events = st.button("🎭 Collect Event Data")

        # Prediction options
        st.subheader("ML Predictions")
        run_predictions = st.button("🤖 Generate Predictions")

        # Map options
        st.subheader("Visualization")
        map_type = st.selectbox(
            "Map Type", ["Venues Only", "Events Only", "Combined", "Predictions"]
        )

        # Date range for events
        st.subheader("Date Range")
        start_date = st.date_input("Start Date", datetime.now())
        end_date = st.date_input("End Date", datetime.now() + timedelta(days=7))

    # Main content area
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Interactive Map")

        # Create and display map
        map_data = create_map(map_type, start_date, end_date)
        if map_data:
            st_folium(map_data, width=700, height=500)
        else:
            st.info("Click 'Collect Data' to populate the map")

    with col2:
        st.subheader("Data Summary")

        # Display data statistics
        display_data_summary()

        # Recent activity
        st.subheader("Recent Activity")
        display_recent_activity()

    # Handle button clicks
    if collect_venues:
        with st.spinner("Collecting venue data..."):
            collect_venue_data()

    if collect_events:
        with st.spinner("Collecting event data..."):
            collect_event_data()

    if run_predictions:
        with st.spinner("Generating ML predictions..."):
            generate_predictions()


def create_map(map_type: str, start_date, end_date) -> Optional[folium.Map]:
    """Create folium map based on selected type"""
    try:
        # Initialize map centered on Kansas City
        m = folium.Map(
            location=[39.0997, -94.5786], zoom_start=11, tiles="OpenStreetMap"
        )

        if map_type == "Venues Only":
            add_venues_to_map(m)
        elif map_type == "Events Only":
            add_events_to_map(m, start_date, end_date)
        elif map_type == "Combined":
            add_venues_to_map(m)
            add_events_to_map(m, start_date, end_date)
        elif map_type == "Predictions":
            add_predictions_to_map(m)

        return m

    except Exception as e:
        st.error(f"Error creating map: {e}")
        return None


def add_venues_to_map(m: folium.Map):
    """Add venue markers to map"""
    try:
        with get_database_connection() as db:
            venues = db.execute_query(
                """
                SELECT name, latitude, longitude, category, avg_rating
                FROM venues 
                WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                LIMIT 100
            """
            )

            for venue in venues:
                folium.Marker(
                    location=[venue["latitude"], venue["longitude"]],
                    popup=f"{venue['name']}<br>Category: {venue['category']}<br>Rating: {venue['avg_rating']}",
                    icon=folium.Icon(color="blue", icon="building"),
                ).add_to(m)

    except Exception as e:
        st.warning(f"Could not load venues: {e}")


def add_events_to_map(m: folium.Map, start_date, end_date):
    """Add event markers to map"""
    try:
        with get_database_connection() as db:
            events = db.execute_query(
                """
                SELECT name, latitude, longitude, start_time, venue_name
                FROM events 
                WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                AND start_time BETWEEN ? AND ?
                LIMIT 100
            """,
                (start_date, end_date),
            )

            for event in events:
                folium.Marker(
                    location=[event["latitude"], event["longitude"]],
                    popup=f"{event['name']}<br>Venue: {event['venue_name']}<br>Time: {event['start_time']}",
                    icon=folium.Icon(color="red", icon="calendar"),
                ).add_to(m)

    except Exception as e:
        st.warning(f"Could not load events: {e}")


def add_predictions_to_map(m: folium.Map):
    """Add ML prediction heatmap to map"""
    try:
        # This would integrate with the ML predictor
        st.info("ML predictions feature coming soon!")
    except Exception as e:
        st.warning(f"Could not load predictions: {e}")


def display_data_summary():
    """Display summary statistics"""
    try:
        with get_database_connection() as db:
            venue_count = db.execute_query("SELECT COUNT(*) as count FROM venues")[0][
                "count"
            ]
            event_count = db.execute_query("SELECT COUNT(*) as count FROM events")[0][
                "count"
            ]

            st.metric("Total Venues", venue_count)
            st.metric("Total Events", event_count)

    except Exception as e:
        st.metric("Total Venues", "N/A")
        st.metric("Total Events", "N/A")


def display_recent_activity():
    """Display recent data collection activity"""
    try:
        with get_database_connection() as db:
            recent_venues = db.execute_query(
                """
                SELECT name, scraped_at FROM venues 
                ORDER BY scraped_at DESC LIMIT 5
            """
            )

            if recent_venues:
                st.write("Recent Venues:")
                for venue in recent_venues:
                    st.write(f"• {venue['name']}")
            else:
                st.write("No recent venue data")

    except Exception as e:
        st.write("Could not load recent activity")


def collect_venue_data():
    """Collect venue data using venue collectors"""
    try:
        collector = VenueCollector()
        result = collector.collect_all_venues()

        if result.get("success"):
            st.success(f"Collected {result.get('venues_collected', 0)} venues")
        else:
            st.error(f"Venue collection failed: {result.get('error', 'Unknown error')}")

    except Exception as e:
        st.error(f"Error collecting venues: {e}")


def collect_event_data():
    """Collect event data using event scrapers"""
    try:
        scraper = KCEventScraper()
        result = scraper.collect_data()

        if result.get("success"):
            st.success(f"Collected {result.get('events_collected', 0)} events")
        else:
            st.error(f"Event collection failed: {result.get('error', 'Unknown error')}")

    except Exception as e:
        st.error(f"Error collecting events: {e}")


def generate_predictions():
    """Generate ML predictions"""
    try:
        predictor = MLPredictor()
        predictions = predictor.generate_venue_predictions()

        if predictions:
            st.success(f"Generated predictions for {len(predictions)} venues")
        else:
            st.warning("No predictions generated")

    except Exception as e:
        st.error(f"Error generating predictions: {e}")


if __name__ == "__main__":
    main()
