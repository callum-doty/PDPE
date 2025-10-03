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
    from shared.orchestration.master_data_orchestrator import MasterDataOrchestrator
    from fix_streamlit_event_discrepancy import refresh_master_data_tables

    # Use the available database connection function
    get_db_conn = get_database_connection
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
        collect_venues = st.button("🏢 Collect Venue Data", key="collect_venues")
        collect_events = st.button("🎭 Collect Event Data", key="collect_events")
        collect_all_data = st.button("🚀 Collect All Data", key="collect_all_data")
        collect_priority_data = st.button(
            "🎯 Collect Priority Data", key="collect_priority_data"
        )

        # Prediction options
        st.subheader("ML Predictions")
        run_predictions = st.button("🤖 Generate Predictions", key="run_predictions")

        # Orchestrator options
        st.subheader("Master Data Orchestrator")
        show_health_report = st.button(
            "📊 Show Health Report", key="show_health_report"
        )

        # Master data refresh
        st.subheader("Data Management")
        refresh_master_data = st.button(
            "🔄 Refresh Master Data", key="refresh_master_data"
        )

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

    # Handle refresh master data
    if refresh_master_data:
        with st.spinner("Refreshing master data tables..."):
            refresh_master_data_tables()
            st.success(
                "Master data refreshed! The map and counts should now be consistent."
            )
            st.rerun()

    # Handle button clicks
    if collect_venues:
        with st.spinner("Collecting venue data..."):
            collect_venue_data()

    if collect_events:
        with st.spinner("Collecting event data..."):
            collect_event_data()

    if collect_all_data:
        with st.spinner("Collecting all data sources..."):
            collect_all_data_orchestrated()

    if collect_priority_data:
        with st.spinner("Collecting priority data sources..."):
            collect_priority_data_orchestrated()

    if run_predictions:
        with st.spinner("Generating ML predictions..."):
            generate_predictions()

    if show_health_report:
        with st.spinner("Generating health report..."):
            show_data_health_report()


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
                SELECT name, lat, lng, category, avg_rating
                FROM venues 
                WHERE lat IS NOT NULL AND lng IS NOT NULL
                LIMIT 100
            """
            )

            for venue in venues:
                folium.Marker(
                    location=[venue["lat"], venue["lng"]],
                    popup=f"{venue['name']}<br>Category: {venue['category']}<br>Rating: {venue['avg_rating']}",
                    icon=folium.Icon(color="blue", icon="building"),
                ).add_to(m)

    except Exception as e:
        st.warning(f"Could not load venues: {e}")


def add_events_to_map(m: folium.Map, start_date, end_date):
    """Add event markers to map"""
    try:
        with get_database_connection() as db:
            # Use the master_events_data view which has the correct columns
            events = db.execute_query(
                """
                SELECT name, lat, lng, start_time, venue_name
                FROM master_events_data 
                WHERE lat IS NOT NULL AND lng IS NOT NULL
                AND start_time BETWEEN ? AND ?
                LIMIT 100
            """,
                (start_date, end_date),
            )

            for event in events:
                folium.Marker(
                    location=[event["lat"], event["lng"]],
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
            # Use master data tables for consistency with map display
            venue_count = db.execute_query(
                "SELECT COUNT(*) as count FROM master_venue_data"
            )[0]["count"]
            event_count = db.execute_query(
                "SELECT COUNT(*) as count FROM master_events_data"
            )[0]["count"]

            # Also show raw counts for comparison
            raw_venue_count = db.execute_query("SELECT COUNT(*) as count FROM venues")[
                0
            ]["count"]
            raw_event_count = db.execute_query("SELECT COUNT(*) as count FROM events")[
                0
            ]["count"]

            st.metric("Mappable Venues", venue_count, delta=f"{raw_venue_count} total")
            st.metric("Mappable Events", event_count, delta=f"{raw_event_count} total")

    except Exception as e:
        st.metric("Mappable Venues", "N/A")
        st.metric("Mappable Events", "N/A")


def display_recent_activity():
    """Display recent data collection activity"""
    try:
        with get_database_connection() as db:
            recent_venues = db.execute_query(
                """
                SELECT name, created_at FROM venues 
                ORDER BY created_at DESC LIMIT 5
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
        results = collector.collect_all_venues()

        # Handle list of results from collect_all_venues
        if isinstance(results, list):
            successful_results = [r for r in results if r.success]
            total_venues = sum(r.venues_collected for r in successful_results)
            total_events = sum(r.events_collected for r in successful_results)

            if successful_results:
                st.success(
                    f"Collected {total_venues} venues and {total_events} events from {len(successful_results)} sources"
                )
            else:
                error_messages = [r.error_message for r in results if r.error_message]
                st.error(
                    f"Venue collection failed: {'; '.join(error_messages) if error_messages else 'Unknown error'}"
                )

        # Handle single result from collect_data
        elif hasattr(results, "success"):
            if results.success:
                st.success(
                    f"Collected {results.venues_collected} venues and {results.events_collected} events"
                )
            else:
                st.error(
                    f"Venue collection failed: {results.error_message or 'Unknown error'}"
                )
        else:
            st.error("Unexpected result format from venue collector")

    except Exception as e:
        st.error(f"Error collecting venues: {e}")


def collect_event_data():
    """Collect event data using event scrapers"""
    try:
        scraper = KCEventScraper()
        result = scraper.collect_data()

        # Handle dataclass result properly
        if hasattr(result, "success"):
            if result.success:
                st.success(f"Collected {result.events_collected} events")
            else:
                st.error(
                    f"Event collection failed: {result.error_message or 'Unknown error'}"
                )
        # Fallback for dictionary-style results
        elif isinstance(result, dict):
            if result.get("success"):
                st.success(f"Collected {result.get('events_collected', 0)} events")
            else:
                st.error(
                    f"Event collection failed: {result.get('error', 'Unknown error')}"
                )
        else:
            st.error("Unexpected result format from event scraper")

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


def collect_all_data_orchestrated():
    """Collect all data using the Master Data Orchestrator"""
    try:
        orchestrator = MasterDataOrchestrator()
        status = orchestrator.collect_all_data()

        if status.health_score > 0.5:
            st.success(
                f"✅ Data collection completed!\n"
                f"- Total venues: {status.total_venues}\n"
                f"- Total events: {status.total_events}\n"
                f"- Health score: {status.health_score:.2f}\n"
                f"- Data completeness: {status.data_completeness:.1%}"
            )
        else:
            st.warning(
                f"⚠️ Data collection completed with issues:\n"
                f"- Health score: {status.health_score:.2f}\n"
                f"- Data completeness: {status.data_completeness:.1%}"
            )

        # Show individual collection results
        with st.expander("Collection Details"):
            for result in status.collection_results:
                status_icon = "✅" if result.success else "❌"
                st.write(
                    f"{status_icon} **{result.source_name}**: "
                    f"{result.venues_collected} items in {result.duration_seconds:.1f}s"
                )
                if result.error_message:
                    st.error(f"Error: {result.error_message}")

    except Exception as e:
        st.error(f"Error in orchestrated data collection: {e}")


def collect_priority_data_orchestrated():
    """Collect priority data using the Master Data Orchestrator"""
    try:
        orchestrator = MasterDataOrchestrator()
        results = orchestrator.collect_priority_data()

        successful_results = [r for r in results if r.success]
        total_items = sum(r.venues_collected for r in successful_results)

        if successful_results:
            st.success(
                f"✅ Priority data collection completed!\n"
                f"- Sources processed: {len(successful_results)}/{len(results)}\n"
                f"- Total items collected: {total_items}"
            )
        else:
            st.error("❌ Priority data collection failed")

        # Show individual results
        with st.expander("Priority Collection Details"):
            for result in results:
                status_icon = "✅" if result.success else "❌"
                st.write(
                    f"{status_icon} **{result.source_name}**: "
                    f"{result.venues_collected} items in {result.duration_seconds:.1f}s"
                )
                if result.error_message:
                    st.error(f"Error: {result.error_message}")

    except Exception as e:
        st.error(f"Error in priority data collection: {e}")


def show_data_health_report():
    """Show comprehensive data health report"""
    try:
        orchestrator = MasterDataOrchestrator()
        health_report = orchestrator.get_data_health_report()

        if "error" in health_report:
            st.error(f"Error generating health report: {health_report['error']}")
            return

        # Overall health score
        health_score = health_report.get("overall_health_score", 0)
        if health_score >= 0.8:
            st.success(f"🟢 System Health: Excellent ({health_score:.1%})")
        elif health_score >= 0.6:
            st.warning(f"🟡 System Health: Good ({health_score:.1%})")
        else:
            st.error(f"🔴 System Health: Needs Attention ({health_score:.1%})")

        # Venue statistics
        venue_stats = health_report.get("venue_statistics", {})
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                "Total Venues",
                venue_stats.get("total_venues", 0),
            )

        with col2:
            st.metric(
                "Geocoded Venues",
                venue_stats.get("geocoded_venues", 0),
                delta=f"{venue_stats.get('geocoding_completeness', 0):.1%} complete",
            )

        with col3:
            st.metric(
                "Psychographic Data",
                venue_stats.get("venues_with_psychographic", 0),
                delta=f"{venue_stats.get('psychographic_completeness', 0):.1%} complete",
            )

        # Event statistics
        event_stats = health_report.get("event_statistics", {})
        st.metric("Total Events", event_stats.get("total_events", 0))

        # Recent collections
        recent_collections = health_report.get("recent_collections", 0)
        st.metric("Recent Collections", recent_collections)

        # Detailed breakdown
        with st.expander("Detailed Health Metrics"):
            st.json(health_report)

    except Exception as e:
        st.error(f"Error showing health report: {e}")


if __name__ == "__main__":
    main()
