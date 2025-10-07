"""
PPM - Psychographic Prediction Machine
Simplified Streamlit Application

Single entry point that uses the consolidated feature services directly.
No orchestration layer - clean, simple, direct feature calls.
"""

import streamlit as st
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

# Import simplified feature services
try:
    from features.venues import get_venue_service
    from features.events import get_event_service
    from features.predictions import get_prediction_service
    from features.maps import get_map_service
    from core.database import get_database

    # Initialize services
    venues = get_venue_service()
    events = get_event_service()
    predictions = get_prediction_service()
    maps = get_map_service()
    db = get_database()

except ImportError as e:
    st.error(f"Import error: {e}")
    st.error(
        "Please ensure all dependencies are installed and you're running from the PPM root directory"
    )
    st.stop()


def main():
    """Main Streamlit application"""
    st.set_page_config(
        page_title="PPM - Where Should I Go Tonight?",
        page_icon="🎯",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("🎯 Where Should I Go Tonight?")
    st.markdown("*Psychographic Prediction Machine - Simplified Architecture*")

    # Sidebar controls
    with st.sidebar:
        st.header("🚀 Data Collection")

        # Individual collection buttons
        if st.button("🏢 Collect Venues", help="Collect venue data from all sources"):
            collect_venues()

        if st.button("🎭 Collect Events", help="Collect event data from KC sources"):
            collect_events()

        if st.button(
            "🤖 Generate Predictions", help="Generate ML predictions for venues"
        ):
            generate_predictions()

        # Comprehensive collection
        st.divider()
        if st.button(
            "🚀 Collect All Data",
            help="Collect venues, events, and generate predictions",
        ):
            collect_all_data()

        st.header("🗺️ Visualization")

        # Map type selection
        map_type = st.selectbox(
            "Map Type",
            ["Combined View", "Venues Only", "Events Only", "Prediction Heatmap"],
            help="Choose what to display on the map",
        )

        # Date range for events
        st.subheader("📅 Date Range")
        start_date = st.date_input("Start Date", datetime.now())
        end_date = st.date_input("End Date", datetime.now() + timedelta(days=30))

    # Main content area
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("🗺️ Interactive Map")
        display_map(map_type, start_date, end_date)

    with col2:
        st.subheader("📊 Data Summary")
        display_data_summary()

        st.subheader("🔍 System Status")
        display_system_status()


def collect_venues():
    """Collect venue data using simplified venue service"""
    with st.spinner("🏢 Collecting venues from all sources..."):
        try:
            result = venues.collect_all()

            if result.success:
                st.success(
                    f"✅ Venue collection completed!\n"
                    f"📊 {result.data} venues collected\n"
                    f"⏱️ {result.message}"
                )
            else:
                st.error(f"❌ Venue collection failed: {result.error}")

        except Exception as e:
            st.error(f"❌ Error during venue collection: {e}")


def collect_events():
    """Collect event data using simplified event service"""
    with st.spinner("🎭 Collecting events from KC sources..."):
        try:
            result = events.collect_all()

            if result.success:
                st.success(
                    f"✅ Event collection completed!\n"
                    f"📊 {result.data} events collected\n"
                    f"⏱️ {result.message}"
                )
            else:
                st.error(f"❌ Event collection failed: {result.error}")

        except Exception as e:
            st.error(f"❌ Error during event collection: {e}")


def generate_predictions():
    """Generate ML predictions using simplified prediction service"""
    with st.spinner("🤖 Training model and generating predictions..."):
        try:
            # Train model first
            train_result = predictions.train_model()

            if not train_result.success:
                st.warning(f"⚠️ Model training had issues: {train_result.error_message}")

            # Generate heatmap predictions
            heatmap_predictions = predictions.generate_heatmap_predictions()

            if heatmap_predictions:
                st.success(
                    f"✅ ML predictions completed!\n"
                    f"📊 {len(heatmap_predictions)} predictions generated\n"
                    f"🎯 Model: {train_result.model_version}\n"
                    f"📈 Validation Score: {train_result.validation_score:.3f}"
                )
            else:
                st.warning(
                    "⚠️ No predictions generated - check if venues have sufficient data"
                )

        except Exception as e:
            st.error(f"❌ Error during prediction generation: {e}")


def collect_all_data():
    """Collect all data and generate predictions"""
    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        # Step 1: Collect venues
        status_text.text("🏢 Collecting venues...")
        progress_bar.progress(25)
        venue_result = venues.collect_all()

        # Step 2: Collect events
        status_text.text("🎭 Collecting events...")
        progress_bar.progress(50)
        event_result = events.collect_all()

        # Step 3: Generate predictions
        status_text.text("🤖 Generating predictions...")
        progress_bar.progress(75)
        train_result = predictions.train_model()
        heatmap_predictions = predictions.generate_heatmap_predictions()

        # Complete
        progress_bar.progress(100)
        status_text.text("✅ Complete!")

        # Show results
        total_venues = venue_result.data if venue_result.success else 0
        total_events = event_result.data if event_result.success else 0
        total_predictions = len(heatmap_predictions) if heatmap_predictions else 0

        st.success(
            f"🎉 **Comprehensive data collection completed!**\n\n"
            f"📊 **Results:**\n"
            f"• Venues: {total_venues}\n"
            f"• Events: {total_events}\n"
            f"• Predictions: {total_predictions}\n\n"
            f"🎯 **Model Performance:** {train_result.validation_score:.3f}"
        )

        # Show any issues
        issues = []
        if not venue_result.success:
            issues.append(f"Venues: {venue_result.error}")
        if not event_result.success:
            issues.append(f"Events: {event_result.error}")
        if not train_result.success:
            issues.append(f"Predictions: {train_result.error_message}")

        if issues:
            with st.expander("⚠️ Issues Encountered"):
                for issue in issues:
                    st.warning(issue)

    except Exception as e:
        st.error(f"❌ Error during comprehensive data collection: {e}")
    finally:
        progress_bar.empty()
        status_text.empty()


def display_map(map_type: str, start_date, end_date):
    """Display interactive map based on selected type"""
    try:
        if map_type == "Venues Only":
            result = maps.create_venue_map(output_path="temp_venue_map.html")
        elif map_type == "Events Only":
            event_filters = {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "has_location": True,
            }
            result = maps.create_event_map(
                event_filters, output_path="temp_event_map.html"
            )
        elif map_type == "Prediction Heatmap":
            result = maps.create_prediction_heatmap(
                output_path="temp_prediction_map.html"
            )
        else:  # Combined View
            result = maps.create_combined_map(
                include_venues=True,
                include_events=True,
                include_predictions=True,
                event_filters={
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "has_location": True,
                },
                output_path="temp_combined_map.html",
            )

        if result.success:
            # Display the map
            with open(result.data, "r") as f:
                map_html = f.read()
            st.components.v1.html(map_html, height=500)

            # Option to open in browser
            if st.button("🌐 Open in Browser"):
                maps.open_map_in_browser(result.data)
                st.info("Map opened in your default browser!")
        else:
            st.error(f"❌ Map creation failed: {result.error}")
            st.info("💡 Try collecting data first using the sidebar buttons")

    except Exception as e:
        st.error(f"❌ Error creating map: {e}")
        st.info("💡 Try collecting data first using the sidebar buttons")


def display_data_summary():
    """Display data summary statistics"""
    try:
        summary = db.get_data_summary()

        if "error" in summary:
            st.error(f"Error loading data summary: {summary['error']}")
            return

        # Main metrics
        col1, col2 = st.columns(2)

        with col1:
            st.metric(
                "Total Venues",
                summary.get("total_venues", 0),
                delta=f"{summary.get('recent_venues', 0)} recent",
            )

        with col2:
            st.metric(
                "Total Events",
                summary.get("total_events", 0),
                delta=f"{summary.get('recent_events', 0)} recent",
            )

        # Additional metrics
        st.metric("ML Predictions", summary.get("total_predictions", 0))

        # Data quality indicator
        location_completeness = summary.get("location_completeness", 0)
        if location_completeness > 0.8:
            st.success(f"📍 Location Data: {location_completeness:.1%} complete")
        elif location_completeness > 0.5:
            st.warning(f"📍 Location Data: {location_completeness:.1%} complete")
        else:
            st.error(f"📍 Location Data: {location_completeness:.1%} complete")

    except Exception as e:
        st.error(f"Error loading data summary: {e}")


def display_system_status():
    """Display system status and health"""
    try:
        # Get prediction summary
        pred_summary = predictions.get_prediction_summary()

        if "error" in pred_summary:
            st.warning("⚠️ ML System: Not Ready")
        else:
            model_exists = pred_summary.get("model_exists", False)
            if model_exists:
                avg_confidence = pred_summary.get("avg_confidence", 0)
                st.success(f"🤖 ML System: Ready ({avg_confidence:.1%} avg confidence)")
            else:
                st.info("🤖 ML System: No trained model")

        # Database status
        try:
            summary = db.get_data_summary()
            if "error" not in summary:
                st.success("💾 Database: Connected")
            else:
                st.error("💾 Database: Error")
        except:
            st.error("💾 Database: Connection Failed")

        # Show last update time
        if pred_summary.get("last_updated"):
            st.caption(f"Last updated: {pred_summary['last_updated'][:19]}")

    except Exception as e:
        st.error(f"Error checking system status: {e}")


if __name__ == "__main__":
    main()
