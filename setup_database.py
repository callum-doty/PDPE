#!/usr/bin/env python3
"""
Complete Database Setup for PPM (Psychographic Prediction Machine)

Creates comprehensive SQLite schema supporting ALL APIs and features:
- Google Places API, Eventbrite, Ticketmaster, PredictHQ
- Weather API, Census Bureau, Social Media APIs
- ML predictions, feature engineering, data quality
- Performance optimization with indexes and views
"""

import sys
import os
import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional, Any

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.database import get_database


def create_sqlite_schema():
    """Create comprehensive SQLite schema for PPM application"""

    return """
    -- ========== CORE TABLES ==========
    
    -- Venues (EXPANDED from existing)
    CREATE TABLE IF NOT EXISTS venues (
        venue_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
        external_id TEXT NOT NULL,
        provider TEXT NOT NULL,
        name TEXT NOT NULL,
        description TEXT,
        category TEXT NOT NULL,
        subcategory TEXT,
        
        -- Location
        lat REAL,
        lng REAL,
        address TEXT,
        city TEXT DEFAULT 'Kansas City',
        state TEXT DEFAULT 'MO',
        zip_code TEXT,
        neighborhood TEXT,
        
        -- Contact
        phone TEXT,
        website TEXT,
        email TEXT,
        
        -- Attributes
        price_tier INTEGER CHECK(price_tier BETWEEN 1 AND 4),
        avg_rating REAL CHECK(avg_rating BETWEEN 0 AND 5),
        review_count INTEGER DEFAULT 0,
        hours_json TEXT, -- JSON string of operating hours
        amenities TEXT, -- JSON array of amenities
        
        -- Capacity & Size
        capacity INTEGER,
        square_footage INTEGER,
        
        -- Psychographic Data (JSON)
        psychographic_relevance TEXT, -- JSON with career_driven, competent, fun, social, adventurous scores
        
        -- Metadata
        data_quality_score REAL DEFAULT 0.0,
        last_verified TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        
        UNIQUE(external_id, provider)
    );

    -- Events (EXPANDED from existing)
    CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
        external_id TEXT NOT NULL,
        provider TEXT NOT NULL,
        name TEXT NOT NULL,
        description TEXT,
        category TEXT NOT NULL,
        subcategory TEXT,
        tags TEXT, -- JSON array
        
        -- Timing
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        duration_minutes INTEGER,
        timezone TEXT DEFAULT 'America/Chicago',
        
        -- Venue relationship
        venue_id TEXT,
        venue_name TEXT,
        venue_address TEXT,
        lat REAL,
        lng REAL,
        
        -- Ticketing
        ticket_price_min REAL,
        ticket_price_max REAL,
        ticket_url TEXT,
        is_free BOOLEAN DEFAULT 0,
        
        -- Attendance
        predicted_attendance INTEGER,
        actual_attendance INTEGER,
        capacity INTEGER,
        
        -- Media
        image_url TEXT,
        video_url TEXT,
        
        -- Social
        social_media_links TEXT, -- JSON
        
        -- Psychographic Data (JSON)
        psychographic_relevance TEXT,
        
        -- Impact
        impact_score REAL, -- PredictHQ impact score
        rank INTEGER, -- PredictHQ rank
        
        -- Metadata
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        
        FOREIGN KEY (venue_id) REFERENCES venues(venue_id) ON DELETE SET NULL,
        UNIQUE(external_id, provider)
    );

    -- ========== FEATURE ENGINEERING TABLES ==========

    -- Demographic data from Census Bureau API
    CREATE TABLE IF NOT EXISTS demographic_data (
        demographic_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
        
        -- Location (can be venue-specific or grid-based)
        lat REAL NOT NULL,
        lng REAL NOT NULL,
        census_tract TEXT,
        census_block_group TEXT,
        
        -- Income
        median_income REAL,
        income_per_capita REAL,
        income_quartile INTEGER, -- 1-4
        
        -- Education
        bachelor_degree_pct REAL CHECK(bachelor_degree_pct BETWEEN 0 AND 1),
        graduate_degree_pct REAL CHECK(graduate_degree_pct BETWEEN 0 AND 1),
        high_school_pct REAL CHECK(high_school_pct BETWEEN 0 AND 1),
        
        -- Age Distribution
        age_18_24_pct REAL,
        age_25_34_pct REAL,
        age_35_44_pct REAL,
        age_20_40_pct REAL, -- Target demographic
        median_age REAL,
        
        -- Occupation
        professional_occupation_pct REAL,
        management_occupation_pct REAL,
        service_occupation_pct REAL,
        
        -- Population
        total_population INTEGER,
        population_density REAL,
        
        -- Housing
        median_home_value REAL,
        rent_pct REAL,
        own_pct REAL,
        
        -- Metadata
        data_source TEXT DEFAULT 'census_bureau',
        year INTEGER,
        collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        
        UNIQUE(lat, lng, census_tract)
    );

    -- Foot traffic data from specialized APIs
    CREATE TABLE IF NOT EXISTS foot_traffic_data (
        traffic_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
        venue_id TEXT NOT NULL,
        
        -- Timestamp
        timestamp TIMESTAMP NOT NULL,
        hour_of_day INTEGER CHECK(hour_of_day BETWEEN 0 AND 23),
        day_of_week INTEGER CHECK(day_of_week BETWEEN 0 AND 6),
        
        -- Traffic Metrics
        visit_count INTEGER,
        unique_visitors INTEGER,
        repeat_visitors INTEGER,
        dwell_time_minutes REAL,
        
        -- Trends
        trend_vs_previous_hour REAL, -- % change
        trend_vs_same_time_last_week REAL,
        trend_vs_same_time_last_month REAL,
        
        -- Demographics of visitors (if available)
        visitor_age_distribution TEXT, -- JSON
        visitor_income_distribution TEXT, -- JSON
        
        -- Data source
        provider TEXT DEFAULT 'foot_traffic_api',
        collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        
        FOREIGN KEY (venue_id) REFERENCES venues(venue_id) ON DELETE CASCADE,
        UNIQUE(venue_id, timestamp)
    );

    -- Weather data from OpenWeather API
    CREATE TABLE IF NOT EXISTS weather_data (
        weather_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
        
        -- Location
        lat REAL NOT NULL,
        lng REAL NOT NULL,
        
        -- Timestamp
        timestamp TIMESTAMP NOT NULL,
        forecast_timestamp TIMESTAMP, -- For forecasts
        
        -- Conditions
        temperature_f REAL,
        feels_like_f REAL,
        humidity_pct REAL,
        precipitation_probability REAL CHECK(precipitation_probability BETWEEN 0 AND 1),
        precipitation_amount_inches REAL,
        
        -- Wind
        wind_speed_mph REAL,
        wind_direction_degrees INTEGER,
        
        -- Sky
        cloud_cover_pct REAL,
        visibility_miles REAL,
        
        -- Categorical
        conditions TEXT, -- 'clear', 'rain', 'snow', 'severe'
        is_severe BOOLEAN DEFAULT 0,
        weather_alerts TEXT, -- JSON array
        
        -- Impact scores for psychographic events
        outdoor_event_suitability_score REAL, -- 0-1
        
        -- Metadata
        provider TEXT DEFAULT 'openweather',
        is_forecast BOOLEAN DEFAULT 0,
        collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        
        UNIQUE(lat, lng, timestamp, is_forecast)
    );

    -- Traffic data from traffic APIs
    CREATE TABLE IF NOT EXISTS traffic_data (
        traffic_data_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
        venue_id TEXT,
        
        -- Location
        lat REAL NOT NULL,
        lng REAL NOT NULL,
        
        -- Timestamp
        timestamp TIMESTAMP NOT NULL,
        
        -- Traffic Metrics
        congestion_level INTEGER CHECK(congestion_level BETWEEN 0 AND 4), -- 0=free flow, 4=severe
        travel_time_minutes REAL, -- To downtown
        typical_travel_time_minutes REAL,
        travel_time_ratio REAL, -- actual/typical
        
        -- Incidents
        has_incidents BOOLEAN DEFAULT 0,
        incident_types TEXT, -- JSON array: ['accident', 'construction', 'event']
        
        -- Metadata
        provider TEXT DEFAULT 'traffic_api',
        collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        
        FOREIGN KEY (venue_id) REFERENCES venues(venue_id) ON DELETE SET NULL,
        UNIQUE(venue_id, timestamp)
    );

    -- Social sentiment data from Twitter/Facebook APIs
    CREATE TABLE IF NOT EXISTS social_sentiment_data (
        sentiment_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
        
        -- Reference (venue or event)
        venue_id TEXT,
        event_id TEXT,
        
        -- Social Platform
        platform TEXT NOT NULL, -- 'twitter', 'facebook', 'instagram'
        
        -- Timestamp
        timestamp TIMESTAMP NOT NULL,
        
        -- Engagement Metrics
        mention_count INTEGER DEFAULT 0,
        like_count INTEGER DEFAULT 0,
        share_count INTEGER DEFAULT 0,
        comment_count INTEGER DEFAULT 0,
        total_engagement INTEGER DEFAULT 0,
        
        -- Sentiment Analysis
        sentiment_score REAL CHECK(sentiment_score BETWEEN -1 AND 1), -- -1=negative, 0=neutral, 1=positive
        sentiment_category TEXT CHECK(sentiment_category IN ('positive', 'neutral', 'negative')),
        
        -- Topic Analysis
        trending_topics TEXT, -- JSON array of topics/hashtags
        keyword_mentions TEXT, -- JSON object with keyword: count
        
        -- Psychographic Indicators
        career_keywords_count INTEGER DEFAULT 0,
        fun_keywords_count INTEGER DEFAULT 0,
        social_keywords_count INTEGER DEFAULT 0,
        
        -- Metadata
        collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        
        FOREIGN KEY (venue_id) REFERENCES venues(venue_id) ON DELETE CASCADE,
        FOREIGN KEY (event_id) REFERENCES events(event_id) ON DELETE CASCADE,
        CHECK((venue_id IS NOT NULL) OR (event_id IS NOT NULL))
    );

    -- Economic indicators from economic APIs
    CREATE TABLE IF NOT EXISTS economic_indicators (
        indicator_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
        
        -- Location
        geographic_area TEXT NOT NULL, -- 'Kansas City', 'Downtown KC', etc.
        lat REAL,
        lng REAL,
        
        -- Timestamp
        date DATE NOT NULL,
        
        -- Economic Metrics
        local_business_sentiment_score REAL, -- 0-100
        new_business_openings_count INTEGER,
        business_closures_count INTEGER,
        unemployment_rate REAL,
        job_growth_rate REAL,
        
        -- Spending
        consumer_spending_index REAL,
        retail_sales_index REAL,
        restaurant_spending_index REAL,
        
        -- Real Estate
        commercial_vacancy_rate REAL,
        commercial_rent_psf REAL,
        
        -- Metadata
        data_source TEXT,
        collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        
        UNIQUE(geographic_area, date)
    );

    -- ========== ML AND PREDICTION TABLES ==========

    -- ML predictions storage
    CREATE TABLE IF NOT EXISTS ml_predictions (
        prediction_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
        venue_id TEXT NOT NULL,
        
        -- Prediction Details
        prediction_type TEXT NOT NULL, -- 'attendance', 'psychographic_match', 'popularity'
        prediction_value REAL NOT NULL CHECK(prediction_value BETWEEN 0 AND 1),
        confidence_score REAL CHECK(confidence_score BETWEEN 0 AND 1),
        confidence_interval_lower REAL,
        confidence_interval_upper REAL,
        
        -- Model Info
        model_version TEXT NOT NULL,
        model_type TEXT, -- 'xgboost', 'neural', 'ensemble', etc.
        
        -- Features Used
        features_used TEXT, -- JSON array of feature names
        feature_importance TEXT, -- JSON object with feature: importance
        
        -- Temporal Context
        prediction_for_datetime TIMESTAMP, -- When this prediction is for
        prediction_for_day_of_week INTEGER,
        prediction_for_hour INTEGER,
        
        -- Psychographic Breakdown
        career_driven_score REAL,
        competent_score REAL,
        fun_score REAL,
        
        -- Metadata
        generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        expires_at TIMESTAMP,
        
        FOREIGN KEY (venue_id) REFERENCES venues(venue_id) ON DELETE CASCADE,
        UNIQUE(venue_id, prediction_type, prediction_for_datetime)
    );

    -- ML model versions and performance tracking
    CREATE TABLE IF NOT EXISTS ml_model_versions (
        model_version_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
        version TEXT NOT NULL UNIQUE,
        model_type TEXT NOT NULL,
        
        -- Training Info
        training_samples INTEGER,
        validation_samples INTEGER,
        training_date TIMESTAMP,
        
        -- Performance Metrics
        auc_roc REAL,
        precision_at_k REAL,
        recall_at_k REAL,
        f1_score REAL,
        calibration_score REAL,
        
        -- Features
        feature_list TEXT, -- JSON array
        feature_importance TEXT, -- JSON object
        
        -- Hyperparameters
        hyperparameters TEXT, -- JSON object
        
        -- Model File
        model_file_path TEXT,
        model_size_mb REAL,
        
        -- Status
        is_active BOOLEAN DEFAULT 0,
        is_production BOOLEAN DEFAULT 0,
        
        -- Metadata
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        notes TEXT
    );

    -- ML training data storage
    CREATE TABLE IF NOT EXISTS ml_training_data (
        training_record_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
        venue_id TEXT NOT NULL,
        
        -- Features (JSON for flexibility)
        features TEXT NOT NULL, -- JSON object with all features
        
        -- Label
        label INTEGER NOT NULL CHECK(label IN (0, 1)),
        label_confidence REAL DEFAULT 1.0,
        label_source TEXT, -- 'manual', 'proxy', 'heuristic', 'synthetic'
        
        -- Context
        timestamp TIMESTAMP,
        day_of_week INTEGER,
        hour_of_day INTEGER,
        
        -- Data Quality
        feature_completeness REAL, -- % of features populated
        is_validated BOOLEAN DEFAULT 0,
        
        -- Metadata
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        used_in_model_version TEXT,
        
        FOREIGN KEY (venue_id) REFERENCES venues(venue_id) ON DELETE CASCADE
    );

    -- ========== SUPPORTING TABLES ==========

    -- API response cache to reduce API calls
    CREATE TABLE IF NOT EXISTS api_cache (
        cache_key TEXT PRIMARY KEY,
        api_source TEXT NOT NULL, -- 'google_places', 'yelp', 'census', etc.
        endpoint TEXT,
        
        -- Request Info
        request_params TEXT, -- JSON
        
        -- Response
        response_data TEXT NOT NULL, -- JSON
        response_status INTEGER,
        
        -- Cache Control
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        expires_at TIMESTAMP NOT NULL,
        access_count INTEGER DEFAULT 0,
        last_accessed TIMESTAMP,
        
        -- Metadata
        response_size_bytes INTEGER,
        is_valid BOOLEAN DEFAULT 1
    );

    -- Collection status tracking (EXPANDED from existing)
    CREATE TABLE IF NOT EXISTS collection_status (
        source_name TEXT PRIMARY KEY,
        source_type TEXT, -- 'venue_api', 'event_api', 'demographic_api', 'scraper'
        
        -- Status
        is_enabled BOOLEAN DEFAULT 1,
        collection_health_score REAL DEFAULT 0.0,
        
        -- Last Run
        last_successful_collection TIMESTAMP,
        last_attempted_collection TIMESTAMP,
        last_error_message TEXT,
        
        -- Statistics
        total_runs INTEGER DEFAULT 0,
        successful_runs INTEGER DEFAULT 0,
        error_count INTEGER DEFAULT 0,
        consecutive_errors INTEGER DEFAULT 0,
        
        -- Records
        total_records_collected INTEGER DEFAULT 0,
        records_last_run INTEGER DEFAULT 0,
        
        -- Performance
        avg_duration_seconds REAL,
        last_duration_seconds REAL,
        
        -- Rate Limiting
        requests_made_today INTEGER DEFAULT 0,
        daily_quota INTEGER,
        requests_made_this_hour INTEGER DEFAULT 0,
        hourly_quota INTEGER,
        
        -- Metadata
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status_details TEXT -- JSON
    );

    -- Geocoding cache for location optimization
    CREATE TABLE IF NOT EXISTS geocoding_cache (
        geocoding_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
        
        -- Input
        address TEXT NOT NULL,
        
        -- Output
        lat REAL NOT NULL,
        lng REAL NOT NULL,
        formatted_address TEXT,
        
        -- Address Components
        street_number TEXT,
        street_name TEXT,
        city TEXT,
        state TEXT,
        zip_code TEXT,
        country TEXT DEFAULT 'USA',
        
        -- Quality
        geocoding_quality TEXT, -- 'rooftop', 'approximate', 'geometric_center'
        confidence_score REAL,
        
        -- Metadata
        provider TEXT DEFAULT 'google_geocoding',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        access_count INTEGER DEFAULT 0,
        
        UNIQUE(address)
    );

    -- Data quality logging
    CREATE TABLE IF NOT EXISTS data_quality_log (
        log_id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
        
        -- Reference
        table_name TEXT NOT NULL,
        record_id TEXT,
        
        -- Validation
        validation_type TEXT NOT NULL, -- 'completeness', 'accuracy', 'consistency', 'timeliness'
        validation_result TEXT NOT NULL CHECK(validation_result IN ('pass', 'fail', 'warning')),
        
        -- Details
        field_name TEXT,
        error_message TEXT,
        expected_value TEXT,
        actual_value TEXT,
        
        -- Severity
        severity TEXT CHECK(severity IN ('low', 'medium', 'high', 'critical')),
        
        -- Metadata
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        validator_version TEXT
    );

    -- System configuration
    CREATE TABLE IF NOT EXISTS system_config (
        config_key TEXT PRIMARY KEY,
        config_value TEXT NOT NULL,
        config_type TEXT DEFAULT 'string', -- 'string', 'integer', 'float', 'boolean', 'json'
        description TEXT,
        is_sensitive BOOLEAN DEFAULT 0,
        last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        modified_by TEXT DEFAULT 'system'
    );
    """


def create_indexes():
    """Create comprehensive indexes for performance optimization"""

    return """
    -- ========== PERFORMANCE INDEXES ==========
    
    -- Venues
    CREATE INDEX IF NOT EXISTS idx_venues_location ON venues(lat, lng);
    CREATE INDEX IF NOT EXISTS idx_venues_category ON venues(category);
    CREATE INDEX IF NOT EXISTS idx_venues_provider ON venues(provider);
    CREATE INDEX IF NOT EXISTS idx_venues_rating ON venues(avg_rating);
    CREATE INDEX IF NOT EXISTS idx_venues_updated ON venues(updated_at);

    -- Events
    CREATE INDEX IF NOT EXISTS idx_events_time ON events(start_time, end_time);
    CREATE INDEX IF NOT EXISTS idx_events_venue ON events(venue_id);
    CREATE INDEX IF NOT EXISTS idx_events_category ON events(category);
    CREATE INDEX IF NOT EXISTS idx_events_location ON events(lat, lng);
    CREATE INDEX IF NOT EXISTS idx_events_provider ON events(provider);

    -- Demographics
    CREATE INDEX IF NOT EXISTS idx_demographics_location ON demographic_data(lat, lng);
    CREATE INDEX IF NOT EXISTS idx_demographics_income ON demographic_data(median_income);
    CREATE INDEX IF NOT EXISTS idx_demographics_education ON demographic_data(bachelor_degree_pct);

    -- Foot Traffic
    CREATE INDEX IF NOT EXISTS idx_foot_traffic_venue_time ON foot_traffic_data(venue_id, timestamp);
    CREATE INDEX IF NOT EXISTS idx_foot_traffic_hour ON foot_traffic_data(hour_of_day, day_of_week);

    -- Weather
    CREATE INDEX IF NOT EXISTS idx_weather_location_time ON weather_data(lat, lng, timestamp);
    CREATE INDEX IF NOT EXISTS idx_weather_forecast ON weather_data(is_forecast, forecast_timestamp);

    -- Traffic
    CREATE INDEX IF NOT EXISTS idx_traffic_location_time ON traffic_data(lat, lng, timestamp);
    CREATE INDEX IF NOT EXISTS idx_traffic_venue ON traffic_data(venue_id);

    -- Social Sentiment
    CREATE INDEX IF NOT EXISTS idx_social_venue ON social_sentiment_data(venue_id);
    CREATE INDEX IF NOT EXISTS idx_social_event ON social_sentiment_data(event_id);
    CREATE INDEX IF NOT EXISTS idx_social_platform_time ON social_sentiment_data(platform, timestamp);

    -- ML Predictions
    CREATE INDEX IF NOT EXISTS idx_predictions_venue ON ml_predictions(venue_id);
    CREATE INDEX IF NOT EXISTS idx_predictions_type ON ml_predictions(prediction_type);
    CREATE INDEX IF NOT EXISTS idx_predictions_datetime ON ml_predictions(prediction_for_datetime);
    CREATE INDEX IF NOT EXISTS idx_predictions_value ON ml_predictions(prediction_value);

    -- ML Training Data
    CREATE INDEX IF NOT EXISTS idx_training_venue ON ml_training_data(venue_id);
    CREATE INDEX IF NOT EXISTS idx_training_label ON ml_training_data(label);
    CREATE INDEX IF NOT EXISTS idx_training_model ON ml_training_data(used_in_model_version);

    -- API Cache
    CREATE INDEX IF NOT EXISTS idx_api_cache_source ON api_cache(api_source);
    CREATE INDEX IF NOT EXISTS idx_api_cache_expires ON api_cache(expires_at);

    -- Geocoding Cache
    CREATE INDEX IF NOT EXISTS idx_geocoding_address ON geocoding_cache(address);
    CREATE INDEX IF NOT EXISTS idx_geocoding_location ON geocoding_cache(lat, lng);

    -- Data Quality Log
    CREATE INDEX IF NOT EXISTS idx_quality_table_record ON data_quality_log(table_name, record_id);
    CREATE INDEX IF NOT EXISTS idx_quality_result ON data_quality_log(validation_result, severity);
    CREATE INDEX IF NOT EXISTS idx_quality_timestamp ON data_quality_log(timestamp);
    """


def create_views():
    """Create useful views for common queries"""

    return """
    -- ========== USEFUL VIEWS ==========
    
    -- Master Venue Data with All Enrichments
    CREATE VIEW IF NOT EXISTS vw_master_venue_data AS
    SELECT 
        v.*,
        d.median_income,
        d.bachelor_degree_pct,
        d.age_20_40_pct,
        d.professional_occupation_pct,
        p.prediction_value,
        p.confidence_score,
        p.career_driven_score,
        p.competent_score,
        p.fun_score,
        COUNT(DISTINCT e.event_id) as upcoming_events_count
    FROM venues v
    LEFT JOIN demographic_data d ON 
        ABS(v.lat - d.lat) < 0.01 AND ABS(v.lng - d.lng) < 0.01
    LEFT JOIN ml_predictions p ON 
        v.venue_id = p.venue_id AND p.prediction_type = 'psychographic_match'
    LEFT JOIN events e ON 
        v.venue_id = e.venue_id AND e.start_time > datetime('now')
    GROUP BY v.venue_id;

    -- Master Event Data with Venue Information
    CREATE VIEW IF NOT EXISTS vw_master_events_data AS
    SELECT 
        e.*,
        v.name as venue_name,
        v.lat,
        v.lng,
        v.address as venue_address,
        v.category as venue_category,
        v.avg_rating as venue_rating,
        w.temperature_f,
        w.conditions as weather_conditions,
        w.outdoor_event_suitability_score
    FROM events e
    LEFT JOIN venues v ON e.venue_id = v.venue_id
    LEFT JOIN weather_data w ON 
        ABS(CAST(strftime('%s', e.start_time) AS INTEGER) - CAST(strftime('%s', w.timestamp) AS INTEGER)) < 3600
        AND w.is_forecast = 1
    WHERE e.start_time >= datetime('now');

    -- High Value Predictions (for recommendations)
    CREATE VIEW IF NOT EXISTS vw_high_value_predictions AS
    SELECT 
        v.venue_id,
        v.name,
        v.category,
        v.lat,
        v.lng,
        v.address,
        p.prediction_value,
        p.confidence_score,
        p.career_driven_score,
        p.competent_score,
        p.fun_score,
        (p.career_driven_score * 0.35 + p.competent_score * 0.30 + p.fun_score * 0.35) as psychographic_fit_score
    FROM venues v
    INNER JOIN ml_predictions p ON v.venue_id = p.venue_id
    WHERE 
        p.prediction_type = 'psychographic_match'
        AND p.prediction_value >= 0.6
        AND p.confidence_score >= 0.7
    ORDER BY psychographic_fit_score DESC;

    -- Data Quality Dashboard
    CREATE VIEW IF NOT EXISTS vw_data_quality_summary AS
    SELECT 
        'venues' as table_name,
        COUNT(*) as total_records,
        SUM(CASE WHEN lat IS NOT NULL AND lng IS NOT NULL THEN 1 ELSE 0 END) as records_with_location,
        SUM(CASE WHEN avg_rating IS NOT NULL THEN 1 ELSE 0 END) as records_with_rating,
        SUM(CASE WHEN psychographic_relevance IS NOT NULL THEN 1 ELSE 0 END) as records_with_psychographic,
        AVG(data_quality_score) as avg_quality_score
    FROM venues
    UNION ALL
    SELECT 
        'events' as table_name,
        COUNT(*) as total_records,
        SUM(CASE WHEN lat IS NOT NULL AND lng IS NOT NULL THEN 1 ELSE 0 END) as records_with_location,
        SUM(CASE WHEN start_time IS NOT NULL THEN 1 ELSE 0 END) as records_with_time,
        SUM(CASE WHEN psychographic_relevance IS NOT NULL THEN 1 ELSE 0 END) as records_with_psychographic,
        0 as avg_quality_score
    FROM events;

    -- Collection Health Monitor
    CREATE VIEW IF NOT EXISTS vw_collection_health AS
    SELECT 
        source_name,
        source_type,
        is_enabled,
        collection_health_score,
        last_successful_collection,
        CASE 
            WHEN last_successful_collection IS NULL THEN 'Never Run'
            WHEN datetime(last_successful_collection) < datetime('now', '-24 hours') THEN 'Stale'
            WHEN consecutive_errors > 3 THEN 'Failing'
            WHEN collection_health_score < 0.5 THEN 'Unhealthy'
            ELSE 'Healthy'
        END as status,
        error_count,
        consecutive_errors,
        total_records_collected,
        records_last_run,
        requests_made_today,
        daily_quota,
        CASE 
            WHEN daily_quota > 0 THEN CAST(requests_made_today AS FLOAT) / daily_quota 
            ELSE 0 
        END as quota_used_pct
    FROM collection_status
    ORDER BY 
        CASE 
            WHEN status = 'Failing' THEN 1
            WHEN status = 'Unhealthy' THEN 2
            WHEN status = 'Stale' THEN 3
            WHEN status = 'Never Run' THEN 4
            ELSE 5
        END;
    """


def initialize_system_config():
    """Initialize system configuration with default values"""

    return """
    -- ========== SYSTEM CONFIGURATION ==========
    
    -- Insert default system configuration
    INSERT OR IGNORE INTO system_config (config_key, config_value, config_type, description) VALUES
    -- Geographic bounds
    ('geographic_bounds_north', '39.3209', 'float', 'Northern boundary of Kansas City area'),
    ('geographic_bounds_south', '38.9517', 'float', 'Southern boundary of Kansas City area'),
    ('geographic_bounds_east', '-94.3461', 'float', 'Eastern boundary of Kansas City area'),
    ('geographic_bounds_west', '-94.7417', 'float', 'Western boundary of Kansas City area'),

    -- Downtown KC coordinates
    ('downtown_kc_lat', '39.0997', 'float', 'Downtown Kansas City latitude'),
    ('downtown_kc_lng', '-94.5786', 'float', 'Downtown Kansas City longitude'),

    -- ML Configuration
    ('ml_min_training_samples', '100', 'integer', 'Minimum samples required for model training'),
    ('ml_validation_split', '0.2', 'float', 'Validation split ratio'),
    ('ml_confidence_threshold', '0.7', 'float', 'Minimum confidence for predictions'),
    ('ml_grid_resolution_meters', '500', 'integer', 'Grid resolution for heatmap predictions'),

    -- Psychographic weights
    ('psychographic_career_weight', '0.35', 'float', 'Weight for career-driven factor'),
    ('psychographic_competent_weight', '0.30', 'float', 'Weight for competent factor'),
    ('psychographic_fun_weight', '0.35', 'float', 'Weight for fun factor'),

    -- Data collection
    ('collection_venue_batch_size', '50', 'integer', 'Venues to collect per API call'),
    ('collection_event_batch_size', '100', 'integer', 'Events to collect per API call'),
    ('collection_max_retries', '3', 'integer', 'Maximum retry attempts for failed API calls'),

    -- Cache settings
    ('cache_api_response_hours', '24', 'integer', 'Hours to cache API responses'),
    ('cache_geocoding_days', '90', 'integer', 'Days to cache geocoding results'),
    ('cache_demographic_days', '365', 'integer', 'Days to cache demographic data'),

    -- Data quality
    ('quality_min_location_accuracy', '0.8', 'float', 'Minimum location data accuracy'),
    ('quality_min_completeness', '0.6', 'float', 'Minimum data completeness score'),
    ('quality_stale_data_days', '30', 'integer', 'Days before data is considered stale'),

    -- Rate limiting
    ('rate_limit_google_per_day', '100000', 'integer', 'Google Places API daily quota'),
    ('rate_limit_yelp_per_hour', '5000', 'integer', 'Yelp API hourly quota'),
    ('rate_limit_predicthq_per_day', '10000', 'integer', 'PredictHQ API daily quota'),
    ('rate_limit_weather_per_day', '1000', 'integer', 'Weather API daily quota'),
    ('rate_limit_census_per_day', '500', 'integer', 'Census API daily quota');
    """


def insert_initial_collection_sources():
    """Initialize collection_status table with all data sources"""
    sources = [
        # Venue APIs
        ("google_places", "venue_api", True, 100000, 50),
        ("yelp_fusion", "venue_api", True, 5000, None),
        ("foursquare", "venue_api", False, None, None),
        # Event APIs
        ("eventbrite", "event_api", True, 1000, None),
        ("ticketmaster", "event_api", True, 5000, None),
        ("predicthq", "event_api", True, 10000, None),
        # KC Event Scrapers
        ("tmobile_center", "scraper", True, None, None),
        ("kauffman_center", "scraper", True, None, None),
        ("starlight_theatre", "scraper", True, None, None),
        ("midland_theatre", "scraper", True, None, None),
        ("uptown_theater", "scraper", True, None, None),
        ("powerandlight", "scraper", True, None, None),
        ("westport", "scraper", True, None, None),
        ("crossroads", "scraper", True, None, None),
        ("visitkc", "scraper", True, None, None),
        ("do816", "scraper", True, None, None),
        # Enrichment APIs
        ("census_bureau", "demographic_api", True, 500, None),
        ("foot_traffic_api", "foot_traffic_api", False, None, None),
        ("openweather", "weather_api", True, 1000, None),
        ("traffic_api", "traffic_api", False, None, None),
        ("twitter_api", "social_api", False, 300, 15),
        ("facebook_graph", "social_api", False, None, None),
        ("economic_indicators_api", "economic_api", False, None, None),
    ]

    db = get_database()
    for source_name, source_type, is_enabled, daily_quota, hourly_quota in sources:
        db.execute_update(
            """
            INSERT OR IGNORE INTO collection_status 
            (source_name, source_type, is_enabled, daily_quota, hourly_quota)
            VALUES (?, ?, ?, ?, ?)
        """,
            (source_name, source_type, is_enabled, daily_quota, hourly_quota),
        )

    print(f"✅ Initialized {len(sources)} collection sources")


def create_database_triggers():
    """Create triggers for automatic timestamp updates"""
    triggers = [
        # Auto-update updated_at for venues
        """
        CREATE TRIGGER IF NOT EXISTS update_venues_timestamp 
        AFTER UPDATE ON venues
        FOR EACH ROW
        BEGIN
            UPDATE venues SET updated_at = CURRENT_TIMESTAMP 
            WHERE venue_id = NEW.venue_id;
        END;
        """,
        # Auto-update updated_at for events
        """
        CREATE TRIGGER IF NOT EXISTS update_events_timestamp 
        AFTER UPDATE ON events
        FOR EACH ROW
        BEGIN
            UPDATE events SET updated_at = CURRENT_TIMESTAMP 
            WHERE event_id = NEW.event_id;
        END;
        """,
        # Auto-update collection_status timestamp
        """
        CREATE TRIGGER IF NOT EXISTS update_collection_status_timestamp 
        AFTER UPDATE ON collection_status
        FOR EACH ROW
        BEGIN
            UPDATE collection_status SET updated_at = CURRENT_TIMESTAMP 
            WHERE source_name = NEW.source_name;
        END;
        """,
        # Increment access_count on api_cache read
        """
        CREATE TRIGGER IF NOT EXISTS increment_api_cache_access
        AFTER UPDATE OF last_accessed ON api_cache
        FOR EACH ROW
        BEGIN
            UPDATE api_cache 
            SET access_count = access_count + 1
            WHERE cache_key = NEW.cache_key;
        END;
        """,
        # Increment access_count on geocoding_cache read
        """
        CREATE TRIGGER IF NOT EXISTS increment_geocoding_cache_access
        AFTER UPDATE OF access_count ON geocoding_cache
        FOR EACH ROW
        BEGIN
            UPDATE geocoding_cache 
            SET access_count = access_count + 1
            WHERE geocoding_id = NEW.geocoding_id;
        END;
        """,
        # Auto-expire old API cache entries
        """
        CREATE TRIGGER IF NOT EXISTS auto_invalidate_expired_cache
        AFTER INSERT ON api_cache
        BEGIN
            DELETE FROM api_cache 
            WHERE expires_at < CURRENT_TIMESTAMP;
        END;
        """,
    ]

    db = get_database()
    for trigger in triggers:
        try:
            db.execute_query(trigger)
        except Exception as e:
            print(f"⚠️  Warning creating trigger: {e}")

    print(f"✅ Created {len(triggers)} database triggers")


def analyze_database_statistics():
    """Analyze and display database statistics"""
    db = get_database()
    stats = {}

    # Table row counts
    tables = [
        "venues",
        "events",
        "demographic_data",
        "foot_traffic_data",
        "weather_data",
        "traffic_data",
        "social_sentiment_data",
        "economic_indicators",
        "ml_predictions",
        "ml_model_versions",
        "ml_training_data",
        "api_cache",
        "collection_status",
        "geocoding_cache",
        "data_quality_log",
        "system_config",
    ]

    print("\n📊 Database Statistics:")
    print("=" * 60)

    for table in tables:
        try:
            result = db.execute_query(f"SELECT COUNT(*) as count FROM {table}")
            count = result[0]["count"] if result else 0
            stats[table] = count
            print(f"  {table:30s}: {count:>8,} records")
        except Exception as e:
            print(f"  {table:30s}: Error - {e}")

    # Database file size
    import os

    db_path = os.getenv("SQLITE_DB_PATH", "ppm.db")
    if os.path.exists(db_path):
        size_mb = os.path.getsize(db_path) / (1024 * 1024)
        print(f"\n  Database file size: {size_mb:.2f} MB")

    return stats


def verify_schema_integrity():
    """Verify database schema integrity"""
    print("\n🔍 Verifying Schema Integrity:")
    print("=" * 60)

    checks = []
    db = get_database()

    # Check all required tables exist
    required_tables = [
        "venues",
        "events",
        "demographic_data",
        "foot_traffic_data",
        "weather_data",
        "traffic_data",
        "social_sentiment_data",
        "economic_indicators",
        "ml_predictions",
        "ml_model_versions",
        "ml_training_data",
        "api_cache",
        "collection_status",
        "geocoding_cache",
        "data_quality_log",
        "system_config",
    ]

    existing_tables = db.execute_query(
        """
        SELECT name FROM sqlite_master 
        WHERE type='table' 
        ORDER BY name
    """
    )
    existing_table_names = [t["name"] for t in existing_tables]

    for table in required_tables:
        if table in existing_table_names:
            checks.append((table, "✅", "Table exists"))
        else:
            checks.append((table, "❌", "Table missing"))

    # Check all required views exist
    required_views = [
        "vw_master_venue_data",
        "vw_master_events_data",
        "vw_high_value_predictions",
        "vw_data_quality_summary",
        "vw_collection_health",
    ]

    existing_views = db.execute_query(
        """
        SELECT name FROM sqlite_master 
        WHERE type='view' 
        ORDER BY name
    """
    )
    existing_view_names = [v["name"] for v in existing_views]

    for view in required_views:
        if view in existing_view_names:
            checks.append((view, "✅", "View exists"))
        else:
            checks.append((view, "❌", "View missing"))

    # Check indexes
    indexes = db.execute_query(
        """
        SELECT name FROM sqlite_master 
        WHERE type='index' AND name LIKE 'idx_%'
        ORDER BY name
    """
    )
    checks.append(("Indexes", "✅", f"{len(indexes)} indexes created"))

    # Check triggers
    triggers = db.execute_query(
        """
        SELECT name FROM sqlite_master 
        WHERE type='trigger'
        ORDER BY name
    """
    )
    checks.append(("Triggers", "✅", f"{len(triggers)} triggers created"))

    # Display results
    for name, status, message in checks:
        print(f"  {status} {name:40s}: {message}")

    # Summary
    failed_checks = [c for c in checks if c[1] == "❌"]
    if failed_checks:
        print(f"\n⚠️  {len(failed_checks)} integrity check(s) failed!")
        return False
    else:
        print(f"\n✅ All integrity checks passed!")
        return True


def create_sample_data():
    """Create sample data for testing"""
    print("\n🧪 Creating Sample Data:")
    print("=" * 60)

    db = get_database()

    # Sample venue
    db.execute_update(
        """
        INSERT OR IGNORE INTO venues (
            venue_id, external_id, provider, name, category, 
            lat, lng, address, avg_rating, review_count,
            psychographic_relevance
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            "sample_venue_1",
            "sample_1",
            "manual",
            "Sample Venue - Downtown KC",
            "restaurant",
            39.0997,
            -94.5786,
            "1200 Main St, Kansas City, MO 64105",
            4.5,
            150,
            '{"career_driven": 0.7, "competent": 0.6, "fun": 0.8}',
        ),
    )

    # Sample event
    db.execute_update(
        """
        INSERT OR IGNORE INTO events (
            event_id, external_id, provider, name, category,
            start_time, venue_id, lat, lng,
            psychographic_relevance
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            "sample_event_1",
            "sample_evt_1",
            "manual",
            "Sample Networking Event",
            "business",
            "2025-11-01 18:00:00",
            "sample_venue_1",
            39.0997,
            -94.5786,
            '{"career_driven": 0.9, "competent": 0.8, "fun": 0.6}',
        ),
    )

    # Sample demographic data
    db.execute_update(
        """
        INSERT OR IGNORE INTO demographic_data (
            demographic_id, lat, lng, median_income,
            bachelor_degree_pct, age_20_40_pct,
            professional_occupation_pct, data_source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            "sample_demo_1",
            39.0997,
            -94.5786,
            75000,
            0.45,
            0.38,
            0.42,
            "census_bureau",
        ),
    )

    print("  ✅ Created sample venue")
    print("  ✅ Created sample event")
    print("  ✅ Created sample demographic data")
    print("  ✅ Sample data ready for testing")


def setup_database():
    """Set up the database with complete schema"""
    print("🔧 Setting up PPM Database...")
    print("=" * 60)

    try:
        # 1. Create all tables
        print("\n📝 Creating database schema...")
        schema = create_sqlite_schema()
        statements = [stmt.strip() for stmt in schema.split(";") if stmt.strip()]

        db = get_database()
        for i, statement in enumerate(statements, 1):
            try:
                db.execute_query(statement)
            except Exception as e:
                print(f"  ⚠️  Statement {i} warning: {e}")

        print(f"  ✅ Executed {len(statements)} schema statements")

        # 2. Create indexes
        print("\n🚀 Creating performance indexes...")
        indexes = create_indexes()
        index_statements = [stmt.strip() for stmt in indexes.split(";") if stmt.strip()]

        for statement in index_statements:
            try:
                db.execute_query(statement)
            except Exception as e:
                print(f"  ⚠️  Index warning: {e}")

        print(f"  ✅ Created {len(index_statements)} indexes")

        # 3. Create views
        print("\n👁️  Creating database views...")
        views = create_views()
        view_statements = [stmt.strip() for stmt in views.split(";") if stmt.strip()]

        for statement in view_statements:
            try:
                db.execute_query(statement)
            except Exception as e:
                print(f"  ⚠️  View warning: {e}")

        print(f"  ✅ Created {len(view_statements)} views")

        # 4. Initialize system configuration
        print("\n⚙️  Initializing system configuration...")
        config = initialize_system_config()
        config_statements = [stmt.strip() for stmt in config.split(";") if stmt.strip()]

        for statement in config_statements:
            try:
                db.execute_query(statement)
            except Exception as e:
                print(f"  ⚠️  Config warning: {e}")

        print("  ✅ System configuration initialized")

        # 5. Create triggers
        print("\n⚡ Creating database triggers...")
        create_database_triggers()

        # 6. Initialize collection sources
        print("\n📡 Initializing collection sources...")
        insert_initial_collection_sources()

        # 7. Verify schema integrity
        integrity_ok = verify_schema_integrity()

        # 8. Analyze database
        stats = analyze_database_statistics()

        # 9. Create sample data
        create_sample_data()

        # 10. Final summary
        print("\n" + "=" * 60)
        print("✅ DATABASE SETUP COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("\n📋 Next Steps:")
        print("  1. Set up API keys in .env file")
        print("  2. Run: python -m core.apis (to test API connections)")
        print("  3. Run: python app.py (to start application)")
        print("  4. Collect initial data using the UI or:")
        print(
            '     python -c "from features.venues import get_venue_service; get_venue_service().collect_all()"'
        )
        print("\n💡 Tips:")
        print("  - Check collection_status table to see data source health")
        print("  - Use vw_collection_health view for monitoring")
        print("  - Review data_quality_log for validation issues")
        print("  - Configure system_config for custom settings")

        return True

    except Exception as e:
        print(f"\n❌ Error setting up database: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = setup_database()
    exit(0 if success else 1)
