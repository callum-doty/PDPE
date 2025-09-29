-- create extension (run once)
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Venues table (enhanced)
CREATE TABLE venues (
  venue_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  external_id TEXT,
  provider TEXT,
  name TEXT,
  category TEXT,
  subcategory TEXT,
  price_tier SMALLINT,
  avg_rating FLOAT,
  review_count INT,
  geo GEOGRAPHY(POINT, 4326),
  lat DOUBLE PRECISION,
  lng DOUBLE PRECISION,
  address TEXT,
  phone TEXT,
  website TEXT,
  hours_json JSONB,                    -- operating hours
  amenities TEXT[],                    -- array of amenities
  psychographic_relevance JSONB,       -- career_driven, competent, fun scores
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP DEFAULT now()
);

-- Events table (enhanced)
CREATE TABLE events (
  event_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  external_id TEXT,
  provider TEXT,
  name TEXT,
  description TEXT,
  category TEXT,
  subcategory TEXT,
  tags TEXT[],                         -- event tags for psychographic matching
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  venue_id UUID REFERENCES venues(venue_id),
  ticket_price_min FLOAT,
  ticket_price_max FLOAT,
  predicted_attendance INT,
  actual_attendance INT,
  psychographic_relevance JSONB,       -- career_driven, competent, fun scores
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP DEFAULT now(),
  UNIQUE(external_id, provider)
);

-- Foot traffic / visits (time-series per venue)
CREATE TABLE venue_traffic (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  venue_id UUID REFERENCES venues(venue_id),
  ts TIMESTAMP,
  visitors_count INT,
  median_dwell_seconds INT,
  visitors_change_24h FLOAT,           -- percentage change from 24h ago
  visitors_change_7d FLOAT,            -- percentage change from 7d ago
  peak_hour_ratio FLOAT,               -- ratio to daily peak hour
  source TEXT,
  created_at TIMESTAMP DEFAULT now()
);

-- Traffic congestion data
CREATE TABLE traffic_data (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  venue_id UUID REFERENCES venues(venue_id),
  ts TIMESTAMP,
  congestion_score FLOAT,              -- 0-1 scale
  travel_time_to_downtown FLOAT,       -- minutes
  travel_time_index FLOAT,             -- ratio to free-flow time
  source TEXT,
  created_at TIMESTAMP DEFAULT now()
);

-- Weather data
CREATE TABLE weather_data (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  ts TIMESTAMP,
  lat DOUBLE PRECISION,
  lng DOUBLE PRECISION,
  temperature_f FLOAT,
  feels_like_f FLOAT,
  humidity FLOAT,
  pressure FLOAT,
  wind_speed_mph FLOAT,
  wind_direction FLOAT,
  weather_condition TEXT,
  weather_description TEXT,
  rain_probability FLOAT,
  precipitation_mm FLOAT,
  uv_index FLOAT,
  visibility FLOAT,
  created_at TIMESTAMP DEFAULT now(),
  UNIQUE(ts, lat, lng)
);

-- Social sentiment data
CREATE TABLE social_sentiment (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  venue_id UUID REFERENCES venues(venue_id),
  event_id UUID REFERENCES events(event_id),
  ts TIMESTAMP,
  platform TEXT,                      -- 'twitter', 'facebook', 'instagram'
  mention_count INT,
  positive_sentiment FLOAT,
  negative_sentiment FLOAT,
  neutral_sentiment FLOAT,
  engagement_score FLOAT,
  psychographic_keywords TEXT[],       -- career, professional, fun, etc.
  created_at TIMESTAMP DEFAULT now()
);

-- Economic indicators
CREATE TABLE economic_data (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  ts TIMESTAMP,
  geographic_area TEXT,                -- zip code, tract, or city
  unemployment_rate FLOAT,
  median_household_income FLOAT,
  business_openings INT,
  business_closures INT,
  consumer_confidence FLOAT,
  local_spending_index FLOAT,
  created_at TIMESTAMP DEFAULT now()
);

-- Census / demographic by tract (enhanced)
CREATE TABLE demographics (
  tract_id TEXT PRIMARY KEY,
  geometry GEOGRAPHY(POLYGON,4326),
  median_income FLOAT,
  median_income_z FLOAT,               -- z-score normalized
  pct_bachelors FLOAT,
  pct_graduate FLOAT,
  pct_age_20_30 FLOAT,
  pct_age_30_40 FLOAT,
  pct_age_20_40 FLOAT,
  population INT,
  population_density FLOAT,
  pct_professional_occupation FLOAT,
  pct_management_occupation FLOAT,
  updated_at TIMESTAMP DEFAULT now()
);

-- Custom psychographic layers
CREATE TABLE psychographic_layers (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  layer_name TEXT,                     -- 'college_density', 'spending_propensity'
  lat DOUBLE PRECISION,
  lng DOUBLE PRECISION,
  grid_cell_id TEXT,                   -- for spatial indexing
  score FLOAT,                         -- 0-1 normalized score
  confidence FLOAT,                    -- confidence in score
  metadata JSONB,                      -- additional layer-specific data
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP DEFAULT now()
);

-- Manual labeling data
CREATE TABLE manual_labels (
  label_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  venue_id UUID REFERENCES venues(venue_id),
  ts TIMESTAMP,
  lat DOUBLE PRECISION,
  lng DOUBLE PRECISION,
  psychographic_density FLOAT,        -- 0-1 ground truth score
  labeler_id TEXT,
  confidence SMALLINT,                 -- 1-5 labeler confidence
  notes TEXT,
  validation_status TEXT,              -- 'pending', 'approved', 'rejected'
  created_at TIMESTAMP DEFAULT now()
);

-- Proxy labels from external sources
CREATE TABLE proxy_labels (
  label_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  venue_id UUID REFERENCES venues(venue_id),
  event_id UUID REFERENCES events(event_id),
  ts TIMESTAMP,
  source TEXT,                         -- 'meetup', 'linkedin', 'eventbrite'
  psychographic_density FLOAT,        -- inferred 0-1 score
  confidence FLOAT,                    -- algorithmic confidence
  source_data JSONB,                   -- raw data from source
  created_at TIMESTAMP DEFAULT now()
);

-- Comprehensive feature table for ML
CREATE TABLE features (
  feature_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  venue_id UUID REFERENCES venues(venue_id),
  ts TIMESTAMP,
  
  -- Venue Demographics (from census tract)
  median_income_z FLOAT,
  education_bachelors_pct FLOAT,
  education_graduate_pct FLOAT,
  age_20_30_pct FLOAT,
  age_30_40_pct FLOAT,
  professional_occupation_pct FLOAT,
  population_density FLOAT,
  
  -- Venue Attributes
  venue_type TEXT,
  venue_subcategory TEXT,
  avg_rating FLOAT,
  review_count INT,
  price_tier SMALLINT,
  psychographic_venue_score FLOAT,
  
  -- Foot Traffic Features
  foot_traffic_hourly FLOAT,
  foot_traffic_change_24h FLOAT,
  foot_traffic_change_7d FLOAT,
  dwell_time_median FLOAT,
  peak_hour_ratio FLOAT,
  
  -- Traffic Features
  traffic_congestion_score FLOAT,
  travel_time_downtown FLOAT,
  travel_time_index FLOAT,
  
  -- Event Features
  event_predicted_attendance FLOAT,
  event_category TEXT,
  event_psychographic_score FLOAT,
  event_ticket_price_avg FLOAT,
  events_nearby_count INT,
  
  -- Weather Features
  temp_fahrenheit FLOAT,
  feels_like_f FLOAT,
  rain_prob FLOAT,
  humidity FLOAT,
  weather_condition TEXT,
  uv_index FLOAT,
  
  -- Economic Features
  unemployment_rate FLOAT,
  business_health_score FLOAT,
  consumer_confidence FLOAT,
  local_spending_index FLOAT,
  
  -- Social Sentiment Features
  social_mention_count FLOAT,
  social_sentiment_score FLOAT,
  social_engagement_score FLOAT,
  psychographic_keyword_count INT,
  
  -- Custom Layer Features
  college_layer_score FLOAT,
  spending_propensity_score FLOAT,
  
  -- Temporal Features
  hour_sin FLOAT,
  hour_cos FLOAT,
  day_of_week SMALLINT,
  is_weekend BOOLEAN,
  is_holiday BOOLEAN,
  
  -- Spatial Features
  distance_to_downtown FLOAT,
  neighborhood_type TEXT,
  venue_density_500m FLOAT,
  
  -- Historical Features
  venue_popularity_trend FLOAT,
  event_success_rate FLOAT,
  
  -- Target Labels
  psychographic_density FLOAT,        -- target variable (0-1)
  label_source TEXT,                   -- 'manual', 'proxy', 'heuristic'
  label_confidence FLOAT,              -- confidence in label
  
  created_at TIMESTAMP DEFAULT now()
);

-- Model predictions and serving
CREATE TABLE predictions (
  prediction_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  venue_id UUID REFERENCES venues(venue_id),
  ts TIMESTAMP,
  lat DOUBLE PRECISION,
  lng DOUBLE PRECISION,
  psychographic_density FLOAT,
  confidence_lower FLOAT,
  confidence_upper FLOAT,
  model_version TEXT,
  model_ensemble JSONB,               -- individual model predictions
  contributing_factors JSONB,         -- feature importance breakdown
  created_at TIMESTAMP DEFAULT now()
);

-- Collection status tracking (for master data orchestrator)
CREATE TABLE collection_status (
  source_name TEXT PRIMARY KEY,
  last_successful_collection TIMESTAMP,
  last_attempted_collection TIMESTAMP,
  collection_health_score FLOAT DEFAULT 0.0,
  error_count INT DEFAULT 0,
  status_details JSONB,
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP DEFAULT now()
);

-- Master Data Aggregation View (Single Source of Truth)
-- This materialized view consolidates all data sources for daily refresh
DROP MATERIALIZED VIEW IF EXISTS master_venue_data;
CREATE MATERIALIZED VIEW master_venue_data AS
SELECT 
    v.venue_id,
    v.external_id,
    v.provider,
    v.name,
    v.category,
    v.subcategory,
    v.lat,
    v.lng,
    v.address,
    v.phone,
    v.website,
    v.psychographic_relevance,
    v.created_at,
    v.updated_at,
    
    -- Latest social sentiment data (priority data)
    ss.mention_count,
    ss.positive_sentiment,
    ss.negative_sentiment,
    ss.neutral_sentiment,
    ss.engagement_score,
    ss.psychographic_keywords,
    ss.ts as social_last_updated,
    
    -- Latest ML predictions (priority data)  
    p.psychographic_density,
    p.confidence_lower,
    p.confidence_upper,
    p.model_version,
    p.contributing_factors,
    p.ts as predictions_last_updated,
    
    -- Latest foot traffic data
    vt.visitors_count,
    vt.median_dwell_seconds,
    vt.visitors_change_24h,
    vt.visitors_change_7d,
    vt.peak_hour_ratio,
    vt.ts as traffic_last_updated,
    
    -- Latest traffic congestion data
    td.congestion_score,
    td.travel_time_to_downtown,
    td.travel_time_index,
    td.ts as congestion_last_updated,
    
    -- Nearest weather data (within 5km)
    wd.temperature_f,
    wd.feels_like_f,
    wd.humidity,
    wd.rain_probability,
    wd.weather_condition,
    wd.weather_description,
    wd.ts as weather_last_updated,
    
    -- Economic data for the area
    ed.unemployment_rate,
    ed.median_household_income,
    ed.consumer_confidence,
    ed.local_spending_index,
    ed.ts as economic_last_updated,
    
    -- Demographic data from census tract
    d.median_income,
    d.median_income_z,
    d.pct_bachelors,
    d.pct_graduate,
    d.pct_age_20_40,
    d.population_density,
    d.pct_professional_occupation,
    
    -- Data completeness and quality scores
    CASE 
        WHEN v.lat IS NOT NULL AND v.lng IS NOT NULL 
             AND ss.mention_count IS NOT NULL 
             AND p.psychographic_density IS NOT NULL 
        THEN 1.0
        WHEN v.lat IS NOT NULL AND v.lng IS NOT NULL 
             AND (ss.mention_count IS NOT NULL OR p.psychographic_density IS NOT NULL)
        THEN 0.8
        WHEN v.lat IS NOT NULL AND v.lng IS NOT NULL 
        THEN 0.6
        ELSE 0.2
    END as data_completeness_score,
    
    -- Comprehensive venue score (for map prioritization)
    CASE 
        WHEN p.psychographic_density IS NOT NULL THEN p.psychographic_density * 0.6
        ELSE 0
    END +
    CASE 
        WHEN ss.positive_sentiment IS NOT NULL THEN ss.positive_sentiment * 0.2
        ELSE 0
    END +
    CASE 
        WHEN vt.visitors_count IS NOT NULL THEN LEAST(vt.visitors_count / 1000.0, 0.2)
        ELSE 0
    END as comprehensive_score,
    
    -- Data source tracking
    CASE 
        WHEN v.provider LIKE '%google%' OR v.provider LIKE '%places%' THEN 'api_places'
        WHEN v.provider IN ('tmobile_center', 'uptown_theater', 'kauffman_center', 'starlight_theatre', 'midland_theatre', 'knuckleheads', 'azura_amphitheater') THEN 'scraped_static'
        WHEN v.provider IN ('visitkc', 'do816', 'thepitchkc', 'aura') THEN 'scraped_dynamic'
        ELSE 'scraped_local'
    END as data_source_type,
    
    NOW() as last_refreshed
    
FROM venues v

-- Latest social sentiment (priority data)
LEFT JOIN LATERAL (
    SELECT mention_count, positive_sentiment, negative_sentiment, neutral_sentiment,
           engagement_score, psychographic_keywords, ts
    FROM social_sentiment 
    WHERE venue_id = v.venue_id 
    ORDER BY ts DESC 
    LIMIT 1
) ss ON true

-- Latest ML predictions (priority data)
LEFT JOIN LATERAL (
    SELECT psychographic_density, confidence_lower, confidence_upper, 
           model_version, contributing_factors, ts
    FROM predictions 
    WHERE ST_DWithin(ST_Point(lng, lat)::geography, ST_Point(v.lng, v.lat)::geography, 100)
    ORDER BY ts DESC 
    LIMIT 1  
) p ON true

-- Latest foot traffic data
LEFT JOIN LATERAL (
    SELECT visitors_count, median_dwell_seconds, visitors_change_24h, 
           visitors_change_7d, peak_hour_ratio, ts
    FROM venue_traffic 
    WHERE venue_id = v.venue_id 
    ORDER BY ts DESC 
    LIMIT 1
) vt ON true

-- Latest traffic congestion data
LEFT JOIN LATERAL (
    SELECT congestion_score, travel_time_to_downtown, travel_time_index, ts
    FROM traffic_data 
    WHERE venue_id = v.venue_id 
    ORDER BY ts DESC 
    LIMIT 1
) td ON true

-- Nearest weather data (within 5km)
LEFT JOIN LATERAL (
    SELECT temperature_f, feels_like_f, humidity, rain_probability,
           weather_condition, weather_description, ts
    FROM weather_data 
    WHERE ST_DWithin(ST_Point(lng, lat)::geography, ST_Point(v.lng, v.lat)::geography, 5000)
    ORDER BY ts DESC 
    LIMIT 1
) wd ON true

-- Economic data for the area (Kansas City)
LEFT JOIN LATERAL (
    SELECT unemployment_rate, median_household_income, consumer_confidence, 
           local_spending_index, ts
    FROM economic_data 
    WHERE geographic_area = 'kansas_city' OR geographic_area LIKE '%kansas%city%'
    ORDER BY ts DESC 
    LIMIT 1
) ed ON true

-- Demographics from census tract
LEFT JOIN LATERAL (
    SELECT median_income, median_income_z, pct_bachelors, pct_graduate,
           pct_age_20_40, population_density, pct_professional_occupation
    FROM demographics 
    WHERE ST_Within(ST_Point(v.lng, v.lat), geometry)
    LIMIT 1
) d ON true

WHERE v.lat IS NOT NULL AND v.lng IS NOT NULL
ORDER BY 
    data_completeness_score DESC,
    comprehensive_score DESC,
    v.updated_at DESC;

-- Create unique index on materialized view
CREATE UNIQUE INDEX idx_master_venue_data_venue_id ON master_venue_data(venue_id);
CREATE INDEX idx_master_venue_data_location ON master_venue_data(lat, lng);
CREATE INDEX idx_master_venue_data_completeness ON master_venue_data(data_completeness_score);
CREATE INDEX idx_master_venue_data_score ON master_venue_data(comprehensive_score);
CREATE INDEX idx_master_venue_data_source ON master_venue_data(data_source_type);

-- Daily refresh function for master data
CREATE OR REPLACE FUNCTION refresh_master_data()
RETURNS TABLE(
    refresh_status TEXT,
    venues_count INTEGER,
    avg_completeness FLOAT,
    refresh_duration INTERVAL
) AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    venue_count INTEGER;
    avg_completeness_score FLOAT;
BEGIN
    start_time := NOW();
    
    -- Refresh the materialized view
    REFRESH MATERIALIZED VIEW master_venue_data;
    
    -- Get statistics
    SELECT COUNT(*), AVG(data_completeness_score)
    INTO venue_count, avg_completeness_score
    FROM master_venue_data;
    
    end_time := NOW();
    
    -- Update collection status
    INSERT INTO collection_status (source_name, last_successful_collection, collection_health_score, status_details)
    VALUES (
        'master_data_refresh', 
        end_time, 
        LEAST(avg_completeness_score, 1.0),
        jsonb_build_object(
            'venues_processed', venue_count,
            'avg_completeness', avg_completeness_score,
            'refresh_duration_seconds', EXTRACT(EPOCH FROM (end_time - start_time))
        )
    )
    ON CONFLICT (source_name) DO UPDATE SET
        last_successful_collection = EXCLUDED.last_successful_collection,
        collection_health_score = EXCLUDED.collection_health_score,
        status_details = EXCLUDED.status_details;
    
    -- Return results
    RETURN QUERY SELECT 
        'SUCCESS'::TEXT,
        venue_count,
        avg_completeness_score,
        end_time - start_time;
END;
$$ LANGUAGE plpgsql;

-- Master Events View (simplified for events)
DROP MATERIALIZED VIEW IF EXISTS master_events_data;
CREATE MATERIALIZED VIEW master_events_data AS
SELECT 
    e.event_id,
    e.external_id,
    e.provider,
    e.name,
    e.description,
    e.category,
    e.subcategory,
    e.start_time,
    e.end_time,
    e.predicted_attendance,
    e.psychographic_relevance,
    
    -- Venue information
    v.name as venue_name,
    v.lat,
    v.lng,
    v.address as venue_address,
    v.category as venue_category,
    
    -- Latest social sentiment for events
    ss.mention_count,
    ss.positive_sentiment,
    ss.engagement_score,
    
    -- Event score calculation
    CASE 
        WHEN e.psychographic_relevance IS NOT NULL THEN 
            COALESCE((e.psychographic_relevance->>'career_driven')::FLOAT, 0) * 0.4 +
            COALESCE((e.psychographic_relevance->>'competent')::FLOAT, 0) * 0.3 +
            COALESCE((e.psychographic_relevance->>'fun')::FLOAT, 0) * 0.3
        ELSE 0.5
    END +
    CASE 
        WHEN ss.positive_sentiment IS NOT NULL THEN ss.positive_sentiment * 0.2
        ELSE 0
    END as event_score,
    
    -- Data source categorization
    CASE 
        WHEN e.provider LIKE '%predicthq%' THEN 'api_events'
        WHEN e.provider IN ('visitkc', 'do816', 'thepitchkc') THEN 'scraped_dynamic'
        ELSE 'scraped_local'
    END as data_source_type,
    
    NOW() as last_refreshed
    
FROM events e
LEFT JOIN venues v ON e.venue_id = v.venue_id
LEFT JOIN LATERAL (
    SELECT mention_count, positive_sentiment, engagement_score
    FROM social_sentiment 
    WHERE event_id = e.event_id 
    ORDER BY ts DESC 
    LIMIT 1
) ss ON true

WHERE v.lat IS NOT NULL AND v.lng IS NOT NULL
AND (e.start_time IS NULL OR e.start_time >= NOW() - INTERVAL '30 days')
ORDER BY 
    COALESCE(e.start_time, NOW() + INTERVAL '1 year') ASC,
    event_score DESC;

-- Create indexes for events view
CREATE UNIQUE INDEX idx_master_events_data_event_id ON master_events_data(event_id);
CREATE INDEX idx_master_events_data_location ON master_events_data(lat, lng);
CREATE INDEX idx_master_events_data_time ON master_events_data(start_time);
CREATE INDEX idx_master_events_data_score ON master_events_data(event_score);

-- Function to refresh both master views
CREATE OR REPLACE FUNCTION refresh_all_master_data()
RETURNS TABLE(
    view_name TEXT,
    refresh_status TEXT,
    record_count INTEGER,
    refresh_duration INTERVAL
) AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    venue_count INTEGER;
    event_count INTEGER;
BEGIN
    -- Refresh venues
    start_time := NOW();
    REFRESH MATERIALIZED VIEW master_venue_data;
    end_time := NOW();
    
    SELECT COUNT(*) INTO venue_count FROM master_venue_data;
    
    RETURN QUERY SELECT 
        'master_venue_data'::TEXT,
        'SUCCESS'::TEXT,
        venue_count,
        end_time - start_time;
    
    -- Refresh events
    start_time := NOW();
    REFRESH MATERIALIZED VIEW master_events_data;
    end_time := NOW();
    
    SELECT COUNT(*) INTO event_count FROM master_events_data;
    
    RETURN QUERY SELECT 
        'master_events_data'::TEXT,
        'SUCCESS'::TEXT,
        event_count,
        end_time - start_time;
END;
$$ LANGUAGE plpgsql;

-- Indexes for performance
CREATE INDEX idx_venues_geo ON venues USING GIST(geo);
CREATE INDEX idx_venues_category ON venues(category);
CREATE INDEX idx_venues_psychographic ON venues USING GIN(psychographic_relevance);

CREATE INDEX idx_events_time ON events(start_time, end_time);
CREATE INDEX idx_events_venue ON events(venue_id);
CREATE INDEX idx_events_category ON events(category);

CREATE INDEX idx_venue_traffic_venue_ts ON venue_traffic(venue_id, ts);
CREATE INDEX idx_traffic_data_venue_ts ON traffic_data(venue_id, ts);
CREATE INDEX idx_weather_data_ts_location ON weather_data(ts, lat, lng);
CREATE INDEX idx_social_sentiment_venue_ts ON social_sentiment(venue_id, ts);

CREATE INDEX idx_demographics_geo ON demographics USING GIST(geometry);
CREATE INDEX idx_psychographic_layers_location ON psychographic_layers(lat, lng);
CREATE INDEX idx_psychographic_layers_grid ON psychographic_layers(grid_cell_id);

CREATE INDEX idx_features_venue_ts ON features(venue_id, ts);
CREATE INDEX idx_features_ts ON features(ts);
CREATE INDEX idx_features_label_source ON features(label_source) WHERE psychographic_density IS NOT NULL;

CREATE INDEX idx_predictions_location_ts ON predictions(lat, lng, ts);
CREATE INDEX idx_predictions_ts ON predictions(ts);
