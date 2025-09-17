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
