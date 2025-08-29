-- psycho_demo_mvp.sql
-- Schema for Psycho-Demographic Whereabouts Prediction Engine (MVP)

PRAGMA foreign_keys = ON;

-- =====================================
-- LOCATIONS
-- Stores static info about venues
-- =====================================
CREATE TABLE locations (
    location_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    address TEXT,
    latitude REAL,
    longitude REAL,
    category TEXT,         -- e.g., 'coworking', 'brewery', 'concert_hall'
    base_score INTEGER     -- demographic affinity score (D(L))
);

-- =====================================
-- EVENTS
-- Stores dynamic events (Eventbrite, Meetup, university calendars)
-- =====================================
CREATE TABLE events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT,           -- 'eventbrite', 'meetup', 'university'
    external_id TEXT,      -- API ID for deduplication
    name TEXT NOT NULL,
    description TEXT,
    start_time DATETIME,
    end_time DATETIME,
    location_id INTEGER,   -- FK to locations
    category TEXT,         -- API-provided category
    tags TEXT,             -- JSON/CSV of keywords
    FOREIGN KEY (location_id) REFERENCES locations(location_id)
);

-- =====================================
-- WEATHER
-- Stores weather data by timestamp
-- =====================================
CREATE TABLE weather (
    weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp DATETIME NOT NULL,
    location_id INTEGER,   -- FK (nullable, city-wide if null)
    condition TEXT,        -- e.g., 'sunny', 'rain', 'snow'
    temperature REAL,
    precipitation REAL,
    FOREIGN KEY (location_id) REFERENCES locations(location_id)
);

-- =====================================
-- SCORES
-- Stores calculated scores per event/location
-- =====================================
CREATE TABLE scores (
    score_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER,
    location_id INTEGER,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    demographic_score INTEGER,   -- D(L)
    event_score INTEGER,         -- E(L,T)
    weather_score INTEGER,       -- W(T)
    total_score INTEGER,         -- sum of the above
    FOREIGN KEY (event_id) REFERENCES events(event_id),
    FOREIGN KEY (location_id) REFERENCES locations(location_id)
);

-- =====================================
-- USERS (Optional, for personalization later)
-- =====================================
CREATE TABLE users (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    preferences TEXT             -- JSON: weights for career/fun/competence
);

-- =====================================
-- INDEXES (for performance)
-- =====================================
CREATE INDEX idx_events_location ON events(location_id);
CREATE INDEX idx_scores_event ON scores(event_id);
CREATE INDEX idx_scores_location ON scores(location_id);
CREATE INDEX idx_weather_timestamp ON weather(timestamp);
