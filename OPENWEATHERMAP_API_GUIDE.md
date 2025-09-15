# OpenWeatherMap API Integration Guide

## Overview

This project now supports both OpenWeatherMap API v2.5 and v3.0 endpoints for comprehensive weather data collection. The implementation includes current weather, forecasts, historical data, and human-readable weather summaries.

## API Endpoints Implemented

### OpenWeatherMap API v2.5 (Free Tier)

- **Current Weather**: `/data/2.5/weather`
- **5-Day Forecast**: `/data/2.5/forecast`

### OpenWeatherMap API v3.0 (Requires Separate Subscription)

- **One Call API**: `/data/3.0/onecall`
- **Historical Weather**: `/data/3.0/onecall/timemachine`
- **Daily Aggregation**: `/data/3.0/onecall/day_summary`
- **Weather Overview**: `/data/3.0/onecall/overview`

## Setup Instructions

### 1. Get an API Key

1. Visit [OpenWeatherMap](https://openweathermap.org/api)
2. Sign up for a free account
3. Generate an API key for basic weather data
4. **For v3.0 One Call API**: Subscribe to the separate One Call API 3.0 plan
   - Visit the [pricing page](https://openweathermap.org/price) for details
   - One Call API 3.0 requires a separate subscription
   - Default: 2000 API calls per day
   - You can adjust limits in "Billing plans" tab in your Personal account

### 2. Configure API Key

Update your `.env` file:

```bash
WEATHER_API_KEY=your_actual_api_key_here
```

**Important**: Replace `your_actual_api_key_here` with your real API key from OpenWeatherMap.

### 3. API Key Status

The current API key in the project (`25b3b6fe5ce360b97baa2defc4815b68`) appears to be invalid or expired, as it returns 401 Unauthorized errors for both v2.5 and v3.0 endpoints.

## Example API Calls

### Using Your API Key

Replace `{API_KEY}` with your actual API key:

#### Current Weather and Forecast (v3.0 One Call)

```
https://api.openweathermap.org/data/3.0/onecall?lat=33.44&lon=-94.04&appid={API_KEY}
```

#### One Call API with Parameters

```
# Current weather only (exclude forecasts)
https://api.openweathermap.org/data/3.0/onecall?lat=33.44&lon=-94.04&exclude=minutely,hourly,daily,alerts&appid={API_KEY}

# Metric units (Celsius, m/s)
https://api.openweathermap.org/data/3.0/onecall?lat=33.44&lon=-94.04&units=metric&appid={API_KEY}

# Spanish language
https://api.openweathermap.org/data/3.0/onecall?lat=33.44&lon=-94.04&lang=es&appid={API_KEY}

# Current + daily only, metric units
https://api.openweathermap.org/data/3.0/onecall?lat=33.44&lon=-94.04&exclude=minutely,hourly,alerts&units=metric&appid={API_KEY}
```

#### Historical Data (v3.0)

```
https://api.openweathermap.org/data/3.0/onecall/timemachine?lat=33.44&lon=-94.04&dt=1609459200&appid={API_KEY}
```

#### Daily Aggregation (v3.0)

```
https://api.openweathermap.org/data/3.0/onecall/day_summary?lat=33.44&lon=-94.04&date=2024-01-01&appid={API_KEY}
```

#### Weather Overview (v3.0)

```
https://api.openweathermap.org/data/3.0/onecall/overview?lat=33.44&lon=-94.04&appid={API_KEY}
```

#### Weather Assistant Web Interface

```
https://openweathermap.org/weather-assistant?apikey={API_KEY}
```

## Available Functions

### Current Implementation (`src/etl/ingest_weather.py`)

#### v2.5 API Functions

- `fetch_current_weather(lat, lng)` - Current weather conditions
- `fetch_weather_forecast(lat, lng, days=5)` - 5-day forecast

#### v3.0 API Functions

- `fetch_onecall_weather(lat, lng, exclude=None, units="imperial", lang="en")` - Current + hourly + daily forecasts
- `fetch_historical_weather(lat, lng, dt)` - Historical weather data
- `fetch_daily_aggregation(lat, lng, date)` - Daily weather summary
- `fetch_weather_overview(lat, lng)` - Human-readable weather summary

#### One Call API Parameters

The `fetch_onecall_weather()` function supports these parameters:

- **lat** (required): Latitude (-90 to 90)
- **lng** (required): Longitude (-180 to 180)
- **exclude** (optional): Comma-delimited list to exclude parts of response
  - Available values: `current`, `minutely`, `hourly`, `daily`, `alerts`
  - Example: `"minutely,hourly,alerts"` to get only current and daily data
- **units** (optional): Units of measurement
  - `"standard"` - Kelvin, m/s (default)
  - `"metric"` - Celsius, m/s
  - `"imperial"` - Fahrenheit, mph
- **lang** (optional): Language for weather descriptions (default: "en")
  - Supports 40+ languages (es, fr, de, it, pt, ru, ja, zh, etc.)

#### Usage Examples

```python
# Full weather data (default)
data = fetch_onecall_weather(39.0997, -94.5786)

# Current weather only
data = fetch_onecall_weather(39.0997, -94.5786, exclude="minutely,hourly,daily,alerts")

# Metric units
data = fetch_onecall_weather(39.0997, -94.5786, units="metric")

# Spanish language
data = fetch_onecall_weather(39.0997, -94.5786, lang="es")

# Current + daily only, metric units, Spanish
data = fetch_onecall_weather(39.0997, -94.5786,
                            exclude="minutely,hourly,alerts",
                            units="metric",
                            lang="es")
```

#### Utility Functions

- `process_current_weather_data(weather_data, lat, lng)` - Process API response
- `process_forecast_data(forecast_data, lat, lng)` - Process forecast data
- `upsert_weather_to_db(weather_records)` - Save to database
- `fetch_weather_for_locations(locations, include_forecast=True)` - Multi-location fetch
- `fetch_weather_for_kansas_city()` - KC area weather

## Testing

### Test Scripts Available

1. **`test_weather_api.py`** - Tests v2.5 API functionality
2. **`test_weather_api_v3.py`** - Tests v3.0 API functionality

### Running Tests

```bash
# Test v2.5 API (requires valid API key)
python test_weather_api.py

# Test v3.0 API (requires valid API key + One Call subscription)
python test_weather_api_v3.py
```

## API Response Examples

### One Call API v3.0 Response Structure

```json
{
  "lat": 33.44,
  "lon": -94.04,
  "timezone": "America/Chicago",
  "current": {
    "dt": 1609459200,
    "temp": 45.32,
    "feels_like": 41.75,
    "humidity": 65,
    "uvi": 2.5,
    "weather": [{"main": "Clear", "description": "clear sky"}]
  },
  "hourly": [...],
  "daily": [...]
}
```

### Weather Overview Response

```json
{
  "weather_overview": "Today will be partly cloudy with temperatures reaching 75°F..."
}
```

## Subscription Requirements

### Free Tier (v2.5)

- Current weather data
- 5-day weather forecast
- 60 calls/minute, 1,000,000 calls/month

### One Call API 3.0 (Paid Subscription)

- All current and forecast data in one call
- Historical weather data (5 days back)
- Daily aggregation data
- Weather overview with AI-generated summaries
- Default: 2000 calls/day (adjustable)

## Integration with Project

### Database Schema

Weather data is stored in the `weather_data` table with fields:

- `ts` - timestamp
- `lat`, `lng` - coordinates
- `temperature_f`, `feels_like_f` - temperatures
- `humidity`, `pressure` - atmospheric data
- `wind_speed_mph`, `wind_direction` - wind data
- `weather_condition`, `weather_description` - conditions
- `rain_probability`, `precipitation_mm` - precipitation
- `uv_index`, `visibility` - additional metrics

### Usage in ETL Pipeline

The weather functions are integrated into the ETL pipeline and can be called from:

- Prefect flows (`src/infra/prefect_flows.py`)
- Data ingestion scripts
- Real-time data collection

## Troubleshooting

### Common Issues

1. **401 Unauthorized**

   - Check API key validity
   - Ensure API key is properly set in `.env`
   - For v3.0 endpoints, verify One Call API subscription

2. **403 Forbidden**

   - API key may not have access to requested endpoint
   - Check subscription status for v3.0 endpoints

3. **429 Too Many Requests**
   - Rate limit exceeded
   - Implement request throttling
   - Consider upgrading subscription

### Getting Help

- Check [OpenWeatherMap FAQ](https://openweathermap.org/faq)
- Contact Ulla, OpenWeather AI assistant
- Review pricing and billing in your Personal account

## Next Steps

1. **Get Valid API Key**: Sign up at OpenWeatherMap and get a working API key
2. **Subscribe to One Call API 3.0**: If you need v3.0 features
3. **Update Configuration**: Replace the API key in `.env`
4. **Test Integration**: Run the test scripts to verify functionality
5. **Deploy**: Use the weather functions in your data pipeline

## Security Notes

- **Never commit API keys to version control**
- Keep API keys in environment variables
- Monitor API usage to avoid unexpected charges
- Rotate API keys periodically for security
