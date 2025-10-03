# PostgreSQL Materialized View Fix Summary

## Problem Resolved

Fixed the PostgreSQL materialized view error:

```
ERROR:shared.database.connection:Query execution failed: cannot change materialized view "master_venue_data"
❌ Error refreshing master data: cannot change materialized view "master_venue_data"
```

## Root Cause Analysis

The error occurred because the application was trying to use SQLite-compatible operations (DELETE/INSERT) on PostgreSQL materialized views, which is not allowed. PostgreSQL materialized views must be refreshed using the `REFRESH MATERIALIZED VIEW` command.

## Solution Implemented

### 1. Updated `refresh_master_data_tables()` Function

**File**: `fix_streamlit_event_discrepancy.py`

- **Added database type detection**: The function now checks `db.db_type` to determine if it's working with PostgreSQL or SQLite
- **PostgreSQL path**: Uses `REFRESH MATERIALIZED VIEW` commands with multiple fallback strategies
- **SQLite path**: Maintains the original DELETE/INSERT approach for SQLite compatibility
- **Enhanced error handling**: Provides detailed error messages and fallback mechanisms

### 2. Updated Master Data Orchestrator

**File**: `shared/orchestration/master_data_orchestrator.py`

- **Added new refresh source**: Added "master_data" as a refreshable source
- **New method**: Added `_refresh_master_data_views()` method that uses the updated refresh function
- **Integration**: The orchestrator now properly handles materialized view refreshes

### 3. Multi-Level Fallback Strategy

For PostgreSQL, the fix implements a robust fallback strategy:

1. **Primary**: `REFRESH MATERIALIZED VIEW` (standard approach)
2. **Secondary**: Use PostgreSQL functions from migrations (`refresh_all_master_data()`)
