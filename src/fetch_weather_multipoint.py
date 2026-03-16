#!/usr/bin/env python3
"""
Fetch multi-point weather data for BE, FR, DE and compute weighted averages.
Supports both historical (archive) and forecast modes.
"""

import requests
import sqlite3
import pandas as pd
import time
import logging
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
import json

# Import weather zones
from weather_zones_real import WEATHER_ZONES

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database path
DB_PATH = Path(__file__).parent.parent / 'energy_dashboard.db'

# Open-Meteo API endpoints
HISTORICAL_API = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_API = "https://api.open-meteo.com/v1/forecast"

# Weather variables to fetch
WEATHER_VARIABLES = [
    'temperature_2m',
    'dew_point_2m', 
    'relative_humidity_2m',
    'pressure_msl',
    'wind_speed_10m',
    'wind_gusts_10m',
    'wind_direction_10m',
    'wind_speed_100m',
    'wind_direction_100m',
    'wind_speed_80m',
    'wind_speed_120m',
    'precipitation',
    'rain',
    'snowfall',
    'shortwave_radiation',
    'direct_radiation',
    'direct_normal_irradiance',
    'diffuse_radiation'
]

def convert_units(df):
    """Convert units to match database schema"""
    conversions = {
        'temperature_2m': ('temperature_2m_k', lambda x: x + 273.15),
        'dew_point_2m': ('dew_point_2m_k', lambda x: x + 273.15),
        'relative_humidity_2m': ('relative_humidity_2m_frac', lambda x: x / 100.0),
        'pressure_msl': ('pressure_msl_hpa', lambda x: x),
        'wind_speed_10m': ('wind_speed_10m_ms', lambda x: x),
        'wind_gusts_10m': ('wind_gusts_10m_ms', lambda x: x),
        'wind_direction_10m': ('wind_direction_10m_deg', lambda x: x),
        'wind_speed_100m': ('wind_speed_100m_ms', lambda x: x),
        'wind_direction_100m': ('wind_direction_100m_deg', lambda x: x),
        'wind_speed_80m': ('wind_speed_80m_ms', lambda x: x),
        'wind_speed_120m': ('wind_speed_120m_ms', lambda x: x),
        'precipitation': ('precip_mm', lambda x: x),
        'rain': ('rain_mm', lambda x: x),
        'snowfall': ('snowfall_mm', lambda x: x),
        'shortwave_radiation': ('shortwave_radiation_wm2', lambda x: x),
        'direct_radiation': ('direct_radiation_wm2', lambda x: x),
        'direct_normal_irradiance': ('direct_normal_irradiance_wm2', lambda x: x),
        'diffuse_radiation': ('diffuse_radiation_wm2', lambda x: x),
    }
    
    for old_col, (new_col, transform) in conversions.items():
        if old_col in df.columns:
            df[new_col] = df[old_col].apply(transform)
            df.drop(columns=[old_col], inplace=True)
    
    return df

def fetch_weather_data(zones, start_date, end_date, mode='historical'):
    """
    Fetch weather data for multiple locations from Open-Meteo API
    
    Args:
        zones: List of zone dictionaries with lat, lon, weight
        start_date: Start date string (YYYY-MM-DD)  
        end_date: End date string (YYYY-MM-DD)
        mode: 'historical' or 'forecast'
    
    Returns:
        pandas.DataFrame: Weather data with timestamps
    """
    
    # Prepare coordinates
    latitudes = [zone['lat'] for zone in zones]
    longitudes = [zone['lon'] for zone in zones]
    
    # Choose API endpoint
    api_url = HISTORICAL_API if mode == 'historical' else FORECAST_API
    
    # Build parameters
    params = {
        'latitude': ','.join(map(str, latitudes)),
        'longitude': ','.join(map(str, longitudes)),
        'hourly': ','.join(WEATHER_VARIABLES),
        'wind_speed_unit': 'ms',  # Important: get m/s not km/h
        'timezone': 'UTC'
    }
    
    if mode == 'historical':
        params['start_date'] = start_date
        params['end_date'] = end_date
    else:
        params['forecast_days'] = 7  # For forecast mode
    
    logger.info(f"Fetching {mode} weather data for {len(zones)} locations")
    logger.debug(f"API URL: {api_url}")
    logger.debug(f"Coordinates: {list(zip(latitudes, longitudes))}")
    
    try:
        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Handle API response structure: can be a single object or list of objects
        if isinstance(data, list):
            location_data_list = data
        else:
            location_data_list = [data]
        
        if not location_data_list or 'hourly' not in location_data_list[0]:
            raise ValueError(f"No hourly data returned from API: {data}")
        
        # Convert to DataFrame
        df_list = []
        
        # Process each location response  
        for i, location_response in enumerate(location_data_list):
            if i >= len(zones):
                break
                
            zone = zones[i]
            hourly_data = location_response['hourly']
            
            timestamps = pd.to_datetime(hourly_data['time'])
            location_data = {'timestamp_utc': timestamps}
            
            # Extract weather variables for this location
            for var in WEATHER_VARIABLES:
                if var in hourly_data:
                    location_data[var] = hourly_data[var]
            
            location_df = pd.DataFrame(location_data)
            location_df['zone_index'] = i
            location_df['weight'] = zone['weight']
            df_list.append(location_df)
        
        if df_list:
            full_df = pd.concat(df_list, ignore_index=True)
            logger.info(f"Fetched {len(full_df)} records for {len(zones)} locations")
            return full_df
        else:
            logger.warning("No data received from API")
            return pd.DataFrame()
            
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Error processing weather data: {e}")
        raise

def compute_weighted_average(df):
    """Compute weighted average across all locations for each timestamp"""
    
    if df.empty:
        logger.warning("Empty DataFrame, cannot compute weighted average")
        return pd.DataFrame()
    
    # Group by timestamp and compute weighted averages
    result_rows = []
    
    for timestamp, group in df.groupby('timestamp_utc'):
        avg_data = {'timestamp_utc': timestamp}
        
        # Compute weighted average for each weather variable
        total_weight = group['weight'].sum()
        
        for var in WEATHER_VARIABLES:
            if var in group.columns:
                # Handle missing values
                valid_mask = group[var].notna()
                if valid_mask.any():
                    weighted_sum = (group.loc[valid_mask, var] * group.loc[valid_mask, 'weight']).sum()
                    valid_weight_sum = group.loc[valid_mask, 'weight'].sum()
                    avg_data[var] = weighted_sum / valid_weight_sum if valid_weight_sum > 0 else None
                else:
                    avg_data[var] = None
        
        avg_data['n_points'] = len(group)
        result_rows.append(avg_data)
    
    result_df = pd.DataFrame(result_rows)
    logger.info(f"Computed weighted averages for {len(result_df)} timestamps")
    
    return result_df

def store_weather_data(df, country_code, forecast_type, mode='historical'):
    """Store weather data in database"""
    
    if df.empty:
        logger.warning("No data to store")
        return
    
    # Convert units
    df = convert_units(df.copy())
    
    # Add metadata columns
    df['country_code'] = country_code
    df['forecast_type'] = forecast_type
    df['model_name'] = 'era5' if mode == 'historical' else 'gfs'
    df['data_quality'] = 'multipoint_weighted'
    df['forecast_run_time'] = datetime.utcnow().isoformat() if mode == 'forecast' else None
    df['created_at'] = datetime.utcnow().isoformat()
    
    # Ensure timestamp is string
    df['timestamp_utc'] = df['timestamp_utc'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Reorder columns to match table schema
    column_order = [
        'country_code', 'forecast_type', 'timestamp_utc', 'forecast_run_time',
        'temperature_2m_k', 'dew_point_2m_k', 'relative_humidity_2m_frac', 'pressure_msl_hpa',
        'wind_speed_10m_ms', 'wind_gusts_10m_ms', 'wind_direction_10m_deg',
        'wind_speed_100m_ms', 'wind_direction_100m_deg', 'wind_speed_80m_ms', 'wind_speed_120m_ms',
        'precip_mm', 'rain_mm', 'snowfall_mm',
        'shortwave_radiation_wm2', 'direct_radiation_wm2', 'direct_normal_irradiance_wm2', 'diffuse_radiation_wm2',
        'model_name', 'data_quality', 'n_points', 'created_at'
    ]
    
    # Keep only columns that exist
    df = df[[col for col in column_order if col in df.columns]]
    
    try:
        with sqlite3.connect(DB_PATH) as conn:
            # Use INSERT OR REPLACE to handle duplicates
            df.to_sql('weather_data_multipoint', conn, if_exists='append', index=False, method='multi')
            
        logger.info(f"Stored {len(df)} records for {country_code}-{forecast_type}")
        
    except sqlite3.Error as e:
        logger.error(f"Database error storing data: {e}")
        raise

def fetch_multipoint_weather(countries=None, forecast_types=None, start_date=None, end_date=None, mode='historical'):
    """
    Main function to fetch multipoint weather data
    
    Args:
        countries: List of country codes (default: ['BE', 'FR', 'DE'])
        forecast_types: List of forecast types (default: all available)
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD) 
        mode: 'historical' or 'forecast'
    """
    
    if countries is None:
        countries = ['BE', 'FR', 'DE']
    
    logger.info(f"Starting multipoint weather fetch for countries: {countries}")
    logger.info(f"Mode: {mode}, Date range: {start_date} to {end_date}")
    
    total_requests = 0
    
    for country in countries:
        if country not in WEATHER_ZONES:
            logger.warning(f"Country {country} not found in weather zones")
            continue
            
        country_zones = WEATHER_ZONES[country]
        
        # Determine forecast types to process
        if forecast_types is None:
            types_to_process = list(country_zones.keys())
        else:
            types_to_process = [ft for ft in forecast_types if ft in country_zones]
        
        logger.info(f"Processing {country}: {types_to_process}")
        
        for forecast_type in types_to_process:
            zones = country_zones[forecast_type]
            
            if not zones:
                logger.warning(f"No zones defined for {country}-{forecast_type}")
                continue
            
            try:
                logger.info(f"Fetching data for {country}-{forecast_type} ({len(zones)} zones)")
                
                # Fetch raw weather data
                raw_df = fetch_weather_data(zones, start_date, end_date, mode)
                
                if raw_df.empty:
                    logger.warning(f"No data received for {country}-{forecast_type}")
                    continue
                
                # Compute weighted average  
                avg_df = compute_weighted_average(raw_df)
                
                # Store in database
                store_weather_data(avg_df, country, forecast_type, mode)
                
                total_requests += 1
                
                # Rate limiting
                logger.debug("Applying rate limit (0.5s)")
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Failed to process {country}-{forecast_type}: {e}")
                continue
    
    logger.info(f"Completed multipoint weather fetch. Processed {total_requests} requests.")

if __name__ == "__main__":
    # Example usage for testing
    if len(sys.argv) > 1:
        mode = sys.argv[1]  # 'historical' or 'forecast'
    else:
        mode = 'historical'
    
    if mode == 'historical':
        # Test with recent data
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        
        logger.info(f"Test run: fetching historical data from {start_date} to {end_date}")
        fetch_multipoint_weather(
            countries=['BE'],  # Test with just Belgium
            forecast_types=['wind_onshore'],  # Test with just one type
            start_date=start_date,
            end_date=end_date,
            mode='historical'
        )
    else:
        logger.info("Test run: fetching forecast data")
        fetch_multipoint_weather(
            countries=['BE'],
            forecast_types=['wind_onshore'],
            mode='forecast'
        )