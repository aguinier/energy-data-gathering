#!/usr/bin/env python3
"""
Migration script to create the weather_data_multipoint table
"""

import sqlite3
import sys
import os
from pathlib import Path

# Add parent directory to path to import config
sys.path.append(str(Path(__file__).parent.parent / 'src'))

DB_PATH = Path(__file__).parent.parent / 'energy_dashboard.db'

def create_multipoint_table():
    """Create the weather_data_multipoint table"""
    
    sql_create_table = """
    CREATE TABLE IF NOT EXISTS weather_data_multipoint (
        country_code TEXT NOT NULL,
        forecast_type TEXT NOT NULL,
        timestamp_utc TEXT NOT NULL,
        forecast_run_time TEXT,
        temperature_2m_k REAL,
        dew_point_2m_k REAL,
        relative_humidity_2m_frac REAL,
        pressure_msl_hpa REAL,
        wind_speed_10m_ms REAL,
        wind_gusts_10m_ms REAL,
        wind_direction_10m_deg REAL,
        wind_speed_100m_ms REAL,
        wind_direction_100m_deg REAL,
        wind_speed_80m_ms REAL,
        wind_speed_120m_ms REAL,
        precip_mm REAL,
        rain_mm REAL,
        snowfall_mm REAL,
        shortwave_radiation_wm2 REAL,
        direct_radiation_wm2 REAL,
        direct_normal_irradiance_wm2 REAL,
        diffuse_radiation_wm2 REAL,
        model_name TEXT DEFAULT 'era5',
        data_quality TEXT DEFAULT 'multipoint_weighted',
        n_points INTEGER,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (country_code, forecast_type, timestamp_utc)
    );
    """
    
    try:
        print(f"Creating weather_data_multipoint table in {DB_PATH}")
        
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(sql_create_table)
            conn.commit()
            
        print("[SUCCESS] Table weather_data_multipoint created successfully")
        
        # Verify table exists and show structure
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(weather_data_multipoint)")
            columns = cursor.fetchall()
            
        print(f"\nTable structure ({len(columns)} columns):")
        for col in columns:
            print(f"  {col[1]} {col[2]} {'NOT NULL' if col[3] else ''} {'PRIMARY KEY' if col[5] else ''}")
            
    except sqlite3.Error as e:
        print(f"[ERROR] Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    print("Creating weather_data_multipoint table...")
    create_multipoint_table()
    print("[SUCCESS] Migration completed successfully")