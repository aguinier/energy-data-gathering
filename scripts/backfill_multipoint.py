#!/usr/bin/env python3
"""
Backfill multipoint weather data for BE, FR, DE (2025-12-15 to 2026-03-15)
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import logging

# Add parent directory to path to import fetch_weather_multipoint
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from fetch_weather_multipoint import fetch_multipoint_weather

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def backfill_multipoint_data():
    """
    Backfill multipoint weather data for the specified date range
    """
    
    # Date range for backfill
    start_date = datetime(2025, 12, 15)
    end_date = datetime(2026, 3, 15)
    
    # Countries and forecast types to process
    countries = ['BE', 'FR', 'DE']
    
    # Calculate total duration
    total_days = (end_date - start_date).days
    logger.info(f"Starting backfill for {total_days} days from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    logger.info(f"Countries: {countries}")
    
    # Chunk into 30-day periods to avoid API limits
    chunk_size = 30
    current_date = start_date
    
    total_chunks = (total_days // chunk_size) + (1 if total_days % chunk_size > 0 else 0)
    chunk_num = 0
    
    while current_date < end_date:
        chunk_num += 1
        chunk_end = min(current_date + timedelta(days=chunk_size), end_date)
        
        logger.info(f"Processing chunk {chunk_num}/{total_chunks}: {current_date.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")
        
        try:
            fetch_multipoint_weather(
                countries=countries,
                forecast_types=None,  # Process all available forecast types
                start_date=current_date.strftime('%Y-%m-%d'),
                end_date=chunk_end.strftime('%Y-%m-%d'),
                mode='historical'
            )
            
            logger.info(f"Successfully completed chunk {chunk_num}/{total_chunks}")
            
        except Exception as e:
            logger.error(f"Failed to process chunk {chunk_num}: {e}")
            logger.info("Continuing with next chunk...")
        
        # Move to next chunk
        current_date = chunk_end + timedelta(days=1)
    
    logger.info("Backfill completed successfully!")
    
    # Show summary statistics
    try:
        import sqlite3
        
        db_path = Path(__file__).parent.parent / 'energy_dashboard.db'
        
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Count records by country and forecast_type
            cursor.execute("""
                SELECT 
                    country_code, 
                    forecast_type, 
                    COUNT(*) as record_count,
                    MIN(timestamp_utc) as earliest,
                    MAX(timestamp_utc) as latest
                FROM weather_data_multipoint 
                WHERE timestamp_utc >= ? AND timestamp_utc <= ?
                GROUP BY country_code, forecast_type
                ORDER BY country_code, forecast_type
            """, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
            
            results = cursor.fetchall()
            
            logger.info("\n=== Backfill Summary ===")
            total_records = 0
            for row in results:
                country, forecast_type, count, earliest, latest = row
                total_records += count
                logger.info(f"{country}-{forecast_type}: {count:,} records ({earliest} to {latest})")
            
            logger.info(f"Total records: {total_records:,}")
            
    except Exception as e:
        logger.warning(f"Could not generate summary statistics: {e}")

if __name__ == "__main__":
    print("Starting multipoint weather data backfill...")
    backfill_multipoint_data()
    print("Backfill completed!")