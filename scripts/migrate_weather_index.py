#!/usr/bin/env python3
"""
One-time migration script to update weather_data index for forecast support.

This migration:
1. Updates existing actuals: SET forecast_run_time = timestamp_utc WHERE forecast_run_time IS NULL
2. Drops the old unique index (country_code, timestamp_utc, model_name)
3. Creates new unique index (country_code, timestamp_utc, model_name, forecast_run_time)

This enables storing multiple forecast vintages for the same target timestamp,
allowing forecast accuracy analysis over different lead times.

Usage:
    python scripts/migrate_weather_index.py
    python scripts/migrate_weather_index.py --dry-run
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import config


def get_connection():
    """Get database connection"""
    import sqlite3
    return sqlite3.connect(config.DATABASE_PATH)


def check_current_state(conn):
    """Check current database state"""
    cursor = conn.cursor()

    # Check if old index exists
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='index' AND name='idx_weather_country_time_model'
    """)
    old_index_exists = cursor.fetchone() is not None

    # Check if new index exists
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='index' AND name='idx_weather_country_time_model_run'
    """)
    new_index_exists = cursor.fetchone() is not None

    # Count records with NULL forecast_run_time
    cursor.execute("""
        SELECT COUNT(*) FROM weather_data WHERE forecast_run_time IS NULL
    """)
    null_count = cursor.fetchone()[0]

    # Total records
    cursor.execute("SELECT COUNT(*) FROM weather_data")
    total_count = cursor.fetchone()[0]

    return {
        'old_index_exists': old_index_exists,
        'new_index_exists': new_index_exists,
        'null_forecast_run_time': null_count,
        'total_records': total_count
    }


def run_migration(dry_run: bool = False):
    """Run the migration"""
    print("=" * 60)
    print("Weather Data Index Migration")
    print("=" * 60)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Database: {config.DATABASE_PATH}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print()

    conn = get_connection()

    try:
        # Check current state
        print("Checking current database state...")
        state = check_current_state(conn)
        print(f"  - Old index exists: {state['old_index_exists']}")
        print(f"  - New index exists: {state['new_index_exists']}")
        print(f"  - Records with NULL forecast_run_time: {state['null_forecast_run_time']:,}")
        print(f"  - Total weather records: {state['total_records']:,}")
        print()

        # Check if migration already complete
        if state['new_index_exists'] and not state['old_index_exists'] and state['null_forecast_run_time'] == 0:
            print("Migration already complete! Nothing to do.")
            return True

        if dry_run:
            print("DRY RUN - No changes will be made")
            print()
            print("Would perform:")
            if state['null_forecast_run_time'] > 0:
                print(f"  1. Update {state['null_forecast_run_time']:,} records: SET forecast_run_time = timestamp_utc")
            if state['old_index_exists']:
                print("  2. DROP INDEX idx_weather_country_time_model")
            if not state['new_index_exists']:
                print("  3. CREATE UNIQUE INDEX idx_weather_country_time_model_run")
            return True

        cursor = conn.cursor()

        # Step 1: Update NULL forecast_run_time values
        if state['null_forecast_run_time'] > 0:
            print(f"Step 1: Updating {state['null_forecast_run_time']:,} records with NULL forecast_run_time...")
            cursor.execute("""
                UPDATE weather_data
                SET forecast_run_time = timestamp_utc
                WHERE forecast_run_time IS NULL
            """)
            conn.commit()
            print(f"  Updated {cursor.rowcount:,} records")
        else:
            print("Step 1: No NULL forecast_run_time values to update")

        # Step 2: Drop old index
        if state['old_index_exists']:
            print("Step 2: Dropping old index idx_weather_country_time_model...")
            cursor.execute("DROP INDEX IF EXISTS idx_weather_country_time_model")
            conn.commit()
            print("  Done")
        else:
            print("Step 2: Old index already dropped")

        # Step 3: Create new index
        if not state['new_index_exists']:
            print("Step 3: Creating new index idx_weather_country_time_model_run...")
            cursor.execute("""
                CREATE UNIQUE INDEX idx_weather_country_time_model_run
                ON weather_data(country_code, timestamp_utc, model_name, forecast_run_time)
            """)
            conn.commit()
            print("  Done")
        else:
            print("Step 3: New index already exists")

        # Verify final state
        print()
        print("Verifying final state...")
        final_state = check_current_state(conn)
        print(f"  - Old index exists: {final_state['old_index_exists']}")
        print(f"  - New index exists: {final_state['new_index_exists']}")
        print(f"  - Records with NULL forecast_run_time: {final_state['null_forecast_run_time']:,}")

        if final_state['new_index_exists'] and not final_state['old_index_exists'] and final_state['null_forecast_run_time'] == 0:
            print()
            print("Migration completed successfully!")
            return True
        else:
            print()
            print("WARNING: Migration may not be complete. Please check manually.")
            return False

    except Exception as e:
        print(f"ERROR: Migration failed: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()
        print()
        print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Migrate weather_data index for forecast support",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This migration enables storing weather forecasts alongside actual data
by modifying the unique index to include forecast_run_time.

Examples:
    # Preview changes without executing
    python scripts/migrate_weather_index.py --dry-run

    # Run the migration
    python scripts/migrate_weather_index.py
        """
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without making changes'
    )

    args = parser.parse_args()

    success = run_migration(dry_run=args.dry_run)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
