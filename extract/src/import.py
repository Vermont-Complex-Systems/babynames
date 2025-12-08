"""Main import orchestrator - loads data from all locations into DuckDB"""
import duckdb
import argparse
from loaders.united_states import UnitedStatesLoader
from loaders.quebec import QuebecLoader


# Registry of all available loaders
LOADERS = {
    "united_states": UnitedStatesLoader(),
    "quebec": QuebecLoader(),
}


def load_location(conn, location):
    """Load data for a specific location using its loader"""
    location_normalized = location.lower().replace(" ", "_")

    if location_normalized not in LOADERS:
        available = ", ".join(LOADERS.keys())
        raise ValueError(f"No loader found for location: {location}. Available: {available}")

    loader = LOADERS[location_normalized]
    print(f"Loading data for: {loader.location_name}")
    loader.load(conn)
    print(f"✓ Successfully loaded {loader.location_name}")


def main(location):
    """Load data for a specific location"""

    conn = duckdb.connect()
    try:
        conn.execute("ATTACH 'ducklake:metadata.ducklake' AS babylake;")
        conn.execute("USE babylake;")
        load_location(conn, location)
    finally:
        conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Import baby names data into DuckDB")
    parser.add_argument('location',
                       help="Location to import (e.g., 'united_states', 'quebec')")
    args = parser.parse_args()
    main(args.location)