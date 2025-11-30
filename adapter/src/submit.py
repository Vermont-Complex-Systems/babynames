"""
Submit babynames dataset registration to datalakes API.
"""

import requests
from pyprojroot import here
import os
from dotenv import load_dotenv
from storywrangler.validation import EndpointValidator

# Load environment variables
load_dotenv()


def get_ducklake_table_metadata(data_path: str):
    """Extract table metadata from ducklake for API registration."""
    import duckdb

    conn = duckdb.connect()
    conn.execute(f"ATTACH 'ducklake:metadata.ducklake' AS babylake (DATA_PATH '{data_path}');")

    # Get current file metadata for each table
    tables_metadata = {}

    for table_name in ['babynames', 'adapter']:
        result = conn.execute(f"""
            SELECT t.path, t.begin_snapshot, df.record_count, t.table_uuid
            FROM __ducklake_metadata_babylake.ducklake_data_file df
            JOIN __ducklake_metadata_babylake.ducklake_table t ON df.table_id = t.table_id
            WHERE t.table_name = '{table_name}'
              AND df.end_snapshot IS NULL
        """).fetchall()

        if result:
            # Take the first/latest file for this table
            path, begin_snapshot, record_count, table_uuid = result[0]
            tables_metadata[table_name] = {
                "path": path,
                "begin_snapshot": begin_snapshot,
                "record_count": record_count,
                "table_uuid": str(table_uuid)
            }

    conn.close()
    return tables_metadata


def register_babynames_datalake(api_url: str = None):
    """Register babynames dataset with the datalakes API."""

    # Get configuration from environment variables
    dataset_id = os.getenv("DATASET_ID")
    data_location = os.getenv("DATA_PATH")
    api_url = api_url or os.getenv("API_URL", "http://localhost:8000")

    # Validate schema against Storywrangler standards before registration
    print("🔍 Validating babynames schema against Storywrangler standards...")
    validator = EndpointValidator()

    import duckdb
    conn = duckdb.connect()
    conn.execute(f"ATTACH 'ducklake:metadata.ducklake' AS babylake (DATA_PATH '{data_location}');")

    try:
        # Get babynames schema
        schema_result = conn.execute("DESCRIBE babylake.babynames").fetchall()
        columns = {row[0]: {'type': row[1]} for row in schema_result}
        schema = {'columns': columns}

        # Validate against top-ngrams endpoint requirements
        validation = validator.validate_top_ngrams_schema(schema)

        if not validation['valid']:
            print("❌ Schema validation failed:")
            for error in validation['errors']:
                print(f"   - {error}")
            print("   Cannot register dataset - schema does not conform to Storywrangler standards")
            return False

        print("✅ Schema validation passed - proceeding with registration")

    finally:
        conn.close()

    # Get current table metadata from ducklake
    tables_metadata = get_ducklake_table_metadata(data_location)

    # Dataset metadata for registration
    dataset_metadata = {
        "dataset_id": dataset_id,
        "data_location": data_location,
        "data_format": "ducklake",
        "description": "US Baby names by popularity and year with entity mappings",
        "tables_metadata": tables_metadata
    }

    print(f"📤 Registering babynames datalake with API at {api_url}")
    print(f"📊 Data location: {data_location}")
    print(f"🗺️  Tables metadata:")
    for table_name, metadata in tables_metadata.items():
        print(f"    {table_name}: {metadata['path']} (snapshot {metadata['begin_snapshot']}, {metadata['record_count']} records)")

    try:
        # Register the datalake
        admin_username = os.getenv("ADMIN_USERNAME", "admin")
        admin_password = os.getenv("ADMIN_PASSWORD")

        response = requests.post(
            f"{api_url}/admin/datalakes/",
            json=dataset_metadata,
            headers={"Content-Type": "application/json"},
            auth=(admin_username, admin_password)
        )

        if response.status_code in [200, 201]:
            print(f"✅ {dataset_id} datalake registered successfully!")
            result = response.json()
            print(f"✅ Response: {result['message']}")
            return True
        else:
            print(f"❌ Registration failed: {response.status_code}")
            print(f"   {response.text}")
            return False

    except requests.exceptions.ConnectionError:
        print(f"❌ Could not connect to {api_url}")
        print(f"   Is the FastAPI server running?")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def main():
    """Run the submitter."""

    import argparse

    # Get default values from environment
    default_api_url = os.getenv("API_URL", "http://localhost:8000")
    dataset_id = os.getenv("DATASET_ID", "babynames")

    parser = argparse.ArgumentParser(description=f"Register {dataset_id} datalake with API")
    parser.add_argument(
        "--api-url",
        default=default_api_url,
        help=f"FastAPI URL (default: {default_api_url})"
    )

    args = parser.parse_args()

    success = register_babynames_datalake(args.api_url)

    if success:
        print(f"\n🚀 {dataset_id} datalake is now available!")
        print(f"📊 Try querying: GET {args.api_url}/datalakes/{dataset_id}/top-ngrams?start_year=1940&end_year=1943")
        print(f"📋 View info: GET {args.api_url}/datalakes/{dataset_id}")
        print(f"📋 List all: GET {args.api_url}/datalakes/")
    else:
        print(f"\n❌ Registration failed")


if __name__ == "__main__":
    main()