"""
Babynames Adapter

Prepares baby names data for submission to Storywrangler API.

Type of submission: Pattern 2 - Location entities with pre-computed n-grams
Schema:
  - types: str (baby name)
  - counts: int (number of babies)
  - countries: str (country/state)
  - year: int (birth year)
  - sex: str (M/F)
Primary key: countries using wikidata identifier
Dataset metadata:
"""

from pathlib import Path
from typing import Dict
from storywrangler.validation import EntityValidator, EndpointValidator
from pyprojroot import here
import duckdb
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class BabynamesAdapter:

    def __init__(self):
        self.project_root = here()
        self.dataset_id = os.getenv("DATASET_ID")
        self.data_path = Path(os.getenv("DATA_PATH"))
        self.entity_validator = EntityValidator()
        self.endpoint_validator = EndpointValidator()
        self.ducklake_path = self.project_root / "metadata.ducklake"
    
    def get_country_entity(self) -> Dict:
        """Map US to entity identifiers"""
        return {
            "local_id": "united_states",
            "entity_id": "wikidata:Q30",
            "entity_ids": ["iso:US", "local:babynames:united_states"],
            "entity_name": "United States",
        }
    
    def connect_ducklake(self) -> duckdb.DuckDBPyConnection:
        """Connect to the ducklake database"""
        if not self.ducklake_path.exists():
            raise FileNotFoundError(f"Ducklake file not found: {self.ducklake_path}")

        conn = duckdb.connect()
        conn.execute(f"ATTACH 'ducklake:metadata.ducklake' AS babylake (DATA_PATH '{self.data_path}');")
        conn.execute("USE babylake;")
        print(f"📊 Connected to ducklake: {self.ducklake_path}")
        print(f"📊 Data path: {self.data_path}")
        return conn

    def create_adapter_table(self, conn: duckdb.DuckDBPyConnection):
        """Create/update the adapter lookup table"""
        print("🔧 Creating adapter lookup table...")

        # Drop existing table if it exists
        conn.execute("DROP TABLE IF EXISTS adapter")

        # Create the lookup table
        conn.execute("""
            CREATE TABLE adapter (
                local_id VARCHAR,
                entity_id VARCHAR,
                entity_name VARCHAR,
                entity_ids VARCHAR[]
            )
        """)

        # Insert mapping for US (should be more general)
        entity_data = self.get_country_entity()

        # Validate entity ID
        if not self.entity_validator.validate(entity_data["entity_id"]):
            raise ValueError(f"Invalid entity_id: {entity_data['entity_id']}")

        conn.execute("""
            INSERT INTO adapter
            (local_id, entity_id, entity_name, entity_ids)
            VALUES (?, ?, ?, ?)
        """, [
            entity_data["local_id"],
            entity_data["entity_id"],
            entity_data["entity_name"],
            entity_data["entity_ids"]
        ])

        print(f"  ✓ {entity_data['entity_name']} → {entity_data['entity_id']}")

    def validate_babynames_schema(self, conn: duckdb.DuckDBPyConnection):
        """Validate that babynames data conforms to top-ngrams endpoint schema"""
        print("🔍 Validating babynames schema against Storywrangler standards...")

        # Get schema information from babynames table
        schema_result = conn.execute("DESCRIBE babynames").fetchall()
        columns = {row[0]: {'type': row[1]} for row in schema_result}

        schema = {'columns': columns}

        # Validate against top-ngrams endpoint requirements
        validation = self.endpoint_validator.validate_top_ngrams_schema(schema)

        if not validation['valid']:
            print("❌ Schema validation failed:")
            for error in validation['errors']:
                print(f"   - {error}")
            raise ValueError("Babynames schema does not conform to Storywrangler top-ngrams endpoint requirements")

        print("✅ Schema validation passed - babynames data conforms to top-ngrams endpoint")
        return validation['column_mapping']

    def prepare(self):
        """Prepare dataset metadata and update DuckDB with entity mappings"""
        print("🔧 Preparing Babynames dataset with DuckDB\n")

        # Connect to DuckDB
        conn = self.connect_ducklake()

        try:
            # Validate babynames schema against Storywrangler standards
            self.validate_babynames_schema(conn)

            # Create/update entity lookup table
            self.create_adapter_table(conn)

            print(f"✅ Entity mappings created in ducklake")
            print(f"✅ Adapter complete")

        finally:
            conn.close()
    


def main():
    """Run the adapter"""

    # Initialize adapter (reads from .env)
    adapter = BabynamesAdapter()

    print(f"📁 Configuration:")
    print(f"  Dataset ID: {adapter.dataset_id}")
    print(f"  Data path: {adapter.data_path}")
    print(f"  DuckDB: {adapter.ducklake_path}")
    print()

    # Check DuckDB exists
    if not adapter.ducklake_path.exists():
        print(f"❌ DuckDB not found: {adapter.ducklake_path}")
        print("   Run the extract pipeline first!")
        return

    adapter.prepare()


if __name__ == "__main__":
    main()