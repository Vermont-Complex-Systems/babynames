"""Location-specific data loaders for baby names"""
from abc import ABC, abstractmethod
import duckdb


class BaseLoader(ABC):
    """Base class for all location loaders"""

    @property
    @abstractmethod
    def location_name(self) -> str:
        """Human-readable location name"""
        pass

    @property
    @abstractmethod
    def geo_id(self) -> str:
        """Geographic identifier used in the 'geo' column"""
        pass

    def ensure_table_exists(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Create babynames table if it doesn't exist"""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS babynames (
                types TEXT,
                sex TEXT,
                counts INT,
                year INT,
                geo TEXT
            );
        """)

    def check_already_loaded(self, conn: duckdb.DuckDBPyConnection) -> bool:
        """Check if data for this location already exists in the database.

        Returns:
            True if data exists (skip import), False otherwise (proceed with import)
        """
        existing = conn.execute(
            "SELECT COUNT(*) FROM babynames WHERE geo = ?",
            [self.geo_id]
        ).fetchone()[0]

        if existing > 0:
            print(f"⚠ {self.location_name} data already exists ({existing:,} rows) - skipping import")
            print(f"  To re-import, first delete existing data: DELETE FROM babynames WHERE geo = '{self.geo_id}'")
            return True

        return False

    @abstractmethod
    def load(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Load data for this location into the database"""
        pass
