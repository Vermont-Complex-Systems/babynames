"""Quebec baby names data loader"""
import duckdb
from . import BaseLoader


class QuebecLoader(BaseLoader):
    """Loader for Quebec baby names data"""

    @property
    def location_name(self) -> str:
        return "Quebec"

    @property
    def geo_id(self) -> str:
        return "quebec"

    def load(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Load Quebec baby names data from CSV files

        Quebec data is in wide format with years as columns.
        We use UNPIVOT to transform it into long format to match the schema.
        Values of '<5' and '0' are excluded for privacy and data quality.
        """

        # Ensure table exists with proper partitioning
        self.ensure_table_exists(conn)

        # Check if data already exists
        if self.check_already_loaded(conn):
            return

        # Single INSERT with UNION ALL to load both boys and girls in one transaction
        conn.execute("""
            INSERT INTO babynames (geo, year, types, sex, counts)
            SELECT
                'quebec' as geo,
                CAST(year as INTEGER) as year,
                concat(upper(left(PRENOM, 1)), lower(substring(PRENOM, 2))) as types,
                'M' as sex,
                CAST(count_str AS INTEGER) as counts
            FROM (
                SELECT * FROM read_csv_auto('extract/input/quebec/Gars1980-2024.csv')
            )
            UNPIVOT (
                count_str FOR year IN (COLUMNS(* EXCLUDE (PRENOM)))
            )
            WHERE count_str != '0' AND count_str != '<5'

            UNION ALL

            SELECT
                'quebec' as geo,
                CAST(year as INTEGER) as year,
                concat(upper(left(PRENOM, 1)), lower(substring(PRENOM, 2))) as types,                'F' as sex,
                CAST(count_str AS INTEGER) as counts
            FROM (
                SELECT * FROM read_csv_auto('extract/input/quebec/Filles1980-2024.csv')
            )
            UNPIVOT (
                count_str FOR year IN (COLUMNS(* EXCLUDE (PRENOM)))
            )
            WHERE count_str != '0' AND count_str != '<5'
        """)
