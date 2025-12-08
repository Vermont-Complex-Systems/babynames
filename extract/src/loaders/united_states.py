"""US baby names data loader"""
import duckdb
from . import BaseLoader


class UnitedStatesLoader(BaseLoader):
    """Loader for United States baby names data"""

    @property
    def location_name(self) -> str:
        return "United States"

    @property
    def geo_id(self) -> str:
        return "united_states"

    def load(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Load US baby names data from extracted ZIP files"""

        # Ensure table exists with proper partitioning
        self.ensure_table_exists(conn)

        # Check if data already exists
        if self.check_already_loaded(conn):
            return

        conn.execute("""
            INSERT INTO babynames (geo, year, types, sex, counts)
            SELECT
                split_part(filename, '/', 3) AS geo,
                CAST(
                    replace(
                        replace(split_part(filename, '/', 4), 'yob', ''),
                        '.txt', ''
                    ) AS INTEGER
                ) AS year,
                name AS types,
                sex,
                CAST(count AS INTEGER) AS counts
            FROM read_csv_auto(
                'extract/input/united_states/yob*.txt',
                columns = {
                    'name': 'TEXT',
                    'sex': 'TEXT',
                    'count': 'INT'
                },
                filename = TRUE
            );
        """)
