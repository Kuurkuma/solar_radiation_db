import logging
import sys
from databases import MySQLHandler, PostgresHandler, ClickHouseHandler, DuckDBHandler
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)

logger = logging.getLogger("main")


class Benchmarker(object):

    def __init__(self):
        self.results = []
        self.database_handlers = dict
        self.data = None
        self.queries = None

    def define_database_handlers(self, database_handlers: dict):
        self.database_handlers = database_handlers

    def get_data(self, url: str):
        self.data = pd.read_csv(url, header=0).convert_dtypes()

    def define_queries(self, queries: list):
        self.queries = queries

    def benchmark_queries(self):
        for database_name, database_handler in self.database_handlers.items():
            # Init DB
            database_handler.start()
            try:
                engine = create_engine(database_handler.sqlalchemy_connection_string)
                with engine.connect() as conn:

                    ### Load Data
                    # Special handling for ClickHouse which requires an engine definition
                    if isinstance(database_handler, ClickHouseHandler):
                        # Create table with engine before loading data
                        # Escape column names with backticks for ClickHouse
                        columns = ", ".join(
                            [
                                f"`{col}` {self._get_clickhouse_type(self.data[col])}"
                                for col in self.data.columns
                            ]
                        )
                        create_table_sql = f"""
                               CREATE TABLE IF NOT EXISTS data (
                                   {columns}
                               ) ENGINE = MergeTree() ORDER BY `{self.data.columns[0]}`
                               """
                        conn.execute(text(create_table_sql))

                        # Now we can load the data
                        self.data.to_sql(
                            con=conn, name="data", if_exists="append", index=False
                        )
                    else:
                        # For other databases, use the standard method
                        self.data.to_sql(
                            con=conn, name="data", if_exists="replace", index=False
                        )

                    ### query data
                    data = pd.read_sql_query(self.queries[0], conn)
                    logger.info(f"Read data: {data.describe}")

            finally:
                database_handler.stop(remove=True)

    def _get_clickhouse_type(self, series):
        """Helper method to map pandas dtypes to ClickHouse types"""
        dtype = str(series.dtype)
        if "int" in dtype:
            return "Int64"
        elif "float" in dtype:
            return "Float64"
        else:
            return "String"


if __name__ == "__main__":
    benchmarker = Benchmarker()

    databases = {
        "mysql": MySQLHandler(name="test-mysql", port=3306, cpu_limit=2),
        "postgres": PostgresHandler(name="test-postgres", port=5432, cpu_limit=2),
        "duckdb": DuckDBHandler(
            name="test-duckdb", db_file="duckdb_data.db", cpu_limit=2
        ),
        "clickhouse": ClickHouseHandler(
            name="test-clickhouse", http_port=8124, tcp_port=9001, cpu_limit=2
        ),
    }
    benchmarker.define_database_handlers(database_handlers=databases)
    benchmarker.get_data(
        url="https://gist.githubusercontent.com/netj/8836201/raw/6f9306ad21398ea43cba4f7d537619d0e07d5ae3/iris.csv"
    )
    queries = ["SELECT * FROM data;"]
    benchmarker.define_queries(queries=queries)
    benchmarker.benchmark_queries()
