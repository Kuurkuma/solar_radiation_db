import logging
import sys
from bench.databases import (
    MySQLHandler,
    PostgresHandler,
    ClickHouseHandler,
    DuckDBHandler,
)
from bench.bench import Benchmarker


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)

logger = logging.getLogger("main")


if __name__ == "__main__":
    benchmarker = Benchmarker()

    databases = {
        "mysql": MySQLHandler(name="test-mysql", port=3306, cpu_limit=2, memory_limit="4G", sql_dialect="mysql"),
        "postgres": PostgresHandler(name="test-postgres", port=5432, cpu_limit=2, sql_dialect="postgres"),
        "duckdb": DuckDBHandler(
            name="test-duckdb", db_file="duckdb_data.db", cpu_limit=2, sql_dialect="duckdb"
        ),
        "clickhouse": ClickHouseHandler(
            name="test-clickhouse", http_port=8124, tcp_port=9001, cpu_limit=2, sql_dialect="clickhouse"
        ),
    }

    benchmarker.define_database_handlers(database_handlers=databases)

    # Load the iris dataset
    benchmarker.get_data(
        url="https://gist.githubusercontent.com/netj/8836201/raw/6f9306ad21398ea43cba4f7d537619d0e07d5ae3/iris.csv"
    )

    # Define a set of test queries with varying complexity
    queries = [
        "SELECT COUNT(*) FROM data;",
        "SELECT * FROM data LIMIT 10;",
        "SELECT AVG(sepalwidth) FROM data GROUP BY variety;",
        "SELECT variety, COUNT(*) FROM data GROUP BY variety ORDER BY COUNT(*) DESC;",
        "SELECT * FROM data WHERE sepallength > 5.0 ORDER BY petallength DESC LIMIT 20;",
    ]

    benchmarker.define_queries(queries=queries)

    # Run the benchmark
    results_df = benchmarker.benchmark_queries()

    # Save results to CSV
    benchmarker.save_metrics_to_csv("database_benchmark_results.csv")
