import logging
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from .databases import ClickHouseHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)

logger = logging.getLogger("Bench")


class Benchmarker(object):
    """
    Benchmarker class for executing and benchmarking SQL queries across multiple
    database implementations.

    This class provides mechanisms to define database handlers, load data from
    a source, execute a set of predefined queries across databases, collect
    benchmark metrics, and analyze the results.

    :ivar results: A collection of benchmark results.
    :type results: list
    :ivar database_handlers: A dictionary containing database handler objects,
        where keys are string identifiers for databases and values are the
        database handler instances.
    :type database_handlers: dict
    :ivar data: The dataset that is loaded from a provided URL to run queries on.
    :type data: pandas.DataFrame or None
    :ivar queries: A list of SQL queries to execute during benchmarking.
    :type queries: list or None
    """
    def __init__(self):
        """
        Represents a class for managing results, database handlers, data, and queries.

        This class initializes with default attributes to store results,
        handlers for database management, data to be processed, and queries
        to be executed.

        Attributes:
            results (list): A list to store the operation results.
            database_handlers (dict): A dictionary to manage database handler mappings.
            data: Variable to hold general data inputs or resources.
            queries: Variable to hold query information for processing.
        """
        self.results = []
        self.database_handlers = dict
        self.data = None
        self.queries = None

    def define_database_handlers(self, database_handlers: dict):
        """
        Defines the database handlers for the given instance. This method assigns
        the provided dictionary of database handlers to the instance variable
        `database_handlers`. It ensures that the handlers necessary for
        database operations are properly set up.

        :param database_handlers: A dictionary where the keys represent database identifiers,
            and the values are the corresponding handler instances responsible for
            handling database operations.
        :type database_handlers: dict
        :return: None
        """
        self.database_handlers = database_handlers

    def get_data(self, url: str):
        """
        Fetches and processes data from a given URL.

        This function reads a CSV file from the specified URL into a pandas
        DataFrame, converts data types to their best possible types, and
        stores the processed DataFrame into the `data` attribute of the class.

        :param url: The URL of the CSV file to be read.
        :type url: str

        :return: None
        """
        self.data = pd.read_csv(url, header=0).convert_dtypes()

    def define_queries(self, queries: list):
        """
        Defines a set of queries to be assigned to the object.

        :param queries: A list of queries to be associated with the object.
        :type queries: list
        """
        self.queries = queries

    def benchmark_queries(self):
        """
        Benchmarks queries across multiple database handlers and collects their performance
        metrics.

        This method iterates over the provided database handlers to benchmark SQL queries
        defined within the class. It manages the lifecycle of database containers, connects
        to them, loads required data, executes the queries while measuring performance, and
        gathers the results.

        :raises RuntimeError: If a critical error occurs during setup or query execution
          for any database.
        :param self: The class instance containing the configuration data and queries.
        :return: A DataFrame object containing the compiled performance metrics of all queries
          executed against the respective database handlers.
        :rtype: pandas.DataFrame
        """
        all_metrics = []

        for database_name, database_handler in self.database_handlers.items():
            logger.info(f"Benchmarking {database_name}...")

            # Start the database container
            database_handler.start()

            try:
                # Connect to database
                engine = create_engine(database_handler.sqlalchemy_connection_string)
                with engine.connect() as conn:
                    # Load data
                    self._load_data_to_database(database_handler, conn)

                    # Run each query and collect metrics
                    for query in self.queries:
                        logger.info(f"Running query: {query[:60]}...")
                        result, metrics = database_handler.run_query_with_metrics(query)
                        all_metrics.append(metrics.to_dict())

                        # Log some sample results
                        if not result.empty:
                            sample_size = min(5, len(result))
                            logger.info(f"Sample result ({len(result)} rows total):\n{result.head(sample_size)}")

            except Exception as e:
                logger.error(f"Error benchmarking {database_name}: {e}")
            finally:
                # Stop and remove the container
                database_handler.stop(remove=True)

        # Convert metrics to DataFrame for analysis
        self.metrics_df = pd.DataFrame(all_metrics)

        # Display summary of results
        self._summarize_results()

        return self.metrics_df

    def _load_data_to_database(self, database_handler, conn):
        """
        Load data into a specified database using the provided database handler and connection.

        This method performs the necessary operations to load a dataset into the
        provided database. For databases like ClickHouse, which require special
        engine definitions, it handles creating the appropriate table with its
        engine before loading the data. For other databases, the standard
        `to_sql` method is used to load the data. The process also includes
        a verification step to ensure that the data has been loaded successfully.

        :param database_handler: The handler object to interact with the database.
            It supports operations such as creating tables and loading data.
        :type database_handler: Union[ClickHouseHandler, Any]
        :param conn: The active connection to the target database where operations
            are performed.
        :type conn: sqlalchemy.engine.base.Connection
        :return: None
        """
        logger.info(f"Loading data to {database_handler.__class__.__name__}...")

        # Special handling for ClickHouse which requires an engine definition
        if isinstance(database_handler, ClickHouseHandler):
            # Create table with engine before loading data
            columns = ", ".join(
                [f"`{col}` {self._get_clickhouse_type(self.data[col])}"
                 for col in self.data.columns]
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

        # Verify data was loaded correctly
        count_query = "SELECT COUNT(*) FROM data"
        count_result = conn.execute(text(count_query)).scalar()
        logger.info(f"Loaded {count_result} rows into the database")

    def _summarize_results(self):
        """
        Generates a summarized benchmark performance report by analyzing the collected
        metrics. Logs an informational summary of average, minimum, and maximum statistics
        grouped by database type. Additionally, identifies and logs the fastest database for
        each query based on execution time.

        This function assumes that the `metrics_df` attribute exists and contains valid
        benchmark data. If no metrics are available or if the dataframe is empty, a warning
        is logged and the function exits early.

        :return: None
        """
        if not hasattr(self, 'metrics_df') or self.metrics_df.empty:
            logger.warning("No metrics collected")
            return

        logger.info("\n===== BENCHMARK SUMMARY =====")

        # Group by database type and calculate averages
        summary = self.metrics_df.groupby('database_type').agg({
            'execution_time_ms': ['mean', 'min', 'max'],
            'cpu_usage_percent': 'mean',
            'memory_usage_mb': 'mean',
            'disk_read_mb': 'sum',
            'disk_write_mb': 'sum'
        })

        logger.info(f"\nPerformance Summary:\n{summary}")

        # Find the fastest database for each query
        for query in self.metrics_df['query'].unique():
            query_df = self.metrics_df[self.metrics_df['query'] == query]
            fastest = query_df.loc[query_df['execution_time_ms'].idxmin()]
            logger.info(f"\nFastest for query '{query[:50]}...': {fastest['database_type']} " +
                        f"({fastest['execution_time_ms']:.2f}ms)")

    def save_metrics_to_csv(self, filename: str = "benchmark_results.csv"):
        """
        Saves the benchmark metrics to a CSV file.

        This method checks if the `metrics_df` attribute exists and is non-empty, and
        if so, saves the DataFrame's contents to a specified CSV file. If `metrics_df`
        is not defined or empty, a warning is logged indicating that there are no
        metrics to save.

        :param filename: The name of the CSV file to which metrics will be saved.
        :type filename: str
        :return: None
        """
        if hasattr(self, 'metrics_df') and not self.metrics_df.empty:
            self.metrics_df.to_csv(filename, index=False)
            logger.info(f"Benchmark results saved to {filename}")
        else:
            logger.warning("No metrics to save")

    def _get_clickhouse_type(self, series):
        """
        Determines the ClickHouse data type based on the data type of a pandas Series.

        This function evaluates the dtype of a pandas Series and returns the corresponding
        ClickHouse data type as a string. The mapping is defined as follows:
        - If the dtype contains "int", it returns "Int64".
        - If the dtype contains "float", it returns "Float64".
        - For any other dtype, it defaults to "String".

        :param series: pandas Series whose data type will be inspected to determine the matching
            ClickHouse data type.
        :type series: pandas.Series
        :return: Corresponding ClickHouse data type as a string. Options include "Int64",
            "Float64", or "String".
        :rtype: str
        """
        dtype = str(series.dtype)
        if "int" in dtype:
            return "Int64"
        elif "float" in dtype:
            return "Float64"
        else:
            return "String"
