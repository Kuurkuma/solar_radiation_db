import logging
import sys
import pandas as pd
from sqlalchemy import create_engine, text
import kagglehub
from kagglehub import KaggleDatasetAdapter
from .databases import ClickHouseHandler, QueryMetrics

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
        df = pd.read_csv(url, header=0).convert_dtypes()
        df.columns = [''.join(char for char in s.lower() if char.islower()) for s in df.columns]
        self.data = df

        self.data = pd.read_csv(url, header=0).convert_dtypes()
        
    def get_local_csv(self, path:str):
        """
        Reads a local CSV file and stores it in the 'data' attribute of the class.

        :param path: The path to the local CSV file to be read.
        :type path: str

        :return: None
        """
        self.data = pd.read_csv(path, parse_dates=["time"], infer_datetime_format=True).convert_dtypes()

    def get_data_from_kaggle(self, handle: str, path: str):
        # Login to Kaggle
        kagglehub.login()

        try:
            # Download the file using the proper method
            file_path = kagglehub.dataset_download(handle, path=path)
            logger.info(f"Downloaded file to: {file_path}")

            # Try to load the file with pandas
            try:
                # First try standard JSON
                self.data = pd.read_json(file_path)
                result = "standard JSON"
            except ValueError:
                # If that fails, try JSONL format
                try:
                    self.data = pd.read_json(file_path, lines=True)
                    result = "JSONL"
                except ValueError:
                    # Last resort: manual JSON fix
                    with open(file_path, 'r') as f:
                        content = f.read().strip()
                        # Fix common JSON issues
                        if content.endswith(','):
                            content = content[:-1]
                        if '[' in content and not content.endswith(']'):
                            content += ']'
                        result = "manual JSON"

                    # Parse the fixed content
                    import json
                    from io import StringIO
                    fixed_json = json.loads(content)
                    self.data = pd.DataFrame(fixed_json)

            logger.info(f"Successfully loaded {len(self.data)} rows with {result} format")

        except Exception as e:
            logger.error(f"Error loading Kaggle dataset: {e}")
            # Fallback to iris dataset if all else fails
            self.data = pd.read_csv("https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv")
            logger.info(f"Fallback: Loaded {len(self.data)} rows from iris dataset")

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
                        try:
                            result, metrics = database_handler.run_query_with_metrics(query)
                            all_metrics.append(metrics.to_dict())
                    
                            # Log some sample results
                            if not result.empty:
                                sample_size = min(5, len(result))
                                logger.info(
                                    f"Sample result ({len(result)} rows total):\n{result.head(sample_size)}"
                                )
                        except Exception as e:
                            logger.error(f"Error running query '{query[:60]}...': {e}")
                            # Create a failed metrics entry
                            failed_metrics = QueryMetrics(query=query, original_query=query, 
                                                        database_type=database_handler.__class__.__name__)
                            failed_metrics.failed = True
                            all_metrics.append(failed_metrics.to_dict())

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

    def _create_clickhouse_table(self, conn, table_name='data'):
        """
        Create a ClickHouse table with correct column types

        Args:
            conn: SQLAlchemy connection
            table_name (str, optional): Name of the table. Defaults to 'data'.
        """

        type_mapping = {
            # Integer types
            'Int8': 'Int8',
            'Int16': 'Int16',
            'Int32': 'Int32',
            'Int64': 'Int64',
            'UInt8': 'UInt8',
            'UInt16': 'UInt16',
            'UInt32': 'UInt32',
            'UInt64': 'UInt64',

            # Floating point types
            'Float32': 'Float32',
            'Float64': 'Float64',

            # String types
            'object': 'String',
            'string': 'String',

            # Date and datetime types
            'datetime64[ns]': 'DateTime',
            'datetime64': 'DateTime',
            'date': 'Date',
        }
        columns_list = []
        logger.info(f"{self.data.items}")
        logger.info(f"{self.data.dtypes}")
        for column_name, column_data in self.data.items():
            logger.info(f"`{column_name}` {type_mapping[str(column_data.dtype)]}")
            columns_list.append(f"`{column_name}` {type_mapping[str(column_data.dtype)]}")


        logger.info(f"Creating ClickHouse table '{table_name}' with columns: {columns_list}")
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {", ".join(columns_list)}
            ) ENGINE = Memory
        """
        logger.info(f"SQL: {create_table_sql}")
        # Execute table creation
        conn.execute(text(create_table_sql))

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

            self._create_clickhouse_table(conn=conn, table_name='data')
            # Now we can load the data
            self.data.to_sql(con=conn, name="data", if_exists="append", index=False)
        else:
            # For other databases, use the standard method
            self.data.to_sql(con=conn, name="data", if_exists="replace", index=False)

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
        if not hasattr(self, "metrics_df") or self.metrics_df.empty:
            logger.warning("No metrics collected")
            return

        logger.info("\n===== BENCHMARK SUMMARY =====")

        # Count failed queries by database type
        if 'failed' in self.metrics_df.columns:
            failed_counts = self.metrics_df.groupby(["database_type", "failed"]).size().unstack(fill_value=0)
            if 1 in failed_counts.columns or True in failed_counts.columns:
                failed_col = 1 if 1 in failed_counts.columns else True
                logger.info(f"\nFailed Queries by Database Type:\n{failed_counts[failed_col]}")
    
        # Group by database type and calculate averages (only for successful queries)
        if 'failed' in self.metrics_df.columns:
            successful_metrics = self.metrics_df[~self.metrics_df['failed']]
        else:
            successful_metrics = self.metrics_df
        
        if not successful_metrics.empty:
            summary = successful_metrics.groupby("database_type").agg(
                {
                    "execution_time_ms": ["mean", "min", "max"],
                    "cpu_usage_percent": "mean",
                    "memory_usage_mb": "mean",
                    "disk_read_mb": "sum",
                    "disk_write_mb": "sum",
                }
            )
    
            logger.info(f"\nPerformance Summary:\n{summary}")
    
            # Find the fastest database for each query
            for query in successful_metrics["query"].unique():
                query_df = successful_metrics[successful_metrics["query"] == query]
                if not query_df.empty:
                    fastest = query_df.loc[query_df["execution_time_ms"].idxmin()]
                    logger.info(
                        f"\nFastest for query '{query[:50]}...': {fastest['database_type']} "
                        + f"({fastest['execution_time_ms']:.2f}ms)"
                    )
        
        # Report queries that failed for all database types
        if 'failed' in self.metrics_df.columns:
            failed_queries = self.metrics_df.groupby('original_query')['failed'].sum()
            completely_failed = failed_queries[failed_queries == len(self.database_handlers)]
            if not completely_failed.empty:
                logger.info("\nQueries that failed across all database types:")
                for query in completely_failed.index:
                    logger.info(f"- {query}")

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
        if hasattr(self, "metrics_df") and not self.metrics_df.empty:
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

        type_mapping = {
            # Integer types
            'int8': 'Int8',
            'int16': 'Int16',
            'int32': 'Int32',
            'int64': 'Int64',
            'uint8': 'UInt8',
            'uint16': 'UInt16',
            'uint32': 'UInt32',
            'uint64': 'UInt64',

            # Floating point types
            'Float32': 'Float32',
            'Float64': 'Float64',

            # String types
            'object': 'String',
            'string': 'String',

            # Date and datetime types
            'datetime64[ns]': 'DateTime',
            'datetime64': 'DateTime',
            'date': 'Date',
        }
        return type_mapping.get(dtype)