import time
import sys
import docker
from docker.models.containers import Container
from typing import Optional, Dict, List, Any, Tuple
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import psutil

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)

logger = logging.getLogger("databases")


class QueryMetrics:
    """
    Represents metrics related to the execution of a database query.

    The ``QueryMetrics`` class holds details of a database query's performance,
    such as execution time, CPU usage, memory consumption, disk read/write operations,
    network activity, and result data metrics. It enables tracking and conversion
    of these metrics into dictionary format for further analysis or reporting.

    :ivar query: The SQL query being executed.
    :type query: str
    :ivar database_type: The type/name of the database where the query is executed (e.g., MySQL, PostgreSQL).
    :type database_type: str
    :ivar execution_time_ms: The time taken to execute the query in milliseconds.
    :type execution_time_ms: int
    :ivar cpu_usage_percent: The percentage of CPU used during the query's execution.
    :type cpu_usage_percent: int
    :ivar memory_usage_mb: The amount of memory consumed during the query's execution in megabytes.
    :type memory_usage_mb: int
    :ivar memory_usage_percent: The percentage of memory used relative to available memory.
    :type memory_usage_percent: int
    :ivar disk_read_mb: The amount of data read from the disk in megabytes during the query's execution.
    :type disk_read_mb: int
    :ivar disk_write_mb: The amount of data written to the disk in megabytes during the query's execution.
    :type disk_write_mb: int
    :ivar network_in_mb: The amount of incoming network traffic in megabytes during the query's execution.
    :type network_in_mb: int
    :ivar network_out_mb: The amount of outgoing network traffic in megabytes during the query's execution.
    :type network_out_mb: int
    :ivar result_rows: The number of rows included in the query result.
    :type result_rows: int
    :ivar result_size_mb: The size of the query results in megabytes.
    :type result_size_mb: int
    """

    def __init__(self, query: str, database_type: str):
        self.query = query
        self.database_type = database_type
        self.execution_time_ms = 0
        self.cpu_usage_percent = 0
        self.memory_usage_mb = 0
        self.memory_usage_percent = 0
        self.disk_read_mb = 0
        self.disk_write_mb = 0
        self.network_in_mb = 0
        self.network_out_mb = 0
        self.result_rows = 0
        self.result_size_mb = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            'query': self.query,
            'database_type': self.database_type,
            'execution_time_ms': self.execution_time_ms,
            'cpu_usage_percent': self.cpu_usage_percent,
            'memory_usage_mb': self.memory_usage_mb,
            'memory_usage_percent': self.memory_usage_percent,
            'disk_read_mb': self.disk_read_mb,
            'disk_write_mb': self.disk_write_mb,
            'network_in_mb': self.network_in_mb,
            'network_out_mb': self.network_out_mb,
            'result_rows': self.result_rows,
            'result_size_mb': self.result_size_mb
        }


class DockerDatabaseHandler:
    """
    Manages the lifecycle and metrics of a Docker-based database container.

    This class is designed to handle Docker containers for database systems.
    It provides methods to manage container lifecycle (start, stop, etc.),
    execute SQL queries, and collect runtime metrics such as CPU usage,
    memory usage, disk I/O, and network transfer statistics. The class
    relies heavily on Docker's Python SDK and requires the Docker engine
    to be properly configured on the host system.

    :ivar image: Name of the Docker image for the database.
    :type image: str
    :ivar tag: Tag of the Docker image, defaults to "latest".
    :type tag: str
    :ivar name: Name of the container. If not provided, Docker generates a random name.
    :type name: Optional[str]
    :ivar port_mapping: Mapping of container ports to host ports.
    :type port_mapping: Optional[Dict[int, int]]
    :ivar environment: Environment variables to pass to the container.
    :type environment: Optional[Dict[str, str]]
    :ivar volumes: List of volume mappings for the container.
    :type volumes: Optional[List[str]]
    :ivar cpu_limit: Maximum CPU cores allocated to the container.
    :type cpu_limit: float
    :ivar memory_limit: Maximum memory allocated to the container, e.g., "2g".
    :type memory_limit: str
    :ivar container: A reference to the Docker container object. None if no container exists.
    :type container: Optional[Container]
    :ivar client: Docker client instance to interact with Docker API.
    :type client: docker.client.DockerClient
    :ivar host: Hostname or IP for connecting to the database.
    :type host: str
    :ivar username: Username for authenticating with the database (set by subclasses).
    :type username: Optional[str]
    :ivar password: Password for authenticating with the database (set by subclasses).
    :type password: Optional[str]
    :ivar database: Target database name for connections (set by subclasses).
    :type database: Optional[str]
    :ivar port: Port to use for database connections (set by subclasses).
    :type port: Optional[int]
    """

    def __init__(
        self,
        image: str,
        tag: str = "latest",
        name: str = None,
        port_mapping: Dict[int, int] = None,
        environment: Dict[str, str] = None,
        volumes: List[str] = None,
        cpu_limit: float = 1.0,
        memory_limit: str = "2g",
    ):
        """
        Initializes a container instance with specified configurations for image,
        tag, resource constraints, environment variables, and volume mappings.
        Provides defaults for optional parameters and establishes client
        connection with the Docker environment. Also includes attributes for
        database connection setup.

        :param image: The name of the container image to be used.
        :param tag: The tag of the image, defaults to 'latest'.
        :param name: An optional name for the container instance.
        :param port_mapping: A mapping of host ports to container ports
            specified as a dictionary of integers.
        :param environment: Environment variables for the container as a
            dictionary of key-value pairs.
        :param volumes: List of volume paths to bind, mapping host paths to
            container paths.
        :param cpu_limit: CPU resource limit for the container, specified as
            a float value (e.g., 1.0 means one CPU core).
        :param memory_limit: Memory resource limit for the container, specified
            as a string in the format like '2g' (2 gigabytes).
        """
        self.image = image
        self.tag = tag
        self.name = name
        self.port_mapping = port_mapping or {}
        self.environment = environment or {}
        self.volumes = volumes or []
        self.cpu_limit = cpu_limit
        self.memory_limit = memory_limit
        self.container: Optional[Container] = None
        self.client = docker.from_env()

        # Database connection properties
        self.host = "localhost"
        self.username = None
        self.password = None
        self.database = None
        self.port = None

    def start(self, wait_time: int = 30) -> None:
        """
        Starts the container if it is not already running. This method initiates the
        container with the configured environment, ports, volumes, CPU, and memory
        settings. If the container is already running, a log entry is produced, and
        no further action is taken. Otherwise, the container is started, and a wait
        process ensures the container becomes ready for use.

        :param wait_time: The maximum time to wait (in seconds) for the container to
            be ready after starting. Default is 30 seconds.
        :return: This method does not return any value.
        """
        if self.is_running():
            logger.info(f"Container {self.name} is already running")
            return

        ports = {
            f"{port}/tcp": host_port for port, host_port in self.port_mapping.items()
        }

        # Create and start container
        self.container = self.client.containers.run(
            f"{self.image}:{self.tag}",
            name=self.name,
            detach=True,
            environment=self.environment,
            ports=ports,
            volumes=self.volumes,
            cpu_quota=int(
                self.cpu_limit * 100000
            ),  # Docker uses CPU quota in microseconds
            mem_limit=self.memory_limit,
        )

        logger.info(f"Started container: {self.name} ({self.container.id[:12]})")

        # Wait for container to be ready
        self._wait_for_ready(wait_time)

    def stop(self, remove: bool = True) -> None:
        """
        Stops the running container associated with this instance. If the 'remove'
        parameter is set to True, the container will also be removed after stopping.
        Logs are generated for both stopping and removal of the container.

        :param remove: A boolean flag indicating whether to remove the container
            after stopping it. Defaults to True.
        :return: None
        """
        if not self.is_running():
            logger.info(f"Container {self.name} is not running")
            return

        self.container.stop()
        logger.info(f"Stopped container: {self.name}")

        if remove:
            self.container.remove()
            logger.info(f"Removed container: {self.name}")
            self.container = None

    def is_running(self) -> bool:
        """
        Checks if the container is currently running.

        This method verifies the running state of a container associated with the
        current object. If the container attribute is not already set, it attempts
        to retrieve the container using the client. If the container does not exist,
        it returns `False`. Otherwise, it reloads the container state to ensure the
        status is up-to-date and checks if the status is "running".

        :raises docker.errors.NotFound: If the container does not exist and fails to
            be retrieved via the client.
        :return: A boolean indicating whether the container is running or not.
        :rtype: bool
        """
        if not self.container:
            try:
                self.container = self.client.containers.get(self.name)
            except docker.errors.NotFound:
                return False

        self.container.reload()
        return self.container.status == "running"

    def _wait_for_ready(self, timeout: int) -> None:
        """
        Waits for the database to become ready within the given timeout duration.

        This method periodically checks whether the database is ready by calling the ``_is_db_ready``
        method. It continues the check in a loop until either the specified timeout duration
        is exceeded or the database is confirmed to be ready. The method logs the readiness status
        or a timeout warning as appropriate.

        :param timeout: The maximum number of seconds to wait for the database to become ready.
        :type timeout: int
        :return: This method does not return any value.
        :rtype: None
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self._is_db_ready():
                logger.info(f"{self.__class__.__name__} is ready")
                return
            time.sleep(1)

        logger.info(
            f"Warning: Timed out waiting for {self.__class__.__name__} to be ready"
        )

    def _is_db_ready(self) -> bool:
        """
        Checks if the database connection is ready.

        This method should be overridden by subclasses to provide specific
        logic for determining the readiness of a database connection. It is
        meant to be used internally and should not be called directly without
        an override.

        :raises NotImplementedError: If the method is not implemented in a subclass.
        :return: Whether the database is ready.
        :rtype: bool
        """
        raise NotImplementedError("Subclasses must implement _is_db_ready")

    @property
    def sqlalchemy_connection_string(self) -> str:
        """
        Retrieves the SQLAlchemy connection string. This property must be implemented by
        all subclasses, as it is essential for defining how the connection to the database
        is established. Subclasses should provide a valid connection string necessary to
        connect to the intended database using SQLAlchemy.

        :raises NotImplementedError: Raised when the subclass does not implement
            the property.
        :return: The SQLAlchemy connection string used to connect to the database.
        :rtype: str
        """
        raise NotImplementedError(
            "Subclasses must implement sqlalchemy_connection_string"
        )

    def run_query_with_metrics(self, query: str) -> Tuple[pd.DataFrame, QueryMetrics]:
        """
        Executes a SQL query on a database container and collects various performance metrics,
        such as execution time, CPU and memory usage, disk I/O, and network usage.

        :param query: SQL query string to be executed.
        :type query: str
        :return: A tuple containing the result of the SQL query as a DataFrame and the collected
            performance metrics as a QueryMetrics instance.
        :rtype: Tuple[pd.DataFrame, QueryMetrics
        :raises RuntimeError: Raised when the database container is not running.
        :raises Exception: Propagates any exception encountered during the query execution.
        """
        if not self.is_running():
            raise RuntimeError(f"Container {self.name} is not running")

        metrics = QueryMetrics(query=query, database_type=self.__class__.__name__)

        # Initialize stats collection
        self.container.reload()
        prev_stats = self._get_container_stats()

        # Connect and execute query
        engine = create_engine(self.sqlalchemy_connection_string)
        start_time = time.time()

        try:
            with engine.connect() as conn:
                # Execute the query and measure time
                result = pd.read_sql_query(query, conn)
                metrics.result_rows = len(result)
                metrics.result_size_mb = result.memory_usage(deep=True).sum() / (1024 * 1024)
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
        finally:
            # Calculate execution time
            end_time = time.time()
            metrics.execution_time_ms = (end_time - start_time) * 1000

            # Get final stats
            self.container.reload()
            curr_stats = self._get_container_stats()

            # Calculate resource usage
            metrics.cpu_usage_percent = curr_stats['cpu_percent']
            metrics.memory_usage_mb = curr_stats['memory_usage_mb']
            metrics.memory_usage_percent = curr_stats['memory_percent']

            # Calculate I/O differences
            metrics.disk_read_mb = (curr_stats['block_read'] - prev_stats['block_read']) / (1024 * 1024)
            metrics.disk_write_mb = (curr_stats['block_write'] - prev_stats['block_write']) / (1024 * 1024)
            metrics.network_in_mb = (curr_stats['network_in'] - prev_stats['network_in']) / (1024 * 1024)
            metrics.network_out_mb = (curr_stats['network_out'] - prev_stats['network_out']) / (1024 * 1024)

        logger.info(f"Query executed in {metrics.execution_time_ms:.2f}ms, " +
                    f"CPU: {metrics.cpu_usage_percent:.2f}%, " +
                    f"Memory: {metrics.memory_usage_mb:.2f}MB ({metrics.memory_usage_percent:.2f}%)")

        return result, metrics

    def _get_container_stats(self) -> Dict[str, float]:
        """
        Retrieve comprehensive performance statistics of a container.

        This method collects and computes detailed metrics on CPU, memory,
        block I/O, and network statistics of a container. If any error
        occurs during data retrieval, the returned stats are initialized
        to zero. The returned data is represented as a dictionary, where
        each metric is associated with a descriptive key.

        :raises Exception: If an error occurs when attempting to gather
            container statistics, an error message is logged.

        :return: A dictionary containing container performance statistics
            including CPU usage percentage, memory usage in megabytes,
            percentage memory usage, total block reads and writes, and
            network input and output traffic in bytes.

        :rtype: Dict[str, float]
        """
        try:
            stats = self.container.stats(stream=False)

            # Extract CPU usage
            cpu_delta = stats['cpu_stats']['cpu_usage']['total_usage'] - \
                        stats['precpu_stats']['cpu_usage']['total_usage']
            system_delta = stats['cpu_stats']['system_cpu_usage'] - \
                           stats['precpu_stats']['system_cpu_usage']
            online_cpus = stats['cpu_stats'].get('online_cpus', len(psutil.cpu_percent(percpu=True)))

            cpu_percent = 0.0
            if system_delta > 0 and cpu_delta > 0:
                cpu_percent = (cpu_delta / system_delta) * online_cpus * 100.0

            # Extract memory usage
            memory_usage = stats['memory_stats'].get('usage', 0)
            memory_limit = stats['memory_stats'].get('limit', 1)
            memory_percent = (memory_usage / memory_limit) * 100.0 if memory_limit > 0 else 0

            # Extract I/O usage
            block_stats = stats.get('blkio_stats', {}).get('io_service_bytes_recursive', [])
            block_read = sum(item['value'] for item in block_stats if item['op'] == 'Read')
            block_write = sum(item['value'] for item in block_stats if item['op'] == 'Write')

            # Extract network usage
            networks = stats.get('networks', {})
            network_in = sum(net_stats.get('rx_bytes', 0) for net_stats in networks.values())
            network_out = sum(net_stats.get('tx_bytes', 0) for net_stats in networks.values())

            return {
                'cpu_percent': cpu_percent,
                'memory_usage_mb': memory_usage / (1024 * 1024),
                'memory_percent': memory_percent,
                'block_read': block_read,
                'block_write': block_write,
                'network_in': network_in,
                'network_out': network_out
            }
        except Exception as e:
            logger.error(f"Failed to get container stats: {e}")
            return {
                'cpu_percent': 0,
                'memory_usage_mb': 0,
                'memory_percent': 0,
                'block_read': 0,
                'block_write': 0,
                'network_in': 0,
                'network_out': 0
            }


class MySQLHandler(DockerDatabaseHandler):
    """
    A handler for managing and interacting with MySQL database containers.

    This class is a specialized implementation for managing MySQL database
    containers using Docker. It provides mechanisms for initializing, running,
    and interacting with a MySQL container. This includes configuration of environment
    variables, port mappings, and resource limits. Additionally, it provides utility
    methods and properties for checking the database readiness and retrieving
    connection strings.

    :ivar username: Username for connecting to the MySQL database.
    :type username: str
    :ivar password: Password for the corresponding user to connect to the MySQL database.
    :type password: str
    :ivar database: Name of the database to connect to within the MySQL server.
    :type database: str
    :ivar port: The externally accessible host port for connecting to the database.
    :type port: int
    :ivar root_password: Root password for the MySQL server.
    :type root_password: str
    """

    def __init__(
        self,
        name: str = "mysql-db",
        port: int = 3306,
        root_password: str = "rootpassword",
        database: str = "testdb",
        user: str = "user",
        password: str = "password",
        tag: str = "8.0",
        cpu_limit: float = 1.0,
        memory_limit: str = "2g",
    ):
        """
        Initialize an instance of a MySQL container with customizable settings for database
        connection, resource limits, and MySQL configuration.

        :param name: Name of the container. Defaults to "mysql-db".
        :type name: str
        :param port: Port number for MySQL container on the host. Defaults to 3306.
        :type port: int
        :param root_password: Root user password for the MySQL database. Defaults to "rootpassword".
        :type root_password: str
        :param database: The name of the database to be created in the container. Defaults to "testdb".
        :type database: str
        :param user: The username for database access. Defaults to "user".
        :type user: str
        :param password: The password for the specified database user. Defaults to "password".
        :type password: str
        :param tag: Docker image tag for MySQL. Defaults to "8.0".
        :type tag: str
        :param cpu_limit: Maximum CPU resources allocated for the container. Defaults to 1.0.
        :type cpu_limit: float
        :param memory_limit: Maximum memory resources allocated for the container. Defaults to "2g".
        :type memory_limit: str
        """
        # Set up environment variables for MySQL
        environment = {
            "MYSQL_ROOT_PASSWORD": root_password,
            "MYSQL_DATABASE": database,
            "MYSQL_USER": user,
            "MYSQL_PASSWORD": password,
        }

        # Port mapping: container_port -> host_port
        port_mapping = {3306: port}

        super().__init__(
            image="mysql",
            tag=tag,
            name=name,
            port_mapping=port_mapping,
            environment=environment,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
        )

        # Set connection properties
        self.username = user
        self.password = password
        self.database = database
        self.port = port
        self.root_password = root_password

    def _is_db_ready(self) -> bool:
        """
        Checks if the database in the container is ready for use.

        The method determines the readiness of the database by verifying that the
        container is running, and then attempting to execute a simple query. If the
        query executes successfully, it indicates that the database is ready.
        Otherwise, it returns False.

        :return: True if the database is ready, False otherwise
        :rtype: bool
        """
        if not self.is_running():
            return False

        try:
            exit_code, _ = self.container.exec_run(
                f"mysql -u{self.username} -p{self.password} -e 'SELECT 1'", stderr=False
            )
            return exit_code == 0
        except Exception:
            return False

    @property
    def sqlalchemy_connection_string(self) -> str:
        """
        Provides the SQLAlchemy connection string for a MySQL database with the given
        username, password, host, port, and database name. This property constructs
        the connection string dynamically based on the instance's credentials and
        connection details.

        :return: A formatted SQLAlchemy connection string for a MySQL database.
        :rtype: str
        """
        return f"mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class PostgresHandler(DockerDatabaseHandler):
    """
    Manages a PostgreSQL database container using Docker.

    This class is a specific implementation of DockerDatabaseHandler that provisions
    and manages a PostgreSQL container with customizable settings such as
    port, user credentials, database name, resource limits, and more. It also
    provides utilities for checking database readiness and building a
    SQLAlchemy connection string.

    :ivar username: The username used to access the PostgreSQL database.
    :type username: str
    :ivar password: The password associated with the PostgreSQL user.
    :type password: str
    :ivar database: The name of the PostgreSQL database being used.
    :type database: str
    :ivar port: The port on the host machine mapped to the PostgreSQL container.
    :type port: int
    """

    def __init__(
        self,
        name: str = "postgres-db",
        port: int = 5432,
        user: str = "postgres",
        password: str = "postgres",
        database: str = "testdb",
        tag: str = "17",
        cpu_limit: float = 1.0,
        memory_limit: str = "2g",
    ):
        """
        Initializes a PostgreSQL database container with customizable configurations. The
        class provides the ability to define PostgreSQL user credentials, database name,
        port mappings, resource limits (CPU and memory), and the image tag for the
        PostgreSQL Docker container. The configuration ensures the environment variables
        and port mappings are properly set for the container runtime.

        :param name: The name assigned to the container.
        :param port: The host machine port to bind for PostgreSQL.
        :param user: Username for the PostgreSQL database.
        :param password: Password for the PostgreSQL database.
        :param database: Name of the database to be created.
        :param tag: The Docker image tag for the PostgreSQL version.
        :param cpu_limit: CPU allocation limit for the container.
        :param memory_limit: Memory allocation limit for the container.
        """
        # Set up environment variables for PostgreSQL
        environment = {
            "POSTGRES_USER": user,
            "POSTGRES_PASSWORD": password,
            "POSTGRES_DB": database,
        }

        # Port mapping: container_port -> host_port
        port_mapping = {5432: port}

        super().__init__(
            image="postgres",
            tag=tag,
            name=name,
            port_mapping=port_mapping,
            environment=environment,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
        )

        # Set connection properties
        self.username = user
        self.password = password
        self.database = database
        self.port = port

    def _is_db_ready(self) -> bool:
        """
        Checks if the database container is ready for accepting connections.

        This method verifies whether the PostgreSQL database identified by the
        configured username and database name is ready to accept connections.
        First, it checks if the container is running. If it is, the method
        executes the `pg_isready` command inside the container to confirm the
        readiness of the database. If any exception occurs during this
        verification process, it will return False.

        :raises Exception: If an issue occurs while executing the readiness
                           command within the database container.
        :return: True if the database container responds positively to the
                 readiness command, False otherwise.
        :rtype: bool
        """
        if not self.is_running():
            return False

        try:
            exit_code, _ = self.container.exec_run(
                f"pg_isready -U {self.username} -d {self.database}", stderr=False
            )
            return exit_code == 0
        except Exception:
            return False

    @property
    def sqlalchemy_connection_string(self) -> str:
        """
        This property constructs and returns the SQLAlchemy connection string
        utilizing the provided credentials and connection details. The
        connection string is formatted to facilitate connections to a PostgreSQL
        database using SQLAlchemy. All required values, such as username,
        password, host, port, and database name, are dynamically incorporated
        into the connection string.

        :return: The constructed PostgreSQL connection string in valid SQLAlchemy
                 format.
        :rtype: str
        """
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class ClickHouseHandler(DockerDatabaseHandler):
    """
    Manages a ClickHouse database container with specific configurations and environment
    settings.

    This class is intended for initializing, configuring, and managing the lifecycle of a
    ClickHouse database container. It provides utility functions for checking the readiness
    of the ClickHouse instance and generating an SQLAlchemy connection string for database
    operations. Usage of this class ensures easy interaction and resource management of
    ClickHouse database containers in a Dockerized environment.

    :ivar username: Username for authenticating with ClickHouse.
    :type username: str
    :ivar password: Password associated with the `username` for authentication.
    :type password: str
    :ivar database: Default database name in the ClickHouse instance.
    :type database: str
    :ivar http_port: Port for accessing the HTTP interface of ClickHouse.
    :type http_port: int
    :ivar tcp_port: Port for accessing the native TCP interface of ClickHouse.
    :type tcp_port: int
    """

    def __init__(
        self,
        name: str = "clickhouse-db",
        http_port: int = 8123,
        tcp_port: int = 9000,
        user: str = "default",
        password: str = "wazzzuuup",
        database: str = "default",
        tag: str = "latest",
        cpu_limit: float = 2.0,
        memory_limit: str = "4g",
    ):
        """
        Initializes a ClickHouse server configuration and deploys a containerized instance
        of ClickHouse with specified resource limits and connection properties. This class
        configures environment variables and port mappings based on input parameters to
        initialize the container runtime environment for the database server.

        :param name: The name assigned to the container.
        :type name: str
        :param http_port: Port on the host machine bound for HTTP access
                          to the ClickHouse server.
        :type http_port: int
        :param tcp_port: Port on the host machine bound for native TCP protocol
                         access to the ClickHouse server.
        :type tcp_port: int
        :param user: Username for accessing the ClickHouse server. Default is
                     "default".
        :type user: str
        :param password: Password corresponding to the provided user for
                         authentication.
        :type password: str
        :param database: Name of the default database to be created in the
                         ClickHouse server.
        :type database: str
        :param tag: Docker image tag for the ClickHouse server. Default is
                    "latest".
        :type tag: str
        :param cpu_limit: Maximum CPU resources to allocate (in cores)
                          for the ClickHouse container.
        :type cpu_limit: float
        :param memory_limit: Maximum memory allocation for the ClickHouse
                             container (e.g., "4g").
        :type memory_limit: str
        """
        # Set up environment variables for ClickHouse
        environment = {
            "CLICKHOUSE_USER": user,
            "CLICKHOUSE_PASSWORD": password,
            "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1",  # Enable access management
            "CLICKHOUSE_DB": database,
        }

        # Port mapping: container_port -> host_port
        port_mapping = {
            8123: http_port,  # HTTP port
            9000: tcp_port,  # Native TCP port
        }

        super().__init__(
            image="clickhouse/clickhouse-server",
            tag=tag,
            name=name,
            port_mapping=port_mapping,
            environment=environment,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
        )

        # Set connection properties
        self.username = user
        self.password = password
        self.database = database
        self.http_port = http_port
        self.tcp_port = tcp_port

    def _is_db_ready(self) -> bool:
        """
        Determines if the database is ready for connections and operations.

        This method checks if the database server is running and performs an
        HTTP POST request to verify readiness by executing a simple `SELECT 1`
        query. If the database is ready, the method returns `True`. Otherwise, it
        returns `False`. Ensures that potential errors are logged during the
        readiness check process.

        :raises ImportError: If the `requests` library is not available.
        :raises Exception: For any other unexpected errors during the readiness check.

        :return: Returns `True` if the database is ready and accessible, otherwise `False`
        :rtype: bool
        """
        if not self.is_running():
            return False

        try:
            import requests

            # Use HTTP interface instead of clickhouse-client
            url = f"http://{self.host}:{self.http_port}/?user={self.username}&password={self.password}"
            response = requests.post(url, data="SELECT 1")
            return response.status_code == 200
        except Exception as e:
            logger.info(f"ClickHouse readiness check error: {e}")
            return False

    @property
    def sqlalchemy_connection_string(self) -> str:
        """
        This property generates the SQLAlchemy connection string required to
        connect to a ClickHouse database using the clickhouse-sqlalchemy dialect.
        The connection string includes credentials, host details, and the database
        name, formatted to meet the requirements of SQLAlchemy.

        :return: A string representing the SQLAlchemy connection URL for ClickHouse.
        :rtype: str
        """
        # Using clickhouse-sqlalchemy dialect
        return f"clickhouse://{self.username}:{self.password}@{self.host}:{self.http_port}/{self.database}"


class DuckDBHandler(DockerDatabaseHandler):
    """
    Manages an ephemeral Docker container running DuckDB.

    This class provides functionality to manage a temporary Docker container
    with DuckDB installed. It enables the use of an in-memory database or
    a file-based database for transient operations. It abstracts container
    operations, initialization, and readiness checks for DuckDB, allowing
    easy integration into development or testing environments.

    :ivar db_file: Name of the DuckDB database file or ``:memory:`` for
                   in-memory operation.
    :type db_file: str
    :ivar database_path: Path within the container for the DuckDB database file.
    :type database_path: str
    :ivar username: Placeholder for compatibility with interfaces requiring
                    a username; not used in DuckDB.
    :type username: NoneType
    :ivar password: Placeholder for compatibility with interfaces requiring
                    a password; not used in DuckDB.
    :type password: NoneType
    :ivar database: Identifier for the DuckDB database; mirrors ``db_file``.
    :type database: str
    """

    def __init__(
        self,
        name: str = "duckdb-container",
        db_file: str = ":memory:",  # Use in-memory database by default
        tag: str = "3.11-slim",  # Using Python image for DuckDB
        cpu_limit: float = 1.0,
        memory_limit: str = "1g",
    ):
        """
        Initialize a class instance that configures and manages a containerized DuckDB
        database environment. The setup ensures an ephemeral and isolated DuckDB
        instance, initialized either in memory or using a provided database file. The
        class also enforces resource constraints such as CPU and memory consumption
        for optimal operation within containerized environments.

        :param name: Name of the container instance. Defaults to 'duckdb-container'.
        :type name: str
        :param db_file: Path to the DuckDB database file. Defaults to ':memory:' for an
            in-memory operation.
        :type db_file: str
        :param tag: Docker image tag to use. Defaults to '3.11-slim', generally a Python image.
        :type tag: str
        :param cpu_limit: Maximum CPU resources assignable to the container. Defaults to `1.0`.
        :type cpu_limit: float
        :param memory_limit: Maximum memory resources assignable to the container, in
            Docker-compatible format (e.g., '1g'). Defaults to '1g'.
        :type memory_limit: str
        """
        # No volumes needed for ephemeral operation
        super().__init__(
            image="python",
            tag=tag,
            name=name,
            volumes=None,  # No volumes
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
        )

        self.db_file = db_file

        # If using in-memory database
        if db_file == ":memory:":
            self.database_path = ":memory:"
        else:
            # Store in container's filesystem (will be lost when container is removed)
            self.database_path = f"/tmp/{db_file}"

        # DuckDB doesn't use traditional username/password
        self.username = None
        self.password = None
        self.database = db_file

    def start(self, wait_time: int = 30) -> None:
        """
        Starts a container for running a custom command to install DuckDB. If the container is
        already running, it logs the information and does nothing. Otherwise, a new container is
        started with specified resource limits (CPU and memory) and is configured to keep
        running persistently.

        :param wait_time: Amount of time, in seconds, to wait for DuckDB to be ready
        :type wait_time: int, optional
        """
        if self.is_running():
            logger.info(f"Container {self.name} is already running")
            return

        # Create and start container with custom command to install DuckDB
        self.container = self.client.containers.run(
            f"{self.image}:{self.tag}",
            name=self.name,
            detach=True,
            cpu_quota=int(self.cpu_limit * 100000),
            mem_limit=self.memory_limit,
            command="sh -c 'pip install duckdb && tail -f /dev/null'",  # Keep container running
        )

        logger.info(f"Started container: {self.name} ({self.container.id[:12]})")

        # Wait for DuckDB to be ready
        self._wait_for_ready(wait_time)

    def _is_db_ready(self) -> bool:
        """
        Determines if the database within the container is ready to be used.

        This method checks the status of the database by verifying if the container is
        running and attempting to execute a test operation using DuckDB. If the container
        is not running or if the test operation fails, the method will return False.

        :raises Exception: This method silently handles any exception raised during the
            execution of the DuckDB test, returning False.

        :return: A boolean indicating whether the database is ready to be used.
        :rtype: bool
        """
        if not self.is_running():
            return False

        try:
            # Wait for pip install to complete and test DuckDB
            exit_code, _ = self.container.exec_run(
                f"python -c \"import duckdb; conn = duckdb.connect('{self.database_path}'); conn.execute('CREATE TABLE IF NOT EXISTS test (id INTEGER); DROP TABLE test;'); conn.close()\"",
                stderr=False,
            )
            return exit_code == 0
        except Exception:
            return False

    @property
    def sqlalchemy_connection_string(self) -> str:
        """
        Provides the SQLAlchemy connection string for a DuckDB database.

        This property dynamically constructs and returns the connection string
        required for SQLAlchemy to interact with a DuckDB database. The constructed
        connection string uses the `duckdb_engine` dialect and ensures compatibility
        for database operations.

        :rtype: str
        :return: A SQLAlchemy connection string formatted for the DuckDB database.
        """
        # Using duckdb_engine dialect
        return f"duckdb:///{self.database_path}"


# Example usage
if __name__ == "__main__":
    # Example for MySQL

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

    for database_name, database_handler in databases.items():
        logger.info(f"testing {database_name}")

        try:
            database_handler.start()
            logger.info(
                f"{database_name} Connection String: {database_handler.sqlalchemy_connection_string}"
            )
            logger.info(f"{database_name} is running!")
            # Do your database operations here

            try:
                # Example SQLAlchemy usage
                engine = create_engine(database_handler.sqlalchemy_connection_string)
                with engine.connect() as conn:

                    if database_name != "clickhouse":
                        # Create a sample table
                        conn.execute(
                            text(
                                "CREATE TABLE IF NOT EXISTS test_table (id INT, name VARCHAR(255))"
                            )
                        )
                    else:  # For ClickHouse
                        conn.execute(
                            text(
                                """
                            CREATE TABLE IF NOT EXISTS test_table
                            (
                                id
                                UInt32,
                                name
                                String
                            ) ENGINE = MergeTree
                            (
                            )
                                ORDER BY id
                            """
                            )
                        )

                    # Insert sample data
                    conn.execute(
                        text(
                            "INSERT INTO test_table (id, name) VALUES (1, 'Test Record')"
                        )
                    )

                    # Query the data
                    result = conn.execute(text("SELECT * FROM test_table"))
                    logger.info("Sample query result:", result.fetchall())

                    # Clean up
                    conn.execute(text("DROP TABLE test_table"))

                logger.info("SQLAlchemy connection test passed!")
            except SQLAlchemyError as e:
                logger.error(f"SQLAlchemy connection test failed with error: {e}")
        except Exception as e:
            logger.error(f"Could startup handler {database_name}: with error {e}")
        finally:
            database_handler.stop(remove=True)
