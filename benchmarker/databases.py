import time
import sys
import docker
from docker.models.containers import Container
from typing import Optional, Dict, Any, List, Tuple
import logging
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout
)

logger = logging.getLogger("databases")


class DockerDatabaseHandler:
    """Base class for managing database containers."""

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
        Initialize a database container manager.

        Args:
            image: Docker image name
            tag: Docker image tag
            name: Container name (optional)
            port_mapping: Dictionary mapping container ports to host ports
            environment: Environment variables for the container
            volumes: List of volume mappings
            cpu_limit: CPU limit (in cores)
            memory_limit: Memory limit (e.g., "1g" for 1 gigabyte)
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
        Start the database container.

        Args:
            wait_time: Time to wait for container to be ready (seconds)
        """
        if self.is_running():
            logger.info(f"Container {self.name} is already running")
            return

        ports = {f"{port}/tcp": host_port for port, host_port in self.port_mapping.items()}

        # Create and start container
        self.container = self.client.containers.run(
            f"{self.image}:{self.tag}",
            name=self.name,
            detach=True,
            environment=self.environment,
            ports=ports,
            volumes=self.volumes,
            cpu_quota=int(self.cpu_limit * 100000),  # Docker uses CPU quota in microseconds
            mem_limit=self.memory_limit,
        )

        logger.info(f"Started container: {self.name} ({self.container.id[:12]})")

        # Wait for container to be ready
        self._wait_for_ready(wait_time)

    def stop(self, remove: bool = True) -> None:
        """
        Stop the database container.

        Args:
            remove: Whether to remove the container after stopping
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
        """Check if the container is running."""
        if not self.container:
            try:
                self.container = self.client.containers.get(self.name)
            except docker.errors.NotFound:
                return False

        self.container.reload()
        return self.container.status == "running"

    def _wait_for_ready(self, timeout: int) -> None:
        """
        Wait for the database to be ready.

        Args:
            timeout: Maximum time to wait in seconds
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self._is_db_ready():
                logger.info(f"{self.__class__.__name__} is ready")
                return
            time.sleep(1)

        logger.info(f"Warning: Timed out waiting for {self.__class__.__name__} to be ready")

    def _is_db_ready(self) -> bool:
        """
        Check if the database is ready to accept connections.
        To be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement _is_db_ready")

    @property
    def sqlalchemy_connection_string(self) -> str:
        """
        Get the SQLAlchemy connection string.
        To be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement sqlalchemy_connection_string")


class MySQLHandler(DockerDatabaseHandler):
    """Manager for MySQL database containers."""

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
        Initialize a MySQL container manager.
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
        """Check if MySQL is ready by trying to run a command in the container."""
        if not self.is_running():
            return False

        try:
            exit_code, _ = self.container.exec_run(
                f"mysql -u{self.username} -p{self.password} -e 'SELECT 1'",
                stderr=False
            )
            return exit_code == 0
        except Exception:
            return False

    @property
    def sqlalchemy_connection_string(self) -> str:
        """Get the SQLAlchemy connection string for MySQL."""
        return f"mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class PostgresHandler(DockerDatabaseHandler):
    """Manager for PostgreSQL database containers."""

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
        Initialize a PostgreSQL container manager.
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
        """Check if PostgreSQL is ready by trying to run a command in the container."""
        if not self.is_running():
            return False

        try:
            exit_code, _ = self.container.exec_run(
                f"pg_isready -U {self.username} -d {self.database}",
                stderr=False
            )
            return exit_code == 0
        except Exception:
            return False

    @property
    def sqlalchemy_connection_string(self) -> str:
        """Get the SQLAlchemy connection string for PostgreSQL."""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class ClickHouseHandler(DockerDatabaseHandler):
    """Manager for ClickHouse database containers."""

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
        Initialize a ClickHouse container manager.
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
        """Check if ClickHouse is ready using HTTP interface."""
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
        """Get the SQLAlchemy connection string for ClickHouse."""
        # Using clickhouse-sqlalchemy dialect
        return f"clickhouse://{self.username}:{self.password}@{self.host}:{self.http_port}/{self.database}"


class DuckDBHandler(DockerDatabaseHandler):
    """
    Manager for DuckDB database container with ephemeral storage.
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
        Initialize an ephemeral DuckDB container manager.
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
        Start the DuckDB container.
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
            command="sh -c 'pip install duckdb && tail -f /dev/null'"  # Keep container running
        )

        logger.info(f"Started container: {self.name} ({self.container.id[:12]})")

        # Wait for DuckDB to be ready
        self._wait_for_ready(wait_time)

    def _is_db_ready(self) -> bool:
        """Check if DuckDB is ready by attempting to create a test table."""
        if not self.is_running():
            return False

        try:
            # Wait for pip install to complete and test DuckDB
            exit_code, _ = self.container.exec_run(
                f"python -c \"import duckdb; conn = duckdb.connect('{self.database_path}'); conn.execute('CREATE TABLE IF NOT EXISTS test (id INTEGER); DROP TABLE test;'); conn.close()\"",
                stderr=False
            )
            return exit_code == 0
        except Exception:
            return False

    @property
    def sqlalchemy_connection_string(self) -> str:
        """Get the SQLAlchemy connection string for DuckDB."""
        # Using duckdb_engine dialect
        return f"duckdb:///{self.database_path}"


# Example usage
if __name__ == "__main__":
    # Example for MySQL

    databases = {
        "mysql": MySQLHandler(name="test-mysql", port=3306, cpu_limit=2),
        "postgres": PostgresHandler(name="test-postgres", port=5432, cpu_limit=2),
        "duckdb": DuckDBHandler(name="test-duckdb", db_file="duckdb_data.db", cpu_limit=2),
        "clickhouse": ClickHouseHandler(name="test-clickhouse", http_port=8124, tcp_port=9001, cpu_limit=2),
    }

    for database_name, database_handler in databases.items():
        logger.info(f"testing {database_name}")

        try:
            database_handler.start()
            logger.info(f"{database_name} Connection String: {database_handler.sqlalchemy_connection_string}")
            logger.info(f"{database_name} is running!")
            # Do your database operations here

            try:
                # Example SQLAlchemy usage
                engine = create_engine(database_handler.sqlalchemy_connection_string)
                with engine.connect() as conn:

                    if database_name != "clickhouse":
                        # Create a sample table
                        conn.execute(text("CREATE TABLE IF NOT EXISTS test_table (id INT, name VARCHAR(255))"))
                    else:  # For ClickHouse
                        conn.execute(text(
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
                        ))

                    # Insert sample data
                    conn.execute(text("INSERT INTO test_table (id, name) VALUES (1, 'Test Record')"))

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

