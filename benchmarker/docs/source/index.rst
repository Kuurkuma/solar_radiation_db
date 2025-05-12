=======================================
Welcome to Benchmarker's documentation!
=======================================

Overview
========

The Benchmarker is a comprehensive database benchmarking tool designed to evaluate and compare the performance of various database systems using containerized environments. This project enables data engineers and developers to make informed decisions about database technologies based on empirical performance metrics.

Architecture
============

The project is structured around two main components:

- **Benchmarker Core** (`bench.py`): Orchestrates the benchmarking process by managing database connections, executing queries, and collecting performance metrics.

- **Database Handlers** (`databases.py`): Provides containerized database environments through Docker, with specialized handlers for MySQL, PostgreSQL, ClickHouse, and DuckDB.

Key Features
============

Database Virtualization
-----------------------

The system uses Docker containers to create isolated, reproducible database environments with controlled resource allocation. Each database operates with specific CPU and memory constraints to ensure fair comparison.

Performance Metrics Collection
------------------------------

The benchmarking framework collects comprehensive performance metrics for each query execution:

- Execution time (milliseconds)
- CPU usage (percent)
- Memory utilization (MB and percent)
- Disk I/O operations (MB read/write)
- Network I/O (MB in/out)
- Result set size (rows and MB)

These metrics provide multi-dimensional insights into database performance characteristics.

Multiple Database Support
-------------------------

The framework currently supports benchmarking for:

- MySQL
- PostgreSQL
- ClickHouse
- DuckDB

Each database implementation is encapsulated in a specialized handler class that manages container lifecycle, connection details, and database-specific requirements.

Usage Example
=============

.. code-block:: python

   from benchmarker.bench import Benchmarker
   from benchmarker.bench.databases import MySQLHandler, PostgresHandler

   # Initialize the benchmarker
   benchmarker = Benchmarker()

   # Define database handlers
   databases = {
       "mysql": MySQLHandler(name="test-mysql", cpu_limit=1.0),
       "postgres": PostgresHandler(name="test-postgres", cpu_limit=1.0),
   }
   benchmarker.define_database_handlers(databases)

   # Load test data
   benchmarker.get_data("https://example.com/dataset.csv")

   # Define test queries
   queries = [
       "SELECT COUNT(*) FROM data;",
       "SELECT * FROM data LIMIT 10;"
   ]
   benchmarker.define_queries(queries)

   # Run the benchmark
   results = benchmarker.benchmark_queries()

   # Save results
   benchmarker.save_metrics_to_csv("results.csv")

API Reference
=============

.. toctree::
   :maxdepth: 2

   bench

External Resources
==================

* `Docker Python SDK <https://docker-py.readthedocs.io/>`_ - The official Python SDK for Docker used for container management
* `SQLAlchemy <https://www.sqlalchemy.org/>`_ - Used for database connectivity and query execution
* `Pandas <https://pandas.pydata.org/>`_ - Used for data manipulation and results analysis

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`