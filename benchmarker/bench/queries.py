from typing import Dict, List, Optional
from pydantic import BaseModel, Field
from pydantic.dataclasses import dataclass


@dataclass
class Query:
    """A database query with name, SQL code, and description."""
    name: str
    query: str
    description: str


class QueryDatabase:
    """A collection of database queries organized by categories."""
    
    def __init__(self):
        self._basic_select_queries = self._initialize_basic_select_queries()
        self._aggregate_queries = self._initialize_aggregate_queries()
        self._join_queries = self._initialize_join_queries()
        self._window_function_queries = self._initialize_window_function_queries()
        
        # Combine all query categories
        self._all_queries = {
            "basic_select": self._basic_select_queries,
            "aggregate": self._aggregate_queries,
            "join": self._join_queries,
            "window_function": self._window_function_queries
        }
    
    @property
    def all_queries(self) -> Dict[str, List[Query]]:
        """Return all queries organized by category."""
        return self._all_queries
    
    @property
    def basic_select_queries(self) -> List[Query]:
        """Return all basic SELECT queries."""
        return self._basic_select_queries
    
    @property
    def aggregate_queries(self) -> List[Query]:
        """Return all aggregate queries."""
        return self._aggregate_queries
    
    @property
    def join_queries(self) -> List[Query]:
        """Return all JOIN queries."""
        return self._join_queries
    
    @property
    def window_function_queries(self) -> List[Query]:
        """Return all window function queries."""
        return self._window_function_queries
    
    def get_query_by_name(self, name: str) -> Optional[Query]:
        """
        Retrieve a query by its name.
        
        Args:
            name: The name of the query to find
            
        Returns:
            The Query object if found, None otherwise
        """
        for category in self._all_queries.values():
            for query in category:
                if query.name == name:
                    return query
        return None
    
    def _initialize_basic_select_queries(self) -> List[Query]:
        """Initialize the basic SELECT queries."""
        return [
            Query(
                name="basic_select_with_limit",
                query="SELECT * FROM data LIMIT 100;",
                description="Retrieve the first 100 rows from the data table"
            ),
            Query(
                name="basic_count",
                query="SELECT COUNT(*) FROM data;",
                description="Count the total number of rows in the data table"
            ),
            Query(
                name="select_with_date_range",
                query="SELECT * FROM data WHERE time >= '2023-01-01' AND time < '2024-01-01';",
                description="Retrieve all rows within a specific date range (year 2023)"
            ),
            Query(
                name="select_with_multiple_filters",
                query="SELECT * FROM data WHERE time >= '2023-01-01' AND time < '2024-01-01' AND P > 0;",
                description="Retrieve rows from 2023 with positive power output values"
            ),
            Query(
                name="select_with_order_and_limit",
                query="SELECT * FROM data ORDER BY P DESC LIMIT 50;",
                description="Retrieve the top 50 rows with highest power output values"
            )
        ]
    
    def _initialize_aggregate_queries(self) -> List[Query]:
        """Initialize the aggregate queries."""
        return [
            Query(
                name="basic_aggregates",
                query="SELECT AVG(P) as avg_power_output, MAX(P) as max_power_output, MIN(P) as min_power_output FROM data;",
                description="Calculate average, maximum and minimum power output across all data"
            ),
            Query(
                name="aggregates_with_date_filter",
                query="SELECT AVG(P) as avg_power_output, MAX(P) as max_power_output, MIN(P) as min_power_output FROM data WHERE time >= '2023-01-01' AND time < '2024-01-01';",
                description="Calculate power output statistics for data from year 2023"
            ),
            Query(
                name="aggregates_grouped_by_year",
                query="""SELECT
                            EXTRACT(YEAR FROM time) as year,
                            AVG(P) as avg_power_output,
                            MAX(P) as max_power_output,
                            MIN(P) as min_power_output
                        FROM data
                        WHERE time >= '2023-01-01' AND time < '2024-01-01'
                        GROUP BY EXTRACT(YEAR FROM time)
                        ORDER BY EXTRACT(YEAR FROM time);""",
                description="Calculate yearly power output statistics, grouped and ordered by year"
            )
        ]
    
    def _initialize_join_queries(self) -> List[Query]:
        """Initialize the JOIN queries."""
        return [
            Query(
                name="self_join_power_change",
                query="""SELECT
                            t1.time,     
                            t1.P as power_output,  
                            t2.P as previous_power, 
                            t1.P - t2.P as power_change
                        FROM data t1
                        JOIN data t2 ON t1.time = t2.time + INTERVAL '1 hour';""",
                description="Calculate hourly power change using a self-join comparing current and previous hour"
            )
        ]
    
    def _initialize_window_function_queries(self) -> List[Query]:
        """Initialize the window function queries."""
        return [
            Query(
                name="power_output_changes",
                query="""SELECT
                            d.time,
                            P as power_output,
                            LAG(P) OVER (ORDER BY time) as previous_P,
                            P - LAG(P) OVER (ORDER BY time) as power_change
                        FROM data as d;""",
                description="Calculate power changes using LAG window function to access previous row values"
            ),
            Query(
                name="running_total",
                query="""SELECT
                            time,
                            P as power_output,
                            SUM(P) OVER (ORDER BY time) as running_total
                        FROM data
                        ORDER BY time;""",
                description="Calculate a running total of power output over time using window functions"
            )
        ]