# queries for time-series data

time_series_queries = [   
"""
"""
]

import re
import logging
logger = logging.getLogger(__name__)

def get_benchmark_queries():
    """
    Parses the query string and extracts queries
    into a dictionary using QUERY_START/QUERY_END markers.

    Returns:
        dict: A dictionary where keys are query names and values are SQL strings.
    """
    queries = {}
    # Regex to find blocks between '-- QUERY_START: <name>' and '-- QUERY_END: <name>'
    # re.DOTALL allows '.' to match newline characters so it matches across lines
    # Capturing groups:
    # (\w+) captures the query name after QUERY_START:
    # (.*?) captures the SQL content non-greedily between the markers
    # \1 backreferences the first capturing group (the query name) to ensure start and end match
    pattern = re.compile(r"-- QUERY_START: (\w+)\n(.*?)\n-- QUERY_END: \1", re.DOTALL)

    # Find all matches in the multi-line string
    matches = pattern.findall(time_series_queries)

    if not matches:
        logger.warning("No benchmark queries found using the specified markers.")
        return {} # Return empty dictionary if no matches

    for query_name, sql_query in matches:
        # Store the extracted query after removing leading/trailing whitespace
        queries[query_name] = sql_query.strip()

    logger.info(f"Loaded {len(queries)} queries from internal string.")

    return queries

# Example of how to use this function (for testing queries.py)
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)
#     benchmark_queries = get_benchmark_queries()
#     print(f"Loaded {len(benchmark_queries)} queries:")
#     for name, query in benchmark_queries.items():
#         print(f"--- {name} ---")
#         print(query)
#         print("-" * (len(name) + 6))