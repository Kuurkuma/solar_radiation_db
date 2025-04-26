import logging
import sys

logger = logging.getLogger(__name__)

def load_queries_split_by_semicolon(filepath):
    """
    Loads queries from a single .sql file by splitting on semicolons.
    """
    try:
        with open(filepath, 'r') as f:
            content = f.read()
        queries = [q.strip() for q in content.split(";") if q.strip()]
        return queries
    except FileNotFoundError:
        logger.error(f"SQL file not found: {filepath}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error reading or processing SQL file {filepath}: {e}")
        sys.exit(1)