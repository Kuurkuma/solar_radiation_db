CREATE OR REPLACE TABLE data 
AS SELECT * 
FROM read_csv('/Users/macbook/Development/database_crash_test/data/no_headers_brandenburger_gate_seriescalc.csv');



-- error queries wih mysql
['-- The INTERVAL creates an error in mysql
-- WINDOW FUNCTION Queries
-- Power output changes
SELECT
    d.time,
    power_output as power_output,\n    
    LAG(power_output) OVER (ORDER BY time) as previous_power_output,\n    
    power_output - LAG(power_output) OVER (ORDER BY time) as power_change\n
    FROM data as d']

-- test solution
SELECT
    time,
    power_output,
    previous_power_output,
    power_output - previous_power_output as power_change
FROM (
    SELECT
        d.time,
        power_output,
        LAG(power_output) OVER (ORDER BY time) as previous_power_output
    FROM data as d
) as subquery;

-- soluton 2
SELECT
    d1.time,
    d1.power_output,
    d2.power_output AS previous_power_output,
    d1.power_output - d2.power_output AS power_change
FROM data d1
LEFT JOIN data d2 ON d2.time = (SELECT MAX(time) FROM data WHERE time < d1.time)
ORDER BY d1.time;


-- 16. Moving Average of Power Output 
SELECT
    time,
    power_output,
    AVG(power_output) OVER (ORDER BY time ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as moving_average_10_row
FROM data
ORDER BY time;

-- 17. Rank Data within Partitions
SELECT
    DAY(time) as day,
    power_output,
    RANK() OVER (PARTITION BY power_output ORDER BY power_output ASC) as power_rank_in_category
FROM data
ORDER BY power_rank_in_category; -- Ordering results is separate from window function ordering


-- DDL Queries (Data Definition Language)
-- Note: DDL syntax for complex operations like partitioning is HIGHLY database-specific.

-- 18. Create a new table with specific columns and types
-- This tests schema creation performance.
-- Example query - needs to be adjusted based on database type in execution
-- CREATE TABLE IF NOT EXISTS new_empty_table (
--     id <AUTO_INCREMENT_SYNTAX> PRIMARY KEY,
--     record_time <TIMESTAMP_TYPE>,
--     value_p <FLOAT_TYPE>,
--     description VARCHAR(255),
--     is_active <BOOLEAN_TYPE>
-- );

-- 19. Create an Index (Tests index creation performance)
-- Indexes are crucial for query performance, especially on filtered/ordered columns.
CREATE INDEX IF NOT EXISTS idx_data_time ON data (time);
-- Notes: Syntax is fairly standard, but IF NOT EXISTS might vary slightly.
-- Creating indexes on multiple columns or using different index types (e.g., B-tree, Hash, GiST/GIN in Postgres)
-- would provide more tests.

-- 20. Create an Index on a Filtered/Grouped Column (Tests index usage)
CREATE INDEX IF NOT EXISTS idx_data_category ON data (category);

-- 21. Drop a table (Tests cleanup performance)
DROP TABLE IF EXISTS new_empty_table; -- Use the table created in query 18
-- Notes: Syntax is fairly standard.

-- DML Queries (Data Manipulation Language)
-- These are less common in read-heavy benchmarks but useful for a full picture.
-- Be extremely cautious running DML queries in a benchmark if you need consistent data state.
-- Consider making copies of the table for DML tests.
-- 22. Basic Update
UPDATE data SET P = P * 1.05 WHERE time = (SELECT MIN(time) FROM data LIMIT 1); -- Added LIMIT 1 for robustness with subquery
-- Notes: Using a subquery to select a specific row makes it deterministic.
-- Updating multiple rows (e.g., UPDATE data SET P = P * 1.05 WHERE category = 'TypeB') is another test.

-- 23. Basic Delete
DELETE FROM data WHERE time = (SELECT MAX(time) FROM data LIMIT 1); -- Added LIMIT 1
-- Notes: Deleting multiple rows (e.g., DELETE FROM data WHERE category = 'TypeC') is another test.


-- Complex / Combination Queries
-- 24. Subquery/CTE to Find Daily Max Power and then Average of Daily Max
WITH DailyMaxPower AS (
    SELECT
        DATE(time) as event_date, -- Or DATE_TRUNC('day', time) for Postgres/DuckDB, or simply DATE(time) for MySQL/DuckDB
        MAX(P) as max_P_day
    FROM data
    GROUP BY DATE(time) -- Ensure consistency with SELECT part
)
SELECT AVG(max_P_day) as average_of_daily_max_power
FROM DailyMaxPower;
-- Notes: DATE(time) is more portable for getting the date part.
-- You could also group by year/month/day components using EXTRACT.

-- 25. Correlation between two numerical columns (e.g., Power and Temperature)
-- Requires a 'temperature' column.
-- Standard SQL doesn't have a built-in CORR function, but many databases do.
SELECT CORR(P, temperature) FROM data; -- Replace temperature with your actual column name
-- Notes: CORR() function availability varies (Postgres, DuckDB usually have it). MySQL might require calculation.

