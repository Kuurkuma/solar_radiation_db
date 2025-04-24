-- SELECT Queries
-- Basic Select with Limit
SELECT * FROM data LIMIT 100;

-- Basic Count
SELECT COUNT(*) FROM data;

-- Select with Simple Date Range Filter 
SELECT * FROM data WHERE time >= '2023-01-01' AND time < '2024-01-01'; 

-- Select with Multiple Filters (Date Range and Category)
SELECT * FROM data WHERE time >= '2023-01-01' AND time < '2024-01-01' AND P > 0; 

-- Select with ORDER BY and Limit
SELECT * FROM data ORDER BY P DESC LIMIT 50; -- Get top 50 rows by power output

-- AGGREGATE Queries 
-- Basic Aggregates
SELECT AVG(P) as avg_power_output, MAX(P) as max_power_output, MIN(P) as min_power_output FROM data;

-- Aggregates with Date Range Filter (Corrected: use AND, standard date format)
SELECT AVG(P) as avg_power_output, MAX(P) as max_power_output, MIN(P) as min_power_output FROM data WHERE time >= '2023-01-01' AND time < '2024-01-01';

-- Aggregates Grouped by Year with Ordering
SELECT
    EXTRACT(YEAR FROM time) as year,
    AVG(P) as avg_power_output,
    MAX(P) as max_power_output,
    MIN(P) as min_power_output
FROM data
WHERE time >= '2023-01-01' AND time < '2024-01-01'
GROUP BY EXTRACT(YEAR FROM time)
ORDER BY EXTRACT(YEAR FROM time);


-- WINDOW FUNCTION Queries
-- Power output changes
SELECT
    d.time,
    P as current_P,
    LAG(P) OVER (ORDER BY time) as previous_P,
    P - LAG(P) OVER (ORDER BY time) as power_change
FROM data as d;

-- Running total
SELECT
    time,
    P,
    SUM(P) OVER (ORDER BY time) as running_total
FROM data
ORDER BY time;
