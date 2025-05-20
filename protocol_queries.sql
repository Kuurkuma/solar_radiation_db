-- SELECT Queries
-- Basic Select with Limit
SELECT * FROM data LIMIT 100;

-- Basic Count
SELECT COUNT(*) FROM data;

-- Select with Simple Date Range Filter 
SELECT * FROM data WHERE time >= '2023-01-01' AND time < '2024-01-01'; 

-- Select with Multiple Filters (Date Range and Category)
SELECT * FROM data WHERE time >= '2023-01-01' AND time < '2024-01-01' AND power_output > 0; 

-- Select with ORDER BY and Limit
SELECT * FROM data ORDER BY power_output DESC LIMIT 50; -- Get top 50 rows by power output

-- AGGREGATE Queries 
-- Basic Aggregates
SELECT AVG(power_output) as avg_power_output, MAX(power_output) as max_power_output, MIN(power_output) as min_power_output FROM data;

-- Aggregates with Date Range Filter (Corrected: use AND, standard date format)
SELECT AVG(power_output) as avg_power_output, MAX(power_output) as max_power_output, MIN(power_output) as min_power_output FROM data WHERE time >= '2023-01-01' AND time < '2024-01-01';

-- Aggregates Grouped by Year with Ordering
SELECT
    EXTRACT(YEAR FROM time) as year,
    AVG(power_output) as avg_power_output,
    MAX(power_output) as max_power_output,
    MIN(power_output) as min_power_output
FROM data
WHERE time >= '2023-01-01' AND time < '2024-01-01'
GROUP BY EXTRACT(YEAR FROM time)
ORDER BY EXTRACT(YEAR FROM time);

-- JOIN Queries
-- Simple Self-Join to Calculate Power Change
SELECT
    t1.time,     
    t1.power_output as power_output,  
    t2.power_output as previous_power, 
    t1.power_output - t2.power_output as power_change
FROM data t1
JOIN data t2 ON t1.time = t2.time; -- The INTERVAL creates an error in mysql

-- WINDOW FUNCTION Queries
-- Power output changes
SELECT
    d.time,
    power_output as power_output,
    LAG(power_output) OVER (ORDER BY time) as previous_power_output,
    power_output - LAG(power_output) OVER (ORDER BY time) as power_change
FROM data as d;

-- Running total
SELECT
    time,
    power_output as power_output,
    SUM(power_output) OVER (ORDER BY time) as running_total
FROM data
ORDER BY time;
