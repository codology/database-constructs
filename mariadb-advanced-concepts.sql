-- Active Database: nation (Connection ID: 1723281396087)
/*
Original post: https://murage.co.ke/deep-dive-into-advanced-sql-techniques-with-examples/
This script contains a variety of SQL queries and operations focused on countries, languages, regions, GDP, population, and more.

Setup    
    - Maria DB Sample Database https://www.mariadbtutorial.com/getting-started/mariadb-sample-database/
    - VS Code Database Client Tool https://database-client.com/#/home
Sections    
    1. Access data from multiple tables – Joins 
    2. Combines results of multiple queries – Unions 
    3. Perform multi-step operations – Subqueries 
    4. Aggregation Functions 
    5. Common Table Expressions (CTEs) 
    6. Recursive Queries 
    7. Conditional Logic – Case 
    8. Reusable modules – User defined functions 
    9. Temporary tables 
    10. Transactions 
    11. Query Optimization Techniques
*/

-- Inner Join Example: Retrieve all languages spoken in Bangladesh
SELECT l.language AS Languages
FROM countries c
INNER JOIN country_languages cl ON c.country_id = cl.country_id
INNER JOIN languages l ON cl.language_id = l.language_id
WHERE c.name = "Bangladesh"
LIMIT 5;

-- Subquery Example: Retrieve all languages spoken in Bangladesh using a subquery
SELECT l.language AS Languages
FROM languages AS l
INNER JOIN country_languages AS cl ON l.language_id = cl.language_id
WHERE cl.country_id = (
    SELECT c.country_id
    FROM countries AS c
    WHERE c.name = 'Bangladesh'
);

-- Union Example: Retrieve countries that speak French and/or Italian
SELECT c.name AS country_name, l.language AS country_language 
FROM (
    SELECT cl.country_id AS cid, cl.language_id AS lid 
    FROM country_languages AS cl 
    WHERE cl.language_id IN (
        SELECT l.language_id 
        FROM languages AS l 
        WHERE l.language = "Italian" 
    )
    UNION ALL
    SELECT cl.country_id AS cid, cl.language_id AS lid 
    FROM country_languages AS cl 
    WHERE cl.language_id IN (
        SELECT l.language_id 
        FROM languages AS l 
        WHERE l.language = "French" 
    )
) a
LEFT JOIN countries c ON c.country_id = a.cid
LEFT JOIN languages l ON l.language_id = a.lid
ORDER BY c.name ASC 
LIMIT 5;

-- Aggregate Query with HAVING: Retrieve countries that speak both French and Italian
SELECT c.name AS country_name, COUNT(cl.language_id) AS langs 
FROM country_languages AS cl 
LEFT JOIN languages AS l ON l.language_id = cl.language_id
LEFT JOIN countries AS c ON c.country_id = cl.country_id
WHERE l.language IN ("French","Italian")
GROUP BY cl.country_id
HAVING langs > 1;

-- CTE Example: Countries whose population is above the average population of the world
WITH country_metrics AS (
    SELECT c.name AS country_name, cs.population 
    FROM country_stats cs
    LEFT JOIN countries c ON c.country_id = cs.country_id
)
SELECT cs1.country_name, cs1.population 
FROM country_metrics cs1
WHERE cs1.population > (
    SELECT AVG(cs2.population) 
    FROM country_metrics cs2
)
ORDER BY cs1.population ASC;

-- Recursive CTE Example: Retrieve countries and regions in a hierarchical manner
WITH RECURSIVE region_metrics AS ( 
    SELECT c.country_id AS c_id
    FROM countries c
    UNION
    SELECT region_id
    FROM countries
    JOIN region_metrics ON countries.region_id = region_metrics.c_id
) 
SELECT * FROM region_metrics;

SELECT c.country_id, c.name, rg.region_id, rg.name 
FROM region_metrics r
JOIN countries c ON c.country_id = r.c_id
JOIN regions rg ON r.reg_id = rg.region_id
LIMIT 100;

-- Recursive CTE Example: Aggregate data for countries in Africa by language
WITH RECURSIVE continent_countries AS (
    -- Anchor Member: Retrieve initial set of countries in Africa
    SELECT
        ct.name AS continent_name,
        c.name AS country_name,
        l.language AS language_name,
        cs.population,
        cs.gdp,
        r.continent_id,
        c.country_id
    FROM countries c
    JOIN regions r ON c.region_id = r.region_id
    JOIN continents ct ON r.continent_id = ct.continent_id
    JOIN country_languages cl ON c.country_id = cl.country_id
    JOIN languages l ON cl.language_id = l.language_id
    JOIN country_stats cs ON c.country_id = cs.country_id
    WHERE ct.name = "Africa"
      AND cl.official = 1  -- Filter to official languages only
      AND cs.year = 2017  -- Focus on the year 2017
    UNION ALL
    -- Recursive Member: Add subsequent countries by continent
    SELECT
        cc.continent_name,
        cc.country_name,
        cc.language_name,
        cc.population,
        cc.gdp,
        cc.continent_id,
        cc.country_id
    FROM continent_countries cc
    JOIN countries c ON cc.country_id = c.country_id
)
-- Final Query: Aggregate results by language
SELECT
    country_name,
    language_name,
    FORMAT(AVG(gdp), 0) AS average_gdp,
    FORMAT(AVG(population), 0) AS average_population
FROM continent_countries
GROUP BY language_name
ORDER BY population DESC
LIMIT 5;

-- CASE Statement Example: Categorize countries by GDP into low, medium, or high
SELECT
    c.name AS country_name,
    cs.gdp,
    CASE
        WHEN cs.gdp < 10000000000 THEN 'Low GDP'
        WHEN cs.gdp BETWEEN 10000000000 AND 100000000000 THEN 'Medium GDP'
        ELSE 'High GDP'
    END AS gdp_category
FROM countries c
JOIN country_stats cs ON c.country_id = cs.country_id
WHERE cs.year = 2017
LIMIT 5;

-- User-Defined Function: Categorize GDP into low, medium, or high
DELIMITER //

CREATE FUNCTION categorize_gdp(gdp_value DECIMAL(15,0))
RETURNS VARCHAR(20)
DETERMINISTIC
BEGIN
    DECLARE gdp_category VARCHAR(20);

    SET gdp_category = CASE
        WHEN gdp_value < 10000000000 THEN 'Low GDP'
        WHEN gdp_value BETWEEN 10000000000 AND 100000000000 THEN 'Medium GDP'
        ELSE 'High GDP'
    END;

    RETURN gdp_category;
END //

DELIMITER ;

-- Using the categorize_gdp() function in a query
SELECT
    c.name AS country_name,
    cs.gdp,
    categorize_gdp(cs.gdp) AS gdp_category
FROM countries c
JOIN country_stats cs ON c.country_id = cs.country_id
WHERE cs.year = 2017
LIMIT 5;

-- Temporary Tables Example: Aggregating and Analyzing Country Data
-- Step 1: Create a temporary table to store country-level data for Africa in 2017
CREATE TEMPORARY TABLE temp_country_data AS
SELECT
    c.country_id,
    c.name AS country_name,
    r.name AS region_name,
    cs.gdp,
    cs.population,
    cl.language_id
FROM countries c
JOIN regions r ON c.region_id = r.region_id
JOIN continents ct ON r.continent_id = ct.continent_id
JOIN country_stats cs ON c.country_id = cs.country_id
JOIN country_languages cl ON c.country_id = cl.country_id
WHERE ct.name = 'Africa'
  AND cs.year = 2017
  AND cl.official = 1;

-- View the temporary table data
SELECT * FROM temp_country_data;

-- Step 2: Create a temporary table to aggregate data by region
CREATE TEMPORARY TABLE temp_region_data AS
SELECT
    region_name,
    AVG(gdp) AS total_gdp,
    AVG(population) AS total_population
FROM temp_country_data
GROUP BY region_name;

-- View the aggregated region data
SELECT * FROM temp_region_data;

-- Step 3: Join aggregated data with language data and present final results
SELECT
    r.region_name,
    l.language,
    FORMAT(r.total_gdp, 0) AS tot_gdp,
    FORMAT(r.total_population, 0) AS tot_pop,
    COUNT(tcd.country_id) AS num_countries
FROM temp_region_data r
JOIN temp_country_data tcd ON r.region_name = tcd.region_name
JOIN languages l ON tcd.language_id = l.language_id
GROUP BY r.region_name
ORDER BY r.total_gdp DESC
LIMIT 5;

-- Cleanup: Drop temporary tables to free up memory
DROP TEMPORARY TABLE IF EXISTS temp_country_data;
DROP TEMPORARY TABLE IF EXISTS temp_region_data;

-- Transaction Example: Update country statistics and insert a new language in a single transaction
-- Step 1: Start the transaction
START TRANSACTION;

-- Step 2: Update the GDP and population for a specific country
UPDATE country_stats
SET gdp = 200000000000,  -- Example GDP value
    population = 50000000 -- Example population value
WHERE country_id = 1 AND year = 2023;

-- Step 3: Insert a new official language for the same country
INSERT INTO country_languages (country_id, language_id, official)
VALUES (15, 9, 1);  -- Example country_id and language_id

-- Step 4: Commit the transaction if everything is successful
COMMIT;

-- Optional: Roll back the transaction in case of an error
ROLLBACK;

-- Stored Procedure Example: Update country statistics and languages using a stored procedure with transaction management
DELIMITER //

CREATE PROCEDURE update_country_stats_and_languages()
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        -- On error, roll back the transaction
        ROLLBACK;
    END;

    -- Start the transaction
    START TRANSACTION;

    -- Update operation
    UPDATE country_stats
    SET gdp = 200000000002,
        population = 50000000
    WHERE country_id = 1 AND year = 2017;

    -- Insert operation
    INSERT INTO country_languages (country_id, language_id, official)
    VALUES (53, 9, 1);

    -- Commit the transaction
    COMMIT;
END //

DELIMITER ;

-- Execute the stored procedure
CALL update_country_stats_and_languages();

-- Country Distances Table: Define a table for storing distances between countries
CREATE TABLE country_distances (
    origin INT NOT NULL,
    destination INT NOT NULL,
    distance INT NOT NULL,
    PRIMARY KEY (origin, destination),
    FOREIGN KEY (origin) REFERENCES countries(country_id),
    FOREIGN KEY (destination) REFERENCES countries(country_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;

-- Populate the country_distances table with random distances
INSERT INTO country_distances (origin, destination, distance)
SELECT 
    c1.country_id AS origin,
    c2.country_id AS destination,
    FLOOR(1 + (RAND() * 500)) AS distance  -- Random distance between 1 and 500
FROM countries c1
CROSS JOIN countries c2;

-- Recursive Query: Calculate the distance from Kenya to other countries using a recursive CTE
WITH RECURSIVE country_destination AS ( 
    SELECT origin AS destination 
    FROM country_distances 
    WHERE origin=1 
  UNION
    SELECT country_distances.destination 
    FROM country_distances 
    JOIN country_destination 
    ON country_destination.destination=country_distances.origin 
) 
SELECT * FROM country_destination;

-- Delete Random Rows: Delete a specified number of random rows from the country_distances table
SET @x = 20000;  -- Set the number of rows to delete
DELETE FROM country_distances
ORDER BY RAND()
LIMIT @x;

SET GLOBAL slow_query_log=1;

-- Indexes
SELECT count(origin) FROM country_distances cd
JOIN countries c ON c.country_id = cd.origin

CREATE INDEX idx_origin ON country_distances(origin);
CREATE INDEX idx_destination ON country_distances(destination);

SELECT count(origin) FROM country_distances cd
JOIN countries c ON c.country_id = cd.origin
WHERE c.region_id = 5

-- Partitioning a large table by range
CREATE TABLE country_stats (
    country_id INT,
    year INT,
    gdp BIGINT,
    population BIGINT
) PARTITION BY RANGE (year) (
    PARTITION p0 VALUES LESS THAN (2000),
    PARTITION p1 VALUES LESS THAN (2010),
    PARTITION p2 VALUES LESS THAN (2020),
    PARTITION p3 VALUES LESS THAN MAXVALUE
);

-- Querying partitioned data
SELECT * FROM country_stats WHERE year = 2017;
194 rows in set (0.035 sec)

-- Partitioning a large table by range
CREATE TABLE country_stats_partitioned (
    country_id INT,
    year INT,
    gdp BIGINT,
    population BIGINT
) PARTITION BY RANGE (year) (
    PARTITION p0 VALUES LESS THAN (2000),
    PARTITION p1 VALUES LESS THAN (2010),
    PARTITION p2 VALUES LESS THAN (2020),
    PARTITION p3 VALUES LESS THAN MAXVALUE
);
-- Updating existing table
ALTER TABLE country_stats
PARTITION BY RANGE (year) (
    PARTITION p0 VALUES LESS THAN (2000),
    PARTITION p1 VALUES LESS THAN (2010),
    PARTITION p2 VALUES LESS THAN (2020),
    PARTITION p3 VALUES LESS THAN MAXVALUE
);
-- Insert / Copy data
INSERT INTO country_stats_partitioned
SELECT * FROM country_stats
-- Verify partition information
SELECT PARTITION_NAME as part_name,PARTITION_ORDINAL_POSITION as part_pos,PARTITION_METHOD as part_meth,PARTITION_EXPRESSION as part_expr, TABLE_ROWS as table_rows, AVG_ROW_LENGTH as row_len, DATA_LENGTH as data_len
FROM information_schema.partitions 
WHERE TABLE_SCHEMA='nation' 
AND TABLE_NAME = 'country_stats_partitioned' 
AND PARTITION_NAME IS NOT NULL

-- Caching
-- Enable query cache (only applicable in versions where query cache is supported)
SET GLOBAL query_cache_size = 1024 * 1024;  -- Set cache size to 1MB
-- First query execution (result will be cached)
SELECT COUNT(*) FROM countries WHERE region_id = 2;
-- Subsequent execution (result will be retrieved from cache)
SELECT COUNT(*) FROM countries WHERE region_id = 2;


-- Profiling
-- Enable profiling
SET profiling = 1;
-- Run the query
SELECT VARIANCE(gdp) FROM country_stats_partitioned WHERE year = 1982;
-- Show profiling results
SHOW PROFILES;
-- Detailed breakdown of time spent in each phase of the query execution
SHOW PROFILE FOR QUERY 1;
--
--- END ---