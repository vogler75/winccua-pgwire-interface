-- Test queries for INTERVAL functionality with date/time arithmetic

-- Basic INTERVAL with CURRENT_TIME
SELECT tag_name, numeric_value, timestamp 
FROM loggedtagvalues 
WHERE timestamp > CURRENT_TIME - INTERVAL '3 minutes' 
  AND tag_name LIKE '%PV%:%' 
LIMIT 10;

-- INTERVAL with CURRENT_TIMESTAMP
SELECT tag_name, numeric_value, timestamp 
FROM loggedtagvalues 
WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '1 hour' 
  AND tag_name = 'TestTag' 
LIMIT 10;

-- INTERVAL with CURRENT_DATE
SELECT tag_name, numeric_value, timestamp 
FROM loggedtagvalues 
WHERE timestamp > CURRENT_DATE - INTERVAL '7 days' 
  AND tag_name = 'TestTag' 
LIMIT 10;

-- Different time units
SELECT * FROM loggedtagvalues 
WHERE timestamp > CURRENT_TIME - INTERVAL '30 seconds' 
  AND tag_name = 'TestTag';

SELECT * FROM loggedtagvalues 
WHERE timestamp > CURRENT_TIME - INTERVAL '15 minutes' 
  AND tag_name = 'TestTag';

SELECT * FROM loggedtagvalues 
WHERE timestamp > CURRENT_TIME - INTERVAL '2 hours' 
  AND tag_name = 'TestTag';

SELECT * FROM loggedtagvalues 
WHERE timestamp > CURRENT_DATE - INTERVAL '1 day' 
  AND tag_name = 'TestTag';

SELECT * FROM loggedtagvalues 
WHERE timestamp > CURRENT_DATE - INTERVAL '1 week' 
  AND tag_name = 'TestTag';

-- Range queries with intervals
SELECT * FROM loggedtagvalues 
WHERE timestamp BETWEEN CURRENT_TIME - INTERVAL '1 hour' 
                    AND CURRENT_TIME - INTERVAL '30 minutes'
  AND tag_name = 'TestTag';

-- Logged alarms with intervals
SELECT * FROM loggedalarms 
WHERE timestamp > CURRENT_TIME - INTERVAL '2 hours' 
LIMIT 10;

SELECT * FROM loggedalarms 
WHERE timestamp > CURRENT_DATE - INTERVAL '1 day' 
LIMIT 10;

-- Addition with intervals (should also work)
SELECT * FROM loggedtagvalues 
WHERE timestamp < CURRENT_TIME + INTERVAL '1 hour' 
  AND tag_name = 'TestTag' 
LIMIT 10;