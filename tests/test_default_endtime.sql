-- Test queries to verify default endtime behavior for logged tables

-- LoggedTagValues queries
-- Query without endtime (should use current UTC time as endtime)
SELECT * FROM loggedtagvalues 
WHERE tag_name = 'TestTag' 
  AND timestamp > '2024-01-01T00:00:00Z' 
LIMIT 10;

-- Query with explicit endtime (should use the specified endtime)
SELECT * FROM loggedtagvalues 
WHERE tag_name = 'TestTag' 
  AND timestamp > '2024-01-01T00:00:00Z' 
  AND timestamp < '2024-12-31T23:59:59Z' 
LIMIT 10;

-- LoggedAlarms queries
-- Query without endtime (should use current UTC time as endtime)
SELECT * FROM loggedalarms 
WHERE timestamp > '2024-01-01T00:00:00Z' 
LIMIT 10;

-- Query with explicit endtime (should use the specified endtime)
SELECT * FROM loggedalarms 
WHERE timestamp > '2024-01-01T00:00:00Z' 
  AND timestamp < '2024-12-31T23:59:59Z' 
LIMIT 10;

-- Query with BETWEEN (both bounds specified, should not add default)
SELECT * FROM loggedtagvalues 
WHERE tag_name = 'TestTag' 
  AND timestamp BETWEEN '2024-01-01T00:00:00Z' AND '2024-12-31T23:59:59Z'
LIMIT 10;