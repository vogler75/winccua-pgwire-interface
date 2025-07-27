-- Test the original user query that was failing
SELECT tag_name, numeric_value, timestamp 
FROM loggedtagvalues 
WHERE timestamp > CURRENT_TIME - INTERVAL '1 hour' 
  AND tag_name LIKE '%PV%Watt%:%';

-- Test variations
SELECT tag_name, numeric_value, timestamp 
FROM loggedtagvalues 
WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '3 minutes' 
  AND tag_name LIKE '%PV%:%';

SELECT * FROM loggedalarms 
WHERE timestamp > CURRENT_DATE - INTERVAL '1 day';

-- Test with addition
SELECT * FROM loggedtagvalues 
WHERE timestamp < CURRENT_TIME + INTERVAL '1 hour' 
  AND tag_name = 'Test';