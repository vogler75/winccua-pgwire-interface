-- Test queries to verify COUNT(*) works
SELECT * FROM tagvalues WHERE tag_name LIKE '%::PV%' LIMIT 5;
SELECT COUNT(*) FROM tagvalues WHERE tag_name LIKE '%::PV%';
SELECT COUNT(*) FROM activealarms;
SELECT COUNT(*) FROM loggedtagvalues WHERE tag_name LIKE '%::PV%';