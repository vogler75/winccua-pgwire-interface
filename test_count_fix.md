# COUNT(*) Fix Verification

## Problem
- `SELECT * FROM tagvalues WHERE tag_name LIKE '%::PV%'` worked ✅
- `SELECT COUNT(*) FROM tagvalues WHERE tag_name LIKE '%::PV%'` returned 0 ❌

## Root Cause
1. COUNT(*) queries were routed to DataFusion-only execution
2. DataFusion tried to populate tables with ALL data (no filters)
3. TagValues GraphQL API requires specific tag filters
4. GraphQL call failed → empty table → COUNT(*) = 0

## Fix Applied
1. **SQL Parsing**: Extract actual WHERE clause filters from the original query
2. **Smart Population**: Use extracted filters to fetch relevant data from GraphQL
3. **DataFusion Execution**: Let DataFusion execute aggregation on filtered data

## Modified Functions
- `populate_virtual_tables_with_data()` - Uses SQL-aware population
- `create_populated_tagvalues_batch_from_sql()` - New function that parses SQL filters
- `has_aggregation_functions()` - Detects when to use DataFusion-only execution

## Expected Result
Both queries should now:
1. Call GraphQL with the same WHERE clause filters (`tag_name LIKE '%::PV%'`)
2. Get the same filtered data
3. Return consistent results (same count as rows)

## Test Queries
```sql
-- Should return same row count
SELECT * FROM tagvalues WHERE tag_name LIKE '%::PV%';
SELECT COUNT(*) FROM tagvalues WHERE tag_name LIKE '%::PV%';
```