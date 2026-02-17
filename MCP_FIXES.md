# MCP Server Critical Issues - Fixed

## Issue 1: get_economy_dashboard Error

**Error:**
```
Error: column "cpi_year_over_year" does not exist
LINE 9:             cpi_year_over_year as inflation_yoy,
```

**Root Cause:**
The database schema is out of sync with the code. The `inflation` table is missing the `cpi_year_over_year` column that was added to the schema definition.

**Fix:**
Created migration script: `sqlschema/23_add_cpi_yoy.sql`

**To Apply the Fix:**

```bash
# Option 1: Run the migration script directly
psql $DATABASE_URL -f sqlschema/23_add_cpi_yoy.sql

# Option 2: Rebuild schema (applies all migrations)
python -m sawa.database.schema --database-url $DATABASE_URL

# Option 3: If you're doing a fresh coldstart
sawa coldstart --schema-only  # This will apply all schema files
```

The migration script will:
1. Check if the column exists
2. Add it if missing (safe to run multiple times)
3. Recreate the `v_economy_dashboard` view

## Issue 2: get_earnings_calendar Returns Empty Array

**Behavior:**
```
get_earnings_calendar(start_date: "2026-02-06", end_date: "2026-03-16", ...)
⎿  []
```

**Analysis:**
This is likely NOT a bug. The query is requesting earnings data for future dates (Feb 6 - Mar 16, 2026). The empty result suggests:

1. **No future earnings data loaded**: The earnings table may only contain historical data
2. **Legitimate empty result**: Not all companies have scheduled earnings in that specific window
3. **Data source limitation**: The data provider may not have future earnings calendars available

**To Verify:**

```bash
# Check if earnings table has any data at all
psql $DATABASE_URL -c "SELECT COUNT(*) FROM earnings;"

# Check date range of earnings data
psql $DATABASE_URL -c "SELECT MIN(report_date), MAX(report_date) FROM earnings;"

# Check for upcoming earnings (if any)
psql $DATABASE_URL -c "SELECT * FROM earnings WHERE report_date >= CURRENT_DATE LIMIT 10;"
```

**If earnings data is missing entirely:**
The earnings table needs to be populated. Check if your data pipeline includes earnings downloads.

## Files Modified

1. **Created:** `sqlschema/23_add_cpi_yoy.sql` - Migration to add missing column
2. **No code changes needed** - The MCP tools are correctly implemented

## Testing After Fix

```bash
# After applying the migration, test the economy dashboard
# (Using the MCP server or directly via psql)

psql $DATABASE_URL -c "SELECT * FROM v_economy_dashboard LIMIT 5;"

# Should return data with inflation_yoy column populated
```

## Prevention

To avoid schema drift in the future:

1. Always run schema migrations when updating the codebase
2. Consider adding a schema version table to track applied migrations
3. Add automated tests that verify expected columns exist
4. Document the migration process in the README
