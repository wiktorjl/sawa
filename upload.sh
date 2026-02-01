#!/bin/bash


export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=postgres
export PGUSER=postgres
export PGPASSWORD=Hello7710


# Load companies first (required for foreign key constraints)
#python3 load_csv_to_postgres.py --mapping data_mappings.json --tables companies --upsert

# Then load other tables
#python3 load_csv_to_postgres.py --mapping data_mappings.json --tables financial_ratios --upsert
python3 load_csv_to_postgres.py --mapping data_mappings.json --tables balance_sheets --upsert
