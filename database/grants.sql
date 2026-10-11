-- Unity Catalog permissions for the trading ETL job. <DRAFT>
-- Execute once during environment setup.
-- Requires an identity authorized to manage these grants.

GRANT USE CATALOG
ON CATALOG cibc
TO `<job_run_as_principal>`;

-- Bronze
GRANT USE SCHEMA
ON SCHEMA cibc.bronze
TO `<job_run_as_principal>`;

GRANT SELECT, MODIFY
ON SCHEMA cibc.bronze
TO `<job_run_as_principal>`;

-- Silver
GRANT USE SCHEMA
ON SCHEMA cibc.silver
TO `<job_run_as_principal>`;

GRANT SELECT, MODIFY
ON SCHEMA cibc.silver
TO `<job_run_as_principal>`;

-- Gold
GRANT USE SCHEMA
ON SCHEMA cibc.gold
TO `<job_run_as_principal>`;

GRANT SELECT, MODIFY
ON SCHEMA cibc.gold
TO `<job_run_as_principal>`;

-- Dirty trades CSV
GRANT READ VOLUME
ON VOLUME cibc.bronze.trade_files
TO `<job_run_as_principal>`;