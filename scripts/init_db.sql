/*
==============================================================
⚠️ WARNING:
This script will permanently DROP the 'DataWarehouse' database
if it exists. Make sure you have a backup if needed before
executing this script.

What this script does:
1. Checks if the 'DataWarehouse' database exists.
2. If it exists, drops it using a DO block.
3. Creates three schemas — bronze, silver, and gold
==============================================================
*/

DO $$
BEGIN
   IF EXISTS (
       SELECT 1 FROM pg_database WHERE datname = 'DataWarehouse'
   ) THEN
       EXECUTE 'DROP DATABASE "DataWarehouse"';
   END IF;
END
$$;

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
