SELECT COLUMN_NAME from information_schema.key_column_usage 
WHERE table_name = $<name> AND table_schema = $<schema> AND constraint_name LIKE '%pkey'