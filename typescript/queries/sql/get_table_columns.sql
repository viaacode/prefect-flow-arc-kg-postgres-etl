SELECT column_name AS name, data_type AS datatype
FROM information_schema.columns
WHERE table_name = $<name> AND table_schema = $<schema> AND column_name NOT IN ('created_at','updated_at')