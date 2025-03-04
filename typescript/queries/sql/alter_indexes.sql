UPDATE pg_index
SET indisready=$<enabled>
WHERE indrelid = (
    SELECT oid
    FROM pg_class
    WHERE relname=$<name> AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $<schema>)
);