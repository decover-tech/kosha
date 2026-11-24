DO $$
BEGIN
    -- Even a no-op ALTER TABLE takes an ACCESS EXCLUSIVE lock.
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute
        WHERE attrelid = 'kosha.tenants'::regclass
          AND attname = 'kms_key_id'
          AND NOT attisdropped
    ) THEN
        ALTER TABLE kosha.tenants ADD COLUMN IF NOT EXISTS kms_key_id TEXT;
    END IF;
END;
$$;
