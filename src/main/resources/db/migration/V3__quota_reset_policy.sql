-- Features such as STORAGE_GB measure what is held, not what was used in a period, so their usage must not reset.

ALTER TABLE plan_quotas
    ADD COLUMN resets_monthly BOOLEAN NOT NULL DEFAULT TRUE;

UPDATE plan_quotas
SET resets_monthly = FALSE
WHERE feature_key = 'STORAGE_GB';

-- A NULL reset_date means that the usage never resets.
ALTER TABLE entitlements
    ALTER COLUMN reset_date DROP NOT NULL;

UPDATE entitlements
SET reset_date = NULL
WHERE feature_key = 'STORAGE_GB';
