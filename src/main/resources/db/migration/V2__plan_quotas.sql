CREATE TABLE plan_quotas
(
    plan_type   plan_type    NOT NULL,
    feature_key VARCHAR(100) NOT NULL,
    quota       BIGINT       NOT NULL CHECK (quota >= 0),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (plan_type, feature_key)
);

INSERT INTO plan_quotas (plan_type, feature_key, quota)
VALUES ('FREE_TIER', 'API_CALLS_MONTHLY', 1000),
       ('FREE_TIER', 'STORAGE_GB', 1),
       ('PRO', 'API_CALLS_MONTHLY', 100000),
       ('PRO', 'STORAGE_GB', 50),
       ('ENTERPRISE', 'API_CALLS_MONTHLY', 10000000),
       ('ENTERPRISE', 'STORAGE_GB', 1000);
