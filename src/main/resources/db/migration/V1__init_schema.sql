-- Schema for customer-api-vertx (spec_technical.md, with the fixes from ARCHITECTURE.md section 6).

CREATE TYPE customer_status AS ENUM ('ACTIVE', 'SUSPENDED', 'DELETED');
CREATE TYPE plan_type AS ENUM ('FREE_TIER', 'PRO', 'ENTERPRISE');
CREATE TYPE subscription_status AS ENUM ('ACTIVE', 'CANCELED', 'EXPIRED');

CREATE TABLE customers
(
    id           UUID PRIMARY KEY         DEFAULT gen_random_uuid(),
    email        VARCHAR(255)    NOT NULL UNIQUE, -- RG-01
    company_name VARCHAR(255),
    status       customer_status NOT NULL DEFAULT 'ACTIVE',
    created_at   TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE subscriptions
(
    id          UUID PRIMARY KEY             DEFAULT gen_random_uuid(),
    customer_id UUID                NOT NULL REFERENCES customers (id) ON DELETE CASCADE,
    plan_type   plan_type           NOT NULL,
    status      subscription_status NOT NULL DEFAULT 'ACTIVE',
    start_date  TIMESTAMPTZ         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_date    TIMESTAMPTZ
);

-- RG-02: one ACTIVE subscription per customer; CANCELED/EXPIRED rows are unlimited.
CREATE UNIQUE INDEX unique_active_subscription
    ON subscriptions (customer_id) WHERE status = 'ACTIVE';

CREATE INDEX idx_subscriptions_customer ON subscriptions (customer_id, start_date DESC);

CREATE TABLE entitlements
(
    id              UUID PRIMARY KEY      DEFAULT gen_random_uuid(),
    customer_id     UUID         NOT NULL REFERENCES customers (id) ON DELETE CASCADE,
    feature_key     VARCHAR(100) NOT NULL,
    allocated_quota BIGINT       NOT NULL CHECK (allocated_quota >= 0),
    consumed_quota  BIGINT       NOT NULL DEFAULT 0 CHECK (consumed_quota >= 0),
    reset_date      TIMESTAMPTZ  NOT NULL,
    CONSTRAINT unique_customer_feature UNIQUE (customer_id, feature_key) -- also serves lookups
);
