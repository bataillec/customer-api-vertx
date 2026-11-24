-- Transactional outbox: an audit event is written in the transaction of the change it describes, and a relay
-- publishes it to Kafka afterwards. `id` orders the events: it is assigned under the customer row lock, so it grows
-- with the commit order of one customer. No foreign key: it would add a lookup on the hot path, and customers are
-- never deleted.
CREATE TABLE outbox
(
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id UUID        NOT NULL,
    payload     JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at     TIMESTAMPTZ
);

-- What the relay reads, and the backlog: only the rows still to send
CREATE INDEX outbox_unsent_idx ON outbox (id) WHERE sent_at IS NULL;

-- What the pruning reads: only the rows already sent
CREATE INDEX outbox_sent_idx ON outbox (sent_at) WHERE sent_at IS NOT NULL;
