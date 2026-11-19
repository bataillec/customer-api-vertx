-- Keyset pagination of GET /customers: ORDER BY created_at DESC, id DESC
CREATE INDEX customers_created_at_id_idx ON customers (created_at DESC, id DESC);
