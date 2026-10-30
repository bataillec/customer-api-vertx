-- RG-01: an email is unique whatever its case, DELETED customers included since they can come back.
-- Two emails that only differ by their case stop this migration and have to be resolved by hand.

ALTER TABLE customers DROP CONSTRAINT customers_email_key;

CREATE UNIQUE INDEX customers_email_lower_key ON customers (lower(email));
