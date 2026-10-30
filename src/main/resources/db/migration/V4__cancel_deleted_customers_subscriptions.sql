-- Deleting a customer now cancels its subscription: align the customers deleted before that rule.

UPDATE subscriptions
SET status   = 'CANCELED',
    end_date = now()
WHERE status = 'ACTIVE'
  AND customer_id IN (SELECT id FROM customers WHERE status = 'DELETED');
