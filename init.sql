CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_name TEXT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE PUBLICATION conduit_pub FOR TABLE orders;

SELECT pg_create_logical_replication_slot('conduit_slot', 'pgoutput');