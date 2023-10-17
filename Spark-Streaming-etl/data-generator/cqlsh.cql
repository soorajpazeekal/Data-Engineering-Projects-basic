CREATE KEYSPACE IF NOT EXISTS live_feed WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

CREATE TABLE live_feed.live_table (
    order_id TEXT PRIMARY KEY,
    name TEXT,
    email TEXT,
    total_price DECIMAL,
    discount_or_promotion BOOLEAN,
    paymentmethod TEXT,
    latitude DOUBLE,
    longitude DOUBLE,
    created_at TIMESTAMP
);