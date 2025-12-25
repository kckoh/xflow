-- ============================================
-- XFlow Fake Data Generator
-- Total: ~1GB
-- NOTE: Run 'alembic upgrade head' first to create tables
-- ============================================

-- 1. Users (500K rows) ~60MB
-- Alembic이 테이블을 이미 생성했으므로 CREATE TABLE 제거

INSERT INTO users (email, password, created_at, updated_at)
SELECT
    'user_' || i || '@test.com',
    'password123',  -- 모든 유저 동일한 비밀번호 (테스트용)
    NOW() - (random() * interval '365 days'),
    NOW() - (random() * interval '365 days')
FROM generate_series(1, 500000) AS i;

-- 2. Products (50K rows) ~4MB
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10,2),
    stock INT
);

INSERT INTO products (name, category, price, stock)
SELECT
    'Product_' || i,
    (ARRAY['Electronics', 'Clothing', 'Food', 'Books', 'Toys'])[1 + (random() * 4)::int],
    (random() * 1000)::decimal(10,2),
    (random() * 1000)::int
FROM generate_series(1, 50000) AS i;

-- 3. Orders (10M rows) ~600MB
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    user_id INT,
    product_id INT,
    quantity INT,
    total_price DECIMAL(10,2),
    status VARCHAR(20),
    created_at TIMESTAMP
);

INSERT INTO orders (user_id, product_id, quantity, total_price, status, created_at)
SELECT
    1 + (random() * 499999)::int,
    1 + (random() * 49999)::int,
    1 + (random() * 10)::int,
    (random() * 500)::decimal(10,2),
    (ARRAY['pending', 'completed', 'cancelled', 'refunded'])[1 + (random() * 3)::int],
    NOW() - (random() * interval '730 days')
FROM generate_series(1, 10000000) AS i;

-- 4. Events (10M rows) ~400MB
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    user_id INT,
    event_type VARCHAR(50),
    page VARCHAR(100),
    ip VARCHAR(50),
    created_at TIMESTAMP
);

INSERT INTO events (user_id, event_type, page, ip, created_at)
SELECT
    1 + (random() * 499999)::int,
    (ARRAY['page_view', 'click', 'scroll', 'purchase', 'search'])[1 + (random() * 4)::int],
    '/page/' || (random() * 100)::int,
    (random() * 255)::int || '.' || (random() * 255)::int || '.' ||
    (random() * 255)::int || '.' || (random() * 255)::int,
    NOW() - (random() * interval '30 days')
FROM generate_series(1, 10000000) AS i;
