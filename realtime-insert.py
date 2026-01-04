"""
Real-time data inserter for PostgreSQL
Inserts 1 row per table every second
"""
import psycopg2
import random
import time
from datetime import datetime

# Database connection
conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="mydb",
    user="postgres",
    password="postgres"
)
conn.autocommit = True
cur = conn.cursor()

# Sample data
CATEGORIES = ['Electronics', 'Clothing', 'Food', 'Books', 'Toys']
STATUSES = ['pending', 'completed', 'cancelled', 'refunded']
EVENT_TYPES = ['page_view', 'click', 'scroll', 'purchase', 'search']

print("🚀 Starting real-time data insertion...")
print("   Press Ctrl+C to stop\n")

count = 0
try:
    while True:
        count += 1
        now = datetime.now()

        # 1. Insert Product
        cur.execute("""
            INSERT INTO products (name, category, price, stock)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """, (
            f"Product_{now.strftime('%H%M%S')}_{count}",
            random.choice(CATEGORIES),
            round(random.uniform(10, 500), 2),
            random.randint(1, 100)
        ))
        product_id = cur.fetchone()[0]

        # 2. Insert Order
        cur.execute("""
            INSERT INTO orders (user_id, product_id, quantity, total_price, status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            random.randint(1, 500000),
            product_id,
            random.randint(1, 5),
            round(random.uniform(20, 300), 2),
            random.choice(STATUSES),
            now
        ))
        order_id = cur.fetchone()[0]

        # 3. Insert Event
        cur.execute("""
            INSERT INTO events (user_id, event_type, page, ip, created_at)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (
            random.randint(1, 500000),
            random.choice(EVENT_TYPES),
            f"/page/{random.randint(1, 100)}",
            f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
            now
        ))
        event_id = cur.fetchone()[0]

        print(f"[{now.strftime('%H:%M:%S')}] Inserted: product={product_id}, order={order_id}, event={event_id}")

        time.sleep(0.2)  # 1초에 5개

except KeyboardInterrupt:
    print(f"\n\n✅ Stopped. Total inserted: {count} rows per table")

finally:
    cur.close()
    conn.close()
