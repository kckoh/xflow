
import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

# Kafka Config
BOOTSTRAP_SERVERS = ['localhost:29092']
TOPIC = 'user-events'  # Set to user's topic

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=json_serializer
)

print(f"🚀 Sending data to Kafka topic: {TOPIC}")
print(f"   Schema: user_id(int), event(str), page(str), amount(int), timestamp(str)")

EVENTS = ['checkout', 'add_to_cart', 'view', 'login', 'signup']
PAGES = ['/home', '/products/1', '/cart', '/profile', '/search']

# Send 50 messages then stop
for i in range(50):
    data = {
        "user_id": random.randint(1, 10000),
        "event": random.choice(EVENTS),
        "page": random.choice(PAGES),
        "amount": random.randint(10, 500),
        "timestamp": datetime.now().isoformat()
    }
    
    producer.send(TOPIC, data)
    print(f"Sent ({i+1}/50): {data}")
    
    time.sleep(0.5)  # Faster generation
producer.close()
