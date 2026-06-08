from kafka import KafkaProducer
from faker import Faker
import json
import time
import random
from datetime import datetime

fake = Faker()

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

PRODUCTS = [
    {"name": "Laptop", "category": "Electronics", "price": 1200},
    {"name": "Phone", "category": "Electronics", "price": 800},
    {"name": "Headphones", "category": "Electronics", "price": 150},
    {"name": "Running Shoes", "category": "Sports", "price": 90},
    {"name": "Yoga Mat", "category": "Sports", "price": 35},
    {"name": "Coffee Maker", "category": "Kitchen", "price": 60},
    {"name": "Blender", "category": "Kitchen", "price": 45},
    {"name": "Novel Book", "category": "Books", "price": 15},
    {"name": "Desk Chair", "category": "Furniture", "price": 300},
    {"name": "Monitor", "category": "Electronics", "price": 400},
]

STATUSES = ["placed", "confirmed", "processing"]

print("Starting order producer... Press Ctrl+C to stop")

order_id = 1
while True:
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)

    order = {
        "order_id": f"ORD-{order_id:06d}",
        "customer_name": fake.name(),
        "customer_email": fake.email(),
        "customer_country": fake.country_code(),
        "product_name": product["name"],
        "category": product["category"],
        "unit_price": product["price"],
        "quantity": quantity,
        "total_amount": product["price"] * quantity,
        "status": random.choice(STATUSES),
        "timestamp": datetime.utcnow().isoformat()
    }

    producer.send('ecommerce-orders', value=order)
    print(f"Sent: {order['order_id']} | {order['product_name']} | ${order['total_amount']}")

    order_id += 1
    time.sleep(1)
