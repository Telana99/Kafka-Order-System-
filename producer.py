import json
import time
import random
from confluent_kafka import Producer
import avro.schema
import avro.io
import io

# Load Avro schema
with open('order.avsc', 'r') as f:
    schema = avro.schema.parse(f.read())

# Kafka Producer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'order-producer'
}

producer = Producer(conf)

PRODUCTS = [
    ("Laptop", 500, 2000),
    ("Mouse", 10, 100),
    ("Keyboard", 30, 200),
    ("Monitor", 150, 800),
    ("Headphones", 20, 300),
    ("Webcam", 40, 200),
    ("Phone", 300, 1500),
    ("Tablet", 200, 1000),
]

CUSTOMER_NAMES = [
    "Person1", "Person2", "Person3", "Person4",
    "Person5", "Person6", "Person7", "Person8",
    "Person9", "Person10", "Person11", "Person12"
]

def serialize_avro(order_data):
    """Serialize order data using Avro schema"""
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(order_data, encoder)
    return bytes_writer.getvalue()

def delivery_report(err, msg):
    """Callback function called when message is delivered or fails"""
    if err is not None:
        print(f'❌ Message delivery failed: {err}')
    else:
        print(f'✅ Order {msg.key().decode("utf-8")} delivered to {msg.topic()} [partition {msg.partition()}]')

def generate_order(order_id):
    """Generate a random order with customer name and timestamp"""
    product_name, min_price, max_price = random.choice(PRODUCTS)
    price = round(random.uniform(min_price, max_price), 2)
    customer_name = random.choice(CUSTOMER_NAMES)
    timestamp = int(time.time() * 1000)  # Current time in milliseconds
    
    order = {
        'orderid': str(order_id),
        'product': product_name,
        'price': price,
        'customer_name': customer_name,
        'timestamp': timestamp
    }
    return order

def main():
    print("🚀 Starting Order Producer...")
    print("📦 Generating and sending orders to Kafka...")
    print("Press Ctrl+C to stop\n")
    
    order_id = 1001  # Starting order ID
    
    try:
        while True:
           
            order = generate_order(order_id)
            
            serialized_order = serialize_avro(order)
            
           
            producer.produce(
                topic='orders',
                key=str(order_id).encode('utf-8'),
                value=serialized_order,
                callback=delivery_report
            )
            
            producer.flush()
            
            time_str = time.strftime('%H:%M:%S', time.localtime(order['timestamp'] / 1000))
            
            print(f"📤 Sent: Order {order['orderid']} | Customer: {order['customer_name']}")
            print(f"   Product: {order['product']} | Price: ${order['price']:.2f} | Time: {time_str}\n")
            
            order_id += 1
            time.sleep(2)  # Send one order every 2 seconds
            
    except KeyboardInterrupt:
        print("\n\n⛔ Producer stopped by user")
    finally:
        print(f"📊 Total orders sent: {order_id - 1001}")
        producer.flush()
        print("👋 Producer shutdown complete")

if __name__ == "__main__":
    main()