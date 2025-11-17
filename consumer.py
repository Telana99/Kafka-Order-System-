import json
import time
import random
from confluent_kafka import Consumer, Producer, KafkaException
import avro.schema
import avro.io
import io

with open('order.avsc', 'r') as f:
    schema = avro.schema.parse(f.read())

# Kafka Consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-consumer-group',
    'auto.offset.reset': 'earliest',  # Start from beginning if no offset
    'enable.auto.commit': False  # Manual commit for retry logic
}

# Kafka Producer for DLQ
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'dlq-producer'
}

consumer = Consumer(consumer_conf)
dlq_producer = Producer(producer_conf)

consumer.subscribe(['orders'])

total_price = 0
order_count = 0
running_average = 0

def deserialize_avro(avro_bytes):
    """Deserialize Avro bytes back to order data"""
    bytes_reader = io.BytesIO(avro_bytes)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    return reader.read(decoder)

def send_to_dlq(message, reason):
    """Send failed message to Dead Letter Queue"""
    try:
        dlq_value = message.value() + f"\nFAILURE_REASON: {reason}".encode('utf-8')
        
        dlq_producer.produce(
            topic='orders-dlq',
            key=message.key(),
            value=dlq_value
        )
        dlq_producer.flush()
        print(f" Sent to DLQ: Order {message.key().decode('utf-8')} | Reason: {reason}")
    except Exception as e:
        print(f"Failed to send to DLQ: {e}")

def process_order(order_data):
    """
    Process the order. 
    Simulates occasional failures (10% chance) to demonstrate retry logic
    """
    global total_price, order_count, running_average
    
    # Simulate random failures (10% chance)
    if random.random() < 0.1:
        raise Exception("Simulated temporary processing failure")
    
    order_count += 1
    total_price += order_data['price']
    running_average = total_price / order_count
    
    print(f" Processed: Order {order_data['orderid']} | Product: {order_data['product']} | Price: ${order_data['price']:.2f}")
    print(f" Running Average Price: ${running_average:.2f} (from {order_count} orders)\n")

def process_message_with_retry(message, max_retries=3):
    """
    Process message with retry logic.
    Returns True if successful, False if permanently failed
    """
    attempt = 0
    
    while attempt < max_retries:
        try:
            order_data = deserialize_avro(message.value())
            
            process_order(order_data)
            
            return True  
            
        except Exception as e:
            attempt += 1
            print(f"⚠️  Attempt {attempt}/{max_retries} failed for Order {message.key().decode('utf-8')}: {e}")
            
            if attempt < max_retries:
                wait_time = 2 ** attempt  # 2, 4, 8 seconds
                print(f"⏳ Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"All retries exhausted for Order {message.key().decode('utf-8')}")
                return False  
    
    return False

def main():
    print("🚀 Starting Order Consumer...")
    print("📥 Listening for orders from Kafka...")
    print("📊 Calculating running average of prices...")
    print("🔄 Retry logic: 3 attempts with exponential backoff")
    print("💀 Failed messages will go to DLQ")
    print("Press Ctrl+C to stop\n")
    print("=" * 60)
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue  
            
            if msg.error():
                raise KafkaException(msg.error())
            
            success = process_message_with_retry(msg)
            
            if success:
                consumer.commit(message=msg)
            else:
                # Send to DLQ after all retries failed
                send_to_dlq(msg, "Max retries exceeded")
                consumer.commit(message=msg) 
    except KeyboardInterrupt:
        print("\n\n Consumer stopped by user")
    finally:
        print(f"\n📊 Final Statistics:")
        print(f"   Total Orders Processed: {order_count}")
        print(f"   Final Running Average: ${running_average:.2f}")
        consumer.close()
        dlq_producer.flush()
        print("👋 Consumer shutdown complete")

if __name__ == "__main__":
    main()