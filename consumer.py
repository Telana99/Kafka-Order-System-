import json
import time
import random
from confluent_kafka import Consumer, Producer, KafkaException
import avro.schema
import avro.io
import io

# Load Avro schema
with open('order.avsc', 'r') as f:
    schema = avro.schema.parse(f.read())

# Kafka Consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

# Kafka Producer for DLQ
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'dlq-producer'
}

consumer = Consumer(consumer_conf)
dlq_producer = Producer(producer_conf)

consumer.subscribe(['orders'])

# Statistics tracking
stats = {
    'total_price': 0,
    'order_count': 0,
    'running_average': 0,
    'max_price': 0,
    'min_price': float('inf'),
    'max_order': None,
    'min_order': None,
    'total_revenue': 0,
    'product_counts': {}
}

def deserialize_avro(avro_bytes):
    """Deserialize Avro bytes back to order data"""
    bytes_reader = io.BytesIO(avro_bytes)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    return reader.read(decoder)

def send_to_dlq(message, reason):
    """Send failed message to Dead Letter Queue with metadata"""
    try:
        order_data = deserialize_avro(message.value())
        
        # Create DLQ message with failure info
        dlq_message = {
            'original_order': order_data,
            'failure_reason': reason,
            'failed_at': int(time.time() * 1000),
            'original_topic': message.topic(),
            'original_partition': message.partition(),
            'original_offset': message.offset()
        }
        
        dlq_value = json.dumps(dlq_message).encode('utf-8')
        
        dlq_producer.produce(
            topic='orders-dlq',
            key=message.key(),
            value=dlq_value
        )
        dlq_producer.flush()
        print(f"💀 Sent to DLQ: Order {order_data['orderid']} | Customer: {order_data['customer_name']} | Reason: {reason}")
    except Exception as e:
        print(f"❌ Failed to send to DLQ: {e}")

def update_statistics(order_data):
    """Update all statistics with new order"""
    price = order_data['price']
    product = order_data['product']
    
    stats['order_count'] += 1
    stats['total_price'] += price
    stats['total_revenue'] += price
    stats['running_average'] = stats['total_price'] / stats['order_count']
    
    if price > stats['max_price']:
        stats['max_price'] = price
        stats['max_order'] = order_data
    
    if price < stats['min_price']:
        stats['min_price'] = price
        stats['min_order'] = order_data
    
    if product in stats['product_counts']:
        stats['product_counts'][product] += 1
    else:
        stats['product_counts'][product] = 1

def display_statistics():
    """Display comprehensive statistics"""
    print(f"\n{'='*70}")
    print(f"📊 REAL-TIME STATISTICS")
    print(f"{'='*70}")
    print(f"Total Orders Processed: {stats['order_count']}")
    print(f"Total Revenue: ${stats['total_revenue']:.2f}")
    print(f"Running Average Price: ${stats['running_average']:.2f}")
    print(f"\n💰 Price Range:")
    print(f"   Highest: ${stats['max_price']:.2f} ({stats['max_order']['product']} - {stats['max_order']['customer_name']})")
    print(f"   Lowest: ${stats['min_price']:.2f} ({stats['min_order']['product']} - {stats['min_order']['customer_name']})")
    
    if stats['product_counts']:
        print(f"\n🏆 Top Products:")
        sorted_products = sorted(stats['product_counts'].items(), key=lambda x: x[1], reverse=True)
        for i, (product, count) in enumerate(sorted_products[:3], 1):
            print(f"   {i}. {product}: {count} orders")
    print(f"{'='*70}\n")

def process_order(order_data):
    """Process the order - Simulates occasional failures (40% chance)"""
    if random.random() < 0.4:
        raise Exception("Simulated temporary processing failure")
    
    update_statistics(order_data)
    
    time_str = time.strftime('%H:%M:%S', time.localtime(order_data['timestamp'] / 1000))
    
    print(f"✅ Processed: Order {order_data['orderid']} | Customer: {order_data['customer_name']}")
    print(f"   Product: {order_data['product']} | Price: ${order_data['price']:.2f} | Time: {time_str}")
    
    # Display statistics every 5 orders
    if stats['order_count'] % 5 == 0:
        display_statistics()

def process_message_with_retry(message, max_retries=3):
    """Process message with retry logic"""
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
                wait_time = 2 ** attempt
                print(f"⏳ Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"❌ All retries exhausted for Order {message.key().decode('utf-8')}")
                return False
    
    return False

def main():
    print("🚀 Starting Order Consumer...")
    print("📥 Listening for orders from Kafka...")
    print("📊 Tracking: Average, Min, Max, Revenue, Product Counts")
    print("🔄 Retry logic: 3 attempts with exponential backoff")
    print("💀 Failed messages will go to DLQ")
    print("Press Ctrl+C to stop\n")
    print("=" * 70)
    
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
                send_to_dlq(msg, "Max retries exceeded")
                consumer.commit(message=msg)
            
    except KeyboardInterrupt:
        print("\n\n⛔ Consumer stopped by user")
    finally:
        print(f"\n{'='*70}")
        print(f"📊 FINAL STATISTICS")
        print(f"{'='*70}")
        print(f"Total Orders Processed: {stats['order_count']}")
        print(f"Total Revenue: ${stats['total_revenue']:.2f}")
        print(f"Final Running Average: ${stats['running_average']:.2f}")
        if stats['order_count'] > 0:
            print(f"Highest Price: ${stats['max_price']:.2f}")
            print(f"Lowest Price: ${stats['min_price']:.2f}")
        print(f"{'='*70}")
        consumer.close()
        dlq_producer.flush()
        print("👋 Consumer shutdown complete")

if __name__ == "__main__":
    main()