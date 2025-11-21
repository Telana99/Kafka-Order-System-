import json
import time
from confluent_kafka import Consumer, KafkaException

# Kafka Consumer configuration for DLQ
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'dlq-viewer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['orders-dlq'])

def format_timestamp(timestamp_ms):
    """Convert timestamp to readable format"""
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp_ms / 1000))

def display_failed_order(dlq_data):
    """Display failed order in a nice format"""
    order = dlq_data['original_order']
    
    print(f"\n{'='*80}")
    print(f"💀 FAILED ORDER DETAILS")
    print(f"{'='*80}")
    print(f"Order ID:        {order['orderid']}")
    print(f"Customer:        {order['customer_name']}")
    print(f"Product:         {order['product']}")
    print(f"Price:           ${order['price']:.2f}")
    print(f"Order Time:      {format_timestamp(order['timestamp'])}")
    print(f"\n❌ FAILURE INFORMATION")
    print(f"Failure Reason:  {dlq_data['failure_reason']}")
    print(f"Failed At:       {format_timestamp(dlq_data['failed_at'])}")
    print(f"Original Topic:  {dlq_data['original_topic']}")
    print(f"Partition:       {dlq_data['original_partition']}")
    print(f"Offset:          {dlq_data['original_offset']}")
    print(f"{'='*80}")

def main():
    print("🔍 Dead Letter Queue Viewer")
    print("📋 Monitoring failed orders from DLQ...")
    print("⏳ Waiting for failed orders to appear...")
    print("Press Ctrl+C to stop\n")
    print("="*80)
    
    failed_count = 0
    total_failed_value = 0
    no_message_count = 0
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                no_message_count += 1
                # Show waiting message every 10 seconds
                if no_message_count % 10 == 0:
                    print(f"⏳ Still waiting for failed orders... ({no_message_count}s)")
                continue
            
            if msg.error():
                raise KafkaException(msg.error())
            
            # Reset the waiting counter when we get a message
            no_message_count = 0
            
            try:
                # Parse DLQ message
                dlq_data = json.loads(msg.value().decode('utf-8'))
                
                # Display the failed order
                display_failed_order(dlq_data)
                
                # Update statistics
                failed_count += 1
                total_failed_value += dlq_data['original_order']['price']
                
                print(f"\n📊 DLQ Statistics:")
                print(f"   Total Failed Orders: {failed_count}")
                print(f"   Total Failed Value: ${total_failed_value:.2f}")
                print(f"   Average Failed Order Value: ${total_failed_value/failed_count:.2f}")
                print()
                
            except json.JSONDecodeError:
                print(f"⚠️  Could not parse DLQ message (might be old format)")
            except KeyError as e:
                print(f"⚠️  Missing field in DLQ message: {e}")
    
    except KeyboardInterrupt:
        print("\n\n⛔ DLQ Viewer stopped by user")
    finally:
        print(f"\n{'='*80}")
        print(f"📊 FINAL DLQ STATISTICS")
        print(f"{'='*80}")
        print(f"Total Failed Orders: {failed_count}")
        if failed_count > 0:
            print(f"Total Failed Value: ${total_failed_value:.2f}")
            print(f"Average Failed Order Value: ${total_failed_value/failed_count:.2f}")
        else:
            print("No failed orders were received during this session.")
        print(f"{'='*80}")
        consumer.close()
        print("👋 DLQ Viewer shutdown complete")

if __name__ == "__main__":
    main()