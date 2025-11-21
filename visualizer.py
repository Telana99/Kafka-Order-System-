import matplotlib.pyplot as plt
import matplotlib.animation as animation
from confluent_kafka import Consumer, KafkaException
import avro.schema
import avro.io
import io
from collections import defaultdict

# Load Avro schema
with open('order.avsc', 'r') as f:
    schema = avro.schema.parse(f.read())

# Kafka Consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'visualizer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['orders'])

# Track product counts
product_counts = defaultdict(int)

def deserialize_avro(avro_bytes):
    """Deserialize Avro bytes back to order data"""
    bytes_reader = io.BytesIO(avro_bytes)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    return reader.read(decoder)

def update_chart(frame):
    """Update the bar chart with new data"""
    # Poll for new messages
    msg = consumer.poll(timeout=0.1)
    
    if msg is not None and not msg.error():
        try:
            order_data = deserialize_avro(msg.value())
            product = order_data['product']
            product_counts[product] += 1
            print(f"📊 Updated: {product} = {product_counts[product]} orders")
        except Exception as e:
            print(f"⚠️  Error processing message: {e}")
    
    # Clear and redraw chart
    plt.cla()
    
    if product_counts:
        # Sort products by count (descending)
        sorted_products = sorted(product_counts.items(), key=lambda x: x[1], reverse=True)
        products = [p[0] for p in sorted_products]
        counts = [p[1] for p in sorted_products]
        
        # Create bar chart
        bars = plt.bar(products, counts, color='skyblue', edgecolor='navy', linewidth=1.5)
        
        # Add value labels on top of bars
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(height)}',
                    ha='center', va='bottom', fontweight='bold')
        
        # Styling
        plt.xlabel('Products', fontsize=12, fontweight='bold')
        plt.ylabel('Order Count', fontsize=12, fontweight='bold')
        plt.title('📊 Real-Time Product Order Counts', fontsize=14, fontweight='bold', pad=20)
        plt.xticks(rotation=45, ha='right')
        plt.grid(axis='y', alpha=0.3, linestyle='--')
        plt.tight_layout()
    else:
        plt.text(0.5, 0.5, 'Waiting for orders...', 
                ha='center', va='center', fontsize=16, transform=plt.gca().transAxes)
        plt.title('📊 Real-Time Product Order Counts', fontsize=14, fontweight='bold')

def main():
    print("📊 Starting Product Visualization...")
    print("📈 Bar chart will update in real-time as orders arrive")
    print("Close the chart window to stop\n")
    
    # Create figure and animation
    fig = plt.figure(figsize=(10, 6))
    
    # Update chart every 1000ms (1 second)
    ani = animation.FuncAnimation(fig, update_chart, interval=1000, cache_frame_data=False)
    
    try:
        plt.show()
    except KeyboardInterrupt:
        print("\n⛔ Visualizer stopped by user")
    finally:
        consumer.close()
        print("👋 Visualizer shutdown complete")

if __name__ == "__main__":
    main()