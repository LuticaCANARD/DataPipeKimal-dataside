from kafka import KafkaProducer
import Adafruit_DHT

# Set up Kafka producer
bootstrap_servers = 'your_kafka_bootstrap_servers'
topic = 'your_kafka_topic'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Set up DHT sensor
sensor = Adafruit_DHT.DHT11
pin = 4

# Read temperature and humidity data
humidity, temperature = Adafruit_DHT.read_retry(sensor, pin)

# Check if data is valid
if humidity is not None and temperature is not None:
    # Format data as string
    data = f'Temperature: {temperature:.2f}°C, Humidity: {humidity:.2f}%'

    # Send data to Kafka
    producer.send(topic, data.encode('utf-8'))
    producer.flush()

    print('Data sent to Kafka successfully.')
else:
    print('Failed to retrieve data from sensor.')