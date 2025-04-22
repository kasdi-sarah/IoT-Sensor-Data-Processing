from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

timestamp = datetime.fromtimestamp(time.time()).isoformat()


TOPIC = 'sensor'

# genere les data
def generate_sensor_data():
    return {
        "sensor_id": random.randint(1, 100),
        "temperature": round(random.uniform(20.0, 40.0), 2),
        "humidity": round(random.uniform(30.0, 70.0), 2),
        "rainfall": round(random.uniform(0.0, 50.0), 2),
        "wind_speed": round(random.uniform(0.0, 20.0), 2),  
        "UV_index": round(random.uniform(0.0, 11.0), 2),
        "timestamp": time.time()
    }

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='pkc-e0zxq.eu-west-3.aws.confluent.cloud:9092', 
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username="LIQN5IFO4VF2U5KV",  
    sasl_plain_password="Qp6Zl3PuSayJSCRD8/pXJBJNNPIE6NCKWIMUsUk701c2HX6Pf/zVFg2Z1kVJzJei",  
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

    
try:
    while True:
        data = generate_sensor_data()
        producer.send(TOPIC, data)
        print(f"Sent: {data}")
        time.sleep(2)

except KeyboardInterrupt:
    print("Stopped by user.")
    
finally:
    # close
    producer.close()
    print("Kafka producer closed.")
