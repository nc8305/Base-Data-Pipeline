import json
import time
import random
import uuid
from kafka import KafkaProducer

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
topic_name = "hpc-raw-metrics"

# Define a list of diverse mock hosts/identities
MOCK_HOSTS = [
    "windows-client-1",
    "windows-client-2",
    "linux-worker-1",
    "linux-worker-2",
    "gpu-node-1"
]

def generate_procstat_mock(host_id):
    return {
        "fields": {
            "cmdline": f"\"C:\\VS Code\\Code.exe\" --version=1.127.0 --uid={uuid.uuid4()}",
            "cpu_time_system": round(random.uniform(0.1, 1.5), 4),
            "memory_rss": random.randint(4000000, 80000000),
            "num_threads": random.randint(5, 20),
            "pid": random.randint(1000, 20000)
        },
        "name": "procstat",
        "tags": {
            "cluster": "hpc-local-poc",
            "datacenter": "vn-hcm-zone-1",
            "host": host_id,
            "process_name": "Code.exe"
        },
        "timestamp": int(time.time() * 1000)
    }

def generate_power_mock(host_id):
    return {
        "fields": {
            "Power": random.randint(3000, 8000)
        },
        "name": "hw_power",
        "tags": {
            "cluster": "hpc-local-poc",
            "datacenter": "vn-hcm-zone-1",
            "host": host_id
        },
        "timestamp": int(time.time() * 1000)
    }

def generate_eventlog_mock(host_id):
    return {
        "fields": {
            "EventID": random.choice([10016, 404, 500]),
            "Message": "The application-specific permission settings do not grant Local Activation permission for the COM Server..."
        },
        "name": "win_eventlog",
        "tags": {
            "Level": str(random.choice([2, 3])),
            "Source": "Microsoft-Windows-DistributedCOM",
            "host": host_id
        },
        "timestamp": int(time.time() * 1000)
    }

print("Starting mock data generation for Kafka (Press Ctrl+C to stop)...")

try:
    while True:
        # Randomly select a payload type and a host identity
        payload_type = random.choice(["procstat", "power", "eventlog"])
        selected_host = random.choice(MOCK_HOSTS)
        
        if payload_type == "procstat":
            data = generate_procstat_mock(selected_host)
        elif payload_type == "power":
            data = generate_power_mock(selected_host)
        else:
            data = generate_eventlog_mock(selected_host)
            
        producer.send(topic_name, value=data)
        
        # Extract the first metric value for logging purposes
        first_metric_val = list(data['fields'].values())[0]
        print(f"[SENT] Type: {data['name']:<15} | Host: {selected_host:<16} | Metric/Event: {first_metric_val}")
        
        # Pause for 1 second between transmissions
        time.sleep(1)
        
except KeyboardInterrupt:
    print("\nStopped mock data generation.")
finally:
    producer.flush()
    producer.close()