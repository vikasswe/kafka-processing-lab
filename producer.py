from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="xxx.xxx.xx.xx:9092",

    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username="spade",
    sasl_plain_password="Vikas@123",

    acks="all",
    retries=5,
    linger_ms=50,
    batch_size=32768,
    enable_idempotence=True
)

i = 0

while True:
    i += 1

    order = {
        "order_id": i,
        "amount": 100,
        "status": "NEW",
        "timestamp": time.time()
    }

    data = json.dumps(order).encode()

    try:
        # BACKPRESSURE (important)
        future = producer.send("stress-topic", data)
        future.get(timeout=10)

        print(f"PRODUCED {i}")

    except Exception as e:
        print("PRODUCER ERROR:", e)
        time.sleep(2)
