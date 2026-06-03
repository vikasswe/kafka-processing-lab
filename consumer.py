from kafka import KafkaConsumer
import time

consumer = KafkaConsumer(
    "stress-topic",
    bootstrap_servers="xxx.xxx.xx.xx:9092",

    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username="spade",
    sasl_plain_password="Vikas@123",

    group_id="order-service",

    auto_offset_reset="earliest",
    enable_auto_commit=True,
    auto_commit_interval_ms=1000,

    session_timeout_ms=15000,
    heartbeat_interval_ms=5000,

    max_poll_records=10,
    max_poll_interval_ms=300000
)

print("Consumer started")

for message in consumer:
    try:
        print(
            f"CONSUMED offset={message.offset} value={message.value.decode()}"
        )

        # simulate processing
#        time.sleep(1)

    except Exception as e:
        print("ERROR:", e)
