from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
gudang_ids = ['G1', 'G2', 'G3']

while True:
    for gid in gudang_ids:
        kelembaban = random.randint(60, 80)
        data = {"gudang_id": gid, "kelembaban": kelembaban}
        producer.send("sensor-kelembaban-gudang", value=data)
        print("Kirim:", data)
    time.sleep(1)
