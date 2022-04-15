from base64 import encode
import paho.mqtt.client as mqtt
from pykafka import KafkaClient
import time 

def on_message(client,userdata,message):
    msg_payload = message.payload
    print(msg_payload)    
    kafka_producer.produce(msg_payload)
     
kafka_client= KafkaClient(hosts='kafka:9092')
kafka_topic = kafka_client.topics['test_topic']
kafka_producer = kafka_topic.get_sync_producer()

hostaddress ="172.17.0.1"
port = 1883

client = mqtt.Client("test_data_id")
client.connect(hostaddress,port)

client.loop_start()
client.subscribe("data")
client.on_message = on_message
time.sleep(30)
client.loop_stop()
