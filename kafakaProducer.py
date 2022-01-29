import time
import json
from confluent_kafka import Producer

with open('config.json') as json_file:
    config = json.load(json_file)

p = Producer(config)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print("Produced record to topic {} partition [{}] @ offset {}"
              .format(msg.topic(), msg.partition(), msg.offset()))

topic=p.list_topics()
print(topic.brokers)
for data in [1,2]:
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)
    record_key = "alice"
    record_value = json.dumps({'count': data})

    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
    print("Producing record: {}\t{}".format(record_key, record_value))
    p.produce('test', key=record_key, value=record_value, callback=delivery_report)

p.flush()

