from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

kafka_url = "broker.kafka.l4lb.thisdcos.directory:9092"
topic_name = "example_topic"
kafka_brokers = []
def create(new_filing):
    print("-------------")
    print (new_filing)
    print ("ABC"+"EFG")


def readTopic():
    consumer = KafkaConsumer(bootstrap_servers=kafka_url,auto_offset_reset='earliest')
    consumer.subscribe([topic_name])

    print("Consumer length")
    print("Completed subscription")

    for msg in consumer:
        print msg

def writeTopic():
    producer = KafkaProducer(bootstrap_servers=kafka_url)

    future = producer.send(topic_name, b'raw_bytes')

    # Block for 'synchronous' sends
    try:
        record_metadata = future.get(timeout=10)
        # Successful result returns assigned partition and offset
        print (record_metadata.topic)
        print (record_metadata.partition)
        print (record_metadata.offset)
    except KafkaError:
        pass



if __name__ == "__main__":
    print("Start writing")
    writeTopic()
    print("start reading")
    readTopic()