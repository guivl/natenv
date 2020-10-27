import os
import time
from google.cloud import pubsub

PROJECT = "natenv"
TOPIC = "cart_value"
INPUT = "/Users/guilhermelana/Documents/fiap/natenv/data/output-00000-of-00001"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/guilhermelana/Documents/fiap/natenv/natenv-055a2ca6fbee.json'


if __name__ == '__main__':
    publisher = pubsub.PublisherClient()
    event_type = publisher.topic_path(PROJECT, TOPIC)
    try:
        publisher.get_topic(event_type)
    except:
        publisher.create_topic(event_type)
        logging.info("Creating pub/sub topic {}".format(TOPIC))

    with open(INPUT, 'rb') as ifp:
        header = ifp.readline()

        for line in ifp:
            event_data = line.decode('utf-8').replace('\n', '').encode()
            print('Publishing {0} to {1}'.format(event_data, TOPIC))
            publisher.publish(event_type, event_data)
            time.sleep(20)