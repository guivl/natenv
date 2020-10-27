import os
import time
import logging
import argparse
import datetime
from google.cloud import pubsub

TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
PROJECT = "natenv"
TOPIC = "traffic"
INPUT = "/Users/guilhermelana/Documents/fiap/natenv/data/ecommerce_traffic_data/2019-Dec.csv"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/Users/guilhermelana/Documents/fiap/natenv/natenv-055a2ca6fbee.json'

def publish(publisher, topic, events):
   numobs = len(events)
   if numobs > 0:
       logging.info('Publishing {0} events from {1}'.format(numobs, get_timestamp(events[0])))
       for event_data in events:
         publisher.publish(topic,event_data)

def get_timestamp(line):
   line = line.decode('utf-8')

   timestamp = line.split(',')[0]
   timestamp = timestamp.replace(" UTC", "")
   return datetime.datetime.strptime(timestamp, TIME_FORMAT)

def simulate(topic, ifp, firstObsTime, programStart, speedFactor):
   def compute_sleep_secs(obs_time):
        time_elapsed = (datetime.datetime.utcnow() - programStart).seconds
        sim_time_elapsed = (obs_time - firstObsTime).seconds / speedFactor
        to_sleep_secs = sim_time_elapsed - time_elapsed
        return to_sleep_secs

   topublish = list()

   for line in ifp:
       event_data = line
       obs_time = get_timestamp(line)

       if compute_sleep_secs(obs_time) > 1:
          publish(publisher, topic, topublish)
          topublish = list()

          to_sleep_secs = compute_sleep_secs(obs_time)
          if to_sleep_secs > 0:
             logging.info('Sleeping {} seconds'.format(to_sleep_secs))
             time.sleep(to_sleep_secs)
       topublish.append(event_data)

   publish(publisher, topic, topublish)

def peek_timestamp(ifp):
   pos = ifp.tell()
   line = ifp.readline()
   ifp.seek(pos)
   return get_timestamp(line)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Send ecommerce traffic data to Cloud Pub/Sub in small groups simulating real-time behavior")
    parser.add_argument('--speedFactor', help="Example: 60 implies 1 hour of data sent to Cloud Pub/Sub in 1 minute", required=True, type=float)
    args = parser.parse_args()

    logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)
    publisher = pubsub.PublisherClient()
    event_type = publisher.topic_path(PROJECT, TOPIC)
    try:
        publisher.get_topic(event_type)
        logging.info("Reusing pub/sub topic {}".format(TOPIC))

    except:
        publisher.create_topic(event_type)
        logging.info("Creating pub/sub topic {}".format(TOPIC))

    programStartTime = datetime.datetime.utcnow()
    with open(INPUT, 'rb') as ifp:
        header = ifp.readline()
        firstObsTime = peek_timestamp(ifp)
        logging.info("Sending sensor data from {}".format(firstObsTime))
        simulate(event_type, ifp, firstObsTime, programStartTime, args.speedFactor)
