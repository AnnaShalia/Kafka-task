import csv
import config
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer, KafkaConsumer


def get_raw(url):
    """Retrieve raw html from given url"""
    try:
        html = requests.get(url).text
    except Exception as ex:
        print('Error while fetching raw html: ')
        print(str(ex))
    finally:
        return html.strip()


def get_listing(url):
    """Get raw html of listings"""
    listings = []
    try:
        html = get_raw(url)
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.select('a.contentCard-wrap')
        for link in links:
            item = get_raw(link['href'])
            listings.append(item)
    except Exception as ex:
        print('Error while fetching listing item raw html: ')
        print(str(ex))
    finally:
        return listings


def publish_message(producer_instance, topic_name, key, value):
    """Publish topic and messages to Kafka"""
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Error while publishing message: ')
        print(str(ex))


def connect_kafka_producer(host):
    """Create Kafka producer instance"""
    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers=[host], api_version=(0, 10), max_block_ms=300000)
    except Exception as ex:
        print('Error while connecting to Kafka producer: ')
        print(str(ex))
    finally:
        return producer


def connect_kafka_consumer(host, topic_name):
    """Connect Kafka consumer and get topic"""
    try:
        consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
                                 bootstrap_servers=[host], api_version=(0, 10), consumer_timeout_ms=1000)
    except Exception as ex:
        print('Error while connecting to Kafka consumer: ')
        print(str(ex))
    finally:
        return consumer


def get_parsed_records(consumer):
    """Parse records from topic using Kafka consumer"""
    parsed_records = []
    for msg in consumer:
        soup = BeautifulSoup(msg.value, 'html.parser')
        print('Message consumed successfully.')
        title = soup.select('h1.txt.txt_h2.m-txt_inlineBlock')[0].text
        parsed_records.append(title)
    return parsed_records


if __name__ == '__main__':
    # get dogs listing
    dogs_listings = get_listing('https://www.petfinder.com/dog-breeds/?size[]=extraLarge&vocality[]='
                                'high&affectionNeeds[]=balanced&viewAllFilters=on&page=1#Filters_Container')
    # connect kafka producer
    kafka_producer = connect_kafka_producer(config.host)
    # publish message for each dogs listing
    for listing in dogs_listings:
        publish_message(kafka_producer, 'raw_dogs_listing', 'raw', listing)
    # connect kafka consumer and get raw_dogs_listing
    consumer_raw = connect_kafka_consumer(config.host, 'raw_dogs_listing')
    # parsed dogs listing received from kafka
    parsed_dogs_listing = get_parsed_records(consumer_raw)
    consumer_raw.close()
    # publish message with parsed dogs listing data to kafka
    for parsed_dogs_data in parsed_dogs_listing:
        publish_message(kafka_producer, 'parsed_dogs_listing', 'parsed', parsed_dogs_data)
    kafka_producer.close()
    # connect kafka consumer and retrieve parsed_dogs_listing
    consumer_parsed = connect_kafka_consumer(config.host, 'parsed_dogs_listing')
    # parse consumed data and store data to list
    consumed_parsed_listing = []
    for msg in consumer_parsed:
        soup = BeautifulSoup(msg.value, 'html.parser')
        consumed_parsed_listing.append(soup)
    consumer_parsed.close()
    # record result to the local file
    with open('result.csv', 'w+', newline='') as file:
        write = csv.writer(file)
        write.writerows(consumed_parsed_listing)
