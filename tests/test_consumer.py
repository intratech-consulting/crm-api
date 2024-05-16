import os
import sys
import threading
import time

import pika
import unittest
from unittest.mock import patch, Mock

from pika.adapters.blocking_connection import BlockingChannel
from testcontainers.rabbitmq import RabbitMqContainer

if os.path.isdir('/app'):
    sys.path.append('/app')
else:
    local_dir = os.path.dirname(os.path.realpath(__file__))
    sys.path.append(local_dir)
import src.consumer as consumer
import config.secrets as secrets

class TestRabbitMQ(unittest.TestCase):
    @patch('src.xml_parser.add_service_id')
    @patch('src.xml_parser.get_service_id')
    def test_rabbitmq_messages(self, add_service_id_mock, get_service_id_mock):
        with RabbitMqContainer("rabbitmq:3-management", None, secrets.RABBITMQ_USER, secrets.RABBITMQ_PASSWORD) as rabbitmq:
            get_service_id_mock.return_value = {
                "crm": "1234"
            }
            add_service_id_mock.return_value = {
                "success": True,
                "message": "Service ID successfully added."
            }

            channel: BlockingChannel = configure_rabbitMQ(rabbitmq)

            consumer_thread = threading.Thread(target=consumer.main)
            consumer_thread.daemon = True
            consumer_thread.start()

            time.sleep(5)

            with open('tests/resources/dummy_user.xml', 'r') as file:
                test_message = file.read()

            channel.basic_publish(exchange='amq.topic', routing_key='user.frontend', body=test_message)
            channel.basic_consume(queue='crm', on_message_callback=callback, auto_ack=False)

            self.assertTrue(channel._consumer_infos)  # If consumer_infos is not empty, message was received

            consumer_thread.join()


def configure_rabbitMQ(rabbitmq) -> BlockingChannel:
    credentials = pika.PlainCredentials(secrets.RABBITMQ_USER, secrets.RABBITMQ_PASSWORD)
    mapped_port = rabbitmq.get_exposed_port(5672)
    secrets.PORT = mapped_port
    print(secrets.PORT)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=secrets.PORT, credentials=credentials))
    channel = connection.channel()


    exchange_name = 'amq.topic'
    services = ['crm', 'frontend', 'facturatie', 'kassa', 'mailing', 'inventory']
    objects = ['user', 'company', 'event', 'attendance', 'product', 'order']

    # Declare the exchange
    channel.exchange_declare(exchange=exchange_name, exchange_type='topic', durable=True)

    # For each service, declare a queue and bind it to the exchange with the appropriate routing key
    for service in services:
        channel.queue_declare(queue=service, durable=True)  # Set durable=True
        for obj in objects:
            for other_service in services:
                if other_service != service:  # Exclude the service's own queue
                    routing_key = f"{obj}.{other_service}"
                    channel.queue_bind(exchange=exchange_name, queue=service, routing_key=routing_key)

    return channel


def callback(ch, method, properties, body):
    print(f"Received message: {body}")


if __name__ == "__main__":
    unittest.main()
