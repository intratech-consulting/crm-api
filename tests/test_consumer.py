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
    def test_user_create_should_make_valid_request(self):
        with RabbitMqContainer("rabbitmq:3-management", None, secrets.RABBITMQ_USER, secrets.RABBITMQ_PASSWORD) as rabbitmq:
            with patch('src.consumer.add_service_id') as add_service_id_mock, \
                    patch('src.xml_parser.get_service_id') as get_service_id_mock, \
                    patch('src.consumer.add_user') as add_user_mock, \
                    patch('src.consumer.log') as log_mock:

                get_service_id_mock.return_value = {
                    "crm": "1234"
                }
                add_service_id_mock.return_value = {
                    "success": True,
                    "message": "Service ID successfully added."
                }
                add_user_mock.return_value = '12345'

                log_mock.return_value = {
                    "success": True,
                    "message": "Log successfully added."
                }

                channel: BlockingChannel = configure_rabbitMQ(rabbitmq)

                def run_consumer():
                    with patch('src.consumer.add_service_id', add_service_id_mock), \
                            patch('src.xml_parser.get_service_id', get_service_id_mock), \
                            patch('src.consumer.add_user', add_user_mock):
                        start_time = time.time()
                        while time.time() - start_time < 10:  # Run for 5 seconds
                            try:
                                consumer.main()
                            except pika.exceptions.StreamLostError:
                                break

                consumer_thread = threading.Thread(target=run_consumer)
                consumer_thread.daemon = True
                consumer_thread.start()

                with open('tests/resources/dummy_user.xml', 'r') as file:
                    test_message = file.read()

                channel.basic_publish(exchange='amq.topic', routing_key='user.frontend', body=test_message)
                time.sleep(10)

                channel.basic_consume(queue='crm', on_message_callback=callback, auto_ack=False)

                self.assertTrue(channel._consumer_infos)  # If consumer_infos is not empty, message was received

                # Assert that add_user was called with the correct options
                add_user_mock.assert_called_once()


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
