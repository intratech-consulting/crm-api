import os
import sys
import threading
import time
import unittest
from unittest.mock import patch

import pika
from pika.adapters.blocking_connection import BlockingChannel
from testcontainers.rabbitmq import RabbitMqContainer

sys.path.append('./')
import src.consumer as consumer
import config.secrets as secrets


class TestRabbitMQ(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rabbitmq = RabbitMqContainer("rabbitmq:3-management", None, secrets.RABBITMQ_USER, secrets.RABBITMQ_PASSWORD)
        cls.rabbitmq.start()

    @classmethod
    def tearDownClass(cls):
        cls.rabbitmq.stop()

    @patch('requests.post')
    def test_user_create_should_make_valid_request(self, mock_post):
        with (patch('src.consumer.add_service_id') as add_service_id_mock,
              patch('src.xml_parser.get_service_id') as get_service_id_mock,
              patch('src.consumer.add_user') as add_user_mock,
              patch('src.consumer.log') as log_mock):

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

            channel: BlockingChannel = self.configure_rabbitMQ()

            def run_consumer():
                with patch('src.consumer.add_service_id', add_service_id_mock), \
                        patch('src.xml_parser.get_service_id', get_service_id_mock), \
                        patch('src.consumer.add_user', add_user_mock), \
                        patch('src.consumer.log', log_mock):
                    start_time = time.time()
                    while time.time() - start_time < 10:
                        try:
                            consumer.main()
                        except pika.exceptions.StreamLostError:
                            break

            consumer_thread = threading.Thread(target=run_consumer)
            consumer_thread.daemon = True
            consumer_thread.start()

            with open('resources/dummy_user.xml', 'r') as file:
                test_message = file.read()

            channel.basic_publish(exchange='amq.topic', routing_key='user.frontend', body=test_message)
            #TODO: Fix this sleep to busy wait
            time.sleep(2)

            channel.basic_consume(queue='crm', on_message_callback=self.callback, auto_ack=False)

            self.assertTrue(channel._consumer_infos)

            add_user_mock.assert_called_once()
            mock_post.assert_called_once()

    @patch('requests.post')
    def test_company_create_should_make_valid_request(self, mock_post):
        with patch('src.consumer.add_service_id') as add_service_id_mock, \
                patch('src.xml_parser.get_service_id') as get_service_id_mock, \
                patch('src.consumer.add_company') as add_company_mock, \
                patch('src.consumer.log') as log_mock:

            get_service_id_mock.return_value = {
                "crm": "1234"
            }
            add_service_id_mock.return_value = {
                "success": True,
                "message": "Service ID successfully added."
            }
            add_company_mock.return_value = '12345'

            log_mock.return_value = {
                "success": True,
                "message": "Log successfully added."
            }

            channel: BlockingChannel = self.configure_rabbitMQ()

            def run_consumer():
                with patch('src.consumer.add_service_id', add_service_id_mock), \
                        patch('src.xml_parser.get_service_id', get_service_id_mock), \
                        patch('src.consumer.add_company', add_company_mock):
                    start_time = time.time()
                    while time.time() - start_time < 10:
                        try:
                            consumer.main()
                        except pika.exceptions.StreamLostError:
                            break

            consumer_thread = threading.Thread(target=run_consumer)
            consumer_thread.daemon = True
            consumer_thread.start()

            with open('resources/dummy_company.xml', 'r') as file:
                test_message = file.read()

            channel.basic_publish(exchange='amq.topic', routing_key='company.frontend', body=test_message)
            #TODO: Fix this sleep to busy wait
            time.sleep(2)

            channel.basic_consume(queue='crm', on_message_callback=self.callback, auto_ack=False)

            self.assertTrue(channel._consumer_infos)

            add_company_mock.assert_called_once()
            mock_post.assert_called_once()

    def configure_rabbitMQ(self) -> BlockingChannel:
        credentials = pika.PlainCredentials(secrets.RABBITMQ_USER, secrets.RABBITMQ_PASSWORD)
        mapped_port = self.rabbitmq.get_exposed_port(5672)
        secrets.PORT = mapped_port
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost', port=secrets.PORT, credentials=credentials))
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

    @staticmethod
    def callback(ch, method, properties, body):
        print(f"Received message: {body}")


if __name__ == "__main__":
    unittest.main()
