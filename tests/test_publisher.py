import os
import sys
import threading
import time
import unittest
from unittest.mock import patch, MagicMock

import pika
from pika.adapters.blocking_connection import BlockingChannel
from testcontainers.rabbitmq import RabbitMqContainer

sys.path.append('./')
import src.publisher as publisher
import config.secrets as secrets


class TestPublisher(unittest.TestCase):
    @classmethod
    def setUp(self):
        self.rabbitmq_message = None
        self.callback_event = threading.Event()

    @classmethod
    def setUpClass(cls):
        cls.rabbitmq = RabbitMqContainer("rabbitmq:3-management", None, secrets.RABBITMQ_USER, secrets.RABBITMQ_PASSWORD)
        cls.rabbitmq.start()

    @classmethod
    def tearDownClass(cls):
        cls.rabbitmq.stop()

    def test_user_create(self):
        # Mock the get updated objects API response
        def mock_response_changed_data():
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "totalSize": 1,
                "done": True,
                "records": [{"Id": "co123", "Name": "u123", "object_type__c": "user", "crud__c": "create"}]
            }
            return response

        # Mock the get create user API response
        def mock_response_user_data():
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "totalSize": 1,
                "done": True,
                "records": [{
                    "attributes": "",
                    "Id": "u123",
                    "first_name__c": "Will",
                    "last_name__c": "This Crash",
                    "email__c": "will.thiscrash@mail.com",
                    "telephone__c": "+32467179912",
                    "birthday__c": "2024-04-14",
                    "country__c": "Belgium",
                    "state__c": "Brussels",
                    "city__c": "Brussels",
                    "zip__c": 1000.0,
                    "street__c": "Nijverheidskaai",
                    "house_number__c": 170.0,
                    "company_email__c": "will.thiscrash@company.com",
                    "company_id__c": "a03Qy000004wqlVIAQ",
                    "source__c": "salesforce",
                    "user_role__c": "speaker",
                    "invoice__c": "BE00 0000 0000 0000",
                    "calendar_link__c": "www.example.com"
                }]
            }
            return response
        
        # Mock the masteruuid API response
        mock_masteruuid_response = MagicMock()
        mock_masteruuid_response.status_code = 200
        mock_masteruuid_response.json.return_value = {
            "success": True,
            "MasterUuid": 'MASTERUUID-12345',
            "UUID": "MASTERUUID-12345"
        }

        # The expected assertion message
        expected_message = '<user><routing_key>user.crm</routing_key><crud_operation>create</crud_operation><id>MASTERUUID-12345</id><first_name>will</first_name><last_name>this crash</last_name><email>will.thiscrash@mail.com</email><telephone>+32467179912</telephone><birthday>2024-04-14</birthday><address><country>belgium</country><state>brussels</state><city>brussels</city><zip>1000</zip><street>nijverheidskaai</street><house_number>170</house_number></address><company_email>will.thiscrash@company.com</company_email><company_id>MASTERUUID-12345</company_id><source>salesforce</source><user_role>speaker</user_role><invoice>be00 0000 0000 0000</invoice><calendar_link>www.example.com</calendar_link></user>'

        with (
            patch('src.API.requests.get', side_effect=[mock_response_changed_data(), mock_response_user_data()]),
            patch('src.xml_parser.requests.post') as mock_request_post,
            patch('src.publisher.delete_change_object', return_value=MagicMock(status_code=201)),
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_request_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.API.requests.get', side_effect=[mock_response_changed_data(), mock_response_user_data()]), \
                        patch('src.xml_parser.requests.post', mock_request_post), \
                        patch('src.publisher.delete_change_object', return_value=MagicMock(status_code=201)), \
                        patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'}):
                    publisher.main()

            publisher_thread = threading.Thread(target=run_publisher)
            publisher_thread.daemon = True
            publisher_thread.start()

            # Consume the message from RabbitMQ in a separate thread
            consume_thread = threading.Thread(target=self.start_consuming, args=(channel,))
            consume_thread.daemon = True
            consume_thread.start()

            # Wait for the callback to be called
            self.callback_event.wait(timeout=10)

            # Assert that the message is the same as the expected message
            self.assertEqual(self.rabbitmq_message, expected_message)

    def start_consuming(self, channel):
        channel.basic_consume(queue='kassa', on_message_callback=self.callback, auto_ack=True)
        channel.start_consuming()

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

    def callback(self, ch, method, properties, body):
        self.rabbitmq_message = body.decode()
        self.callback_event.set()

if __name__ == "__main__":
    unittest.main()