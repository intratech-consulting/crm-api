import os
import sys
import threading
import time
import unittest
from unittest.mock import patch, MagicMock
import warnings

import pika
from pika.adapters.blocking_connection import BlockingChannel
from testcontainers.rabbitmq import RabbitMqContainer

sys.path.append('./')
import src.publisher as publisher
import config.secrets as secrets

class text_colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class TestPublisher(unittest.TestCase):
    warnings.filterwarnings("ignore")
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

    def start_consuming(self, channel):
        channel.basic_consume(queue='kassa', on_message_callback=self.callback, auto_ack=True)
        channel.start_consuming()

    def configure_rabbitMQ(self) -> BlockingChannel:
        credentials = pika.PlainCredentials(secrets.RABBITMQ_USER, secrets.RABBITMQ_PASSWORD)
        mapped_port = self.rabbitmq.get_exposed_port(5672)
        secrets.RABBITMQ_PORT = mapped_port
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost', port=secrets.RABBITMQ_PORT, credentials=credentials))
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
        ch.stop_consuming()
    
    ##########################
    ## Test functions users ##
    ##########################
    def test_01_user_create(self):
        # Mock the get updated objects API response
        def mock_response_changed_data():
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "totalSize": 1,
                "done": True,
                "records": [{
                    "Id": "co123",
                    "Name": "u123",
                    "object_type__c": "user",
                    "crud__c": "create"
                }]
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
        expected_message = '''
        <user>
            <routing_key>user.crm</routing_key>
            <crud_operation>create</crud_operation>
            <id>MASTERUUID-12345</id>
            <first_name>Will</first_name>
            <last_name>This Crash</last_name>
            <email>will.thiscrash@mail.com</email>
            <telephone>+32467179912</telephone>
            <birthday>2024-04-14</birthday>
            <address>
                <country>Belgium</country>
                <state>Brussels</state>
                <city>Brussels</city>
                <zip>1000</zip>
                <street>Nijverheidskaai</street>
                <house_number>170</house_number>
            </address>
            <company_email>will.thiscrash@company.com</company_email>
            <company_id>MASTERUUID-12345</company_id>
            <source>salesforce</source>
            <user_role>speaker</user_role>
            <invoice>BE00 0000 0000 0000</invoice>
            <calendar_link>www.example.com</calendar_link>
        </user>
        '''

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
            try:
                self.assertEqual(''.join((self.rabbitmq_message).split()), ''.join(expected_message.split()))
            except AssertionError:
                print(f"{text_colors.FAIL}##### Test 1 failed #####{text_colors.ENDC}")
                raise
            else:
                print(f"{text_colors.OKGREEN}##### Test 1 passed #####{text_colors.ENDC}")

    def test_02_user_update(self):
        # Mock the get updated objects API response
        def mock_response_changed_data():
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "totalSize": 1,
                "done": True,
                "records": [{
                    "Id": "co123",
                    "Name": "u123",
                    "object_type__c": "user",
                    "crud__c": "update"
                }]
            }
            return response# Mock the get update user API response
        
        def mock_response_changed_user_data_columns():
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "totalSize": 1,
                "done": True,
                "records": [{
                    "attributes": "",
                    "Id": "co123",
                    "first_name__c": False,
                    "last_name__c": True,
                    "email__c": False,
                    "telephone__c": True,
                    "birthday__c": False,
                    "country__c": False,
                    "state__c": False,
                    "city__c": False,
                    "zip__c": False,
                    "street__c": False,
                    "house_number__c": True,
                    "company_email__c": False,
                    "company_id__c": False,
                    "source__c": False,
                    "user_role__c": False,
                    "invoice__c": False,
                    "calendar_link__c": False
                }]
            }
            return response

        # Mock the get update user API response
        def mock_response_user_data():
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "totalSize": 1,
                "done": True,
                "records": [{
                    "attributes": "",
                    "Id": "u123",
                    "last_name__c": "This Crash",
                    "telephone__c": "+32467179912",
                    "house_number__c": 170.0
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
        expected_message = '''
        <user>
            <routing_key>user.crm</routing_key>
            <crud_operation>update</crud_operation>
            <id>MASTERUUID-12345</id>
            <first_name></first_name>
            <last_name>This Crash</last_name>
            <email></email>
            <telephone>+32467179912</telephone>
            <birthday></birthday>
            <address>
                <country></country>
                <state></state>
                <city></city>
                <zip></zip>
                <street></street>
                <house_number>170</house_number>
            </address>
            <company_email></company_email>
            <company_id></company_id>
            <source></source>
            <user_role></user_role>
            <invoice></invoice>
            <calendar_link></calendar_link>
        </user>
        '''

        with (
            patch('src.API.requests.get', side_effect=[mock_response_changed_data(), mock_response_changed_user_data_columns(), mock_response_user_data()]),
            patch('src.xml_parser.requests.post') as mock_request_post,
            patch('src.publisher.delete_change_object', return_value=MagicMock(status_code=201)),
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_request_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.API.requests.get', side_effect=[mock_response_changed_data(), mock_response_changed_user_data_columns(), mock_response_user_data()]), \
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
            try:
                self.assertEqual(''.join((self.rabbitmq_message).split()), ''.join(expected_message.split()))
            except AssertionError:
                print(f"{text_colors.FAIL}##### Test 2 failed #####{text_colors.ENDC}")
                raise
            else:
                print(f"{text_colors.OKGREEN}##### Test 2 passed #####{text_colors.ENDC}")

    def test_03_user_delete(self):
        # Mock the get updated objects API response
        mock_change_data_response = MagicMock()
        mock_change_data_response.status_code = 200
        mock_change_data_response.json.return_value = {
            "totalSize": 1,
            "done": True,
            "records": [{
                "Id": "co123",
                "Name": "u123",
                "object_type__c": "user",
                "crud__c": "delete"
            }]
        }
        
        # Mock the masteruuid API response
        mock_masteruuid_response = MagicMock()
        mock_masteruuid_response.status_code = 200
        mock_masteruuid_response.json.return_value = {
            "success": True,
            "MasterUuid": 'MASTERUUID-12345',
            "UUID": "MASTERUUID-12345"
        }

        # The expected assertion message
        expected_message = '''
        <user>
            <routing_key>user.crm</routing_key>
            <crud_operation>delete</crud_operation>
            <id>MASTERUUID-12345</id>
            <first_name></first_name>
            <last_name></last_name>
            <email></email>
            <telephone></telephone>
            <birthday></birthday>
            <address>
                <country></country>
                <state></state>
                <city></city>
                <zip></zip>
                <street></street>
                <house_number></house_number>
            </address>
            <company_email></company_email>
            <company_id></company_id>
            <source></source>
            <user_role></user_role>
            <invoice></invoice>
            <calendar_link></calendar_link>
        </user>
        '''

        with (
            patch('src.API.requests.get') as mock_request_changed_data,
            patch('src.xml_parser.requests.post') as mock_request_post,
            patch('src.publisher.delete_change_object', return_value=MagicMock(status_code=201)),
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_request_changed_data.return_value = mock_change_data_response
            mock_request_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.API.requests.get', mock_request_changed_data), \
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
            try:
                self.assertEqual(''.join((self.rabbitmq_message).split()), ''.join(expected_message.split()))
            except AssertionError:
                print(f"{text_colors.FAIL}##### Test 3 failed #####{text_colors.ENDC}")
                raise
            else:
                print(f"{text_colors.OKGREEN}##### Test 3 passed #####{text_colors.ENDC}")


    ############################
    ## Test functions company ##
    ############################
    def test_04_comapny_create(self):
        # Mock the get updated objects API response
        def mock_response_changed_data():
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "totalSize": 1,
                "done": True,
                "records": [{
                    "Id": "co123",
                    "Name": "c123",
                    "object_type__c": "company",
                    "crud__c": "create"
                }]
            }
            return response

        # Mock the get create company API response
        def mock_response_company_data():
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "totalSize": 1,
                "done": True,
                "records": [{
                    "attributes": "",
                    "Id": "c123",
                    "Name": "Erasmushogeschool Brussel",
                    "email__c": "info@ehb.be",
                    "telephone__c": "0479444444",
                    "country__c": "Belgium",
                    "state__c": "Brussels",
                    "city__c": "Brussels",
                    "zip__c": "1000",
                    "street__c": "Nijverheidskaai",
                    "house_number__c": 170,
                    "type__c": "customer",
                    "invoice__c": "BE00 0000 0000 0000"
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
        expected_message = '''
        <company>
            <routing_key>company.crm</routing_key>
            <crud_operation>create</crud_operation>
            <id>MASTERUUID-12345</id>
            <name>Erasmushogeschool Brussel</name>
            <email>info@ehb.be</email>
            <telephone>0479444444</telephone>
            <logo/>
            <address>
                    <country>Belgium</country>
                    <state>Brussels</state>
                    <city>Brussels</city>
                    <zip>1000</zip>
                    <street>Nijverheidskaai</street>
                    <house_number>170</house_number>
            </address>
            <type>customer</type>
            <invoice>BE00 0000 0000 0000</invoice>
        </company>
        '''

        with (
            patch('src.API.requests.get', side_effect=[mock_response_changed_data(), mock_response_company_data()]),
            patch('src.xml_parser.requests.post') as mock_request_post,
            patch('src.publisher.delete_change_object', return_value=MagicMock(status_code=201)),
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_request_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.API.requests.get', side_effect=[mock_response_changed_data(), mock_response_company_data()]), \
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
            try:
                self.assertEqual(''.join((self.rabbitmq_message).split()), ''.join(expected_message.split()))
            except AssertionError:
                print(f"{text_colors.FAIL}##### Test 4 failed #####{text_colors.ENDC}")
                raise
            else:
                print(f"{text_colors.OKGREEN}##### Test 4 passed #####{text_colors.ENDC}")

    def test_05_company_update(self):
        # Mock the get updated objects API response
        def mock_response_changed_data():
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "totalSize": 1,
                "done": True,
                "records": [{
                    "Id": "co123",
                    "Name": "c123",
                    "object_type__c": "company",
                    "crud__c": "update"
                }]
            }
            return response# Mock the get update company API response
        
        def mock_response_changed_company_data_columns():
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "totalSize": 1,
                "done": True,
                "records": [{
                    "attributes": "",
                    "Id": "co123",
                    "Name": True,
                    "email__c": True,
                    "telephone__c": False,
                    "country__c": False,
                    "state__c": False,
                    "city__c": False,
                    "zip__c": False,
                    "street__c": False,
                    "house_number__c": False,
                    "type__c": True,
                    "invoice__c": False
                }]
            }
            return response

        # Mock the get update user API response
        def mock_response_company_data():
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "totalSize": 1,
                "done": True,
                "records": [{
                    "attributes": "",
                    "Id": "u123",
                    "Name": "New Horizon",
                    "email__c": "newhorizon@gmail.com",
                    "type__c": "sponsor"
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
        expected_message = '''
        <company>
            <routing_key>company.crm</routing_key>
            <crud_operation>update</crud_operation>
            <id>MASTERUUID-12345</id>
            <name>New Horizon</name>
            <email>newhorizon@gmail.com</email>
            <telephone></telephone>
            <logo></logo>
            <address>
                    <country></country>
                    <state></state>
                    <city></city>
                    <zip></zip>
                    <street></street>
                    <house_number></house_number>
            </address>
            <type>sponsor</type>
            <invoice></invoice>
        </company>
        '''

        with (
            patch('src.API.requests.get', side_effect=[mock_response_changed_data(), mock_response_changed_company_data_columns(), mock_response_company_data()]),
            patch('src.xml_parser.requests.post') as mock_request_post,
            patch('src.publisher.delete_change_object', return_value=MagicMock(status_code=201)),
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_request_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.API.requests.get', side_effect=[mock_response_changed_data(), mock_response_changed_company_data_columns(), mock_response_company_data()]), \
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
            try:
                self.assertEqual(''.join((self.rabbitmq_message).split()), ''.join(expected_message.split()))
            except AssertionError:
                print(f"{text_colors.FAIL}##### Test 5 failed #####{text_colors.ENDC}")
                raise
            else:
                print(f"{text_colors.OKGREEN}##### Test 5 passed #####{text_colors.ENDC}")

    def test_06_company_delete(self):
        # Mock the get updated objects API response
        mock_change_data_response = MagicMock()
        mock_change_data_response.status_code = 200
        mock_change_data_response.json.return_value = {
            "totalSize": 1,
            "done": True,
            "records": [{
                "Id": "co123",
                "Name": "c123",
                "object_type__c": "company",
                "crud__c": "delete"
            }]
        }
        
        # Mock the masteruuid API response
        mock_masteruuid_response = MagicMock()
        mock_masteruuid_response.status_code = 200
        mock_masteruuid_response.json.return_value = {
            "success": True,
            "MasterUuid": 'MASTERUUID-12345',
            "UUID": "MASTERUUID-12345"
        }

        # The expected assertion message
        expected_message = '''
        <company>
            <routing_key>company.crm</routing_key>
            <crud_operation>delete</crud_operation>
            <id>MASTERUUID-12345</id>
            <name></name>
            <email></email>
            <telephone></telephone>
            <logo></logo>
            <address>
                    <country></country>
                    <state></state>
                    <city></city>
                    <zip></zip>
                    <street></street>
                    <house_number></house_number>
            </address>
            <type></type>
            <invoice></invoice>
        </company>
        '''

        with (
            patch('src.API.requests.get') as mock_request_changed_data,
            patch('src.xml_parser.requests.post') as mock_request_post,
            patch('src.publisher.delete_change_object', return_value=MagicMock(status_code=201)),
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_request_changed_data.return_value = mock_change_data_response
            mock_request_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.API.requests.get', mock_request_changed_data), \
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
            try:
                self.assertEqual(''.join((self.rabbitmq_message).split()), ''.join(expected_message.split()))
            except AssertionError:
                print(f"{text_colors.FAIL}##### Test 6 failed #####{text_colors.ENDC}")
                raise
            else:
                print(f"{text_colors.OKGREEN}##### Test 6 passed #####{text_colors.ENDC}")
    

    ###########################
    ## Test functions events ##
    ###########################
    def test_07_event_create(self):
        # Mock the get updated objects API response
        def mock_response_changed_data():
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "totalSize": 1,
                "done": True,
                "records": [{
                    "Id": "co123",
                    "Name": "e123",
                    "object_type__c": "event",
                    "crud__c": "create"
                }]
            }
            return response

        # Mock the get create event API response
        def mock_response_event_data():
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "totalSize": 1,
                "done": True,
                "records": [{
                    "attributes": "",
                    "Id": "e123",
                    "title__c": "Introduction to XML",
                    "date__c": "2024-05-31",
                    "start_time__c": "09:00:00.000Z",
                    "end_time__c": "11:00:00.000Z",
                    "location__c": "Aula 8",
                    "user_id__c": "u123",
                    "company_id__c": "c123",
                    "max_registrations__c": 200.0,
                    "available_seats__c": 150.0,
                    "description__c": "Introduction to XML"
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
        expected_message = '''
        <event>
            <routing_key>event.crm</routing_key>
            <crud_operation>create</crud_operation>
            <id>MASTERUUID-12345</id>
            <title>Introduction to XML</title>
            <date>2024-05-31</date>
            <start_time>09:00:00</start_time>
            <end_time>11:00:00</end_time>
            <location>Aula 8</location>
            <speaker>
                <user_id>MASTERUUID-12345</user_id>
                <company_id>MASTERUUID-12345</company_id> 
            </speaker>
            <max_registrations>200</max_registrations>
            <available_seats>150</available_seats>
            <description>Introduction to XML</description>
        </event>
        '''

        with (
            patch('src.API.requests.get', side_effect=[mock_response_changed_data(), mock_response_event_data()]),
            patch('src.xml_parser.requests.post') as mock_request_post,
            patch('src.publisher.delete_change_object', return_value=MagicMock(status_code=201)),
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_request_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.API.requests.get', side_effect=[mock_response_changed_data(), mock_response_event_data()]), \
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
            try:
                self.assertEqual(''.join((self.rabbitmq_message).split()), ''.join(expected_message.split()))
            except AssertionError:
                print(f"{text_colors.FAIL}##### Test 7 failed #####{text_colors.ENDC}")
                raise
            else:
                print(f"{text_colors.OKGREEN}##### Test 7 passed #####{text_colors.ENDC}")

    def test_08_event_update(self):
        # Mock the get updated objects API response
        def mock_response_changed_data():
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "totalSize": 1,
                "done": True,
                "records": [{
                    "Id": "co123",
                    "Name": "e123",
                    "object_type__c": "event",
                    "crud__c": "update"
                }]
            }
            return response# Mock the get update event API response
        
        def mock_response_changed_event_data_columns():
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "totalSize": 1,
                "done": True,
                "records": [{
                    "attributes": "",
                    "Id": "co123",
                    "title__c": False,
                    "date__c": False,
                    "start_time__c": True,
                    "end_time__c": False,
                    "location__c": False,
                    "user_id__c": False,
                    "company_id__c": False,
                    "max_registrations__c": False,
                    "available_seats__c": True,
                    "description__c": True
                }]
            }
            return response

        # Mock the get update event API response
        def mock_response_event_data():
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "totalSize": 1,
                "done": True,
                "records": [{
                    "attributes": "",
                    "Id": "u123",
                    "start_time__c": "08:00:00.000Z",
                    "available_seats__c": 100.0,
                    "description__c": "This is an introduction to XML"
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
        expected_message = '''
        <event>
            <routing_key>event.crm</routing_key>
            <crud_operation>update</crud_operation>
            <id>MASTERUUID-12345</id>
            <title></title>
            <date></date>
            <start_time>08:00:00</start_time>
            <end_time></end_time>
            <location></location>
            <speaker>
                <user_id></user_id>
                <company_id></company_id> 
            </speaker>
            <max_registrations></max_registrations>
            <available_seats>100</available_seats>
            <description>This is an introduction to XML</description>
        </event>
        '''

        with (
            patch('src.API.requests.get', side_effect=[mock_response_changed_data(), mock_response_changed_event_data_columns(), mock_response_event_data()]),
            patch('src.xml_parser.requests.post') as mock_request_post,
            patch('src.publisher.delete_change_object', return_value=MagicMock(status_code=201)),
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_request_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.API.requests.get', side_effect=[mock_response_changed_data(), mock_response_changed_event_data_columns(), mock_response_event_data()]), \
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
            try:
                self.assertEqual(''.join((self.rabbitmq_message).split()), ''.join(expected_message.split()))
            except AssertionError:
                print(f"{text_colors.FAIL}##### Test 8 failed #####{text_colors.ENDC}")
                raise
            else:
                print(f"{text_colors.OKGREEN}##### Test 8 passed #####{text_colors.ENDC}")

    def test_09_event_delete(self):
        # Mock the get updated objects API response
        mock_change_data_response = MagicMock()
        mock_change_data_response.status_code = 200
        mock_change_data_response.json.return_value = {
            "totalSize": 1,
            "done": True,
            "records": [{
                "Id": "co123",
                "Name": "e123",
                "object_type__c": "event",
                "crud__c": "delete"
            }]
        }
        
        # Mock the masteruuid API response
        mock_masteruuid_response = MagicMock()
        mock_masteruuid_response.status_code = 200
        mock_masteruuid_response.json.return_value = {
            "success": True,
            "MasterUuid": 'MASTERUUID-12345',
            "UUID": "MASTERUUID-12345"
        }

        # The expected assertion message
        expected_message = '''
        <event>
            <routing_key>event.crm</routing_key>
            <crud_operation>delete</crud_operation>
            <id>MASTERUUID-12345</id>
            <title></title>
            <date></date>
            <start_time></start_time>
            <end_time></end_time>
            <location></location>
            <speaker>
                <user_id></user_id>
                <company_id></company_id> 
            </speaker>
            <max_registrations></max_registrations>
            <available_seats></available_seats>
            <description></description>
        </event>
        '''

        with (
            patch('src.API.requests.get') as mock_request_changed_data,
            patch('src.xml_parser.requests.post') as mock_request_post,
            patch('src.publisher.delete_change_object', return_value=MagicMock(status_code=201)),
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_request_changed_data.return_value = mock_change_data_response
            mock_request_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.API.requests.get', mock_request_changed_data), \
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
            try:
                self.assertEqual(''.join((self.rabbitmq_message).split()), ''.join(expected_message.split()))
            except AssertionError:
                print(f"{text_colors.FAIL}##### Test 9 failed #####{text_colors.ENDC}")
                raise
            else:
                print(f"{text_colors.OKGREEN}##### Test 9 passed #####{text_colors.ENDC}")


    ###############################
    ## Test functions attendance ##
    ###############################
    def test_10_attendance_create(self):
        # Mock the get updated objects API response
        def mock_response_changed_data():
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "totalSize": 1,
                "done": True,
                "records": [{
                    "Id": "co123",
                    "Name": "a123",
                    "object_type__c": "attendance",
                    "crud__c": "create"
                }]
            }
            return response

        # Mock the get create attendance API response
        def mock_response_attendance_data():
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "totalSize": 1,
                "done": True,
                "records": [{
                    "attributes": "",
                    "Id": "a123",
                    "user_id__c": "u123",
                    "event_id__c": "e123"
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
        expected_message = '''
        <attendance>
            <routing_key>attendance.crm</routing_key>
            <crud_operation>create</crud_operation>
            <id>MASTERUUID-12345</id>
            <user_id>MASTERUUID-12345</user_id>
            <event_id>MASTERUUID-12345</event_id>
        </attendance>
        '''

        with (
            patch('src.API.requests.get', side_effect=[mock_response_changed_data(), mock_response_attendance_data()]),
            patch('src.xml_parser.requests.post') as mock_request_post,
            patch('src.publisher.delete_change_object', return_value=MagicMock(status_code=201)),
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_request_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.API.requests.get', side_effect=[mock_response_changed_data(), mock_response_attendance_data()]), \
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
            try:
                self.assertEqual(''.join((self.rabbitmq_message).split()), ''.join(expected_message.split()))
            except AssertionError:
                print(f"{text_colors.FAIL}##### Test 10 failed #####{text_colors.ENDC}")
                raise
            else:
                print(f"{text_colors.OKGREEN}##### Test 10 passed #####{text_colors.ENDC}")

    def test_11_attendance_update(self):
        # Mock the get updated objects API response
        def mock_response_changed_data():
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "totalSize": 1,
                "done": True,
                "records": [{
                    "Id": "co123",
                    "Name": "a123",
                    "object_type__c": "attendance",
                    "crud__c": "update"
                }]
            }
            return response# Mock the get update attendance API response
        
        def mock_response_changed_attendance_data_columns():
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "totalSize": 1,
                "done": True,
                "records": [{
                    "attributes": "",
                    "Id": "co123",
                    "user_id__c": False,
                    "event_id__c": True
                }]
            }
            return response

        # Mock the get update attendance API response
        def mock_response_attendance_data():
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "totalSize": 1,
                "done": True,
                "records": [{
                    "attributes": "",
                    "Id": "u123",
                    "event_id__c": "e123"
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
        expected_message = '''
        <attendance>
            <routing_key>attendance.crm</routing_key>
            <crud_operation>update</crud_operation>
            <id>MASTERUUID-12345</id>
            <user_id></user_id>
            <event_id>MASTERUUID-12345</event_id>
        </attendance>
        '''

        with (
            patch('src.API.requests.get', side_effect=[mock_response_changed_data(), mock_response_changed_attendance_data_columns(), mock_response_attendance_data()]),
            patch('src.xml_parser.requests.post') as mock_request_post,
            patch('src.publisher.delete_change_object', return_value=MagicMock(status_code=201)),
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_request_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.API.requests.get', side_effect=[mock_response_changed_data(), mock_response_changed_attendance_data_columns(), mock_response_attendance_data()]), \
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
            try:
                self.assertEqual(''.join((self.rabbitmq_message).split()), ''.join(expected_message.split()))
            except AssertionError:
                print(f"{text_colors.FAIL}##### Test 11 failed #####{text_colors.ENDC}")
                raise
            else:
                print(f"{text_colors.OKGREEN}##### Test 11 passed #####{text_colors.ENDC}")

    def test_12_attendance_delete(self):
        # Mock the get updated objects API response
        mock_change_data_response = MagicMock()
        mock_change_data_response.status_code = 200
        mock_change_data_response.json.return_value = {
            "totalSize": 1,
            "done": True,
            "records": [{
                "Id": "co123",
                "Name": "a123",
                "object_type__c": "attendance",
                "crud__c": "delete"
            }]
        }
        
        # Mock the masteruuid API response
        mock_masteruuid_response = MagicMock()
        mock_masteruuid_response.status_code = 200
        mock_masteruuid_response.json.return_value = {
            "success": True,
            "MasterUuid": 'MASTERUUID-12345',
            "UUID": "MASTERUUID-12345"
        }

        # The expected assertion message
        expected_message = '''
        <attendance>
            <routing_key>attendance.crm</routing_key>
            <crud_operation>delete</crud_operation>
            <id>MASTERUUID-12345</id>
            <user_id></user_id>
            <event_id></event_id>
        </attendance>
        '''

        with (
            patch('src.API.requests.get') as mock_request_changed_data,
            patch('src.xml_parser.requests.post') as mock_request_post,
            patch('src.publisher.delete_change_object', return_value=MagicMock(status_code=201)),
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_request_changed_data.return_value = mock_change_data_response
            mock_request_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.API.requests.get', mock_request_changed_data), \
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
            try:
                self.assertEqual(''.join((self.rabbitmq_message).split()), ''.join(expected_message.split()))
            except AssertionError:
                print(f"{text_colors.FAIL}##### Test 12 failed #####{text_colors.ENDC}")
                raise
            else:
                print(f"{text_colors.OKGREEN}##### Test 12 passed #####{text_colors.ENDC}")


if __name__ == "__main__":
    unittest.main()