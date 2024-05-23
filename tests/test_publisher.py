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
        user_object = {
            'ChangeEventHeader': {
                'entityName': 'user__c',
                'recordIds': ['a04Qy000000FLybIAG'],
                'changeType': 'CREATE',
                'changeOrigin': 'com/salesforce/api/soap/60.0;client=SfdcInternalAPI/',
                'transactionKey': '00003357-a4cd-8697-1ea3-1e779d24d4d9',
                'sequenceNumber': 1,
                'commitTimestamp': 1716381784000,
                'commitNumber': 1716381784922292225,
                'commitUser': '005Qy000003Cg1lIAC',
                'nulledFields': [],
                'diffFields': [],
                'changedFields': []
            },
            'OwnerId': '005Qy000003Cg1lIAC',
            'Name': 'PU-000237-2024',
            'RecordTypeId': '012Qy000001mv1pIAA',
            'CreatedDate': 1716381784000,
            'CreatedById': '005Qy000003Cg1lIAC',
            'LastModifiedDate': 1716381784000,
            'LastModifiedById': '005Qy000003Cg1lIAC',
            'email__c': 'john.doe@example.com',
            'first_name__c': 'John',
            'last_name__c': 'Doe',
            'company_id__c': 'a03Qy0000056oGzIAI',
            'company_email__c': 'john.doe@company.com',
            'source__c': 'Salesforce',
            'calendar_link__c': None,
            'country__c': 'België',
            'state__c': 'Brusselshoofdstedelijk gewest',
            'city__c': 'Brussel',
            'zip__c': 1000.0,
            'street__c': 'Nijverheidskaai',
            'house_number__c': 170.0,
            'telephone__c': '+3200000000',
            'birthday__c': 88387200000,
            'invoice__c': 'BE00 0000 0000',
            'user_role__c': 'employee'
        }

        # Mock the masteruuid API response
        mock_masteruuid_response = MagicMock()
        mock_masteruuid_response.status_code = 200
        mock_masteruuid_response.json.return_value = {
            "success": True,
            "MasterUuid": 'df59f548-538c-46f5-9f7f-66dacc3fcd82',
            "UUID": '702805f8-aa90-47e7-8f67-67d996817598'
        }

        # The expected assertion message
        expected_message = '''
        <user>
            <routing_key>user.crm</routing_key>
            <crud_operation>create</crud_operation>
            <id>df59f548-538c-46f5-9f7f-66dacc3fcd82</id>
            <first_name>John</first_name>
            <last_name>Doe</last_name>
            <email>john.doe@example.com</email>
            <telephone>+3200000000</telephone>
            <birthday>1972-10-20</birthday>
            <address>
                <country>België</country>
                <state>Brusselshoofdstedelijk gewest</state>
                <city>Brussel</city>
                <zip>1000</zip>
                <street>Nijverheidskaai</street>
                <house_number>170</house_number>
            </address>
            <company_email>john.doe@company.com</company_email>
            <company_id>702805f8-aa90-47e7-8f67-67d996817598</company_id>
            <source>Salesforce</source>
            <user_role>employee</user_role>
            <invoice>BE00 0000 0000</invoice>
            <calendar_link />
        </user>
        '''

        with (
            patch('src.uuidapi.requests.post') as mock_post,
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.uuidapi.requests.post', mock_post), \
                        patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'}):
                    publisher.handle_change_event(user_object)

            # Consume the message from RabbitMQ in a separate thread
            consume_thread = threading.Thread(target=self.start_consuming, args=(channel,))
            consume_thread.daemon = True
            consume_thread.start()

            publisher_thread = threading.Thread(target=run_publisher)
            publisher_thread.daemon = True
            publisher_thread.start()

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
        user_object = {
            'ChangeEventHeader': {
                'entityName': 'user__c',
                'recordIds': ['a04Qy000000FLybIAG'],
                'changeType': 'UPDATE',
                'changeOrigin': 'com/salesforce/api/soap/60.0;client=SfdcInternalAPI/',
                'transactionKey': '00003357-a4cd-8697-1ea3-1e779d24d4d9',
                'sequenceNumber': 1,
                'commitTimestamp': 1716381784000,
                'commitNumber': 1716381784922292225,
                'commitUser': '005Qy000003Cg1lIAC',
                'nulledFields': [],
                'diffFields': [],
                'changedFields': ['0x1F8040']
            },
            'OwnerId': None,
            'Name': None,
            'RecordTypeId': None,
            'CreatedDate': None,
            'CreatedById': None,
            'LastModifiedDate': 1716451799000,
            'LastModifiedById': None,
            'email__c': None,
            'first_name__c': None,
            'last_name__c': None,
            'company_id__c': None,
            'company_email__c': None,
            'source__c': None,
            'calendar_link__c': None,
            'country__c': 'Nederland',
            'state__c': 'BA',
            'city__c': 'Amsterdam',
            'zip__c': 1045.0,
            'street__c': 'Australiëhavenweg',
            'house_number__c': 21.0,
            'telephone__c': None,
            'birthday__c': None,
            'invoice__c': None,
            'user_role__c': None
        }

        # Mock the masteruuid API response
        mock_masteruuid_response = MagicMock()
        mock_masteruuid_response.status_code = 200
        mock_masteruuid_response.json.return_value = {
            "success": True,
            "UUID": 'df59f548-538c-46f5-9f7f-66dacc3fcd82'
        }

        # The expected assertion message
        expected_message = '''
        <user>
            <routing_key>user.crm</routing_key>
            <crud_operation>update</crud_operation>
            <id>df59f548-538c-46f5-9f7f-66dacc3fcd82</id>
            <first_name />
            <last_name />
            <email />
            <telephone />
            <birthday />
            <address>
                <country>Nederland</country>
                <state>BA</state>
                <city>Amsterdam</city>
                <zip>1045</zip>
                <street>Australiëhavenweg</street>
                <house_number>21</house_number>
            </address>
            <company_email />
            <company_id />
            <source />
            <user_role />
            <invoice />
            <calendar_link />
        </user>
        '''

        with (
            patch('src.uuidapi.requests.post') as mock_post,
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.uuidapi.requests.post', mock_post), \
                        patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'}):
                    publisher.handle_change_event(user_object)

            # Consume the message from RabbitMQ in a separate thread
            consume_thread = threading.Thread(target=self.start_consuming, args=(channel,))
            consume_thread.daemon = True
            consume_thread.start()

            publisher_thread = threading.Thread(target=run_publisher)
            publisher_thread.daemon = True
            publisher_thread.start()

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
        user_object = {
            'ChangeEventHeader': {
                'entityName': 'user__c',
                'recordIds': ['a04Qy000000FLybIAG'],
                'changeType': 'DELETE',
                'changeOrigin': 'com/salesforce/api/soap/60.0;client=SfdcInternalAPI/',
                'transactionKey': '00003357-a4cd-8697-1ea3-1e779d24d4d9',
                'sequenceNumber': 1,
                'commitTimestamp': 1716381784000,
                'commitNumber': 1716381784922292225,
                'commitUser': '005Qy000003Cg1lIAC',
                'nulledFields': [],
                'diffFields': [],
                'changedFields': []
            },
            'OwnerId': None,
            'Name': None,
            'RecordTypeId': None,
            'CreatedDate': None,
            'CreatedById': None,
            'LastModifiedDate': None,
            'LastModifiedById': None,
            'email__c': None,
            'first_name__c': None,
            'last_name__c': None,
            'company_id__c': None,
            'company_email__c': None,
            'source__c': None,
            'calendar_link__c': None,
            'country__c': None,
            'state__c': None,
            'city__c': None,
            'zip__c': None,
            'street__c': None,
            'house_number__c': None,
            'telephone__c': None,
            'birthday__c': None,
            'invoice__c': None,
            'user_role__c': None
        }
        
        # Mock the masteruuid API response
        mock_masteruuid_response = MagicMock()
        mock_masteruuid_response.status_code = 200
        mock_masteruuid_response.json.return_value = {
            "success": True,
            "UUID": 'df59f548-538c-46f5-9f7f-66dacc3fcd82'
        }

        # The expected assertion message
        expected_message = '''
        <user>
            <routing_key>user.crm</routing_key>
            <crud_operation>delete</crud_operation>
            <id>df59f548-538c-46f5-9f7f-66dacc3fcd82</id>
            <first_name />
            <last_name />
            <email />
            <telephone />
            <birthday />
            <address>
                <country />
                <state />
                <city />
                <zip />
                <street />
                <house_number />
            </address>
            <company_email />
            <company_id />
            <source />
            <user_role />
            <invoice />
            <calendar_link />
        </user>
        '''

        with (
            patch('src.uuidapi.requests.post') as mock_post,
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.uuidapi.requests.post', mock_post), \
                        patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'}):
                    publisher.handle_change_event(user_object)

            # Consume the message from RabbitMQ in a separate thread
            consume_thread = threading.Thread(target=self.start_consuming, args=(channel,))
            consume_thread.daemon = True
            consume_thread.start()

            publisher_thread = threading.Thread(target=run_publisher)
            publisher_thread.daemon = True
            publisher_thread.start()

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

    def test_04_company_create(self):
        # Mock the get updated objects API response
        company_object = {
            'ChangeEventHeader':{
                'entityName':'company__c',
                'recordIds':['a03Qy0000058YGAIA2'],
                'changeType':'CREATE',
                'changeOrigin':'com/salesforce/api/soap/60.0;client=SfdcInternalAPI/',
                'transactionKey':'0000221f-05a0-322f-f154-683bb4336159',
                'sequenceNumber':1,
                'commitTimestamp':1716452964000,
                'commitNumber':1716452964293705700,
                'commitUser':'005Qy000003Cg1lIAC',
                'nullFields': [],
                'diffFields' : [],
                'changedFields':[]
            },
            'OwnerId': '005Qy000003Cg1lIAC',
            'Name': 'a03Qy0000058YGA',
            'RecordTypeId': '012Qy000001mv1pIAA',
            'CreatedDate': 1716381784000,
            'CreatedById': '005Qy000003Cg1lIAC',
            'LastModifiedDate': 1716381784000,
            'LastModifiedById': '005Qy000003Cg1lIAC',
            'name__c':'Example Company',
            'email__c':'company@example.com',
            'telephone__c':'+3200000000',
            'country__c':'België',
            'state__c':'Brusselshoofdstedelijk gewest',
            'city__c':'Brussel',
            'zip__c':'1000',
            'street__c':'Nijverheidskaai',
            'house_number__c':170,
            'type__c':'sponsor',
            'invoice__c':'BE00 0000 0000'
        }

        # Mock the masteruuid API response
        mock_masteruuid_response = MagicMock()
        mock_masteruuid_response.status_code = 200
        mock_masteruuid_response.json.return_value = {
            "success": True,
            "MasterUuid": '702805f8-aa90-47e7-8f67-67d996817598',
        }

        # The expected assertion message
        expected_message = '''
        <company>
            <routing_key>company.crm</routing_key>
            <crud_operation>create</crud_operation>
            <id>702805f8-aa90-47e7-8f67-67d996817598</id>
            <name>Example Company</name>
            <email>company@example.com</email>
            <telephone>+3200000000</telephone>
            <logo />
            <address>
                <country>België</country>
                <state>Brusselshoofdstedelijk gewest</state>
                <city>Brussel</city>
                <zip>1000</zip>
                <street>Nijverheidskaai</street>
                <house_number>170</house_number>
            </address>
            <type>sponsor</type>
            <invoice>BE00 0000 0000</invoice>
        </company>
        '''

        with (
            patch('src.uuidapi.requests.post') as mock_post,
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.uuidapi.requests.post', mock_post), \
                        patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'}):
                    publisher.handle_change_event(company_object)

            # Consume the message from RabbitMQ in a separate thread
            consume_thread = threading.Thread(target=self.start_consuming, args=(channel,))
            consume_thread.daemon = True
            consume_thread.start()

            publisher_thread = threading.Thread(target=run_publisher)
            publisher_thread.daemon = True
            publisher_thread.start()

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
        company_object = {
            'ChangeEventHeader':{
                'entityName':'company__c',
                'recordIds':['a03Qy0000058YGAIA2'],
                'changeType':'UPDATE',
                'changeOrigin':'com/salesforce/api/soap/60.0;client=SfdcInternalAPI/',
                'transactionKey':'0000221f-05a0-322f-f154-683bb4336159',
                'sequenceNumber':1,
                'commitTimestamp':1716452964000,
                'commitNumber':1716452964293705700,
                'commitUser':'005Qy000003Cg1lIAC',
                'nullFields': [],
                'diffFields' : [],
                'changedFields':['0x1F8040']
            },
            'OwnerId': None,
            'Name': None,
            'RecordTypeId': None,
            'CreatedDate': None,
            'CreatedById': None,
            'LastModifiedDate': 1716381784000,
            'LastModifiedById': None,
            'name__c': None,
            'email__c': None,
            'telephone__c': None,
            'country__c': 'Nederland',
            'state__c': 'BA',
            'city__c': 'Amsterdam',
            'zip__c': 1045,
            'street__c': 'Australiëhavenweg',
            'house_number__c': 21,
            'type__c': None,
            'invoice__c': None
        }

        # Mock the masteruuid API response
        mock_masteruuid_response = MagicMock()
        mock_masteruuid_response.status_code = 200
        mock_masteruuid_response.json.return_value = {
            "success": True,
            "UUID": '702805f8-aa90-47e7-8f67-67d996817598'
        }

        # The expected assertion message
        expected_message = '''
        <company>
            <routing_key>company.crm</routing_key>
            <crud_operation>update</crud_operation>
            <id>702805f8-aa90-47e7-8f67-67d996817598</id>
            <name />
            <email />
            <telephone />
            <logo />
            <address>
                <country>Nederland</country>
                <state>BA</state>
                <city>Amsterdam</city>
                <zip>1045</zip>
                <street>Australiëhavenweg</street>
                <house_number>21</house_number>
            </address>
            <type />
            <invoice />
        </company>
        '''

        with (
            patch('src.uuidapi.requests.post') as mock_post,
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.uuidapi.requests.post', mock_post), \
                        patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'}):
                    publisher.handle_change_event(company_object)

            # Consume the message from RabbitMQ in a separate thread
            consume_thread = threading.Thread(target=self.start_consuming, args=(channel,))
            consume_thread.daemon = True
            consume_thread.start()

            publisher_thread = threading.Thread(target=run_publisher)
            publisher_thread.daemon = True
            publisher_thread.start()

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
        company_object = {
            'ChangeEventHeader':{
                'entityName':'company__c',
                'recordIds':['a03Qy0000058YGAIA2'],
                'changeType':'DELETE',
                'changeOrigin':'com/salesforce/api/soap/60.0;client=SfdcInternalAPI/',
                'transactionKey':'0000221f-05a0-322f-f154-683bb4336159',
                'sequenceNumber':1,
                'commitTimestamp':1716452964000,
                'commitNumber':1716452964293705700,
                'commitUser':'005Qy000003Cg1lIAC',
                'nullFields': [],
                'diffFields' : [],
                'changedFields':[]
            },
            'OwnerId': None,
            'Name': None,
            'RecordTypeId': None,
            'CreatedDate': None,
            'CreatedById': None,
            'LastModifiedDate': None,
            'LastModifiedById': None,
            'name__c': None,
            'email__c': None,
            'telephone__c': None,
            'country__c': None,
            'state__c': None,
            'city__c': None,
            'zip__c': None,
            'street__c': None,
            'house_number__c': None,
            'type__c': None,
            'invoice__c': None
        }

        # Mock the masteruuid API response
        mock_masteruuid_response = MagicMock()
        mock_masteruuid_response.status_code = 200
        mock_masteruuid_response.json.return_value = {
            "success": True,
            "UUID": '702805f8-aa90-47e7-8f67-67d996817598'
        }

        # The expected assertion message
        expected_message = '''
        <company>
            <routing_key>company.crm</routing_key>
            <crud_operation>delete</crud_operation>
            <id>702805f8-aa90-47e7-8f67-67d996817598</id>
            <name />
            <email />
            <telephone />
            <logo />
            <address>
                <country />
                <state />
                <city />
                <zip />
                <street />
                <house_number />
            </address>
            <type />
            <invoice />
        </company>
        '''

        with (
            patch('src.uuidapi.requests.post') as mock_post,
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.uuidapi.requests.post', mock_post), \
                        patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'}):
                    publisher.handle_change_event(company_object)

            # Consume the message from RabbitMQ in a separate thread
            consume_thread = threading.Thread(target=self.start_consuming, args=(channel,))
            consume_thread.daemon = True
            consume_thread.start()

            publisher_thread = threading.Thread(target=run_publisher)
            publisher_thread.daemon = True
            publisher_thread.start()

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

    ##########################
    ## Test functions event ##
    ##########################

    def test_07_event_create(self):
        # Mock the get updated objects API response
        event_object = {
            'ChangeEventHeader':{
                'entityName':'event__c',
                'recordIds':['a00Qy00000DjELtIAN'],
                'changeType':'CREATE',
                'changeOrigin':'com/salesforce/api/soap/60.0;client=SfdcInternalAPI/',
                'transactionKey':'0000221f-05a0-322f-f154-683bb4336159',
                'sequenceNumber':1,
                'commitTimestamp':1716452964000,
                'commitNumber':1716452964293705700,
                'commitUser':'005Qy000003Cg1lIAC',
                'nullFields': [],
                'diffFields' : [],
                'changedFields':[]
            },
            'OwnerId': '005Qy000003Cg1lIAC',
            'Name': 'E-000000008-2024',
            'RecordTypeId': '012Qy000001cnlVIAQ',
            'CreatedDate': 1716381784000,
            'CreatedById': '005Qy000003Cg1lIAC',
            'LastModifiedDate': 1716381784000,
            'LastModifiedById': '005Qy000003Cg1lIAC',
            'title__c': 'Example Talk',
            'date__c': 1717027200000,
            'start_time__c':28800000,
            'end_time__c':36000000,
            'location__c':'Aula 1',
            'user_id__c':'a04Qy000000FQdJIAW',
            'company_id__c':'a03Qy00000582HwIAI',
            'max_registrations__c':200,
            'available_seats__c':200,
            'description__c':'They will talk about examples in here.'
        }

        # Mock the masteruuid API response
        mock_masteruuid_response = MagicMock()
        mock_masteruuid_response.status_code = 200
        mock_masteruuid_response.json.return_value = {
            "success": True,
            "MasterUuid": '702805f8-aa90-47e7-8f67-67d996817598',
            "UUID": 'df59f548-538c-46f5-9f7f-66dacc3fcd82'
        }

        # The expected assertion message
        expected_message = '''
        <event>
            <routing_key>event.crm</routing_key>
            <crud_operation>create</crud_operation>
            <id>702805f8-aa90-47e7-8f67-67d996817598</id>
            <title>Example Talk</title>
            <date>2024-05-30</date>
            <start_time>09:00:00</start_time>
            <end_time>11:00:00</end_time>
            <location>Aula 1</location>
            <speaker>
                <user_id>df59f548-538c-46f5-9f7f-66dacc3fcd82</user_id>
                <company_id>df59f548-538c-46f5-9f7f-66dacc3fcd82</company_id>
            </speaker>
            <max_registrations>200</max_registrations>
            <available_seats>200</available_seats>
            <description>They will talk about examples in here.</description>
        </event>
        '''

        with (
            patch('src.uuidapi.requests.post') as mock_post,
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.uuidapi.requests.post', mock_post), \
                        patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'}):
                    publisher.handle_change_event(event_object)

            # Consume the message from RabbitMQ in a separate thread
            consume_thread = threading.Thread(target=self.start_consuming, args=(channel,))
            consume_thread.daemon = True
            consume_thread.start()

            publisher_thread = threading.Thread(target=run_publisher)
            publisher_thread.daemon = True
            publisher_thread.start()

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
        event_object = {
            'ChangeEventHeader':{
                'entityName':'event__c',
                'recordIds':['a00Qy00000DjELtIAN'],
                'changeType':'UPDATE',
                'changeOrigin':'com/salesforce/api/soap/60.0;client=SfdcInternalAPI/',
                'transactionKey':'0000221f-05a0-322f-f154-683bb4336159',
                'sequenceNumber':1,
                'commitTimestamp':1716452964000,
                'commitNumber':1716452964293705700,
                'commitUser':'005Qy000003Cg1lIAC',
                'nullFields': [],
                'diffFields' : [],
                'changedFields':['0x1F8040']
            },
            'OwnerId': None,
            'Name': None,
            'RecordTypeId': None,
            'CreatedDate': None,
            'CreatedById': None,
            'LastModifiedDate': 1716381784000,
            'LastModifiedById': None,
            'title__c': None,
            'date__c': None,
            'start_time__c': None,
            'end_time__c': None,
            'location__c': None,
            'user_id__c': None,
            'company_id__c': None,
            'max_registrations__c': None,
            'available_seats__c': 199,
            'description__c': None
        }

        # Mock the masteruuid API response
        mock_masteruuid_response = MagicMock()
        mock_masteruuid_response.status_code = 200
        mock_masteruuid_response.json.return_value = {
            "success": True,
            "UUID": '702805f8-aa90-47e7-8f67-67d996817598'
        }

        # The expected assertion message
        expected_message = '''
        <event>
            <routing_key>event.crm</routing_key>
            <crud_operation>update</crud_operation>
            <id>702805f8-aa90-47e7-8f67-67d996817598</id>
            <title />
            <date />
            <start_time />
            <end_time />
            <location />
            <speaker>
                <user_id />
                <company_id />
            </speaker>
            <max_registrations />
            <available_seats>199</available_seats>
            <description />
        </event>
        '''

        with (
            patch('src.uuidapi.requests.post') as mock_post,
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.uuidapi.requests.post', mock_post), \
                        patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'}):
                    publisher.handle_change_event(event_object)

            # Consume the message from RabbitMQ in a separate thread
            consume_thread = threading.Thread(target=self.start_consuming, args=(channel,))
            consume_thread.daemon = True
            consume_thread.start()

            publisher_thread = threading.Thread(target=run_publisher)
            publisher_thread.daemon = True
            publisher_thread.start()

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
        event_object = {
            'ChangeEventHeader':{
                'entityName':'event__c',
                'recordIds':['a00Qy00000DjELtIAN'],
                'changeType':'DELETE',
                'changeOrigin':'com/salesforce/api/soap/60.0;client=SfdcInternalAPI/',
                'transactionKey':'0000221f-05a0-322f-f154-683bb4336159',
                'sequenceNumber':1,
                'commitTimestamp':1716452964000,
                'commitNumber':1716452964293705700,
                'commitUser':'005Qy000003Cg1lIAC',
                'nullFields': [],
                'diffFields' : [],
                'changedFields':[]
            },
            'OwnerId': None,
            'Name': None,
            'RecordTypeId': None,
            'CreatedDate': None,
            'CreatedById': None,
            'LastModifiedDate': None,
            'LastModifiedById': None,
            'title__c': None,
            'date__c': None,
            'start_time__c': None,
            'end_time__c': None,
            'location__c': None,
            'user_id__c': None,
            'company_id__c': None,
            'max_registrations__c': None,
            'available_seats__c': None,
            'description__c': None
        }

        # Mock the masteruuid API response
        mock_masteruuid_response = MagicMock()
        mock_masteruuid_response.status_code = 200
        mock_masteruuid_response.json.return_value = {
            "success": True,
            "UUID": '702805f8-aa90-47e7-8f67-67d996817598'
        }

        # The expected assertion message
        expected_message = '''
        <event>
            <routing_key>event.crm</routing_key>
            <crud_operation>delete</crud_operation>
            <id>702805f8-aa90-47e7-8f67-67d996817598</id>
            <title />
            <date />
            <start_time />
            <end_time />
            <location />
            <speaker>
                <user_id />
                <company_id />
            </speaker>
            <max_registrations />
            <available_seats />
            <description />
        </event>
        '''

        with (
            patch('src.uuidapi.requests.post') as mock_post,
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.uuidapi.requests.post', mock_post), \
                        patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'}):
                    publisher.handle_change_event(event_object)

            # Consume the message from RabbitMQ in a separate thread
            consume_thread = threading.Thread(target=self.start_consuming, args=(channel,))
            consume_thread.daemon = True
            consume_thread.start()

            publisher_thread = threading.Thread(target=run_publisher)
            publisher_thread.daemon = True
            publisher_thread.start()

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
        attendance_object = {
            'ChangeEventHeader':{
                'entityName':'attendance__c',
                'recordIds':['a02Qy000001vKkoIAE'],
                'changeType':'CREATE',
                'changeOrigin':'com/salesforce/api/soap/60.0;client=SfdcInternalAPI/',
                'transactionKey':'0000221f-05a0-322f-f154-683bb4336159',
                'sequenceNumber':1,
                'commitTimestamp':1716452964000,
                'commitNumber':1716452964293705700,
                'commitUser':'005Qy000003Cg1lIAC',
                'nullFields': [],
                'diffFields' : [],
                'changedFields':[]
            },
            'OwnerId': '005Qy000003Cg1lIAC',
            'Name': 'A-000000022-2024',
            'RecordTypeId': '012Qy000001cnlVIAQ',
            'CreatedDate': 1716381784000,
            'CreatedById': '005Qy000003Cg1lIAC',
            'LastModifiedDate': 1716381784000,
            'LastModifiedById': '005Qy000003Cg1lIAC',
            'event_id__c': 'a00Qy00000DjELtIAN',
            'user_id__c': 'a04Qy000000FQdJIAW',
        }

        # Mock the masteruuid API response
        mock_masteruuid_response = MagicMock()
        mock_masteruuid_response.status_code = 200
        mock_masteruuid_response.json.return_value = {
            "success": True,
            "MasterUuid": '702805f8-aa90-47e7-8f67-67d996817598',
            "UUID": 'df59f548-538c-46f5-9f7f-66dacc3fcd82'
        }

        # The expected assertion message
        expected_message = '''
        <attendance>
            <routing_key>attendance.crm</routing_key>
            <crud_operation>create</crud_operation>
            <id>702805f8-aa90-47e7-8f67-67d996817598</id>
            <user_id>df59f548-538c-46f5-9f7f-66dacc3fcd82</user_id>
            <event_id>df59f548-538c-46f5-9f7f-66dacc3fcd82</event_id>
        </attendance>
        '''

        with (
            patch('src.uuidapi.requests.post') as mock_post,
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.uuidapi.requests.post', mock_post), \
                        patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'}):
                    publisher.handle_change_event(attendance_object)

            # Consume the message from RabbitMQ in a separate thread
            consume_thread = threading.Thread(target=self.start_consuming, args=(channel,))
            consume_thread.daemon = True
            consume_thread.start()

            publisher_thread = threading.Thread(target=run_publisher)
            publisher_thread.daemon = True
            publisher_thread.start()

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

    def test_11_attendance_delete(self):
        # Mock the get updated objects API response
        attendance_object = {
            'ChangeEventHeader':{
                'entityName':'attendance__c',
                'recordIds':['a02Qy000001vKkoIAE'],
                'changeType':'DELETE',
                'changeOrigin':'com/salesforce/api/soap/60.0;client=SfdcInternalAPI/',
                'transactionKey':'0000221f-05a0-322f-f154-683bb4336159',
                'sequenceNumber':1,
                'commitTimestamp':1716452964000,
                'commitNumber':1716452964293705700,
                'commitUser':'005Qy000003Cg1lIAC',
                'nullFields': [],
                'diffFields' : [],
                'changedFields':['0x1F8040']
            },
            'OwnerId': None,
            'Name': None,
            'RecordTypeId': None,
            'CreatedDate': None,
            'CreatedById': None,
            'LastModifiedDate': 1716381784000,
            'LastModifiedById': None,
            'event_id__c': None,
            'user_id__c': None,
        }

        # Mock the masteruuid API response
        mock_masteruuid_response = MagicMock()
        mock_masteruuid_response.status_code = 200
        mock_masteruuid_response.json.return_value = {
            "success": True,
            "UUID": '702805f8-aa90-47e7-8f67-67d996817598'
        }

        # The expected assertion message
        expected_message = '''
        <attendance>
            <routing_key>attendance.crm</routing_key>
            <crud_operation>delete</crud_operation>
            <id>702805f8-aa90-47e7-8f67-67d996817598</id>
            <user_id />
            <event_id />
        </attendance>
        '''

        with (
            patch('src.uuidapi.requests.post') as mock_post,
            patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'})
        ):
            channel: BlockingChannel = self.configure_rabbitMQ()
            mock_post.return_value = mock_masteruuid_response

            def run_publisher():
                with patch('src.uuidapi.requests.post', mock_post), \
                        patch('src.publisher.log', return_value={'success': True, 'message': 'Log successfully added.'}):
                    publisher.handle_change_event(attendance_object)
            
            # Consume the message from RabbitMQ in a separate thread
            consume_thread = threading.Thread(target=self.start_consuming, args=(channel,))
            consume_thread.daemon = True
            consume_thread.start()

            publisher_thread = threading.Thread(target=run_publisher)
            publisher_thread.daemon = True
            publisher_thread.start()

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


if __name__ == "__main__":
    unittest.main()