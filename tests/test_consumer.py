import os
import sys
import threading
import time
import unittest
from unittest.mock import patch, MagicMock
from xml.etree import ElementTree as ET

import requests
from busypie import wait, SECOND

import pika
from pika.adapters.blocking_connection import BlockingChannel
from testcontainers.rabbitmq import RabbitMqContainer

sys.path.append('./')
import src.consumer as consumer
import config.secrets as secrets


class TestConsumer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rabbitmq = RabbitMqContainer("rabbitmq:3-management", None, secrets.RABBITMQ_USER, secrets.RABBITMQ_PASSWORD)
        cls.rabbitmq.start()

    @classmethod
    def tearDownClass(cls):
        cls.rabbitmq.stop()

    def setUp(self):
        self.stop = threading.Event()
        self.consumer_thread = None

    def tearDown(self):
        if self.consumer_thread:
            self.stop.set()

    @patch('requests.post')
    def test_01_user_create_should_make_valid_request(self, mock_post):
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
                    while not self.stop.is_set():
                        try:
                            consumer.main()
                        except pika.exceptions.StreamLostError:
                            break

            self.consumer_thread = threading.Thread(target=run_consumer)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()

            with open('resources/dummy_user.xml', 'r') as file:
                test_message = file.read()

            channel.basic_publish(exchange='amq.topic', routing_key='user.frontend', body=test_message)
            wait().at_most(5, SECOND).until(lambda: add_user_mock.called)

            add_user_mock.assert_called_once()

    @patch('requests.post')
    def test_02_user_update_should_make_valid_request(self, mock_post):
        with (patch('src.consumer.add_service_id') as add_service_id_mock,
              patch('src.xml_parser.get_service_id') as get_service_id_mock,
              patch('src.consumer.update_user') as update_user_mock,
              patch('src.consumer.log') as log_mock):

            get_service_id_mock.return_value = {
                "crm": "1234"
            }
            add_service_id_mock.return_value = {
                "success": True,
                "message": "Service ID successfully added."
            }
            update_user_mock.return_value = '12345'

            log_mock.return_value = {
                "success": True,
                "message": "Log successfully added."
            }

            channel: BlockingChannel = self.configure_rabbitMQ()

            def run_consumer():
                with patch('src.consumer.add_service_id', add_service_id_mock), \
                        patch('src.xml_parser.get_service_id', get_service_id_mock), \
                        patch('src.consumer.update_user', update_user_mock), \
                        patch('src.consumer.log', log_mock):
                    while not self.stop.is_set():
                        try:
                            consumer.main()
                        except pika.exceptions.StreamLostError:
                            break

            self.consumer_thread = threading.Thread(target=run_consumer)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()

            with open('resources/dummy_user.xml', 'r') as file:
                tree = ET.parse(file)
                root = tree.getroot()
                crud_operation = root.find('crud_operation')
                crud_operation.text = 'update'
                test_message = ET.tostring(root, encoding='unicode')

            channel.basic_publish(exchange='amq.topic', routing_key='user.frontend', body=test_message)
            wait().at_most(5, SECOND).until(lambda: update_user_mock.called)

            update_user_mock.assert_called_once()

    @patch('requests.post')
    def test_03_user_delete_should_make_valid_request(self, mock_post):
        with (patch('src.consumer.add_service_id') as add_service_id_mock,
              patch('src.consumer.get_service_id') as get_service_id_mock,
              patch('src.consumer.delete_user') as delete_user_mock,
              patch('src.consumer.log') as log_mock):

            get_service_id_mock.return_value = {
                "crm": "1234"
            }
            add_service_id_mock.return_value = {
                "success": True,
                "message": "Service ID successfully added."
            }

            log_mock.return_value = {
                "success": True,
                "message": "Log successfully added."
            }

            channel: BlockingChannel = self.configure_rabbitMQ()

            def run_consumer():
                with patch('src.consumer.add_service_id', add_service_id_mock), \
                        patch('src.consumer.get_service_id', get_service_id_mock), \
                        patch('src.consumer.delete_user', delete_user_mock), \
                        patch('src.consumer.log', log_mock):
                    while not self.stop.is_set():
                        try:
                            consumer.main()
                        except pika.exceptions.StreamLostError:
                            break

            self.consumer_thread = threading.Thread(target=run_consumer)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()

            with open('resources/dummy_user.xml', 'r') as file:
                tree = ET.parse(file)
                root = tree.getroot()

                # Set crud_operation to 'delete'
                crud_operation = root.find('crud_operation')
                crud_operation.text = 'delete'

                # Set all other fields to empty, except for 'crud_operation' and 'id'
                for elem in root.iter():
                    if elem.tag not in ['crud_operation', 'id', 'routing_key']:
                        elem.text = ''

                test_message = ET.tostring(root, encoding='unicode')

            channel.basic_publish(exchange='amq.topic', routing_key='user.frontend', body=test_message)
            wait().at_most(5, SECOND).until(lambda: delete_user_mock.called)

            delete_user_mock.assert_called_once()

    @patch('requests.post')
    def test_04_company_create_should_make_valid_request(self, mock_post):
        with patch('src.consumer.add_service_id') as add_service_id_mock, \
                patch('src.xml_parser.get_service_id') as get_service_id_mock, \
                patch('src.consumer.add_company') as add_company_mock, \
                patch('src.consumer.log') as log_mock:

            get_service_id_mock.return_value = "1234"
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
                    while not self.stop.is_set():
                        try:
                            consumer.main()
                        except pika.exceptions.StreamLostError:
                            break

            self.consumer_thread = threading.Thread(target=run_consumer)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()

            with open('resources/dummy_company.xml', 'r') as file:
                test_message = file.read()

            channel.basic_publish(exchange='amq.topic', routing_key='company.frontend', body=test_message)
            wait().at_most(5, SECOND).until(lambda: add_company_mock.called)

            add_company_mock.assert_called_once()

    @patch('requests.post')
    def test_05_company_update_should_make_valid_request(self, mock_post):
        with patch('src.consumer.add_service_id') as add_service_id_mock, \
                patch('src.xml_parser.get_service_id') as get_service_id_mock, \
                patch('src.consumer.update_company') as update_company_mock, \
                patch('src.consumer.log') as log_mock:

            get_service_id_mock.return_value = {
                "crm": "1234"
            }
            add_service_id_mock.return_value = {
                "success": True,
                "message": "Service ID successfully added."
            }
            update_company_mock.return_value = '12345'

            log_mock.return_value = {
                "success": True,
                "message": "Log successfully added."
            }

            channel: BlockingChannel = self.configure_rabbitMQ()

            def run_consumer():
                with patch('src.consumer.add_service_id', add_service_id_mock), \
                        patch('src.xml_parser.get_service_id', get_service_id_mock), \
                        patch('src.consumer.update_company', update_company_mock):
                    while not self.stop.is_set():
                        try:
                            consumer.main()
                        except pika.exceptions.StreamLostError:
                            break

            self.consumer_thread = threading.Thread(target=run_consumer)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()

            with open('resources/dummy_company.xml', 'r') as file:
                tree = ET.parse(file)
                root = tree.getroot()
                crud_operation = root.find('crud_operation')
                crud_operation.text = 'update'
                test_message = ET.tostring(root, encoding='unicode')

            channel.basic_publish(exchange='amq.topic', routing_key='company.frontend', body=test_message)
            wait().at_most(5, SECOND).until(lambda: update_company_mock.called)

            update_company_mock.assert_called_once()

    @patch('requests.post')
    def test_06_company_delete_should_make_valid_request(self, mock_post):
        with (patch('src.consumer.add_service_id') as add_service_id_mock,
              patch('src.xml_parser.get_service_id') as get_service_id_mock,
              patch('src.consumer.delete_company') as delete_company_mock,
              patch('src.consumer.log') as log_mock):

            get_service_id_mock.return_value = {
                "crm": "1234"
            }
            add_service_id_mock.return_value = {
                "success": True,
                "message": "Service ID successfully added."
            }

            log_mock.return_value = {
                "success": True,
                "message": "Log successfully added."
            }

            channel: BlockingChannel = self.configure_rabbitMQ()

            def run_consumer():
                with patch('src.consumer.add_service_id', add_service_id_mock), \
                        patch('src.consumer.get_service_id', get_service_id_mock), \
                        patch('src.consumer.delete_company', delete_company_mock), \
                        patch('src.consumer.log', log_mock):
                    while not self.stop.is_set():
                        try:
                            consumer.main()
                        except pika.exceptions.StreamLostError:
                            break

            self.consumer_thread = threading.Thread(target=run_consumer)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()

            with open('resources/dummy_company.xml', 'r') as file:
                tree = ET.parse(file)
                root = tree.getroot()

                # Set crud_operation to 'delete'
                crud_operation = root.find('crud_operation')
                crud_operation.text = 'delete'

                # Set all other fields to empty, except for 'crud_operation' and 'id'
                for elem in root.iter():
                    if elem.tag not in ['crud_operation', 'id', 'routing_key']:
                        elem.text = ''

                test_message = ET.tostring(root, encoding='unicode')

            channel.basic_publish(exchange='amq.topic', routing_key='company.frontend', body=test_message)
            wait().at_most(5, SECOND).until(lambda: delete_company_mock.called)

            delete_company_mock.assert_called_once()

    @patch('requests.post')
    def test_07_event_create_should_make_valid_request(self, mock_post):
        with (patch('src.consumer.add_service_id') as add_service_id_mock, \
              patch('src.consumer.get_service_id') as get_service_id_mock, \
              patch('src.consumer.add_event') as add_event_mock, \
              patch('src.consumer.log') as log_mock):

            add_service_id_mock.return_value = {
                "success": True,
                "message": "Service ID successfully added."
            }

            get_service_id_mock.return_value = {
                "crm": "1234"
            }

            add_event_mock.return_value = '12345'

            log_mock.return_value = {
                "success": True,
                "message": "Log successfully added."
            }

            channel: BlockingChannel = self.configure_rabbitMQ()

            def run_consumer():
                with patch('src.consumer.add_service_id', add_service_id_mock), \
                        patch('src.consumer.get_service_id', get_service_id_mock), \
                        patch('src.consumer.add_event', add_event_mock), \
                        patch('src.consumer.log', log_mock):
                    while not self.stop.is_set():
                        try:
                            consumer.main()
                        except pika.exceptions.StreamLostError:
                            break

            self.consumer_thread = threading.Thread(target=run_consumer)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()

            with open('resources/dummy_event.xml', 'r') as file:
                test_message = file.read()

            channel.basic_publish(exchange='amq.topic', routing_key='event.frontend', body=test_message)
            wait().at_most(5, SECOND).until(lambda: add_event_mock.called)

            add_event_mock.assert_called_once()

    @patch('requests.post')
    def test_08_event_update_should_make_valid_request(self, mock_post):
        with (patch('src.consumer.add_service_id') as add_service_id_mock,
              patch('src.xml_parser.get_service_id') as get_service_id_mock,
              patch('src.consumer.update_event') as update_event_mock,
              patch('src.consumer.log') as log_mock):

            get_service_id_mock.return_value = {
                "crm": "1234"
            }
            add_service_id_mock.return_value = {
                "success": True,
                "message": "Service ID successfully added."
            }
            update_event_mock.return_value = '12345'

            log_mock.return_value = {
                "success": True,
                "message": "Log successfully added."
            }

            channel: BlockingChannel = self.configure_rabbitMQ()

            def run_consumer():
                with patch('src.consumer.add_service_id', add_service_id_mock), \
                        patch('src.xml_parser.get_service_id', get_service_id_mock), \
                        patch('src.consumer.update_event', update_event_mock), \
                        patch('src.consumer.log', log_mock):
                    while not self.stop.is_set():
                        try:
                            consumer.main()
                        except pika.exceptions.StreamLostError:
                            break

            self.consumer_thread = threading.Thread(target=run_consumer)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()

            with open('resources/dummy_event.xml', 'r') as file:
                tree = ET.parse(file)
                root = tree.getroot()
                crud_operation = root.find('crud_operation')
                crud_operation.text = 'update'
                test_message = ET.tostring(root, encoding='unicode')

            channel.basic_publish(exchange='amq.topic', routing_key='event.frontend', body=test_message)
            wait().at_most(5, SECOND).until(lambda: update_event_mock.called)

            update_event_mock.assert_called_once()

    @patch('requests.post')
    def test_09_event_delete_should_make_valid_request(self, mock_post):
        with (patch('src.consumer.add_service_id') as add_service_id_mock,
              patch('src.consumer.get_service_id') as get_service_id_mock,
              patch('src.consumer.delete_event') as delete_event_mock,
              patch('src.consumer.log') as log_mock):

            get_service_id_mock.return_value = {
                "crm": "1234"
            }
            add_service_id_mock.return_value = {
                "success": True,
                "message": "Service ID successfully added."
            }

            log_mock.return_value = {
                "success": True,
                "message": "Log successfully added."
            }

            channel: BlockingChannel = self.configure_rabbitMQ()

            def run_consumer():
                with patch('src.consumer.add_service_id', add_service_id_mock), \
                        patch('src.consumer.get_service_id', get_service_id_mock), \
                        patch('src.consumer.delete_event', delete_event_mock), \
                        patch('src.consumer.log', log_mock):
                    while not self.stop.is_set():
                        try:
                            consumer.main()
                        except pika.exceptions.StreamLostError:
                            break

            self.consumer_thread = threading.Thread(target=run_consumer)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()

            with open('resources/dummy_event.xml', 'r') as file:
                tree = ET.parse(file)
                root = tree.getroot()

                # Set crud_operation to 'delete'
                crud_operation = root.find('crud_operation')
                crud_operation.text = 'delete'

                # Set all other fields to empty, except for 'crud_operation' and 'id'
                for elem in root.iter():
                    if elem.tag not in ['crud_operation', 'id', 'routing_key']:
                        elem.text = ''

                test_message = ET.tostring(root, encoding='unicode')

            channel.basic_publish(exchange='amq.topic', routing_key='event.frontend', body=test_message)
            wait().at_most(5, SECOND).until(lambda: delete_event_mock.called)

            delete_event_mock.assert_called_once()

    @patch('requests.post')
    def test_10_attendance_create_should_make_valid_request(self, mock_post):
        with (patch('src.consumer.add_service_id') as add_service_id_mock,
              patch('src.xml_parser.get_service_id') as get_service_id_mock,
              patch('src.consumer.add_attendance') as add_attendance_mock,
              patch('src.consumer.log') as log_mock):

            get_service_id_mock.return_value = {
                "crm": "1234"
            }
            add_service_id_mock.return_value = {
                "success": True,
                "message": "Service ID successfully added."
            }
            add_attendance_mock.return_value = '12345'

            log_mock.return_value = {
                "success": True,
                "message": "Log successfully added."
            }

            channel: BlockingChannel = self.configure_rabbitMQ()

            def run_consumer():
                with patch('src.consumer.add_service_id', add_service_id_mock), \
                        patch('src.xml_parser.get_service_id', get_service_id_mock), \
                        patch('src.consumer.add_attendance', add_attendance_mock), \
                        patch('src.consumer.log', log_mock):
                    while not self.stop.is_set():
                        try:
                            consumer.main()
                        except pika.exceptions.StreamLostError:
                            break

            self.consumer_thread = threading.Thread(target=run_consumer)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()

            with open('resources/dummy_attendance.xml', 'r') as file:
                test_message = file.read()

            channel.basic_publish(exchange='amq.topic', routing_key='attendance.frontend', body=test_message)
            wait().at_most(5, SECOND).until(lambda: add_attendance_mock.called)

            add_attendance_mock.assert_called_once()

    @patch('requests.post')
    def test_11_attendance_update_should_make_valid_request(self, mock_post):
        with (patch('src.consumer.add_service_id') as add_service_id_mock,
              patch('src.xml_parser.get_service_id') as get_service_id_mock,
              patch('src.consumer.update_attendance') as update_attendance_mock,
              patch('src.consumer.log') as log_mock):

            get_service_id_mock.return_value = {
                "crm": "1234"
            }
            add_service_id_mock.return_value = {
                "success": True,
                "message": "Service ID successfully added."
            }
            update_attendance_mock.return_value = '12345'

            log_mock.return_value = {
                "success": True,
                "message": "Log successfully added."
            }

            channel: BlockingChannel = self.configure_rabbitMQ()

            def run_consumer():
                with patch('src.consumer.add_service_id', add_service_id_mock), \
                        patch('src.xml_parser.get_service_id', get_service_id_mock), \
                        patch('src.consumer.update_attendance', update_attendance_mock), \
                        patch('src.consumer.log', log_mock):
                    while not self.stop.is_set():
                        try:
                            consumer.main()
                        except pika.exceptions.StreamLostError:
                            break

            self.consumer_thread = threading.Thread(target=run_consumer)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()

            with open('resources/dummy_attendance.xml', 'r') as file:
                tree = ET.parse(file)
                root = tree.getroot()
                crud_operation = root.find('crud_operation')
                crud_operation.text = 'update'
                test_message = ET.tostring(root, encoding='unicode')

            channel.basic_publish(exchange='amq.topic', routing_key='attendance.frontend', body=test_message)
            wait().at_most(5, SECOND).until(lambda: update_attendance_mock.called)

            update_attendance_mock.assert_called_once()

    @patch('requests.post')
    def test_12_attendance_delete_should_make_valid_request(self, mock_post):
        with (patch('src.consumer.add_service_id') as add_service_id_mock,
              patch('src.consumer.get_service_id') as get_service_id_mock,
              patch('src.consumer.delete_attendance') as delete_attendance_mock,
              patch('src.consumer.log') as log_mock):

            get_service_id_mock.return_value = {
                "crm": "1234"
            }
            add_service_id_mock.return_value = {
                "success": True,
                "message": "Service ID successfully added."
            }

            log_mock.return_value = {
                "success": True,
                "message": "Log successfully added."
            }

            channel: BlockingChannel = self.configure_rabbitMQ()

            def run_consumer():
                with patch('src.consumer.add_service_id', add_service_id_mock), \
                        patch('src.consumer.get_service_id', get_service_id_mock), \
                        patch('src.consumer.delete_attendance', delete_attendance_mock), \
                        patch('src.consumer.log', log_mock):
                    while not self.stop.is_set():
                        try:
                            consumer.main()
                        except pika.exceptions.StreamLostError:
                            break

            self.consumer_thread = threading.Thread(target=run_consumer)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()

            with open('resources/dummy_attendance.xml', 'r') as file:
                tree = ET.parse(file)
                root = tree.getroot()

                # Set crud_operation to 'delete'
                crud_operation = root.find('crud_operation')
                crud_operation.text = 'delete'

                # Set all other fields to empty, except for 'crud_operation' and 'id'
                for elem in root.iter():
                    if elem.tag not in ['crud_operation', 'id', 'routing_key']:
                        elem.text = ''

                test_message = ET.tostring(root, encoding='unicode')

            channel.basic_publish(exchange='amq.topic', routing_key='attendance.frontend', body=test_message)
            wait().at_most(5, SECOND).until(lambda: delete_attendance_mock.called)

            delete_attendance_mock.assert_called_once()

    @patch('requests.post')
    def test_13_product_create_should_make_valid_request(self, mock_post):
        with (patch('src.consumer.add_service_id') as add_service_id_mock,
              patch('src.xml_parser.get_service_id') as get_service_id_mock,
              patch('src.consumer.add_product') as add_product_mock,
              patch('src.consumer.log') as log_mock):

            get_service_id_mock.return_value = {
                "crm": "1234"
            }
            add_service_id_mock.return_value = {
                "success": True,
                "message": "Service ID successfully added."
            }
            add_product_mock.return_value = '12345'

            log_mock.return_value = {
                "success": True,
                "message": "Log successfully added."
            }

            channel: BlockingChannel = self.configure_rabbitMQ()

            def run_consumer():
                with patch('src.consumer.add_service_id', add_service_id_mock), \
                        patch('src.xml_parser.get_service_id', get_service_id_mock), \
                        patch('src.consumer.add_product', add_product_mock), \
                        patch('src.consumer.log', log_mock):
                    while not self.stop.is_set():
                        try:
                            consumer.main()
                        except pika.exceptions.StreamLostError:
                            break

            self.consumer_thread = threading.Thread(target=run_consumer)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()

            with open('resources/dummy_product.xml', 'r') as file:
                test_message = file.read()

            channel.basic_publish(exchange='amq.topic', routing_key='product.frontend', body=test_message)
            wait().at_most(5, SECOND).until(lambda: add_product_mock.called)

            add_product_mock.assert_called_once()

    @patch('requests.post')
    def test_14_product_update_should_make_valid_request(self, mock_post):
        with (patch('src.consumer.add_service_id') as add_service_id_mock,
              patch('src.xml_parser.get_service_id') as get_service_id_mock,
              patch('src.consumer.update_product') as update_product_mock,
              patch('src.consumer.log') as log_mock):

            get_service_id_mock.return_value = {
                "crm": "1234"
            }
            add_service_id_mock.return_value = {
                "success": True,
                "message": "Service ID successfully added."
            }
            update_product_mock.return_value = '12345'

            log_mock.return_value = {
                "success": True,
                "message": "Log successfully added."
            }

            channel: BlockingChannel = self.configure_rabbitMQ()

            def run_consumer():
                with patch('src.consumer.add_service_id', add_service_id_mock), \
                        patch('src.xml_parser.get_service_id', get_service_id_mock), \
                        patch('src.consumer.update_product', update_product_mock), \
                        patch('src.consumer.log', log_mock):
                    while not self.stop.is_set():
                        try:
                            consumer.main()
                        except pika.exceptions.StreamLostError:
                            break

            self.consumer_thread = threading.Thread(target=run_consumer)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()

            with open('resources/dummy_product.xml', 'r') as file:
                tree = ET.parse(file)
                root = tree.getroot()
                crud_operation = root.find('crud_operation')
                crud_operation.text = 'update'
                test_message = ET.tostring(root, encoding='unicode')

            channel.basic_publish(exchange='amq.topic', routing_key='product.frontend', body=test_message)
            wait().at_most(5, SECOND).until(lambda: update_product_mock.called)

            update_product_mock.assert_called_once()

    @patch('requests.post')
    def test_15_order_create_should_make_valid_request_when_no_order_exists(self, mock_post):
        with (patch('src.consumer.add_service_id') as add_service_id_mock,
              patch('src.uuidapi.requests.post') as get_service_id_mock,
              patch('src.consumer.get_order') as get_order_mock,
              patch('src.consumer.update_order') as update_order_mock,
              patch('src.consumer.add_order') as add_order_mock,
              patch('src.consumer.log') as log_mock):

            get_service_id_mock = MagicMock()
            get_service_id_mock.status_code = 200
            get_service_id_mock.json.return_value = {
                "crm": '1234'
            }

            add_service_id_mock.return_value = {
                "success": True,
                "message": "Service ID successfully added."
            }
            add_order_mock.return_value = '12345'

            log_mock.return_value = {
                "success": True,
                "message": "Log successfully added."
            }

            get_order_mock.return_value = None, None

            channel: BlockingChannel = self.configure_rabbitMQ()

            def run_consumer():
                with patch('src.consumer.add_service_id', add_service_id_mock), \
                        patch('src.uuidapi.requests.post', get_service_id_mock), \
                        patch('src.consumer.get_order', get_order_mock), \
                        patch('src.consumer.update_order', update_order_mock), \
                        patch('src.consumer.add_order', add_order_mock), \
                        patch('src.consumer.log', log_mock):
                    while not self.stop.is_set():
                        try:
                            consumer.main()
                        except pika.exceptions.StreamLostError:
                            break

            self.consumer_thread = threading.Thread(target=run_consumer)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()

            with open('resources/dummy_order.xml', 'r') as file:
                test_message = file.read()

            channel.basic_publish(exchange='amq.topic', routing_key='order.frontend', body=test_message)
            wait().at_most(5, SECOND).until(lambda: add_order_mock.called)

            add_order_mock.assert_called_once()

    def test_16_order_create_should_make_valid_request_when_order_exists(self):
        get_service_id = MagicMock()
        get_service_id.status_code = 200
        get_service_id.json.return_value = {
            "crm": '1234'
        }
        with (patch('src.consumer.add_service_id') as add_service_id_mock,
              patch('src.uuidapi.requests.post') as get_service_id_mock,
              patch('src.consumer.get_order') as get_order_mock,
              patch('src.consumer.update_order') as update_order_mock,
              patch('src.consumer.add_order') as add_order_mock,
              patch('src.consumer.log') as log_mock):

            get_service_id_mock.return_value = get_service_id

            add_service_id_mock.return_value = {
                "success": True,
                "message": "Service ID successfully added."
            }
            add_order_mock.return_value = '12345'

            log_mock.return_value = {
                "success": True,
                "message": "Log successfully added."
            }

            get_order_mock.return_value = '12345', '5'

            channel: BlockingChannel = self.configure_rabbitMQ()

            def run_consumer():
                with patch('src.consumer.add_service_id', add_service_id_mock), \
                        patch('src.uuidapi.requests.post', get_service_id_mock), \
                        patch('src.consumer.get_order', get_order_mock), \
                        patch('src.consumer.update_order', update_order_mock), \
                        patch('src.consumer.add_order', add_order_mock), \
                        patch('src.consumer.log', log_mock):
                    while not self.stop.is_set():
                        try:
                            consumer.main()
                        except pika.exceptions.StreamLostError:
                            break

            self.consumer_thread = threading.Thread(target=run_consumer)
            self.consumer_thread.daemon = True
            self.consumer_thread.start()

            with open('resources/dummy_order.xml', 'r') as file:
                test_message = file.read()

            channel.basic_publish(exchange='amq.topic', routing_key='order.frontend', body=test_message)
            wait().at_most(5, SECOND).until(lambda: update_order_mock.called)

            update_order_mock.assert_called_once()

    def configure_rabbitMQ(self) -> BlockingChannel:
        credentials = pika.PlainCredentials(secrets.RABBITMQ_USER, secrets.RABBITMQ_PASSWORD)
        mapped_port = self.rabbitmq.get_exposed_port(5672)
        secrets.RABBITMQ_PORT = mapped_port
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=secrets.HOST, port=secrets.RABBITMQ_PORT, credentials=credentials))
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
