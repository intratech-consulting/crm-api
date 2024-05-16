import os
import sys
import unittest
from unittest.mock import patch, Mock, call
import xml.etree.ElementTree as ET
if os.path.isdir('/app'):
    sys.path.append('/app')
else:
    local_dir = os.path.dirname(os.path.realpath(__file__))
    sys.path.append(local_dir)
import src.consumer as consumer

class TestConsumer(unittest.TestCase):

    @patch('consumer.pika.BlockingConnection')
    @patch('consumer.pika.ConnectionParameters')
    @patch('consumer.pika.PlainCredentials')
    def test_main(self, mock_credentials, mock_connection_params, mock_blocking_connection):
        mock_channel = Mock()
        mock_blocking_connection.return_value.channel.return_value = mock_channel
        consumer.main()
        mock_channel.queue_declare.assert_called_with(queue='crm', durable=True)
        mock_channel.basic_consume.assert_called_with(queue='crm', on_message_callback=consumer.callback, auto_ack=False)
        mock_channel.start_consuming.assert_called()

    @patch('consumer.logger')
    @patch('consumer.ET.fromstring')
    @patch('consumer.read_xml_user')
    @patch('consumer.add_user')
    @patch('consumer.add_service_id')
    def test_callback_create_user(self, mock_add_service_id, mock_add_user, mock_read_xml_user, mock_fromstring, mock_logger):
        mock_ch = Mock()
        mock_method = Mock()
        mock_properties = Mock()
        mock_body = b'<user><crud_operation>create</crud_operation><id>123</id></user>'

        root = ET.Element('user')
        ET.SubElement(root, 'crud_operation').text = 'create'
        ET.SubElement(root, 'id').text = '123'
        mock_fromstring.return_value = root

        mock_read_xml_user.side_effect = lambda variables, root: variables.update({'id': '123'})

        consumer.callback(mock_ch, mock_method, mock_properties, mock_body)

        mock_ch.basic_ack.assert_called_once_with(delivery_tag=mock_method.delivery_tag)
        mock_add_user.assert_called_once()
        mock_add_service_id.assert_called_once_with('123', mock_add_user.return_value, 'crm')

    @patch('consumer.logger')
    @patch('consumer.ET.fromstring')
    @patch('consumer.read_xml_user')
    @patch('consumer.update_user')
    def test_callback_update_user(self, mock_update_user, mock_read_xml_user, mock_fromstring, mock_logger):
        mock_ch = Mock()
        mock_method = Mock()
        mock_properties = Mock()
        mock_body = b'<user><crud_operation>update</crud_operation><id>123</id></user>'

        root = ET.Element('user')
        ET.SubElement(root, 'crud_operation').text = 'update'
        ET.SubElement(root, 'id').text = '123'
        mock_fromstring.return_value = root

        mock_read_xml_user.side_effect = lambda variables, root: variables.update({'id': '123', 'other_field': 'value'})

        consumer.callback(mock_ch, mock_method, mock_properties, mock_body)

        mock_ch.basic_ack.assert_called_once_with(delivery_tag=mock_method.delivery_tag)
        mock_update_user.assert_called_once_with('123', mock.ANY)

    @patch('consumer.logger')
    @patch('consumer.ET.fromstring')
    @patch('consumer.delete_user')
    @patch('consumer.get_service_id')
    def test_callback_delete_user(self, mock_get_service_id, mock_delete_user, mock_fromstring, mock_logger):
        mock_ch = Mock()
        mock_method = Mock()
        mock_properties = Mock()
        mock_body = b'<user><crud_operation>delete</crud_operation><id>123</id></user>'

        root = ET.Element('user')
        ET.SubElement(root, 'crud_operation').text = 'delete'
        ET.SubElement(root, 'id').text = '123'
        mock_fromstring.return_value = root

        mock_get_service_id.return_value = 'service_id_123'

        consumer.callback(mock_ch, mock_method, mock_properties, mock_body)

        mock_ch.basic_ack.assert_called_once_with(delivery_tag=mock_method.delivery_tag)
        mock_delete_user.assert_called_once_with('service_id_123')

if __name__ == '__main__':
    unittest.main()
