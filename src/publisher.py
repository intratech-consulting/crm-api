#!/usr/bin/env python
import pika, sys, os
from lxml import etree
import xml.etree.ElementTree as ET
import time

sys.path.append('/app')
import config.secrets as secrets
from monitoring import log
from API import *
from uuidapi import *
from xml_parser import *
from logger import init_logger

TEAM = 'crm'

def main():
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=secrets.HOST, credentials=credentials))
    channel = connection.channel()
    channel.exchange_declare(exchange="amq.topic", exchange_type="topic", durable=True)

    while True:
        publish(channel)
        time.sleep(20)

def publish(channel):
    try:
        updates = get_changed_data()
        if updates is None:
            return None, None

        changed_objects = {}
        root = ET.fromstring(updates)
        read_xml_changed_data(changed_objects, root)
        logger.debug(f"Changed objects: {changed_objects}")

        for changed_object in changed_objects['changed_data']:
            message = None
            rc = f"{changed_object['object_type']}.{TEAM}"

            match changed_object['object_type'], changed_object['crud_operation']:
                case 'user', 'create':
                    message = get_new_user(changed_object['changed_object_name'])
                    xsd_tree = etree.parse('./resources/user_xsd.xml')

                case 'user', 'update':
                    updated_fields = get_updated_user(changed_object['changed_object_id'])
                    updated_values = get_updated_values(f"query?q=SELECT+{','.join(updated_fields)}+FROM+user__c+WHERE+Id+=+'{changed_object['changed_object_name']}'")
                    message = update_xml_user(rc, changed_object['crud_operation'], changed_object['changed_object_name'], updated_values)
                    xsd_tree = etree.parse('./resources/user_xsd.xml')

                case 'user', 'delete':
                    message = update_xml_user(rc, changed_object['crud_operation'], changed_object['changed_object_name'], {})
                    xsd_tree = etree.parse('./resources/user_xsd.xml')

                case 'company', 'create':
                    message = get_new_company(changed_object['changed_object_name'])
                    xsd_tree = etree.parse('./resources/company_xsd.xml')

                case 'company', 'update':
                    rc = "company.crm"
                    updated_fields = get_updated_company(changed_object['changed_object_id'])
                    updated_values = get_updated_values(f"query?q=SELECT+{','.join(updated_fields)}+FROM+company__c+WHERE+Id+=+'{changed_object['changed_object_name']}'")
                    message = update_xml_company(rc, changed_object['crud_operation'], changed_object['changed_object_name'], updated_values)
                    xsd_tree = etree.parse('./resources/company_xsd.xml')

                case 'company', 'delete':
                    message = update_xml_company(rc, changed_object['crud_operation'], changed_object['changed_object_name'], {})
                    xsd_tree = etree.parse('./resources/company_xsd.xml')

                case 'event', 'create':
                    message = get_new_event(changed_object['changed_object_name'])
                    xsd_tree = etree.parse('./resources/event_xsd.xml')

                case 'event', 'update':
                    updated_fields = get_updated_event(changed_object['changed_object_id'])
                    updated_values = get_updated_values(f"query?q=SELECT+{','.join(updated_fields)}+FROM+event__c+WHERE+Id+=+'{changed_object['changed_object_name']}'")
                    message = update_xml_event(rc, changed_object['crud_operation'], changed_object['changed_object_name'], updated_values)        
                    xsd_tree = etree.parse('./resources/event_xsd.xml')

                case 'event', 'delete':
                    message = update_xml_event(rc, changed_object['crud_operation'], changed_object['changed_object_name'], {})
                    xsd_tree = etree.parse('./resources/event_xsd.xml')

                case 'attendance', 'create':
                    message = get_new_attendance(changed_object['changed_object_name'])
                    xsd_tree = etree.parse('./resources/attendance_xsd.xml')

                case 'attendance', 'update':
                    updated_fields = get_updated_attendance(changed_object['changed_object_id'])
                    updated_values = get_updated_values(f"query?q=SELECT+{','.join(updated_fields)}+FROM+attendance__c+WHERE+Id+=+'{changed_object['changed_object_name']}'")
                    message = update_xml_attendance(rc, changed_object['crud_operation'], changed_object['changed_object_name'], updated_values)
                    xsd_tree = etree.parse('./resources/attendance_xsd.xml')

                case 'attendance', 'delete':
                    message = update_xml_attendance(rc, changed_object['crud_operation'], changed_object['changed_object_name'], {})
                    xsd_tree = etree.parse('./resources/attendance_xsd.xml')

            logger.debug(f"Message: {message}")
            schema = etree.XMLSchema(xsd_tree)
            xml_doc = etree.fromstring(message.encode())
            if not schema.validate(xml_doc):
                logger.error('Invalid XML')
            else:
                logger.info('Object sent successfully')
                delete_change_object(changed_object['changed_object_id'])

                channel.basic_publish(exchange='amq.topic', routing_key=rc, body=message)
                log(logger, f'PUBLISHER: {changed_object['crud_operation']} {changed_object['object_type']}', f'Succesfully published "{changed_object['crud_operation']} {changed_object['object_type']}" on RabbitMQ!')
                        
    except Exception as e:
        logger.error(f"An error occurred while processing the message: {e}")
        log(logger, f'PUBLISHER: {changed_object['crud_operation']} {changed_object['object_type']}', f'An error occurred while processing "{changed_object['crud_operation']} {changed_object['object_type']}": {e}', 'true')

if __name__ == '__main__':
    # Create a custom logger
    logger = init_logger("__publisher__")
    try:
        authenticate()
        main()
    except Exception as e:
        logger.error(f"Failed to start publisher: {e}")
        sys.exit(1)
