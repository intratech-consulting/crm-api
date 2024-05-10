#!/usr/bin/env python
import pika, sys, os
from lxml import etree
import xml.etree.ElementTree as ET
import time
sys.path.append('/app')
import config.secrets as secrets
import src.API as API

def main():
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=secrets.HOST, credentials=credentials))
    channel = connection.channel()
    channel.exchange_declare(exchange="amq.topic", exchange_type="topic", durable=True)

    updates = API.get_changed_data()
    if updates:
        root = ET.fromstring(updates)
        for changed_object in root.findall('ChangedObject__c'):
            id_element = changed_object.find('Id')
            name_element = changed_object.find('Name')
            object_type_element = changed_object.find('object_type__c')
            crud_operation = changed_object.find('crud__c')

            if id_element is not None and name_element is not None and object_type_element is not None and crud_operation is not None:
                id_value = id_element.text
                name_value = name_element.text
                object_type_value = object_type_element.text
                crud_operation = crud_operation.text

                message = None

                print(f" [x] Sending: Id={id_value}, Name={name_value}, Object Type={object_type_value}, Crud={crud_operation}")
                match object_type_value, crud_operation:
                    case 'user', 'create':
                        message = API.get_new_user(name_value)
                        xsd_tree = etree.parse('./resources/user_xsd.xml')
                        rc = "user.crm"

                    case 'user', 'update':
                        message = 'update user'
                        xsd_tree = etree.parse('./resources/user_xsd.xml')
                        rc = "user.crm"

                    case 'user', 'delete':
                        message = 'delete user'
                        xsd_tree = etree.parse('./resources/user_xsd.xml')
                        rc = "user.crm"

                    case 'company', 'create':
                        message = API.get_new_company(name_value)
                        xsd_tree = etree.parse('./resources/company_xsd.xml')
                        rc = "company.crm"

                    case 'company', 'update':
                        message = 'update company'
                        xsd_tree = etree.parse('./resources/company_xsd.xml')
                        rc = "company.crm"

                    case 'company', 'delete':
                        message = 'delete company'
                        xsd_tree = etree.parse('./resources/company_xsd.xml')
                        rc = "company.crm"
                        
                    case 'event', 'create':
                        message = API.get_new_event(name_value)
                        xsd_tree = etree.parse('./resources/event_xsd.xml')
                        rc = "event.crm"

                    case 'event', 'update':
                        message = 'update event'
                        xsd_tree = etree.parse('./resources/event_xsd.xml')
                        rc = "event.crm"

                    case 'event', 'delete':
                        message = 'delete event'
                        xsd_tree = etree.parse('./resources/event_xsd.xml')
                        rc = "event.crm"

                    case 'attendance', 'create':
                        message = API.get_new_attendance(name_value)
                        xsd_tree = etree.parse('./resources/attendance_xsd.xml')
                        rc = "attendance.crm"

                    case 'attendance', 'update':
                        message = 'update attendance'
                        xsd_tree = etree.parse('./resources/attendance_xsd.xml')
                        rc = "attendance.crm"

                    case 'attendance', 'delete':
                        message = 'delete attendance'
                        xsd_tree = etree.parse('./resources/attendance_xsd.xml')
                        rc = "attendance.crm"

                    case _:
                        print(f" [x] Object type {object_type_value} not supported")

                print(f" [x] Message: {message}")

                schema = etree.XMLSchema(xsd_tree)
                xml_doc = etree.fromstring(message.encode())
                if not schema.validate(xml_doc):
                    print(' [x] Invalid XML')
                else:
                    print(' [x] Object sent successfully')
                    API.delete_change_object(id_value)

            if message:
                channel.basic_publish(exchange='amq.topic', routing_key=rc, body=message)

if __name__ == '__main__':
    try:
        print(' [*] Sending messages. To exit press CTRL+C')
        API.authenticate()
        while True:
            main()
            time.sleep(120)
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)
