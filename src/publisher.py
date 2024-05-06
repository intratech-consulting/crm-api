#!/usr/bin/env python
import pika, sys, os
from lxml import etree
import API
import xml.etree.ElementTree as ET
import time

def main():
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.2.160.51', credentials=credentials))
    channel = connection.channel()
    channel.exchange_declare(exchange="amq.topic", exchange_type="topic", durable=True)

    updates = API.get_changed_data()
    if updates:
        root = ET.fromstring(updates)
        for changed_object in root.findall('ChangedObject__c'):
            id_element = changed_object.find('Id')
            name_element = changed_object.find('Name')
            object_type_element = changed_object.find('object_type__c')

            if id_element is not None and name_element is not None and object_type_element is not None:
                id_value = id_element.text
                name_value = name_element.text
                object_type_value = object_type_element.text

                message = None

                print(f" [x] Sent: Id={id_value}, Name={name_value}, Object Type={object_type_value}")
                match object_type_value:
                    case 'user':
                        message = API.get_user(name_value)
                        xsd_tree = etree.parse('./resource/user_xsd.xml')
                        schema = etree.XMLSchema(xsd_tree)

                        # Parse the documents
                        xml_doc = etree.fromstring(message.encode())

                        if not schema.validate(xml_doc):
                            print('Invalid XML')
                            return
                        
                        print('Valid XML')

                    case 'company':
                        message = API.get_companies(name_value)
                    case 'event':
                        message = API.get_talk(name_value)
                    case _:
                        print(f" [x] Object type {object_type_value} not supported")
                API.delete_change_object(id_value)

            if message:
                channel.basic_publish(exchange='amq.topic', routing_key='user', body=message)

if __name__ == '__main__':
    try:
        print(' [*] Sending messages. To exit press CTRL+C')
        API.authenticate()
        while True:
            main()
            time.sleep(5)
            print(' [*] Waiting for next update...')
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)
