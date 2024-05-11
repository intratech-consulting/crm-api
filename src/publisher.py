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
                        rc = "user.crm"
                        message = API.get_new_user(name_value)
                        xsd_tree = etree.parse('./resources/user_xsd.xml')

                    case 'user', 'update':
                        rc = "user.crm"
                        message = 'update user' #TODO
                        xsd_tree = etree.parse('./resources/user_xsd.xml')

                    case 'user', 'delete':
                        rc = "user.crm"
                        message = f'''
                            <user>
                                <routing_key>{rc}</routing_key>
                                <crud_operation>{crud_operation}</crud_operation>
                                <id>{name_value}</id>
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
                            </user>'''
                        xsd_tree = etree.parse('./resources/user_xsd.xml')

                    case 'company', 'create':
                        rc = "company.crm"
                        message = API.get_new_company(name_value)
                        xsd_tree = etree.parse('./resources/company_xsd.xml')

                    case 'company', 'update':
                        rc = "company.crm"
                        message = 'update company' #TODO
                        xsd_tree = etree.parse('./resources/company_xsd.xml')

                    case 'company', 'delete':
                        rc = "company.crm"
                        message = f'''
                            <company>
                                <routing_key>{rc}</routing_key>
                                <crud_operation>{crud_operation}</crud_operation>
                                <id>{name_value}</id>
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
                            </company>'''
                        xsd_tree = etree.parse('./resources/company_xsd.xml')
                        
                    case 'event', 'create':
                        rc = "event.crm"
                        message = API.get_new_event(name_value)
                        xsd_tree = etree.parse('./resources/event_xsd.xml')

                    case 'event', 'update':
                        message = 'update event' #TODO
                        xsd_tree = etree.parse('./resources/event_xsd.xml')

                    case 'event', 'delete':
                        rc = "event.crm"
                        message = f'''
                            <event>
                                    <routing_key>{rc}</routing_key>
                                    <crud_operation>{crud_operation}</crud_operation>
                                    <id>{name_value}</id>
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
                            </event>'''
                        xsd_tree = etree.parse('./resources/event_xsd.xml')
                        rc = "event.crm"

                    case 'attendance', 'create':
                        rc = "attendance.crm"
                        message = API.get_new_attendance(name_value)
                        xsd_tree = etree.parse('./resources/attendance_xsd.xml')

                    case 'attendance', 'update':
                        rc = "attendance.crm"
                        message = 'update attendance' #TODO
                        xsd_tree = etree.parse('./resources/attendance_xsd.xml')

                    case 'attendance', 'delete':
                        rc = "attendance.crm"
                        message = f'''
                            <attendance>
                                <routing_key>{rc}</routing_key>
                                <crud_operation>{crud_operation}</crud_operation>
                                <id>{name_value}</id>
                                <user_id></user_id>
                                <event_id></event_id>
                            </attendance>'''
                        xsd_tree = etree.parse('./resources/attendance_xsd.xml')

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
