#!/usr/bin/env python
import pika, sys, os
from lxml import etree
import xml.etree.ElementTree as ET
import time

sys.path.append('/app')
import config.secrets as secrets
import src.API as API
import monitoring as m
from uuidapi import *
from logger import init_logger

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

                logger.debug(f"Sending: Object Type={object_type_value}, Crud={crud_operation}")
                match object_type_value, crud_operation:
                    case 'user', 'create':
                        rc = "user.crm"
                        message = API.get_new_user(name_value)
                        root = ET.fromstring(message)
                        id_element = root.find('id')
                        if id_element is not None:
                            master_id_value = id_element.text
                            master_uuid = create_master_uuid(master_id_value, "crm")
                            id_element.text = master_uuid
                        company_id_element = root.find('company_id')
                        if company_id_element is not None:
                            company_id_value = company_id_element.text
                            company_master_uuid = get_master_uuid(company_id_value, "crm")
                            company_id_element.text = company_master_uuid
                        message = ET.tostring(root, encoding='utf-8').decode('utf-8')
                        xsd_tree = etree.parse('./resources/user_xsd.xml')
                        m.log(logger, f'{crud_operation} {object_type_value}', f'Succesfully published "{crud_operation} {object_type_value}" with UUID {master_uuid} on RabbitMQ!')

                    case 'user', 'update':
                        rc = "user.crm"
                        updated_fields = API.get_updated_user(id_value)
                        updated_values = API.get_updated_values(f"query?q=SELECT+{','.join(updated_fields)}+FROM+user__c+WHERE+Id+=+'{name_value}'")

                        try:
                            master_uuid = get_master_uuid(name_value, "crm")
                        except Exception as e:
                            logger.error(f"An error occurred while getting the master_uuid: {e}")
                            return
                        
                        first_name__c = updated_values.get('first_name__c', '')
                        last_name__c = updated_values.get('last_name__c', '')
                        email__c = updated_values.get('email__c', '')
                        telephone__c = updated_values.get('telephone__c', '')
                        birthday__c = updated_values.get('birthday__c', '')
                        country__c = updated_values.get('country__c', '')
                        state__c = updated_values.get('state__c', '')
                        city__c = updated_values.get('city__c', '')
                        zip__c = updated_values.get('zip__c', '')
                        if zip__c != '':
                            zip__c = str(int(zip__c))
                        street__c = updated_values.get('street__c', '')
                        house_number__c = updated_values.get('house_number__c', '')
                        if house_number__c != '':
                            house_number__c = str(int(house_number__c))
                        company_email__c = updated_values.get('company_email__c', '')
                        company_id__c = updated_values.get('company_id__c', '')
                        if company_id__c != '':
                            company_id__c = get_master_uuid(company_id__c, "crm")
                        source__c = updated_values.get('source__c', '')
                        user_role__c = updated_values.get('user_role__c', '')
                        invoice__c = updated_values.get('invoice__c', '')
                        calendar_link__c = updated_values.get('calendar_link__c', '')
                        message = f'''
                            <user>
                                <routing_key>{rc}</routing_key>
                                <crud_operation>{crud_operation}</crud_operation>
                                <id>{master_uuid}</id>
                                <first_name>{first_name__c}</first_name>
                                <last_name>{last_name__c}</last_name>
                                <email>{email__c}</email>
                                <telephone>{telephone__c}</telephone>
                                <birthday>{birthday__c}</birthday>
                                <address>
                                    <country>{country__c}</country>
                                    <state>{state__c}</state>
                                    <city>{city__c}</city>
                                    <zip>{zip__c}</zip>
                                    <street>{street__c}</street>
                                    <house_number>{house_number__c}</house_number>
                                </address>
                                <company_email>{company_email__c}</company_email>
                                <company_id>{company_id__c}</company_id>
                                <source>{source__c}</source>
                                <user_role>{user_role__c}</user_role>
                                <invoice>{invoice__c}</invoice>
                                <calendar_link>{calendar_link__c}</calendar_link>
                            </user>'''
                        xsd_tree = etree.parse('./resources/user_xsd.xml')
                        m.log(logger, f'{crud_operation} {object_type_value}', f'Succesfully published "{crud_operation} {object_type_value}" with UUID {master_uuid} on RabbitMQ!')

                    case 'user', 'delete':
                        rc = "user.crm"
                        try:
                            master_uuid = get_master_uuid(name_value, "crm")
                        except Exception as e:
                            logger.error(f"An error occurred while getting the master_uuid: {e}")
                            return
                        message = f'''
                            <user>
                                <routing_key>{rc}</routing_key>
                                <crud_operation>{crud_operation}</crud_operation>
                                <id>{master_uuid}</id>
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
                        m.log(logger, f'{crud_operation} {object_type_value}', f'Succesfully published "{crud_operation} {object_type_value}" with UUID {master_uuid} on RabbitMQ!')

                    case 'company', 'create':
                        rc = "company.crm"
                        message = API.get_new_company(name_value)
                        root = ET.fromstring(message)
                        id_element = root.find('id')
                        if id_element is not None:
                            master_id_value = id_element.text
                            master_uuid = create_master_uuid(master_id_value, "crm")
                            id_element.text = master_uuid
                        message = ET.tostring(root, encoding='utf-8').decode('utf-8')
                        xsd_tree = etree.parse('./resources/company_xsd.xml')

                    case 'company', 'update':
                        rc = "company.crm"
                        updated_fields = API.get_updated_company(id_value)
                        updated_values = API.get_updated_values(f"query?q=SELECT+{','.join(updated_fields)}+FROM+company__c+WHERE+Id+=+'{name_value}'")

                        try:
                            master_uuid = get_master_uuid(name_value, "crm")
                        except Exception as e:
                            logger.error(f"An error occurred while getting the master_uuid: {e}")
                            return

                        name__c = updated_values.get('Name', '')
                        email__c = updated_values.get('email__c', '')
                        telephone__c = updated_values.get('telephone__c', '')
                        country__c = updated_values.get('country__c', '')
                        state__c = updated_values.get('state__c', '')
                        city__c = updated_values.get('city__c', '')
                        zip__c = updated_values.get('zip__c', '')
                        street__c = updated_values.get('street__c', '')
                        house_number__c = updated_values.get('house_number__c', '')
                        type__c = updated_values.get('type__c', '')
                        invoice__c = updated_values.get('invoice__c', '')

                        message = f'''
                            <company>
                                <routing_key>{rc}</routing_key>
                                <crud_operation>{crud_operation}</crud_operation>
                                <id>{master_uuid}</id>
                                <name>{name__c}</name>
                                <email>{email__c}</email>
                                <telephone>{telephone__c}</telephone>
                                <logo></logo>
                                <address>
                                    <country>{country__c}</country>
                                    <state>{state__c}</state>
                                    <city>{city__c}</city>
                                    <zip>{zip__c}</zip>
                                    <street>{street__c}</street>
                                    <house_number>{house_number__c}</house_number>
                                </address>
                                <type>{type__c}</type>
                                <invoice>{invoice__c}</invoice>
                            </company>'''
                        xsd_tree = etree.parse('./resources/company_xsd.xml')

                    case 'company', 'delete':
                        rc = "company.crm"
                        try:
                            master_uuid = get_master_uuid(name_value, "crm")
                        except Exception as e:
                            logger.error(f"An error occurred while getting the master_uuid: {e}")
                            return
                        message = f'''
                            <company>
                                <routing_key>{rc}</routing_key>
                                <crud_operation>{crud_operation}</crud_operation>
                                <id>{master_uuid}</id>
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
                        root = ET.fromstring(message)
                        id_element = root.find('id')
                        if id_element is not None:
                            master_id_value = id_element.text
                            master_uuid = create_master_uuid(master_id_value, "crm")
                            id_element.text = master_uuid
                        message = ET.tostring(root, encoding='utf-8').decode('utf-8')
                        xsd_tree = etree.parse('./resources/event_xsd.xml')

                    case 'event', 'update':
                        rc = "event.crm"
                        updated_fields = API.get_updated_event(id_value)
                        updated_values = API.get_updated_values(f"query?q=SELECT+{','.join(updated_fields)}+FROM+event__c+WHERE+Id+=+'{name_value}'")

                        try:
                            master_uuid = get_master_uuid(name_value, "crm")
                        except Exception as e:
                            logger.error(f"An error occurred while getting the master_uuid: {e}")
                            return

                        date__c = updated_values.get('date__c', '')
                        start_time__c = updated_values.get('start_time__c', '')
                        end_time__c = updated_values.get('end_time__c', '')
                        location__c = updated_values.get('location__c', '')
                        user_id__c = updated_values.get('user_id__c', '')
                        company_id__c = updated_values.get('company_id__c', '')
                        max_registrations__c = updated_values.get('max_registrations__c', '')
                        available_seats__c = updated_values.get('available_seats__c', '')
                        description__c = updated_values.get('description__c', '')
                        message = f'''
                            <event>
                                <routing_key>{rc}</routing_key>
                                <crud_operation>{crud_operation}</crud_operation>
                                <id>{master_uuid}</id>
                                <date>{date__c}</date>
                                <start_time>{start_time__c}</start_time>
                                <end_time>{end_time__c}</end_time>
                                <location>{location__c}</location>
                                <speaker>
                                    <user_id>{user_id__c}</user_id>
                                    <company_id>{company_id__c}</company_id>
                                </speaker>
                                <max_registrations>{max_registrations__c}</max_registrations>
                                <available_seats>{available_seats__c}</available_seats>
                                <description>{description__c}</description>
                            </event>'''
                        xsd_tree = etree.parse('./resources/event_xsd.xml')

                    case 'event', 'delete':
                        rc = "event.crm"
                        try:
                            master_uuid = get_master_uuid(name_value, "crm")
                        except Exception as e:
                            logger.error(f"An error occurred while getting the master_uuid: {e}")
                            return
                        message = f'''
                            <event>
                                    <routing_key>{rc}</routing_key>
                                    <crud_operation>{crud_operation}</crud_operation>
                                    <id>{master_uuid}</id>
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
                        root = ET.fromstring(message)
                        id_element = root.find('id')
                        if id_element is not None:
                            master_id_value = id_element.text
                            master_uuid = create_master_uuid(master_id_value, "crm")
                            id_element.text = master_uuid
                        user_id_element = root.find('user_id')
                        if user_id_element is not None:
                            user_id_value = user_id_element.text
                            user_master_uuid = get_master_uuid(user_id_value, "crm")
                            user_id_element.text = user_master_uuid
                        event_id_element = root.find('event_id')
                        if event_id_element is not None:
                            event_id_value = event_id_element.text
                            event_master_uuid = get_master_uuid(event_id_value, "crm")
                            event_id_element.text = event_master_uuid
                        message = ET.tostring(root, encoding='utf-8').decode('utf-8')
                        xsd_tree = etree.parse('./resources/attendance_xsd.xml')

                    case 'attendance', 'update':
                        rc = "attendance.crm"
                        updated_fields = API.get_updated_attendance(id_value)
                        updated_values = API.get_updated_values(f"query?q=SELECT+{','.join(updated_fields)}+FROM+attendance__c+WHERE+Id+=+'{name_value}'")
                        master_uuid, user_master_uuid, event_master_uuid = None, None, None

                        try:
                            master_uuid = get_master_uuid(name_value, "crm")
                            master_uuid = create_master_uuid(master_uuid, "crm")
                        except Exception as e:
                            logger.error(f"An error occurred while getting the master_uuid: {e}")
                            return
                        
                        user_id_value = updated_values.get('user_id__c')
                        if user_id_value is not None:
                            user_master_uuid = get_master_uuid(user_id_value, "crm")

                        event_id_value = updated_values.get('event_id__c')
                        if event_id_value is not None:
                            event_master_uuid = get_master_uuid(event_id_value, "crm")

                        message = f'''
                            <attendance>
                                <routing_key>{rc}</routing_key>
                                <crud_operation>{crud_operation}</crud_operation>
                                <id>{master_uuid}</id>
                                <user_id>{"" if user_master_uuid == None else user_master_uuid}</user_id>
                                <event_id>{"" if event_master_uuid == None else event_master_uuid}</event_id>
                            </attendance>'''
                        xsd_tree = etree.parse('./resources/attendance_xsd.xml')

                    case 'attendance', 'delete':
                        rc = "attendance.crm"
                        try:
                            master_uuid = get_master_uuid(name_value, "crm")
                        except Exception as e:
                            logger.error(f"An error occurred while getting the master_uuid: {e}")
                            return
                        message = f'''
                            <attendance>
                                <routing_key>{rc}</routing_key>
                                <crud_operation>{crud_operation}</crud_operation>
                                <id>{master_uuid}</id>
                                <user_id></user_id>
                                <event_id></event_id>
                            </attendance>'''
                        xsd_tree = etree.parse('./resources/attendance_xsd.xml')

                    case _:
                        logger.info(f"Object type {object_type_value} not supported")

                logger.debug(f"Message: {message}")

                schema = etree.XMLSchema(xsd_tree)
                xml_doc = etree.fromstring(message.encode())
                if not schema.validate(xml_doc):
                    logger.error('Invalid XML')
                else:
                    logger.info('Object sent successfully')
                    API.delete_change_object(id_value)
                    if message:
                        channel.basic_publish(exchange='amq.topic', routing_key=rc, body=message)

if __name__ == '__main__':
    # Create a custom logger
    logger = init_logger("__publisher__")
    try:
        API.authenticate()
        logger.info("Waiting for messages to send. To exit press CTRL+C")
        while True:
            main()
            time.sleep(120)
    except Exception as e:
        logger.error(f"Request Failed {e}")
        sys.exit(1)
