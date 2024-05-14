#!/usr/bin/env python
import pika, sys, os
import xml.etree.ElementTree as ET

sys.path.append('/app')
import config.secrets as secrets
from API import *
from uuidapi import *
from xml_parser import *
from logger import init_logger

def main():
    # Global variables
    TEAM = 'crm'

    # Connect to RabbitMQ
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=secrets.HOST, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue=TEAM, durable=True)   

    # Callback function
    def callback(ch, method, properties, body):

        # Parce XML
        xml_string = body
        xml_string = xml_string.decode().strip()

        # Get MATCH CASE attributes
        root = ET.fromstring(xml_string)
        crud_operation = root.find('crud_operation').text
        logger.info(f"Received a {crud_operation} request for {root.tag}")
        logger.debug(f"Message: {xml_string}")

        try:
            variables = {}
            # MATCH CASE
            match root.tag, crud_operation:
                # Case: create user request from RabbitMQ
                case 'user', 'create':
                        read_xml_user(variables, root)
                        service_id = add_user(**variables)
                        add_service_id(root.find('id').text, service_id, TEAM)

                # Case: update user request from RabbitMQ
                case 'user', 'update':
                        read_xml_user(variables, root)
                        update_user(**variables)

                # Case: delete user request from RabbitMQ
                case 'user', 'delete':
                    master_uuid = root.find('id').text
                    service_id = get_service_id(master_uuid, TEAM)
                    if service_id is not None:
                        delete_user(service_id)
                        delete_service_id(master_uuid, TEAM)

                # Case: create company request from RabbitMQ
                case 'company', 'create':
                    read_xml_company(variables, root)
                    service_id = add_company(**variables)
                    add_service_id(root.find('id').text, service_id, TEAM)

                # Case: update company request from RabbitMQ
                case 'company', 'update':
                    read_xml_company(variables, root)
                    update_company(**variables)

                # Case: delete company request from RabbitMQ
                case 'company', 'delete':
                    master_uuid = root.find('id').text
                    service_id = get_service_id(service_name="crm", master_uuid=master_uuid)
                    if service_id is not None:
                        delete_company(service_id)
                        delete_service_id(master_uuid, TEAM)

                # Case: create event request from RabbitMQ
                case 'event', 'create':
                        read_xml_event(variables, root)
                        service_id = add_event(**variables)
                        add_service_id(root.find('id').text, service_id, TEAM)

                # Case: update event request from RabbitMQ
                case 'event', 'update':
                        read_xml_event(variables, root)
                        update_event(**variables)

                # Case: delete event request from RabbitMQ
                case 'event', 'delete':
                    master_uuid = root.find('id').text
                    service_id = get_service_id(master_uuid, TEAM)
                    if service_id is not None:
                        delete_event(service_id)
                        delete_service_id(master_uuid, TEAM)
    
                # Case: create attendance request from RabbitMQ
                case 'attendance', 'create':
                        read_xml_attendance(variables, root)
                        service_id =add_attendance(**variables)
                        add_service_id(root.find('id').text, service_id, TEAM)

                # Case: update attendance request from RabbitMQ
                case 'attendance', 'update':
                        read_xml_attendance(variables, root)
                        update_attendance(**variables)

                # Case: delete attendance request from RabbitMQ
                case 'attendance', 'delete':
                        master_uuid = root.find('id').text
                        service_id = get_service_id(master_uuid, TEAM)
                        if service_id is not None:
                            delete_attendance(service_id)
                            delete_service_id(master_uuid, TEAM)


                case 'order', 'create':
                    try:
                        variables = {}
                        for child in root:
                            if child.tag == "user_id":
                                variables["user_id"] = child.text.strip()
                            elif child.tag == "products":
                                product_id = None
                                product_name = None
                                for product in child:
                                    for product_field in product:
                                        if product_field.tag == "id":
                                            product_id = product_field.text.strip()
                                        elif product_field.tag == "name":
                                            product_name = product_field.text.strip()
                                        elif product_field.tag == "amount":
                                            variables["amount"] = product_field.text.strip()

                                    # Happens after every product
                                    if not API.product_exists(product_id):
                                        product_id = API.add_product(product_name)
                                    variables["product"] = product_id

                                    order_id, old_amount = API.get_order(variables["user_id"], variables["product"])
                                    if order_id is not None:
                                        new_amount = str(int(old_amount) + int(variables["amount"]))
                                        API.update_order(order_id, new_amount)
                                    else:
                                        API.add_order(**variables)

                            else:
                                pass

                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        logger.info("Request Succeeded")
                    except Exception as e:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                        logger.error(f"Request Failed: {e}")

            # Acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(f'Processed {crud_operation} request for {root.tag}')

        # Handle exceptions from the consumer
        except Exception as e:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            logger.error(f'Failed to process {crud_operation} request for {root.tag}: {e}')

    # Start consuming messages
    channel.basic_consume(queue=TEAM, on_message_callback=callback, auto_ack=False)
    logger.info("Waiting for messages to receive. To exit press CTRL+C")
    channel.start_consuming()

if __name__ == '__main__':
    # Create a custom logger
    logger = init_logger("__consumer__")
    try:
        API.authenticate()
        main()
    except Exception as e:
        logger.error(f"Failed to start consumer: {e}")
        sys.exit(1)
