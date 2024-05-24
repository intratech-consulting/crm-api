#!/usr/bin/env python
import pika, sys, os
import xml.etree.ElementTree as ET

if os.path.isdir('/app'):
    sys.path.append('/app')
else:
    local_dir = os.path.dirname(os.path.realpath(__file__))
    sys.path.append(local_dir)
import config.secrets as secrets
from monitoring import log
from API import *
from xml_parser import *
from logger import init_logger
from config.secrets import *

def main():
    # Global variables
    TEAM = 'crm'

    # Connect to RabbitMQ
    credentials = pika.PlainCredentials(secrets.RABBITMQ_USER, secrets.RABBITMQ_PASSWORD)
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=secrets.HOST, port=secrets.RABBITMQ_PORT, credentials=credentials))
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        log(logger, "CONSUMER", f"Failed to connect to RabbitMQ: {e}", error='true')
        sys.exit(1)
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
                    payload = write_xml_user(**variables)
                    service_id = add_user(payload)
                    add_service_id(root.find('id').text, service_id, TEAM)

                # Case: update user request from RabbitMQ
                case 'user', 'update':
                    read_xml_user(variables, root)
                    payload = write_xml_user(**variables)
                    update_user(variables['id'], payload)

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
                    payload = write_xml_company(**variables)
                    service_id = add_company(payload)
                    add_service_id(root.find('id').text, service_id, TEAM)

                # Case: update company request from RabbitMQ
                case 'company', 'update':
                    read_xml_company(variables, root)
                    payload = write_xml_company(**variables)
                    update_company(variables['id'], payload)

                # Case: delete company request from RabbitMQ
                case 'company', 'delete':
                    master_uuid = root.find('id').text
                    service_id = get_service_id(service_name="crm", master_uuid=master_uuid)
                    if service_id is not None:
                        delete_company(service_id)
                        delete_service_id(master_uuid, TEAM)

                # Case: create event request from RabbitMQ
                case 'event', 'create':
                    logger.debug("Creating event request from RabbitMQ")
                    read_xml_event(variables, root)
                    payload = write_xml_event(**variables)
                    service_id = add_event(payload)
                    add_service_id(root.find('id').text, service_id, TEAM)

                # Case: update event request from RabbitMQ
                case 'event', 'update':
                    read_xml_event(variables, root)
                    payload = write_xml_event(**variables)
                    update_event(variables['id'], payload)

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
                    payload = write_xml_attendance(**variables)
                    service_id = add_attendance(payload)
                    add_service_id(root.find('id').text, service_id, TEAM)

                # Case: update attendance request from RabbitMQ
                case 'attendance', 'update':
                    read_xml_attendance(variables, root)
                    payload = write_xml_attendance(**variables)
                    update_attendance(variables['id'], payload)

                # Case: delete attendance request from RabbitMQ
                case 'attendance', 'delete':
                    master_uuid = root.find('id').text
                    service_id = get_service_id(master_uuid, TEAM)
                    if service_id is not None:
                        delete_attendance(service_id)
                        delete_service_id(master_uuid, TEAM)

                # Case: create product request from RabbitMQ
                case 'product', 'create':
                    read_xml_product(variables, root)
                    payload = write_xml_product(**variables)
                    logger.debug(f"Payload: {payload}")
                    service_id = add_product(payload)
                    add_service_id(root.find('id').text, service_id, TEAM)

                # Case: update product request from RabbitMQ
                case 'product', 'update':
                    read_xml_product(variables, root)
                    payload = write_xml_product(**variables)
                    update_product(variables['id'], payload)

                # Case: create order request from RabbitMQ (STILL NEEDS REFACTORING)
                case 'order', 'create':
                    read_xml_order(variables, root)
                    for product in variables['products']:
                        order_id, old_amount = get_order(variables['user_id'], product['product_id'])
                        logger.debug(f"Order ID: {order_id}, Old Amount: {old_amount}")
                        if order_id is not None:
                            payload = write_xml_existing_order(str(int(product['amount']) + int(old_amount)))
                            update_order(order_id, payload)
                        else:
                            payload = write_xml_order(variables['user_id'], **product)
                            add_order(payload)

            # Acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(f'Processed {crud_operation} request for {root.tag}')
            log(logger, f"CONSUMER: {root.tag}.{crud_operation}", f"Processed {crud_operation} request for {root.tag}")

        # Handle exceptions from the consumer
        except Exception as e:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            logger.error(f'Failed to process {crud_operation} request for {root.tag}: {e}')
            log(logger, f"CONSUMER: {root.tag}.{crud_operation}", f"Failed to process {crud_operation} request for {root.tag}: {e}", error='true')

    # Start consuming messages
    channel.basic_consume(queue=TEAM, on_message_callback=callback, auto_ack=False)
    logger.info("Waiting for messages to receive. To exit press CTRL+C")
    channel.start_consuming()

if __name__ == '__main__':
    # Create a custom logger
    logger = init_logger("__consumer__")
    try:
        authenticate()
        main()
    except Exception as e:
        logger.error(f"Failed to start consumer: {e}")
        sys.exit(1)
