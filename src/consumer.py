#!/usr/bin/env python
import logging
import pika, sys, os
import xml.etree.ElementTree as ET

import requests
import json
sys.path.append('/app')
import config.secrets as secrets
import src.API as API
from uuidapi import *


def main():
    # Create a custom logger
    logger = logging.getLogger(__name__)
    initialize_logger(logger)

    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=secrets.HOST, credentials=credentials))
    channel = connection.channel()

    queue_name = 'crm'

    channel.queue_declare(queue=queue_name, durable=True)

    service_name = 'crm'

    def callback(ch, method, properties, body):

        # Parces the xml string
        xml_string = body
        xml_string = xml_string.decode().strip()
        root = ET.fromstring(xml_string)
        crud_operation = root.find('crud_operation').text

        match root.tag, crud_operation:
            case 'user', 'create':
                logger.info("Consumer received a create user request")
                try:
                    variables = {}
                    for child in root:
                        if child.tag == "routing_key" or child.tag == "crud_operation":
                            continue
                        elif child.tag == "address":
                            for address_field in child:
                                variables[address_field.tag] = address_field.text
                        elif child.tag == "company_id":
                            if(child.text is not None):
                                variables[child.tag] = get_service_id('crm', child.text)
                        else:
                            variables[child.tag] = child.text
                    service_id = API.add_user(**variables)
                    add_service_id(master_uuid=root.find('id').text, service="crm", service_id=service_id)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    print("[ERROR] Request Failed", e)

            case 'user', 'update':
                logger.info("Consumer received an update user request")
                try:
                    variables = {}
                    for child in root:
                        if child.tag == "routing_key" or child.tag == "crud_operation":
                            continue
                        elif child.tag == "id" or child.tag == "company_id":
                            if(child.text is not None):
                                variables[child.tag] = get_service_id('crm', child.text)
                            else:
                                variables[child.tag] = ""
                        elif child.tag == "address":
                            for address_field in child:
                                variables[address_field.tag] = "" if address_field.text == None else address_field.text
                        else:
                            variables[child.tag] = "" if child.text == None else child.text
                    logger.debug(variables)
                    API.update_user(**variables)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    print("[ERROR] Request Failed", e)

            case 'user', 'delete':
                logger.info("Consumer received a delete user request")
                try:
                    master_uuid = root.find('id').text
                    service_id = get_service_id(service_name="crm", master_uuid=master_uuid)
                    if service_id is not None:
                        API.delete_user(service_id)
                        delete_service_id(master_uuid=master_uuid, service="crm")
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        print("[INFO] Deleted User with Master UUID:", master_uuid)
                    else:
                        print("Service ID not found for Master UUID:", master_uuid)
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    print("[ERROR] Request Failed", e)

            case 'company', 'create':
                logger.info("Consumer received a create company request")
                try:
                    variables = {}
                    for child in root:
                        if child.tag == "routing_key" or child.tag == "crud_operation":
                            continue
                        if child.tag == "address":
                            for address_field in child:
                                variables[address_field.tag] = address_field.text
                        elif child.tag == "logo":
                            pass
                        else:
                            variables[child.tag] = child.text
                    service_id = API.add_company(**variables)
                    add_service_id(master_uuid=root.find('id').text, service="crm", service_id=service_id)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    print("[ERROR] Request Failed", e)

            case 'company', 'update':
                logger.info("Consumer received an update company request")
                try:
                    variables = {}
                    for child in root:
                        if child.tag == "routing_key" or child.tag == "crud_operation" or child.tag == "logo":
                            continue
                        elif child.tag == "id":
                            variables[child.tag] = get_service_id('crm', child.text)
                        elif child.tag == "address":
                            for address_field in child:
                                variables[address_field.tag] = "" if address_field.text == None else address_field.text
                        else:
                            variables[child.tag] = "" if child.text == None else child.text
                    print(variables)
                    API.update_company(**variables)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    print("[ERROR] Request Failed", e)


            case 'company', 'delete':
                logger.info("Consumer received a delete company request")
                try:
                    master_uuid = root.find('id').text
                    service_id = get_service_id(service_name="crm", master_uuid=master_uuid)
                    if service_id is not None:
                        API.delete_company(service_id)
                        delete_service_id(master_uuid=master_uuid, service="crm")
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        print("[INFO] Deleted Company with Master UUID:", master_uuid)
                    else:
                        print("Service ID not found for Master UUID:", master_uuid)
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    print("[ERROR] Request Failed", e)

            case 'event', 'create':
                logger.info("Consumer received a create event request")
                try:
                    variables = {}
                    for child in root:
                        if child.tag == "routing_key" or child.tag == "crud_operation":
                            continue
                        if child.tag == "speaker":
                            for speaker_field in child:
                                variables[speaker_field.tag] = speaker_field.text
                        else:
                            variables[child.tag] = child.text.strip()
                    service_id = API.add_event(**variables)
                    add_service_id(master_uuid=root.find('id').text, service="crm", service_id=service_id)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    print("[ERROR] Request Failed", e)

            case 'event', 'update':
                logger.info("Consumer received an update event request")
                try:
                    variables = {}
                    for child in root:
                        if child.tag == "routing_key" or child.tag == "crud_operation":
                            continue
                        elif child.tag == "id":
                            variables[child.tag] = get_service_id('crm', child.text)
                        elif child.tag == "speaker":
                            for speaker_field in child:
                                variables[speaker_field.tag] = "" if speaker_field.text == None else speaker_field.text
                        elif child.tag == "max_registrations" or child.tag == "available_seats":
                            variables[child.tag] = "" if child.text == None else str(int(child.text))
                        else:
                            variables[child.tag] = "" if child.text == None else child.text
                    API.update_event(**variables)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    print("[ERROR] Request Failed", e)

            case 'event', 'delete':
                logger.info("Consumer received a delete event request")
                try:
                    master_uuid = root.find('id').text
                    service_id = get_service_id(service_name="crm", master_uuid=master_uuid)
                    API.delete_event(service_id)
                    delete_service_id(master_uuid=master_uuid, service="crm")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    print("[INFO] Deleted Event with Master UUID:", master_uuid)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    print("[ERROR] Request Failed", e)

            case 'attendance', 'create':
                logger.info("Consumer received a create attendance request")
                try:
                    variables = {}
                    for child in root:
                        if child.tag == "routing_key" or child.tag == 'crud_operation' or child.tag == "id":
                            pass
                        else:
                            variables[child.tag] = child.text.strip()
                    service_id = API.add_attendance(**variables)
                    print(root.find('id').text, "crm", service_id)
                    add_service_id(master_uuid=root.find('id').text, service="crm", service_id=service_id)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    print("[ERROR] Request Failed", e)

            case 'attendance', 'update':
                logger.info("Consumer received an update attendance request")
                try:
                    variables = {}
                    for child in root:
                        print(child.tag, child.text)
                        if child.tag == "routing_key" or child.tag == "crud_operation":
                            continue
                        elif child.tag == "id":
                            variables[child.tag] = get_service_id('crm', child.text)
                        else:
                            variables[child.tag] = "" if child.text == None else child.text
                    API.update_attendance(**variables)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    print("[ERROR] Request Failed", e)

            case 'attendance', 'delete':
                logger.info("Consumer received a delete attendance request")
                try:
                    master_uuid = root.find('id').text
                    service_id = get_service_id(service_name="crm", master_uuid=master_uuid)
                    API.delete_attendance(service_id)
                    delete_service_id(master_uuid=master_uuid, service="crm")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    print("[INFO] Deleted Event with Master UUID:", master_uuid)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    print("[ERROR] Request Failed", e)

            case 'order', 'create':
                logger.info("Consumer received a create order request")
                try:
                    variables = {}
                    for child in root:
                        print(child.tag, child.text)
                        if child.tag == "user_id":
                            variables["user_id"] = child.text.strip()
                        elif child.tag == "products":
                            product_id = None
                            product_name = None
                            for product in child:
                                for product_field in product:
                                    print(product_field.tag, product_field.text)
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

                                print(variables)
                                order_id, old_amount = API.get_order(variables["user_id"], variables["product"])
                                if order_id is not None:
                                    new_amount = str(int(old_amount) + int(variables["amount"]))
                                    API.update_order(order_id, new_amount)
                                else:
                                    API.add_order(**variables)

                        else:
                            pass

                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    print("[INFO] Request Succeeded")
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    print("[ERROR] Request Failed:", e)

            case _:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                # logger.error("[ERROR] This message is not valid")

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

def initialize_logger(logger):
    # Set the level of this logger.
    # DEBUG, INFO, WARNING, ERROR, CRITICAL can be used depending on the granularity of log you want.
    logger.setLevel(logging.DEBUG)

    # Create handlers
    c_handler = logging.StreamHandler()
    s_handler = logging.StreamHandler(sys.stdout)
    c_handler.setLevel(logging.DEBUG)
    s_handler.setLevel(logging.DEBUG)

    # Create formatters and add it to handlers
    c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    s_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)
    s_handler.setFormatter(s_format)

    # Add handlers to the logger
    logger.addHandler(c_handler)
    logger.addHandler(s_handler)

    logger.debug('This is a debug message')
    logger.info('This is an info message')

    logger.warning('This is a warning')
    logger.error('This is an error')


if __name__ == '__main__':
    try:
        API.authenticate()
        print(API.ACCESS_TOKEN)
        main()
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)
