#!/usr/bin/env python
import pika, sys, os
import xml.etree.ElementTree as ET

sys.path.append('/app')
import config.secrets as secrets
import src.API as API
from uuidapi import *
from logger import init_logger


def main():
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=secrets.HOST, credentials=credentials))
    channel = connection.channel()

    team_name = 'crm'

    channel.queue_declare(queue=team_name, durable=True)

    def callback(ch, method, properties, body):

        # Parces the xml string
        xml_string = body
        xml_string = xml_string.decode().strip()
        root = ET.fromstring(xml_string)
        crud_operation = root.find('crud_operation').text
        logger.info(f"Received a {crud_operation} request for {root.tag}")
        logger.debug(f"Message: {xml_string}")

        match root.tag, crud_operation:
            case 'user', 'create':
                try:
                    variables = {}
                    for child in root:
                        if child.tag == "routing_key" or child.tag == "crud_operation":
                            continue
                        elif child.tag == "address":
                            for address_field in child:
                                variables[address_field.tag] = address_field.text
                        elif child.tag == "company_id":
                            if (child.text is not None):
                                variables[child.tag] = get_service_id(team_name, child.text)
                        else:
                            variables[child.tag] = child.text
                    service_id = API.add_user(**variables)
                    add_service_id(master_uuid=root.find('id').text, service="crm", service_id=service_id)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    logger.info("Request Succeeded")
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    logger.error(f"Request Failed {e}")

            case 'user', 'update':
                try:
                    variables = {}
                    for child in root:
                        if child.tag == "routing_key" or child.tag == "crud_operation":
                            continue
                        elif child.tag == "id" or child.tag == "company_id":
                            if (child.text is not None):
                                variables[child.tag] = get_service_id(team_name, child.text)
                            else:
                                variables[child.tag] = ""
                        elif child.tag == "address":
                            for address_field in child:
                                variables[address_field.tag] = "" if address_field.text == None else address_field.text
                        else:
                            variables[child.tag] = "" if child.text == None else child.text
                    API.update_user(**variables)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    logger.info("Request Succeeded")
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    logger.error(f"Request Failed {e}")

            case 'user', 'delete':
                try:
                    master_uuid = root.find('id').text
                    service_id = get_service_id(service_name="crm", master_uuid=master_uuid)
                    if service_id is not None:
                        API.delete_user(service_id)
                        delete_service_id(master_uuid=master_uuid, service=team_name)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    logger.info("Request Succeeded")
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    logger.error(f"Request Failed {e}")

            case 'company', 'create':
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
                    add_service_id(master_uuid=root.find('id').text, service=team_name, service_id=service_id)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    logger.info("Request Succeeded")
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    logger.error(f"Request Failed {e}")

            case 'company', 'update':
                try:
                    variables = {}
                    for child in root:
                        if child.tag == "routing_key" or child.tag == "crud_operation" or child.tag == "logo":
                            continue
                        elif child.tag == "id":
                            variables[child.tag] = get_service_id(team_name, child.text)
                        elif child.tag == "address":
                            for address_field in child:
                                variables[address_field.tag] = "" if address_field.text == None else address_field.text
                        else:
                            variables[child.tag] = "" if child.text == None else child.text
                    API.update_company(**variables)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    logger.info("Request Succeeded")
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    logger.error(f"Request Failed {e}")

            case 'company', 'delete':
                try:
                    master_uuid = root.find('id').text
                    service_id = get_service_id(service_name="crm", master_uuid=master_uuid)
                    if service_id is not None:
                        API.delete_company(service_id)
                        delete_service_id(master_uuid=master_uuid, service=team_name)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    logger.info("Request Succeeded")
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    logger.error(f"Request Failed {e}")

            case 'event', 'create':
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
                    add_service_id(master_uuid=root.find('id').text, service=team_name, service_id=service_id)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    logger.info("Request Succeeded")
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    logger.error(f"Request Failed {e}")

            case 'event', 'update':
                try:
                    variables = {}
                    for child in root:
                        if child.tag == "routing_key" or child.tag == "crud_operation":
                            continue
                        elif child.tag == "id":
                            variables[child.tag] = get_service_id(team_name, child.text)
                        elif child.tag == "speaker":
                            for speaker_field in child:
                                variables[speaker_field.tag] = "" if speaker_field.text == None else speaker_field.text
                        elif child.tag == "max_registrations" or child.tag == "available_seats":
                            variables[child.tag] = "" if child.text == None else str(int(child.text))
                        else:
                            variables[child.tag] = "" if child.text == None else child.text
                    API.update_event(**variables)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    logger.info("Request Succeeded")
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    logger.error(f"Request Failed {e}")

            case 'event', 'delete':
                try:
                    master_uuid = root.find('id').text
                    service_id = get_service_id(service_name=team_name, master_uuid=master_uuid)
                    API.delete_event(service_id)
                    delete_service_id(master_uuid=master_uuid, service=team_name)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    logger.info("Request Succeeded")
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    logger.error(f"Request Failed {e}")

            case 'attendance', 'create':
                try:
                    variables = {}
                    for child in root:
                        if child.tag == "routing_key" or child.tag == 'crud_operation' or child.tag == "id":
                            pass
                        else:
                            variables[child.tag] = child.text.strip()
                    service_id = API.add_attendance(**variables)
                    add_service_id(master_uuid=root.find('id').text, service=team_name, service_id=service_id)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    logger.info("Request Succeeded")
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    logger.error(f"Request Failed {e}")

            case 'attendance', 'update':
                try:
                    variables = {}
                    for child in root:
                        if child.tag == "routing_key" or child.tag == "crud_operation":
                            continue
                        elif child.tag == "id":
                            variables[child.tag] = get_service_id(team_name, child.text)
                        else:
                            variables[child.tag] = "" if child.text == None else child.text
                    API.update_attendance(**variables)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    logger.info("Request Succeeded")
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    logger.error(f"Request Failed {e}")
            case 'attendance', 'delete':
                try:
                    master_uuid = root.find('id').text
                    service_id = get_service_id(service_name=team_name, master_uuid=master_uuid)
                    API.delete_attendance(service_id)
                    delete_service_id(master_uuid=master_uuid, service=team_name)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    logger.info("Request Succeeded")
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    logger.error(f"Request Failed {e}")

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

            case _:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                logger.error("This message is not valid")

    channel.basic_consume(queue=team_name, on_message_callback=callback, auto_ack=False)

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
