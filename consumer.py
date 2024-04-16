#!/usr/bin/env python
import pika, sys, os
import API
import xml.etree.ElementTree as ET
import logging

#Test CI/CD

def main():
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.2.160.51', credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue='CRM')

    logger = logging.getLogger(__name__)

    # Create a file handler
    handler = logging.FileHandler('consumer.log')
    handler.setLevel(logging.INFO)

    # Create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(handler)

    def callback(ch, method, properties, body):

        # Parces the xml string
        xml_string = body
        xml_string = xml_string.decode().strip()
        root = ET.fromstring(xml_string)

        match root.tag:
            case 'portal_users':
                try:
                    for child in root:
                        variables = {}
                        for field in child:
                            variables[field.tag] = field.text.strip()
                        API.add_user(**variables)
                    ch.basic_ack(delivery_tag = method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag = method.delivery_tag, requeue=False)
                    logger.error("[ERROR] Requeued Message", e)

            case 'companies':
                try:
                    for child in root:
                        variables = {}
                        for field in child:
                            if field.tag == "Address":
                                for address_field in field:
                                    variables[address_field.tag] = address_field.text.strip()
                            else:
                                variables[field.tag] = field.text.strip()
                        API.add_company(**variables)
                    ch.basic_ack(delivery_tag = method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag = method.delivery_tag, requeue=False)
                    logger.error("[ERROR] Requeued Message", e)

            case 'Talks':
                try:
                    for child in root:
                        variables = {}
                        for field in child:
                            variables[field.tag] = field.text.strip()
                        API.add_talk(**variables)
                    ch.basic_ack(delivery_tag = method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag = method.delivery_tag, requeue=False)
                    logger.error("[ERROR] Requeued Message", e)

            case 'Talk_attendances':
                try:
                    for child in root:
                        variables = {}
                        for field in child:
                            variables[field.tag] = field.text.strip()
                        API.add_attendance(**variables)
                    ch.basic_ack(delivery_tag = method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag = method.delivery_tag, requeue=False)
                    logger.error("[ERROR] Requeued Message", e)

            case _:
                ch.basic_nack(delivery_tag = method.delivery_tag, requeue=False)
                logger.error("[ERROR] This message is not valid")

    channel.basic_consume(queue='CRM', on_message_callback=callback, auto_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        API.authenticate()
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)