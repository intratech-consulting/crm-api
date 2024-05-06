#!/usr/bin/env python
import pika, sys, os
import API
import xml.etree.ElementTree as ET


#Test CI/CD

def main():
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.2.160.51', credentials=credentials))
    channel = connection.channel()

    queue_name = 'crm'

    channel.queue_declare(queue=queue_name, durable=True)

    def callback(ch, method, properties, body):

        # Parces the xml string
        xml_string = body
        xml_string = xml_string.decode().strip()
        root = ET.fromstring(xml_string)

        match root.tag:
            case 'user':
                try:
                    variables = {}
                    for child in root:
                        if child.tag == "address":
                            for address_field in child:
                                variables[address_field.tag] = address_field.text
                        else:
                            variables[child.tag] = child.text
                    API.add_user(**variables)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    # logger.error("[ERROR] Request Failed", e)

            case 'company':
                try:
                    variables = {}
                    for child in root:
                        if child.tag == "address":
                            for address_field in child:
                                variables[address_field.tag] = address_field.text
                        elif child.tag == "logo":
                            pass
                        else:
                            variables[child.tag] = child.text
                    API.add_company(**variables)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    # logger.error("[ERROR] Request Failed", e)

            case 'event':
                try:
                    variables = {}
                    for child in root:
                        if child.tag == "speaker":
                            for speaker_field in child:
                                if speaker_field.tag == "user_id":
                                    variables[speaker_field.tag] = speaker_field.text
                        elif child.tag == "location" or child.tag == "max_registrations":
                            pass
                        else:
                            variables[child.tag] = child.text.strip()
                    print(variables)
                    API.add_talk(**variables)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    # logger.error("[ERROR] Request Failed", e)

            case 'Talk_attendances':
                try:
                    for child in root:
                        variables = {}
                        for field in child:
                            variables[field.tag] = field.text.strip()
                        API.add_attendance(**variables)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    # logger.error("[ERROR] Request Failed", e)

            case 'order':
                try:
                    variables = {}
                    for child in root:
                        if child.tag == "user_id":
                            variables[child.tag] = child.text.strip()
                        elif child.tag == "products":
                            for products in child:
                                for product_field in products:
                                    if product_field.tag == "name":
                                        product_id = API.product_exists(product_field.text.strip())
                                        if product_id == None:
                                            API.add_product(product_field.text.strip())
                                            product_id = API.product_exists(product_field.text.strip())
                                        variables["product"] = product_id
                                    elif product_field.tag == "amount":
                                        variables[product_field.tag] = product_field.text.strip()
                                id, old_amount = API.get_order(variables["user_id"], variables["product"])
                                print(id, old_amount)
                                if id != None:
                                    variables["amount"] = str(int(old_amount) + int(variables["amount"]))
                                    API.update_order(id, variables["amount"])
                                else:
                                    API.add_order(**variables)
                        else:
                            pass
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    # logger.error("[ERROR] Request Failed", e)

            case _:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                # logger.error("[ERROR] This message is not valid")

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        API.authenticate()
        print(API.ACCESS_TOKEN)
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
