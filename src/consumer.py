#!/usr/bin/env python
import pika, sys, os
import xml.etree.ElementTree as ET
sys.path.append('/app')
import config.secrets as secrets
import src.API as API


def main():
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=secrets.HOST, credentials=credentials))
    channel = connection.channel()

    queue_name = 'crm'

    channel.queue_declare(queue=queue_name, durable=True)

    def callback(ch, method, properties, body):

        # Parces the xml string
        xml_string = body
        xml_string = xml_string.decode().strip()
        root = ET.fromstring(xml_string)
        crud_operation = root.find('crud_operation').text

        match root.tag, crud_operation:
            case 'user', 'create':
                try:
                    variables = {}
                    for child in root:
                        if child.tag == "routing_key" or child.tag == "crud_operation":
                            continue
                        if child.tag == "address":
                            for address_field in child:
                                variables[address_field.tag] = address_field.text
                        else:
                            variables[child.tag] = child.text
                    API.add_user(**variables)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    print("[ERROR] Request Failed", e)
                    # logger.error("[ERROR] Request Failed", e)

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
                    API.add_company(**variables)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    print("[ERROR] Request Failed", e)

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
                    print(variables)
                    API.add_event(**variables)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    print("[ERROR] Request Failed", e)

            case 'attendance', 'create':
                try:
                    variables = {}
                    for child in root:
                        if child.tag == "routing_key" or child.tag == 'crud_operation' or child.tag == "id":
                            pass
                        else:
                            variables[child.tag] = child.text.strip()
                    print(variables)
                    API.add_attendance(**variables)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    print("[ERROR] Request Failed", e)

            case 'order', 'create':
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


if __name__ == '__main__':
    try:
        API.authenticate()
        print(API.ACCESS_TOKEN)
        main()
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)
