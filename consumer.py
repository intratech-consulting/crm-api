#!/usr/bin/env python
import pika, sys, os
import API
import xml.etree.ElementTree as ET

def main():
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.2.160.51', credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue='CRM')

    def callback(ch, method, properties, body):
        print(f" [x] Received {body}")
        xml_string = body

        # Convert bytes to string and remove leading/trailing whitespace
        xml_string = xml_string.decode().strip()
        # Parse XML
        root = ET.fromstring(xml_string)
        
        # Extract each field
        fields = {}
        for child in root:
            fields[child.tag] = child.text.strip()

        # Call the add_user function with extracted fields
        API.add_user(
            fields['First_name__c'],
            fields['Last_name__c'],
            fields['Email__c'],
            fields['Company__c'],
            fields['Company_email__c'],
            fields['Signup_source__c']
        )

    channel.basic_consume(queue='CRM', on_message_callback=callback, auto_ack=True)

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