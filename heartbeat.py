from lxml import etree
import pika, sys, os
import time
from datetime import datetime

TEAM = 'crm'

def main(timestamp):
    global TEAM

    # Define your XML and XSD as strings
    heartbeat_xml = f'''
    <Heartbeat>
        <Timestamp>{timestamp.isoformat()}</Timestamp>
        <Status>Active</Status>
        <SystemName>{TEAM}</SystemName>
    </Heartbeat>
    '''

    heartbeat_xsd = f'''
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
        <xs:element name="Heartbeat">
            <xs:complexType>
                <xs:sequence>
                    <xs:element name="Timestamp" type="xs:dateTime" />
                    <xs:element name="Status" type="xs:string" />
                    <xs:element name="SystemName" type="xs:string" />
                </xs:sequence>
            </xs:complexType>
        </xs:element>
    </xs:schema>
    '''

    # Parse the documents
    xml_doc = etree.fromstring(heartbeat_xml.encode())
    xsd_doc = etree.fromstring(heartbeat_xsd.encode())

    # Create a schema object
    schema = etree.XMLSchema(xsd_doc)

    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.2.160.51', credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue='heartbeat_queue', durable=True)
    channel.basic_publish(exchange='', routing_key='heartbeat_queue', body=heartbeat_xml)


if __name__ == '__main__':
    try:
        while True:
            main(datetime.now())
            time.sleep(1)
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)