from lxml import etree
import pika, sys, os
import time
from datetime import datetime
import logging

def main(timestamp):
    logger = logging.getLogger(__name__)

    # Create a file handler
    handler = logging.FileHandler('heartbeat.log')
    handler.setLevel(logging.INFO)

    # Create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(handler)

    # Define your XML and XSD as strings
    heartbeat_xml = f'''
    <Heartbeat>
        <Timestamp>{timestamp.isoformat()}</Timestamp>
        <Status>Active</Status>
        <SystemName>CRM</SystemName>
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

    # Validate
    if schema.validate(xml_doc):
        logger.info('XML is valid')
    else:
        logger.error('XML is not valid')

    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.2.160.51', credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue='heartbeat_queue')
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
