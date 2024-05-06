from lxml import etree
import pika, sys, os
import time
from datetime import datetime
sys.path.append('/app')
import config.secrets as secrets

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

    xsd_tree = etree.parse('./resources/heartbeat_xsd.xml')
    schema = etree.XMLSchema(xsd_tree)

    # Parse the documents
    xml_doc = etree.fromstring(heartbeat_xml.encode())

    if not schema.validate(xml_doc):
        print('Invalid XML')
        return

    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=secrets.HOST, credentials=credentials))
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