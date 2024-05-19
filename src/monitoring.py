import colorlog
from lxml import etree
import pika, sys, os
import time
from datetime import datetime
sys.path.append('/app')
import config.secrets as secrets
import src.API as API
from logger import init_logger

TEAM = 'crm'

def heartbeat(timestamp):
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=secrets.HOST, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue='heartbeat_queue', durable=True)

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

    channel.basic_publish(exchange='', routing_key='heartbeat_queue', body=heartbeat_xml)

def log(logger, process, message, error='false'):
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=secrets.HOST, credentials=credentials))
    channel = connection.channel()
    channel.exchange_declare(exchange="amq.topic", exchange_type="topic", durable=True)

    loggin_xml = f'''
        <LogEntry>
            <SystemName>{TEAM}</SystemName>
            <FunctionName>{process}</FunctionName>
            <Logs>{message}</Logs>
            <Error>{error}</Error>
            <Timestamp>{datetime.now().isoformat()}</Timestamp>
        </LogEntry>
    '''

    xsd_tree = etree.parse('./resources/log_xsd.xml')
    schema = etree.XMLSchema(xsd_tree)

    # Parse the documents
    xml_doc = etree.fromstring(loggin_xml.encode())

    if not schema.validate(xml_doc):
        logger.error('Invalid XML')
        return

    channel.basic_publish(exchange='amq.topic', routing_key='logs', body=loggin_xml)
    logger.info('Sent logs to controlroom.')

if __name__ == '__main__':
    # Create a custom logger
    logger = init_logger('__monitoring__')
    try:
        while True:
            heartbeat(datetime.now())
            time.sleep(1)
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)