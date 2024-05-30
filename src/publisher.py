#!/usr/bin/env python
import io
import threading
import avro.schema
import avro.io
import certifi
import grpc
import pika, sys, os
from lxml import etree
import xml.etree.ElementTree as ET
import time

if os.path.isdir('/app'):
    sys.path.append('/app')
else:
    local_dir = os.path.dirname(os.path.realpath(__file__))
    sys.path.append(local_dir)
import config.secrets as secrets
import config.pubsub_api_pb2 as pb2
import config.pubsub_api_pb2_grpc as pb2_grpc
from monitoring import log
from API import *
from uuidapi import *
from xml_parser import *
from logger import init_logger

TEAM = 'crm'
semaphore = threading.Semaphore(1)
latest_replay_id = None

def fetchReqStream(topic):
    while True:
        semaphore.acquire()
        yield pb2.FetchRequest(
            topic_name=topic,
            replay_preset=pb2.ReplayPreset.LATEST,
            num_requested=1
        )

def decode(schema, payload):
    schema = avro.schema.parse(schema)
    buf = io.BytesIO(payload)
    decoder = avro.io.BinaryDecoder(buf)
    reader = avro.io.DatumReader(schema)
    return reader.read(decoder)

def authenticate_rabbitmq():
    credentials = pika.PlainCredentials(secrets.RABBITMQ_USER, secrets.RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=secrets.HOST, port=secrets.RABBITMQ_PORT, credentials=credentials))
    channel = connection.channel()
    channel.exchange_declare(exchange='amq.topic', exchange_type='topic', durable=True)
    return channel


def handle_change_event(change_event):
    change_event_header = change_event['ChangeEventHeader']

    if (change_event_header['changeOrigin'] == 'com/salesforce/api/rest/60.0'):
        return # This means the event was generated by the REST API, so we received it through our consumer so we DO NOT want to send it back to the queue

    object_type = f"{change_event_header['entityName']}"[:-3]
    crud_operation = f"{change_event_header['changeType']}".lower()
    rc = f"{object_type}.{TEAM}"

    try:
        changed_fields = get_changed_values(change_event)

        match object_type:
            case 'user':
                message, uuid = create_xml_user(changed_fields, rc, crud_operation)
                logger.debug(f"Message: {message}")
                logger.debug(f"UUID: {uuid}")
                xsd_tree = etree.parse('./resources/user_xsd.xml')

            case 'company':
                message, uuid = create_xml_company(changed_fields, rc, crud_operation)
                xsd_tree = etree.parse('./resources/company_xsd.xml')

            case 'event':
                message, uuid = create_xml_event(changed_fields, rc, crud_operation)
                xsd_tree = etree.parse('./resources/event_xsd.xml')

            case 'attendance':
                message, uuid = create_xml_attendance(changed_fields, rc, crud_operation)
                xsd_tree = etree.parse('./resources/attendance_xsd.xml')

        logger.debug(f"Message: {message}")
        schema = etree.XMLSchema(xsd_tree)
        xml_doc = etree.fromstring(message.encode())

        logger.debug(f"Message: {message}")

        if not uuid:
            logger.warning('No UUID found. Not sending to queue.')
            log(logger, f'PUBLISHER: {crud_operation} {object_type}', 'No UUID found. Not sending to queue.', 'true')
            return

        if not schema.validate(xml_doc):
            logger.warning('Invalid XML. Not sending to queue.')
            log(logger, f'PUBLISHER: {crud_operation} {object_type}', f'Invalid XML. Not sending to queue.', 'true')
            return

        if message:
            channel = authenticate_rabbitmq()
            channel.basic_publish(exchange="amq.topic", routing_key=rc, body=message)
            logger.info(f"Successfully published {crud_operation} {object_type} with UUID {uuid} as\n{message}")
            log(logger, f'PUBLISHER: {crud_operation} {object_type}', f'Successfully published {crud_operation} {object_type} with UUID {uuid}', 'false')

    except Exception as e:
        logger.error(f"An error occurred while processing the message: {e}")
        log(logger, f'PUBLISHER: {crud_operation} {object_type}', f'An error occurred while processing "{crud_operation} {object_type}": {e}', 'true')

if __name__ == '__main__':
    logger = init_logger("__publisher__")
    try:
        authenticate()
        with open(certifi.where(), 'rb') as f:
            creds = grpc.ssl_channel_credentials(f.read())
        with grpc.secure_channel('api.pubsub.salesforce.com:7443', creds) as channel:
            authmetadata = (
                ('accesstoken', secrets.ACCESS_TOKEN),
                ('instanceurl', secrets.INSTANCEURL),
                ('tenantid', secrets.TENANT_ID)
            )
            stub = pb2_grpc.PubSubStub(channel)

            mysubtopic = "/data/ChangeEvents"
            logger.info('Subscribing to ' + mysubtopic)
            substream = stub.Subscribe(fetchReqStream(mysubtopic), metadata=authmetadata)
            
            for event in substream:
                if event.events:
                    payloadbytes = event.events[0].event.payload
                    schemaid = event.events[0].event.schema_id
                    schema = stub.GetSchema(pb2.SchemaRequest(schema_id=schemaid), metadata=authmetadata).schema_json
                    decoded_change_event = decode(schema, payloadbytes)
                    handle_change_event(decoded_change_event)
                    semaphore.release()

    except Exception as e:
        logger.error(f"Failed to start publisher: {e}")
        sys.exit(1)
