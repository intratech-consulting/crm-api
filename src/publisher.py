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
            topic_name = topic,
            replay_preset = pb2.ReplayPreset.LATEST,
            num_requested = 1)
        
def decode(schema, payload):
    schema = avro.schema.parse(schema)
    buf = io.BytesIO(payload)
    decoder = avro.io.BinaryDecoder(buf)
    reader = avro.io.DatumReader(schema)
    return reader.read(decoder)

def handle_change_event(decoded, message, rc):
    logger.debug(f"Decoded event: {decoded}")

    change_event_header = decoded['ChangeEventHeader']
    if change_event_header['changeType'] != 'CREATE':
        return

    object_type = f'{decoded['object_type__c']}'
    crud_operation = f'{decoded['crud__c']}'
    object_id = f'{decoded['Name']}'
    changed_object_id = change_event_header['recordIds'][0]

    logger.debug(f"Object type: {object_type}, CRUD operation: {crud_operation}, Object ID: {object_id}")
    try:

        rc = f"{object_type}.{TEAM}"

        match object_type, crud_operation:
            case 'user', 'create':
                message = get_new_user(object_id)
                xsd_tree = etree.parse('./resources/user_xsd.xml')

            case 'user', 'update':
                updated_fields = get_updated_user(changed_object_id)
                updated_values = get_updated_values(f"query?q=SELECT+{','.join(updated_fields)}+FROM+user__c+WHERE+Id+=+'{object_id}'")
                message = update_xml_user(rc, crud_operation, object_id, updated_values)
                xsd_tree = etree.parse('./resources/user_xsd.xml')

            case 'user', 'delete':
                message = update_xml_user(rc, crud_operation, object_id, {})
                xsd_tree = etree.parse('./resources/user_xsd.xml')

            case 'company', 'create':
                message = get_new_company(object_id)
                xsd_tree = etree.parse('./resources/company_xsd.xml')

            case 'company', 'update':
                rc = "company.crm"
                updated_fields = get_updated_company(changed_object_id)
                updated_values = get_updated_values(f"query?q=SELECT+{','.join(updated_fields)}+FROM+company__c+WHERE+Id+=+'{object_id}'")
                message = update_xml_company(rc, crud_operation, object_id, updated_values)
                xsd_tree = etree.parse('./resources/company_xsd.xml')

            case 'company', 'delete':
                message = update_xml_company(rc, crud_operation, object_id, {})
                xsd_tree = etree.parse('./resources/company_xsd.xml')

            case 'event', 'create':
                message = get_new_event(object_id)
                xsd_tree = etree.parse('./resources/event_xsd.xml')

            case 'event', 'update':
                updated_fields = get_updated_event(changed_object_id)
                updated_values = get_updated_values(f"query?q=SELECT+{','.join(updated_fields)}+FROM+event__c+WHERE+Id+=+'{object_id}'")
                message = update_xml_event(rc, crud_operation, object_id, updated_values)
                xsd_tree = etree.parse('./resources/event_xsd.xml')

            case 'event', 'delete':
                message = update_xml_event(rc, crud_operation, object_id, {})
                xsd_tree = etree.parse('./resources/event_xsd.xml')

            case 'attendance', 'create':
                message = get_new_attendance(object_id)
                xsd_tree = etree.parse('./resources/attendance_xsd.xml')

            case 'attendance', 'update':
                updated_fields = get_updated_attendance(changed_object_id)
                updated_values = get_updated_values(f"query?q=SELECT+{','.join(updated_fields)}+FROM+attendance__c+WHERE+Id+=+'{object_id}'")
                message = update_xml_attendance(rc, crud_operation, object_id, updated_values)
                xsd_tree = etree.parse('./resources/attendance_xsd.xml')

            case 'attendance', 'delete':
                message = update_xml_attendance(rc, crud_operation, object_id, {})
                xsd_tree = etree.parse('./resources/attendance_xsd.xml')

        schema = etree.XMLSchema(xsd_tree)
        xml_doc = etree.fromstring(message.encode())
        if not schema.validate(xml_doc):
                logger.error('Invalid XML')
        else:
            logger.debug('Valid XML')
            delete_change_object(changed_object_id)

        logger.debug(f"Message: {message}")
        

    except Exception as e:
        logger.error(f"An error occurred while processing the message: {e}")
        log(logger, f'PUBLISHER: {crud_operation} {object_type}', f'An error occurred while processing "{crud_operation} {object_type}": {e}', 'true')

if __name__ == '__main__':
    # Create a custom logger
    logger = init_logger("__publisher__")
    try:
        authenticate()
        # Setup RabbitMQ
        credentials = pika.PlainCredentials('user', 'password')
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=secrets.HOST, credentials=credentials))
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            log(logger, "CONSUMER", f"Failed to connect to RabbitMQ: {e}", error='true')
            sys.exit(1)
        channel = connection.channel()
        channel.exchange_declare(exchange="amq.topic", exchange_type="topic", durable=True)

        # Setup gRPC
        with open(certifi.where(), 'rb') as f:
            creds = grpc.ssl_channel_credentials(f.read())
        with grpc.secure_channel('api.pubsub.salesforce.com:7443', creds) as channel:
            authmetadata = (
                ('accesstoken', secrets.ACCESS_TOKEN),
                ('instanceurl', 'https://erasmushogeschoolbrussel4-dev-ed.develop.my.salesforce.com'),
                ('tenantid', '00DQy0000050h7YMAQ')
            )
            stub = pb2_grpc.PubSubStub(channel)

            mysubtopic = "/data/user__ChangeEvent"
            logger.info('Subscribing to ' + mysubtopic)
            substream = stub.Subscribe(fetchReqStream(mysubtopic),
                                    metadata=authmetadata)
            for event in substream:
                if event.events:
                    semaphore.release()
                    logger.info(f"Number of events received: {len(event.events)}")
                    payloadbytes = event.events[0].event.payload
                    schemaid = event.events[0].event.schema_id
                    schema = stub.GetSchema(
                        pb2.SchemaRequest(schema_id=schemaid),
                        metadata=authmetadata).schema_json
                    decoded = decode(schema, payloadbytes)
                    logger.info(f"Got an event! {json.dumps(decoded)}")
                    message = ""
                    rc = ""
                    handle_change_event(decoded, message, rc)
                else:
                    logger.info("Ik kan niets")
            channel.basic_publish(exchange="amq.topic", routing_key=rc, body=message)

    except Exception as e:
        logger.error(f"Failed to start publisher: {e}")
        sys.exit(1)
