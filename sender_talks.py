#!/usr/bin/env python
import pika
from API import get_talk, authenticate

def send_message_to_rabbitmq(message):
    # Establish connection to RabbitMQ
    credentials = pika.PlainCredentials('user', 'password')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="10.2.160.51", credentials=credentials))
    channel = connection.channel()

    # Declare the queue
    channel.queue_declare(queue="Test")

    # Publish the message to the queue
    channel.basic_publish(exchange="", routing_key="Test", body=message)
    print(" [x] Sent message:", message)

    # Close the connection
    connection.close()

# Authenticate to Salesforce
authenticate()

# Get users from Salesforce
talk_xml = get_talk()

# Check if users_xml is not None before sending to RabbitMQ
if talk_xml is not None:
    # Send users to RabbitMQ
    send_message_to_rabbitmq(talk_xml)
else:
    print("No users data retrieved from Salesforce.")