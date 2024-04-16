#!/usr/bin/env python
import pika
from API import get_talk, authenticate
import time

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

# Initialize variable to keep track of the last retrieved talks
last_talk_xml = ""

# Infinite loop to continuously check for new talks
while True:
    # Get the current talks from Salesforce
    talk_xml = get_talk()
    
    # Check if there are new talks
    if talk_xml != last_talk_xml:
        # Send the new talks to RabbitMQ
        send_message_to_rabbitmq(talk_xml)
        # Update the last retrieved talks
        last_talk_xml = talk_xml

    # Wait for 10 minutes before checking again
    time.sleep(600)