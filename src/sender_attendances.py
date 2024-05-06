#!/usr/bin/env python
import pika
from src.API import get_attendance, authenticate
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

# Initialize variable to keep track of the last retrieved attendances
last_attendance_xml = ""

# Infinite loop to continuously check for new attendances
while True:
    # Get the current attendances from Salesforce
    attendance_xml = get_attendance()
    
    # Check if there are new attendances
    if attendance_xml != last_attendance_xml:
        # Send the new attendances to RabbitMQ
        send_message_to_rabbitmq(attendance_xml)
        # Update the last retrieved attendances
        last_attendance_xml = attendance_xml

    # Wait for 10 minutes before checking again
    time.sleep(600)