#!/usr/bin/env python
import pika
from API import get_companies, authenticate
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

# Initialize variable to keep track of the last retrieved companies
last_company_xml = ""

# Infinite loop to continuously check for new companies
while True:
    # Get the current companies from Salesforce
    company_xml = get_companies()
    
    # Check if there are new companies
    if company_xml != last_company_xml:
        # Send the new companies to RabbitMQ
        send_message_to_rabbitmq(company_xml)
        # Update the last retrieved companies
        last_company_xml = company_xml

    # Wait for 10 minutes before checking again
    time.sleep(600)