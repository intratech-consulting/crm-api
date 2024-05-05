import pika
from src.API import get_users, authenticate
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

# Initialize variable to keep track of the last retrieved users
last_users_xml = ""

# Infinite loop to continuously check for new users
while True:
    # Get the current users from Salesforce
    users_xml = get_users()
    
    # Check if there are new users
    if users_xml != last_users_xml:
        # Send the new users to RabbitMQ
        send_message_to_rabbitmq(users_xml)
        # Update the last retrieved users
        last_users_xml = users_xml

    # Wait for 10 minutes before checking again
    time.sleep(600)
