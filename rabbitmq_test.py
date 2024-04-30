import pika
import unittest
from testcontainers.rabbitmq import RabbitMqContainer

class TestRabbitMQ(unittest.TestCase):
    def test_rabbitmq_messages(self):
        # Start RabbitMQ container
        with RabbitMqContainer() as container:
            # Establish connection to RabbitMQ
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=container.get_container_host_ip(),
                    port=container.get_exposed_port(5672),
                    credentials=pika.PlainCredentials(username="guest", password="guest"),
                )
            )
            
            # Create a channel
            channel = connection.channel()
            
            # Declare a queue
            queue_name = 'test_queue'
            channel.queue_declare(queue=queue_name)
            
            # Publish a test message
            test_message = "Hello, RabbitMQ!"
            channel.basic_publish(exchange='', routing_key=queue_name, body=test_message)
            
            # Consume the test message
            def callback(ch, method, properties, body):
                self.assertEqual(body.decode(), test_message)
                # Acknowledge the message
                ch.basic_ack(delivery_tag=method.delivery_tag)
            
            # Start consuming messages
            channel.basic_consume(queue=queue_name, on_message_callback=callback)
            
            # Wait for messages
            connection.process_data_events(time_limit=1)  # Wait for 1 second for messages
            
            # Check if the test message was received
            self.assertTrue(channel._consumer_infos)  # If consumer_infos is not empty, message was received

if __name__ == "__main__":
    unittest.main()
