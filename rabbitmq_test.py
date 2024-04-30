import pika
import unittest
from testcontainers.rabbitmq import RabbitMqContainer


class TestApi(unittest.TestCase):
    def test_message_consumption(self):
        # Start RabbitMQ container
        with RabbitMqContainer("rabbitmq:3.9.10") as rabbitmq:
            # Get connection parameters
            connection_params = rabbitmq.get_connection_params()

            # Establish connection
            connection = pika.BlockingConnection(connection_params)
            channel = connection.channel()

            # Declare a queue
            channel.queue_declare(queue='test_queue')

            # Define a callback function to process received messages
            def callback(ch, method, properties, body):
                print("Received message:", body.decode())

            # Consume messages from the queue
            channel.basic_consume(queue='test_queue', on_message_callback=callback, auto_ack=True)

            # Start consuming messages
            print("Waiting for messages. Press CTRL+C to exit.")
            try:
                channel.start_consuming()
            except KeyboardInterrupt:
                pass


if __name__ == "__main__":
    unittest.main()
