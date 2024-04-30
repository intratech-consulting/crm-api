import pika
import unittest
from testcontainers.rabbitmq import RabbitMqContainer

class TestRabbitMQ(unittest.TestCase):
    def test_rabbitmq_connection(self):
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
            
            # Check if the connection is successful
            self.assertTrue(connection.is_open)

if __name__ == "__main__":
    unittest.main()
