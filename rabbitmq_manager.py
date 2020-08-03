import json
import pika
from pika.exceptions import ChannelError


class RabbitMQManager:

    def __init__(self,
                 queue_name,
                 feed_name,
                 rabbitmq_prefix_name,
                 rabbitmq_user,
                 rabbitmq_pass,
                 rabbitmq_host,
                 rabbitmq_port,
                 rabbitmq_vhost
                 ):

        self.__credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
        self.__parameters = pika.ConnectionParameters(host=rabbitmq_host,
                                                      port=rabbitmq_port,
                                                      virtual_host=rabbitmq_vhost,
                                                      credentials=self.__credentials)
        self.__connection = pika.BlockingConnection(self.__parameters)
        self.channel = self.__connection.channel()
        self.feed_name = feed_name
        self.queue_name = "{}_{}_{}".format(rabbitmq_prefix_name, queue_name, self.feed_name)
        self.create_queue()

    def create_queue(self):
        try:
            return self.channel.queue_declare(self.queue_name)
        except ChannelError as e:
            raise Exception(e)

    def purge_queue(self):
        return self.channel.queue_purge(self.queue_name)

    def delete_queue(self):
        return self.channel.queue_delete(self.queue_name)

    def close_connection(self):
        self.__connection.close()

    def write_message(self, message_content):
        message_content = json.dumps(message_content)

        response = self.channel.basic_publish(
            exchange='',
            routing_key=self.queue_name,
            body=message_content
        )
        return response

    def read_messages_consumer(self):
        msg_count = self.channel.queue_declare(queue=self.queue_name, passive=True).method.message_count
        if msg_count:
            for _ in range(msg_count):
                method_frame, properties, body = self.channel.basic_get(self.queue_name)
                self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                yield json.loads(body)
                if method_frame.delivery_tag == msg_count:
                    break
        self.channel.close()
        self.close_connection()
