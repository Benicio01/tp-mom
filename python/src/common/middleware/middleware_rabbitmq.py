import pika
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from .middleware import (
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
)
from pika.exceptions import AMQPConnectionError, AMQPError


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.queue_name = queue_name
        self.host = host
        self._consuming = False
        try:
            self.conn = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.chan = self.conn.channel()
            self.chan.queue_declare(queue=queue_name, durable=True)
        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError
        except AMQPError:
            raise MessageMiddlewareMessageError

    def send(self, message):
        try:
            self.chan.basic_publish(
                exchange="", routing_key=self.queue_name, body=message
            )
        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError
        except AMQPError:
            raise MessageMiddlewareMessageError

    def start_consuming(self, on_message_callback):
        def _pika_callback(ch, method, properties, body):
            message = body

            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(message, ack, nack)

        try:
            self._consuming = True
            self.chan.basic_consume(
                queue=self.queue_name,
                on_message_callback=_pika_callback,
                auto_ack=False,
            )
            self.chan.start_consuming()
        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError
        except AMQPError:
            raise MessageMiddlewareMessageError

    def stop_consuming(self):
        if not self._consuming:
            return
        try:
            self.chan.stop_consuming()
        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError
        except AMQPError:
            raise MessageMiddlewareMessageError
        finally:
            self._consuming = False

    def close(self):
        try:
            self.chan.close()
            self.conn.close()
        except AMQPError:
            raise MessageMiddlewareCloseError


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.host = host
        self.queue_name = None
        self._consuming = False
        try:
            self.conn = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.chan = self.conn.channel()
            self.chan.exchange_declare(
                exchange=exchange_name, exchange_type="direct", durable=True
            )
            result = self.chan.queue_declare(queue="", exclusive=True)
            self.queue_name = result.method.queue
            for routing_key in routing_keys:
                self.chan.queue_bind(
                    exchange=exchange_name,
                    queue=self.queue_name,
                    routing_key=routing_key,
                )

        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError
        except AMQPError:
            raise MessageMiddlewareMessageError

    def send(self, message):
        try:
            for routing_key in self.routing_keys:
                self.chan.basic_publish(
                    exchange=self.exchange_name, routing_key=routing_key, body=message
                )
        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError
        except AMQPError:
            raise MessageMiddlewareMessageError

    def start_consuming(self, on_message_callback):
        def _pika_callback(ch, method, properties, body):
            message = body

            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(message, ack, nack)

        try:
            self._consuming = True
            self.chan.basic_consume(
                queue=self.queue_name,
                on_message_callback=_pika_callback,
                auto_ack=False,
            )
            self.chan.start_consuming()
        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError
        except AMQPError:
            raise MessageMiddlewareMessageError

    def stop_consuming(self):
        if not self._consuming:
            return
        try:
            self.chan.stop_consuming()
        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError
        except AMQPError:
            raise MessageMiddlewareMessageError
        finally:
            self._consuming = False

    def close(self):
        try:
            self.chan.close()
            self.conn.close()
        except AMQPError:
            raise MessageMiddlewareCloseError
