import time

import pika

from .events import CONSUMER_TYPES, ROUTING_KEYS

EXCHANGE = "cidade.eventos"
DEAD_EXCHANGE = "cidade.invalidos"
DEAD_QUEUE = "cidade.mensagens_invalidas"


def queue_name(consumer):
    return f"cidade.{consumer}"


def connect(settings):
    return pika.BlockingConnection(pika.ConnectionParameters(
        host=settings.host, port=settings.port, virtual_host=settings.virtual_host,
        credentials=pika.PlainCredentials(settings.user, settings.password),
        heartbeat=60, blocked_connection_timeout=15, socket_timeout=5,
        stack_timeout=10, connection_attempts=1,
    ))


def declare_topology(channel):
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=DEAD_EXCHANGE, exchange_type="fanout", durable=True)
    channel.queue_declare(queue=DEAD_QUEUE, durable=True)
    channel.queue_bind(queue=DEAD_QUEUE, exchange=DEAD_EXCHANGE)
    for consumer, types in CONSUMER_TYPES.items():
        queue = queue_name(consumer)
        channel.queue_declare(queue=queue, durable=True,
                              arguments={"x-dead-letter-exchange": DEAD_EXCHANGE,
                                         "x-single-active-consumer": True})
        for event_type in types:
            channel.queue_bind(queue=queue, exchange=EXCHANGE, routing_key=ROUTING_KEYS[event_type])


def close(connection):
    if connection and connection.is_open:
        try:
            connection.close()
        except (pika.exceptions.AMQPError, OSError):
            pass


class Publisher:
    def __init__(self, settings):
        self.settings = settings
        self.connection = None
        self.channel = None

    def publish(self, event):
        body = event.to_bytes()
        for attempt in range(3):
            try:
                if not self.connection or self.connection.is_closed:
                    self.connection = connect(self.settings)
                    self.channel = self.connection.channel()
                    # Filas existem mesmo se os consumidores ainda estiverem desligados.
                    declare_topology(self.channel)
                    self.channel.confirm_delivery()
                self.channel.basic_publish(
                    exchange=EXCHANGE, routing_key=ROUTING_KEYS[event.tipo], body=body,
                    properties=pika.BasicProperties(content_type="application/json", delivery_mode=2,
                                                    message_id=event.evento_id), mandatory=True,
                )
                return
            except (pika.exceptions.AMQPError, OSError) as exc:
                self.close()
                if attempt == 2:
                    raise ConnectionError("Publicação não confirmada após 3 tentativas. Verifique o RabbitMQ.") from exc
                time.sleep(1)

    def close(self):
        close(self.connection)
        self.connection = None
        self.channel = None
