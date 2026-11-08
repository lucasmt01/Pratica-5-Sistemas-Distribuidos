import logging
import sqlite3
import time

import pika

from .events import Event, ROUTING_KEYS
from .messaging import close, connect, declare_topology, queue_name
from .storage import Storage

LOG = logging.getLogger(__name__)


def make_callback(consumer, storage):
    def callback(channel, method, properties, body):
        try:
            event = Event.from_bytes(body)
            if ROUTING_KEYS[event.tipo] != method.routing_key:
                raise ValueError("Routing key incompatível com o tipo do evento.")
            state = storage.process(consumer, event)
        except ValueError as exc:
            LOG.error("Mensagem inválida enviada à fila de rejeitados: %s", exc)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        except sqlite3.Error:
            LOG.exception("Falha de persistência; mensagem será reenviada.")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            time.sleep(1)
            return
        channel.basic_ack(delivery_tag=method.delivery_tag)
        if state is None:
            print(f"[{consumer}] Evento repetido ignorado: {event.evento_id}", flush=True)
        else:
            print(f"[{consumer}] {state['decisao']} (evento {event.evento_id})", flush=True)
            if consumer == "semaforo":
                print(" | ".join(f"{d}: {s}" for d, s in state["sinais"].items()), flush=True)
    return callback


def run_consumer(consumer, settings):
    storage = Storage(settings.db_path)
    while True:
        connection = None
        try:
            connection = connect(settings)
            channel = connection.channel()
            declare_topology(channel)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=queue_name(consumer), on_message_callback=make_callback(consumer, storage),
                                  auto_ack=False)
            print(f"[{consumer}] Aguardando eventos. Ctrl+C para sair.", flush=True)
            channel.start_consuming()
        except (pika.exceptions.AMQPError, OSError) as exc:
            LOG.warning("[%s] Conexão indisponível (%s). Nova tentativa em 3 s.", consumer, exc)
        finally:
            close(connection)
        time.sleep(3)
