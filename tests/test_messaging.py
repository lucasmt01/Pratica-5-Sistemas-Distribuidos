import sqlite3
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pika
import pytest

from cidade_inteligente.config import Settings
from cidade_inteligente.consumer import make_callback
from cidade_inteligente.events import CONSUMER_TYPES, Event, ROUTING_KEYS
from cidade_inteligente.messaging import EXCHANGE, Publisher, declare_topology, queue_name
from cidade_inteligente.storage import Storage


def test_topology_binds_all_events_to_durable_queues():
    channel = Mock()
    declare_topology(channel)
    channel.exchange_declare.assert_any_call(exchange=EXCHANGE, exchange_type="topic", durable=True)
    for consumer, types in CONSUMER_TYPES.items():
        channel.queue_declare.assert_any_call(
            queue=queue_name(consumer), durable=True,
            arguments={"x-dead-letter-exchange": "cidade.invalidos", "x-single-active-consumer": True})
        for tipo in types:
            channel.queue_bind.assert_any_call(queue=queue_name(consumer), exchange=EXCHANGE,
                                               routing_key=ROUTING_KEYS[tipo])


def test_ack_is_sent_after_commit(tmp_path):
    storage = Storage(tmp_path / "cidade.db")
    event = Event.create("limpador", "LIGA")
    channel = Mock()
    channel.basic_ack.side_effect = lambda **kw: (
        storage.snapshot()["limpador"]["ativo"] is True or pytest.fail("ACK antes do commit"))
    make_callback("limpador", storage)(channel, SimpleNamespace(delivery_tag=1, routing_key="comando.limpador"),
                                      None, event.to_bytes())
    channel.basic_ack.assert_called_once_with(delivery_tag=1)


def test_invalid_message_is_dead_lettered():
    channel = Mock()
    storage = Mock()
    make_callback("jardim", storage)(channel, SimpleNamespace(delivery_tag=2), None, b"bad json")
    channel.basic_nack.assert_called_once_with(delivery_tag=2, requeue=False)
    storage.process.assert_not_called()


def test_mismatched_routing_key_is_rejected():
    channel = Mock()
    storage = Mock()
    event = Event.create("chuva", True)
    make_callback("jardim", storage)(channel, SimpleNamespace(delivery_tag=3, routing_key="sensor.umidade"),
                                     None, event.to_bytes())
    storage.process.assert_not_called()
    channel.basic_nack.assert_called_once_with(delivery_tag=3, requeue=False)


def test_database_error_requeues_without_ack():
    channel, storage = Mock(), Mock()
    storage.process.side_effect = sqlite3.OperationalError("locked")
    event = Event.create("chuva", True)
    with patch("cidade_inteligente.consumer.time.sleep"):
        make_callback("jardim", storage)(channel, SimpleNamespace(delivery_tag=4, routing_key="sensor.chuva"),
                                         None, event.to_bytes())
    channel.basic_nack.assert_called_once_with(delivery_tag=4, requeue=True)
    channel.basic_ack.assert_not_called()


def test_publisher_confirms_persistent_event_without_changing_state(tmp_path):
    settings = Settings(db_path=tmp_path / "cidade.db")
    storage = Storage(settings.db_path)
    event = Event.create("limpador", "LIGA")
    connection = Mock(is_closed=False)
    with patch("cidade_inteligente.messaging.connect", return_value=connection):
        Publisher(settings).publish(event)
    channel = connection.channel.return_value
    channel.confirm_delivery.assert_called_once()
    kwargs = channel.basic_publish.call_args.kwargs
    assert kwargs["exchange"] == EXCHANGE
    assert kwargs["routing_key"] == "comando.limpador"
    assert kwargs["properties"].delivery_mode == 2
    assert kwargs["mandatory"] is True
    assert Event.from_bytes(kwargs["body"]) == event
    assert storage.snapshot()["limpador"]["ativo"] is False
    assert storage.history() == []


def test_reconnect_reuses_event_identifier():
    failed = Mock(is_closed=False)
    failed.channel.return_value.basic_publish.side_effect = pika.exceptions.StreamLostError("lost")
    good = Mock(is_closed=False)
    event = Event.create("limpador", "LIGA")
    with patch("cidade_inteligente.messaging.connect", side_effect=[failed, good]), \
            patch("cidade_inteligente.messaging.time.sleep"):
        Publisher(Settings()).publish(event)
    bodies = [c.channel.return_value.basic_publish.call_args.kwargs["body"] for c in (failed, good)]
    assert bodies == [event.to_bytes(), event.to_bytes()]
