"""Integração isolada: usa vhost temporário, nunca as filas da demonstração."""
import os
import subprocess
import sys
import time
from dataclasses import replace
from uuid import uuid4

import pytest

from cidade_inteligente.config import Settings
from cidade_inteligente.events import CONSUMER_TYPES, Event, ROUTING_KEYS
from cidade_inteligente.messaging import DEAD_QUEUE, EXCHANGE, Publisher, close, connect, declare_topology
from cidade_inteligente.storage import Storage

pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    os.getenv("RUN_RABBITMQ_TESTS") != "1", reason="Defina RUN_RABBITMQ_TESTS=1 para testar o broker real.")]


def wait_for(predicate, description, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    pytest.fail(f"Timeout esperando {description}")


@pytest.fixture
def broker(tmp_path):
    # API de gerenciamento usada somente na preparação/limpeza do teste.
    import base64
    import urllib.parse
    import urllib.request

    base = Settings.from_env()
    vhost = "teste_" + uuid4().hex
    api = os.getenv("RABBITMQ_MANAGEMENT_URL", "http://localhost:15672/api")
    auth = base64.b64encode(f"{base.user}:{base.password}".encode()).decode()

    def request(method, path, body=None):
        req = urllib.request.Request(api + path, data=body, method=method,
                                     headers={"Authorization": "Basic " + auth, "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.read()

    request("PUT", "/vhosts/" + vhost, b"{}")
    try:
        request("PUT", f"/permissions/{vhost}/{urllib.parse.quote(base.user, safe='')}",
                b'{"configure":".*","write":".*","read":".*"}')
        yield replace(base, virtual_host=vhost, db_path=tmp_path / "cidade.db")
    finally:
        request("DELETE", "/vhosts/" + vhost)


def test_real_pubsub_with_four_independent_consumers(broker):
    storage = Storage(broker.db_path)
    publisher = Publisher(broker)
    processes = []
    connection = None
    environment = dict(os.environ, RABBITMQ_HOST=broker.host, RABBITMQ_PORT=str(broker.port),
                       RABBITMQ_USER=broker.user, RABBITMQ_PASSWORD=broker.password,
                       RABBITMQ_VHOST=broker.virtual_host, CIDADE_DB=str(broker.db_path), PYTHONUNBUFFERED="1")
    try:
        # Publicação com consumidores desligados: filas guardam eventos, estado não antecipa decisões.
        events = [Event.create(tipo, value) for tipo, value in [
            ("configurar_umidade", 60), ("umidade", 30), ("chuva", True),
            ("luminosidade", 99), ("emergencia", "Norte"), ("limpador", "LIGA")]]
        for event in events:
            publisher.publish(event)
        assert storage.history() == []

        # Segundo assinante independente recebe a mesma chuva: broadcast por bindings.
        connection = connect(broker)
        channel = connection.channel()
        channel.queue_declare(queue="teste.auditoria", durable=False, auto_delete=True)
        channel.queue_bind(queue="teste.auditoria", exchange=EXCHANGE, routing_key="sensor.#")
        extra = Event.create("chuva", True)
        publisher.publish(extra)
        method, _, body = channel.basic_get(queue="teste.auditoria", auto_ack=True)
        assert method is not None and Event.from_bytes(body) == extra

        for consumer in CONSUMER_TYPES:
            processes.append(subprocess.Popen(
                [sys.executable, "-m", "cidade_inteligente", "consumer", consumer], env=environment,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        wait_for(lambda: len(storage.history()) == 7, "processamento dos quatro consumidores")
        snapshot = storage.snapshot()
        assert snapshot["jardim"]["ativo"] is True
        assert snapshot["iluminacao"]["lampada_acesa"] is True
        assert snapshot["limpador"]["ativo"] is True
        assert snapshot["semaforo"]["sinais"] == {"Norte": "AVANÇAR", "Sul": "ESPERAR",
                                                   "Leste": "ESPERAR", "Oeste": "ESPERAR"}
        assert all(p.poll() is None for p in processes)

        for event in [Event.create("luminosidade", 100), Event.create("encerrar_emergencia", None),
                      Event.create("limpador", "DESLIGA")]:
            publisher.publish(event)
        wait_for(lambda: len(storage.history()) == 10, "encerramento das ações")
        assert storage.snapshot()["iluminacao"]["lampada_acesa"] is True
        assert storage.snapshot()["limpador"]["ativo"] is False
        assert storage.snapshot()["semaforo"]["emergencia"] is None

        # Reentrega com o mesmo identificador não repete ação nem altera histórico.
        publisher.publish(events[-1])
        marker = Event.create("limpador", "DESLIGA")
        publisher.publish(marker)
        wait_for(lambda: len(storage.history()) == 11, "deduplicação")
        assert storage.snapshot()["limpador"]["ativo"] is False

        channel.basic_publish(exchange=EXCHANGE, routing_key=ROUTING_KEYS["chuva"], body=b"not json")
        wait_for(lambda: channel.queue_declare(queue=DEAD_QUEUE, passive=True).method.message_count == 1,
                 "envio de mensagem inválida à fila de rejeitados")
        assert Storage(broker.db_path).snapshot() == storage.snapshot()
    finally:
        publisher.close()
        close(connection)
        for process in processes:
            process.terminate()
        for process in processes:
            process.wait(timeout=10)
