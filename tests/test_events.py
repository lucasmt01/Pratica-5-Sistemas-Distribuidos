import json

import pytest

from cidade_inteligente.events import Event


@pytest.mark.parametrize("tipo,valor", [
    ("umidade", -1), ("umidade", 101), ("umidade", True),
    ("configurar_umidade", 101), ("luminosidade", -1),
    ("luminosidade", float("nan")), ("luminosidade", float("inf")),
    ("chuva", "sim"), ("emergencia", "Cima"), ("limpador", "ON"),
    ("encerrar_emergencia", "Norte"), ("desconhecido", 0),
])
def test_invalid_values(tipo, valor):
    with pytest.raises(ValueError):
        Event.create(tipo, valor)


def test_json_roundtrip():
    event = Event.create("chuva", True)
    assert Event.from_bytes(event.to_bytes()) == event


@pytest.mark.parametrize("body", [b"broken", b"[]", b"{}", b"null", b"\xff", b"x" * 65537],
                         ids=["invalid-json", "array", "empty-object", "null", "invalid-utf8", "oversized"])
def test_malformed_messages(body):
    with pytest.raises(ValueError):
        Event.from_bytes(body)


@pytest.mark.parametrize("field,value", [("evento_id", "invalid"), ("timestamp", "yesterday"),
                                         ("timestamp", "2026-11-07T12:00:00"), ("tipo", [])])
def test_invalid_envelope(field, value):
    data = json.loads(Event.create("chuva", True).to_bytes())
    data[field] = value
    with pytest.raises(ValueError):
        Event.from_bytes(json.dumps(data).encode())


def test_number_too_large_is_rejected():
    with pytest.raises(ValueError):
        Event.create("luminosidade", 10 ** 400)
