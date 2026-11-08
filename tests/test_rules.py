import pytest

from cidade_inteligente.domain import normal_traffic
from cidade_inteligente.events import DIRECTIONS, Event
from cidade_inteligente.storage import Storage


@pytest.fixture
def storage(tmp_path):
    return Storage(tmp_path / "cidade.db")


def send(storage, consumer, tipo, valor):
    return storage.process(consumer, Event.create(tipo, valor))


def test_rain_and_dry_soil_activate_garden(storage):
    send(storage, "jardim", "configurar_umidade", 60)
    send(storage, "jardim", "umidade", 30)
    state = send(storage, "jardim", "chuva", True)
    assert state["ativo"] is True
    assert "água da chuva" in state["decisao"]
    assert "30%" in state["decisao"] and "60%" in state["decisao"]


def test_no_rain_means_no_adjustment(storage):
    send(storage, "jardim", "umidade", 20)
    state = send(storage, "jardim", "chuva", False)
    assert state["ativo"] is False
    assert "Não é necessário realizar ajuste" in state["decisao"]


@pytest.mark.parametrize("moisture", [50, 80])
def test_at_or_above_reference_means_no_adjustment(storage, moisture):
    send(storage, "jardim", "chuva", True)
    state = send(storage, "jardim", "umidade", moisture)
    assert state["ativo"] is False


def test_reference_change_is_processed_as_event(storage):
    send(storage, "jardim", "chuva", True)
    send(storage, "jardim", "umidade", 55)
    state = send(storage, "jardim", "configurar_umidade", 60)
    assert state["ativo"] is True
    state = send(storage, "jardim", "chuva", False)
    assert state["ativo"] is False


def test_rain_without_reading_does_not_activate(storage):
    state = send(storage, "jardim", "chuva", True)
    assert state["ativo"] is False
    assert "Aguardando leitura" in state["decisao"]


def test_99_lux_activates_lamp(storage):
    state = send(storage, "iluminacao", "luminosidade", 99)
    assert state["lampada_acesa"] is True


@pytest.mark.parametrize("lux", [100, 101, 500])
@pytest.mark.parametrize("previously_on", [False, True])
def test_bright_light_keeps_previous_state(storage, lux, previously_on):
    if previously_on:
        send(storage, "iluminacao", "luminosidade", 99)
    state = send(storage, "iluminacao", "luminosidade", lux)
    assert state["lampada_acesa"] is previously_on
    assert state["lux"] == lux
    assert "Nenhum ajuste" in state["decisao"]


@pytest.mark.parametrize("direction", DIRECTIONS)
def test_emergency_has_exactly_one_green(storage, direction):
    state = send(storage, "semaforo", "emergencia", direction)
    assert state["sinais"][direction] == "AVANÇAR"
    assert all(s == "ESPERAR" for d, s in state["sinais"].items() if d != direction)
    assert list(state["sinais"].values()).count("AVANÇAR") == 1


def test_emergency_end_restores_normal_operation(storage):
    send(storage, "semaforo", "emergencia", "Leste")
    state = send(storage, "semaforo", "encerrar_emergencia", None)
    assert state["emergencia"] is None
    assert state["sinais"] == normal_traffic()


def test_new_emergency_replaces_old_direction(storage):
    send(storage, "semaforo", "emergencia", "Norte")
    state = send(storage, "semaforo", "emergencia", "Oeste")
    assert state["sinais"]["Norte"] == "ESPERAR"
    assert state["sinais"]["Oeste"] == "AVANÇAR"


def test_cleaner_on_and_off(storage):
    assert send(storage, "limpador", "limpador", "LIGA")["ativo"] is True
    assert send(storage, "limpador", "limpador", "DESLIGA")["ativo"] is False


def test_all_readings_and_configuration_survive_restart(storage):
    send(storage, "jardim", "configurar_umidade", 75)
    send(storage, "jardim", "umidade", 40)
    send(storage, "jardim", "chuva", True)
    send(storage, "iluminacao", "luminosidade", 99)
    send(storage, "semaforo", "emergencia", "Sul")
    send(storage, "limpador", "limpador", "LIGA")
    reopened = Storage(storage.path)
    assert reopened.snapshot() == storage.snapshot()
    assert reopened.snapshot()["jardim"]["referencia"] == 75
    assert reopened.snapshot()["jardim"]["umidade"] == 40
    assert reopened.snapshot()["iluminacao"]["lux"] == 99
    assert len(reopened.history()) == 6


def test_duplicate_delivery_does_not_override_newer_state(storage):
    old = Event.create("limpador", "LIGA")
    storage.process("limpador", old)
    send(storage, "limpador", "limpador", "DESLIGA")
    assert Storage(storage.path).process("limpador", old) is None
    assert storage.snapshot()["limpador"]["ativo"] is False
    assert len(storage.history()) == 2


def test_consumer_rejects_event_from_other_object(storage):
    with pytest.raises(ValueError):
        send(storage, "jardim", "limpador", "LIGA")
    assert storage.history() == []


def test_failed_state_write_rolls_back_event_history(storage):
    import sqlite3

    with storage.connect() as db:
        db.execute("""CREATE TRIGGER falhar_estado BEFORE INSERT ON estados
                      BEGIN SELECT RAISE(ABORT, 'falha simulada'); END""")
    with pytest.raises(sqlite3.IntegrityError):
        send(storage, "limpador", "limpador", "LIGA")
    assert storage.history() == []
    assert storage.snapshot()["limpador"]["ativo"] is False
