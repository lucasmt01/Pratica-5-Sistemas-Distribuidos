from unittest.mock import patch

import pytest

from cidade_inteligente.cli import event_for_option, run_menu
from cidade_inteligente.config import Settings
from cidade_inteligente.storage import Storage


@pytest.mark.parametrize("option,answer,tipo,valor", [
    ("1", "60,5", "configurar_umidade", 60.5), ("2", "30", "umidade", 30),
    ("3", "NÃO", "chuva", False), ("3", "sim", "chuva", True),
    ("4", "99", "luminosidade", 99), ("5", "n", "emergencia", "Norte"),
])
def test_menu_builds_valid_events(option, answer, tipo, valor):
    with patch("builtins.input", return_value=answer):
        event = event_for_option(option)
    assert (event.tipo, event.valor) == (tipo, valor)


def test_menu_consultation_uses_persisted_state(tmp_path, capsys):
    settings = Settings(db_path=tmp_path / "cidade.db")
    with patch("builtins.input", side_effect=["7", "9", "0"]), \
            patch("cidade_inteligente.cli.Publisher") as publisher:
        run_menu(settings)
    publisher.return_value.publish.assert_called_once()
    assert Storage(settings.db_path).snapshot()["limpador"]["ativo"] is False
    assert "Limpeza interrompida" in capsys.readouterr().out


@pytest.mark.parametrize("option,answer", [("3", "talvez"), ("5", "cima"), ("2", "NaN"), ("4", "-1")])
def test_bad_input_is_rejected(option, answer):
    with patch("builtins.input", return_value=answer), pytest.raises(ValueError):
        event_for_option(option)
