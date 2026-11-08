from .events import DIRECTIONS, Event
from .messaging import Publisher
from .storage import Storage

MENU = """
=== CIDADE INTELIGENTE ===
1. Configurar umidade de referência do Jardim
2. Simular sensor de umidade
3. Simular detecção de chuva
4. Simular sensor de luminosidade
5. Simular emergência no semáforo
6. Encerrar emergência
7. Ligar limpador de ruas
8. Desligar limpador de ruas
9. Consultar estado atual da cidade
0. Sair
"""


def read_number(prompt):
    try:
        return float(input(prompt).strip().replace(",", "."))
    except ValueError as exc:
        raise ValueError("Informe um número válido.") from exc


def event_for_option(option):
    if option in ("1", "2", "4"):
        tipo, prompt = {
            "1": ("configurar_umidade", "Referência de umidade (0 a 100%): "),
            "2": ("umidade", "Umidade do solo (0 a 100%): "),
            "4": ("luminosidade", "Luminosidade (lux, >= 0): "),
        }[option]
        return Event.create(tipo, read_number(prompt))
    if option == "3":
        answer = input("Está chovendo? (sim/não): ").strip().casefold()
        if answer not in ("sim", "s", "não", "nao", "n"):
            raise ValueError("Responda sim ou não.")
        return Event.create("chuva", answer in ("sim", "s"))
    if option == "5":
        answer = input("Direção (Norte/Sul/Leste/Oeste ou N/S/L/O): ").strip().casefold()
        directions = {d.casefold(): d for d in DIRECTIONS}
        directions.update({d[0].casefold(): d for d in DIRECTIONS})
        if answer not in directions:
            raise ValueError("Informe Norte, Sul, Leste ou Oeste.")
        return Event.create("emergencia", directions[answer])
    if option == "6":
        return Event.create("encerrar_emergencia", None)
    if option in ("7", "8"):
        return Event.create("limpador", "LIGA" if option == "7" else "DESLIGA")
    raise ValueError("Opção inválida.")


def show_snapshot(storage):
    print("\n=== ESTADOS PROCESSADOS PELOS CONSUMIDORES ===")
    for consumer, state in storage.snapshot().items():
        print(f"\n{consumer.upper()}: {state['decisao']}")
        for key, value in state.items():
            if key != "decisao":
                print(f"  {key}: {value}")
    print("\nPublicação e processamento são assíncronos. Consulte novamente se houver mensagens pendentes.")


def run_menu(settings):
    storage = Storage(settings.db_path)
    publisher = Publisher(settings)
    try:
        while True:
            print(MENU)
            option = input("Escolha: ").strip()
            if option == "0":
                return
            if option == "9":
                show_snapshot(storage)
                continue
            try:
                event = event_for_option(option)
                publisher.publish(event)
                print(f"Evento publicado e confirmado pelo RabbitMQ: {event.evento_id}. "
                      "Aguardando processamento pelo consumidor.")
            except (ValueError, ConnectionError) as exc:
                print(f"Erro: {exc}")
    finally:
        publisher.close()
