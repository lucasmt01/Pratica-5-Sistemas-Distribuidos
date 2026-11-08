import argparse
import logging

from .config import Settings
from .events import CONSUMER_TYPES


def main():
    parser = argparse.ArgumentParser(description="Cidade inteligente com RabbitMQ Pub/Sub")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("menu", help="Inicia a interface publicadora")
    consumer = commands.add_parser("consumer", help="Inicia um consumidor independente")
    consumer.add_argument("name", choices=CONSUMER_TYPES)
    commands.add_parser("estado", help="Consulta os estados persistidos sem publicar eventos")
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
    try:
        settings = Settings.from_env()
        if args.command == "consumer":
            from .consumer import run_consumer
            run_consumer(args.name, settings)
        elif args.command == "menu":
            from .cli import run_menu
            run_menu(settings)
        else:
            from .cli import show_snapshot
            from .storage import Storage
            show_snapshot(Storage(settings.db_path))
    except (KeyboardInterrupt, EOFError):
        print("\nAplicação encerrada.")


if __name__ == "__main__":
    main()
