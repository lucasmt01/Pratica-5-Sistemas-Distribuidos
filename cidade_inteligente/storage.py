import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .domain import initial_state, transition
from .events import CONSUMER_TYPES


class Storage:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS estados (
                    consumidor TEXT PRIMARY KEY, estado TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS eventos (
                    evento_id TEXT PRIMARY KEY, consumidor TEXT NOT NULL,
                    tipo TEXT NOT NULL, valor TEXT NOT NULL,
                    timestamp TEXT NOT NULL, processado_em TEXT NOT NULL
                );
            """)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=15)
        try:
            with db:
                yield db
        finally:
            db.close()

    def process(self, consumer, event):
        event.validate()
        if event.tipo not in CONSUMER_TYPES[consumer]:
            raise ValueError("Evento destinado a outro consumidor.")
        with self.connect() as db:
            # Estado, histórico e deduplicação pertencem à mesma transação.
            db.execute("BEGIN IMMEDIATE")
            if db.execute("SELECT 1 FROM eventos WHERE evento_id = ?", (event.evento_id,)).fetchone():
                return None
            row = db.execute("SELECT estado FROM estados WHERE consumidor = ?", (consumer,)).fetchone()
            previous = json.loads(row[0]) if row else initial_state(consumer)
            state = transition(consumer, previous, event)
            db.execute("INSERT INTO eventos VALUES (?, ?, ?, ?, ?, ?)",
                       (event.evento_id, consumer, event.tipo, json.dumps(event.valor), event.timestamp,
                        datetime.now(timezone.utc).isoformat()))
            db.execute("INSERT INTO estados VALUES (?, ?) ON CONFLICT(consumidor) DO UPDATE SET estado=excluded.estado",
                       (consumer, json.dumps(state, ensure_ascii=False)))
            return state

    def snapshot(self):
        with self.connect() as db:
            rows = db.execute("SELECT consumidor, estado FROM estados").fetchall()
        states = {name: initial_state(name) for name in CONSUMER_TYPES}
        states.update({name: json.loads(state) for name, state in rows})
        return states

    def history(self):
        with self.connect() as db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute("SELECT * FROM eventos ORDER BY rowid")]
