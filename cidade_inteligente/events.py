import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from uuid import UUID, uuid4


ROUTING_KEYS = {
    "configurar_umidade": "jardim.configuracao.umidade",
    "umidade": "sensor.umidade",
    "chuva": "sensor.chuva",
    "luminosidade": "sensor.luminosidade",
    "emergencia": "transito.emergencia.iniciar",
    "encerrar_emergencia": "transito.emergencia.encerrar",
    "limpador": "comando.limpador",
}
CONSUMER_TYPES = {
    "jardim": ("configurar_umidade", "umidade", "chuva"),
    "semaforo": ("emergencia", "encerrar_emergencia"),
    "iluminacao": ("luminosidade",),
    "limpador": ("limpador",),
}
DIRECTIONS = ("Norte", "Sul", "Leste", "Oeste")


@dataclass(frozen=True)
class Event:
    tipo: str
    valor: object
    timestamp: str
    evento_id: str

    def validate(self):
        if not isinstance(self.tipo, str) or self.tipo not in ROUTING_KEYS:
            raise ValueError("Tipo de evento desconhecido.")
        if not isinstance(self.evento_id, str):
            raise ValueError("Identificador inválido.")
        try:
            UUID(self.evento_id)
            instant = datetime.fromisoformat(self.timestamp)
            if instant.utcoffset() is None:
                raise ValueError("Timestamp deve informar o fuso horário.")
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError("Identificador ou timestamp inválido.") from exc
        if self.tipo in ("umidade", "configurar_umidade", "luminosidade"):
            try:
                finite_number = type(self.valor) in (int, float) and math.isfinite(self.valor)
            except OverflowError:
                finite_number = False
            if not finite_number:
                raise ValueError("O valor deve ser um número finito.")
            if self.valor < 0 or (self.tipo != "luminosidade" and self.valor > 100):
                raise ValueError("Umidade deve estar entre 0 e 100; lux deve ser não negativo.")
        elif self.tipo == "chuva" and type(self.valor) is not bool:
            raise ValueError("Chuva deve ser um booleano.")
        elif self.tipo == "emergencia" and self.valor not in DIRECTIONS:
            raise ValueError("Direção inválida.")
        elif self.tipo == "encerrar_emergencia" and self.valor is not None:
            raise ValueError("Encerramento deve ter valor null.")
        elif self.tipo == "limpador" and self.valor not in ("LIGA", "DESLIGA"):
            raise ValueError("Comando deve ser LIGA ou DESLIGA.")
        return self

    @classmethod
    def create(cls, tipo, valor):
        return cls(tipo, valor, datetime.now(timezone.utc).isoformat(), str(uuid4())).validate()

    def to_bytes(self):
        self.validate()
        return json.dumps(asdict(self), ensure_ascii=False, allow_nan=False).encode("utf-8")

    @classmethod
    def from_bytes(cls, body):
        if len(body) > 65536:
            raise ValueError("Mensagem excede 64 KiB.")
        try:
            data = json.loads(body)
            if not isinstance(data, dict) or set(data) != {"tipo", "valor", "timestamp", "evento_id"}:
                raise ValueError("Formato da mensagem inválido.")
            return cls(**data).validate()
        except (TypeError, UnicodeError, OverflowError) as exc:
            raise ValueError("Mensagem JSON inválida.") from exc
