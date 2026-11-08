from .events import DIRECTIONS


def normal_traffic():
    return {direction: "AVANÇAR" if direction in ("Norte", "Sul") else "ESPERAR"
            for direction in DIRECTIONS}


def initial_state(consumer):
    return {
        "jardim": {"referencia": 50.0, "umidade": None, "chovendo": False,
                   "ativo": False, "decisao": "Aguardando leitura de umidade."},
        "semaforo": {"emergencia": None, "sinais": normal_traffic(),
                     "decisao": "Operação normal: Norte/Sul liberados."},
        "iluminacao": {"lux": None, "lampada_acesa": False,
                       "decisao": "Aguardando leitura de luminosidade."},
        "limpador": {"ativo": False, "decisao": "Limpeza interrompida."},
    }[consumer]


def transition(consumer, previous, event):
    """Regra de decisão executada exclusivamente pelo consumidor responsável."""
    state = dict(previous)
    value = event.valor
    if consumer == "jardim":
        field = {"configurar_umidade": "referencia", "umidade": "umidade", "chuva": "chovendo"}[event.tipo]
        state[field] = value
        state["ativo"] = bool(state["chovendo"] and state["umidade"] is not None
                              and state["umidade"] < state["referencia"])
        values = (f"Chuva: {'sim' if state['chovendo'] else 'não'}; "
                  f"umidade: {state['umidade']}%; referência: {state['referencia']}%.")
        if state["umidade"] is None:
            decision = "Aguardando leitura de umidade; nenhum ajuste."
        elif state["ativo"]:
            decision = "Jardim ativado: liberando água da chuva para o solo."
        else:
            decision = "Não é necessário realizar ajuste."
        state["decisao"] = f"{values} {decision}"
    elif consumer == "semaforo":
        state["emergencia"] = value if event.tipo == "emergencia" else None
        state["sinais"] = ({d: "AVANÇAR" if d == value else "ESPERAR" for d in DIRECTIONS}
                           if state["emergencia"] else normal_traffic())
        state["decisao"] = (f"Emergência: somente {value} liberado." if state["emergencia"]
                            else "Emergência encerrada. Operação normal: Norte/Sul liberados.")
    elif consumer == "iluminacao":
        state["lux"] = value
        if value < 100:
            state["lampada_acesa"] = True
            state["decisao"] = f"Luminosidade: {value} lux < 100 lux. Lâmpada ativada."
        else:
            state["decisao"] = f"Luminosidade: {value} lux >= 100 lux. Nenhum ajuste; estado mantido."
    elif consumer == "limpador":
        state["ativo"] = value == "LIGA"
        state["decisao"] = "Limpeza simulada iniciada." if state["ativo"] else "Limpeza simulada interrompida."
    state["ultimo_evento"] = event.evento_id
    state["timestamp_evento"] = event.timestamp
    return state
