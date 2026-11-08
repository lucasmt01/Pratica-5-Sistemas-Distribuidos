# Cidade Inteligente utilizando RabbitMQ

Aplicação acadêmica de Sistemas Distribuídos em Python: sensores simulados pelo
usuário publicam leituras e comandos em um broker RabbitMQ. Quatro processos
consumidores independentes controlam jardim, semáforos, iluminação e limpeza de ruas.
Toda comunicação de sensores, configuração e comandos com esses objetos usa
**Publish/Subscribe via um exchange `topic`**. A consulta do menu lê exclusivamente
o estado persistido pelos consumidores em SQLite.

## Tecnologias

- Python 3.10 ou superior (Docker usa Python 3.12).
- RabbitMQ com painel de gerenciamento, inicializado por Docker Compose.
- `pika` para AMQP; `sqlite3` da biblioteca padrão para persistência.
- `pytest` para testes de regras, transporte e integração com broker real.

## Arquitetura Pub/Sub

```text
Menu / sensores simulados (publisher)
          | JSON, routing key, confirmação de publicação
          v
    exchange cidade.eventos (topic, durável)
          |
          +-- bindings de jardim ----> cidade.jardim ----> consumer jardim
          +-- bindings de trânsito --> cidade.semaforo --> consumer semaforo
          +-- binding de lux --------> cidade.iluminacao -> consumer iluminacao
          +-- binding de comando ----> cidade.limpador ---> consumer limpador
                                                               |
                  SQLite <--- estado + histórico <--------------+
                    |
                    +---- leitura pela opção 9 do menu
```

| Tipo de evento | Routing key / binding | Fila | Consumidor |
| --- | --- | --- | --- |
| `configurar_umidade` | `jardim.configuracao.umidade` | `cidade.jardim` | Jardim |
| `umidade` | `sensor.umidade` | `cidade.jardim` | Jardim |
| `chuva` | `sensor.chuva` | `cidade.jardim` | Jardim |
| `luminosidade` | `sensor.luminosidade` | `cidade.iluminacao` | Iluminação |
| `emergencia` | `transito.emergencia.iniciar` | `cidade.semaforo` | Semáforo |
| `encerrar_emergencia` | `transito.emergencia.encerrar` | `cidade.semaforo` | Semáforo |
| `limpador` | `comando.limpador` | `cidade.limpador` | Limpador |

O menu publica no exchange, sem endereçar consumidores e sem executar suas regras.
Cada fila recebe as publicações que correspondem aos seus bindings. Bindings exatos
são válidos em exchanges `topic`; um novo assinante pode usar `sensor.#` para receber
todos os sensores, sem alterar o publicador. Assinantes que precisam receber cópias
independentes devem ter **filas diferentes**. Dois processos na mesma fila não recebem
ambos cada evento: neste projeto, `x-single-active-consumer` mantém um ativo e outro
em espera, preservando a sequência de decisões por objeto.

Exemplo do corpo JSON (identificador UUID e instante em UTC):

```json
{
  "tipo": "umidade",
  "valor": 30.0,
  "timestamp": "2026-11-07T15:00:00+00:00",
  "evento_id": "85533932-ac0a-46c2-bdca-0f49ea115491"
}
```

Filas e exchanges são duráveis, mensagens são persistentes (`delivery_mode=2`) e o
publicador usa publisher confirms e `mandatory=True`. A topologia é criada também
pelo publicador: eventos podem esperar mesmo quando os consumidores estão desligados.
O ACK só ocorre após salvar estado e evento na mesma transação SQLite. Reentregas são
deduplicadas pelo UUID, inclusive após reinício. Isso implementa entrega pelo menos
uma vez com processamento idempotente dos estados locais; não promete entrega
exatamente uma vez do RabbitMQ.

Mensagens inválidas recebem NACK sem requeue e seguem ao exchange de rejeitados
`cidade.invalidos` e à fila `cidade.mensagens_invalidas`. Falhas SQLite causam
reentrega; falhas de conexão fazem o consumidor tentar novamente a cada 3 segundos.
O publicador tenta 3 vezes reutilizando o mesmo UUID. Se todas falharem, informa que
a publicação **não foi confirmada**: uma confirmação perdida pode corresponder a um
evento já aceito. O menu não tem outbox local para envios enquanto o broker está fora.

## Regras e interpretações do enunciado

- **Jardim:** umidade percentual entre 0 e 100, referência inicial de 50%. A opção 1
  publica a nova referência, que só passa a valer quando o consumidor a processa.
  Com chuva e umidade estritamente abaixo da referência, libera água da chuva.
  Fora dessa condição informa que nenhum ajuste é necessário. Não simula irrigação
  alternativa. Sem leitura de umidade, aguarda a primeira leitura. Cada evento de
  chuva, umidade ou referência reavalia a condição usando os valores mais recentes
  já processados; não altera artificialmente a umidade depois de liberar água.
- **Semáforo:** um cruzamento simples com Norte/Sul verdes e Leste/Oeste vermelhos
  na operação normal. Norte e Sul são fluxos opostos compatíveis, sem simulação de
  conversões. Não há ciclo automático. Na emergência, somente a direção solicitada
  recebe `AVANÇAR`; todas as outras recebem `ESPERAR`. Uma nova emergência substitui
  a anterior. Encerrar restaura o estado normal. Todos os sinais são exibidos juntos.
- **Iluminação:** abaixo de 100 lux ativa a lâmpada. Em 100 lux ou mais, **não realiza
  ajuste e mantém o estado anterior**, inclusive se já estiver acesa. Não inventa um
  desligamento automático. Lux aceita números finitos não negativos; limite fixo.
- **Limpador:** `LIGA` inicia e `DESLIGA` interrompe a limpeza simulada.
- A opção 9 mostra estados efetivamente processados, o UUID e instante do último
  evento por objeto. Antes do primeiro evento, exibe os estados iniciais. Como o
  fluxo é assíncrono, uma consulta imediata pode anteceder o processamento; consulte
  novamente e acompanhe o terminal do consumidor.
- SQLite é armazenamento local compartilhado entre os processos nesta máquina,
  não um canal de comandos entre eles. Todos devem usar o mesmo caminho de banco.
  Decisões são ordenadas por fila/processamento, sem ordenação global entre objetos.

## Estrutura dos arquivos

```text
cidade_inteligente/
  __main__.py      # comandos menu, consumer e estado
  cli.py           # entradas do usuário e consulta dos estados
  config.py        # configuração por variáveis de ambiente
  events.py        # contrato JSON, validação e routing keys
  messaging.py     # conexão, topologia e publicador RabbitMQ
  consumer.py      # consumo, ACK/NACK, reconexão e saída no terminal
  domain.py        # regras de decisão executadas pelos consumidores
  storage.py       # SQLite: estados, histórico e deduplicação
scripts/
  iniciar_consumidores.ps1
tests/
  test_rules.py
  test_events.py
  test_messaging.py
  test_cli.py
  test_integration.py
docker-compose.yml
Dockerfile
requirements.txt
pytest.ini
README.md
```

## Execução no Windows: Docker Desktop + Python

Instale Python 3.10+ com o launcher `py` e Docker Desktop com containers Linux.
Abra o Docker Desktop e espere o mecanismo iniciar. Execute os comandos PowerShell
na pasta deste projeto.

### 1. Criar o ambiente e instalar dependências

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Não é necessário ativar a venv nem alterar a política de execução para usar Python.
Se `py` não encontrar Python, use o caminho completo do `python.exe` instalado no
primeiro comando. Neste computador, por exemplo:

```powershell
& "$env:LOCALAPPDATA\Programs\Python\Python313\python.exe" -m venv .venv
```

### 2. Iniciar RabbitMQ

```powershell
docker compose up -d rabbitmq
docker compose ps
```

Aguarde o status `healthy`. Painel: [http://localhost:15672](http://localhost:15672).
Usuário **cidade**, senha **cidade123**. AMQP em `localhost:5672`. São credenciais
locais para a atividade e as portas são publicadas somente em loopback.

### 3. Iniciar os quatro consumidores

Abra quatro terminais PowerShell na pasta do projeto e execute **um comando por terminal**:

```powershell
.\.venv\Scripts\python.exe -m cidade_inteligente consumer jardim
```

```powershell
.\.venv\Scripts\python.exe -m cidade_inteligente consumer semaforo
```

```powershell
.\.venv\Scripts\python.exe -m cidade_inteligente consumer iluminacao
```

```powershell
.\.venv\Scripts\python.exe -m cidade_inteligente consumer limpador
```

Cada terminal mostra as decisões do respectivo objeto. Para encerrar, use Ctrl+C.
Alternativamente, o script abaixo inicia os quatro processos em segundo plano,
grava saída em `data/logs/` e exibe os PIDs para encerramento com `Stop-Process`:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\iniciar_consumidores.ps1
Get-Content .\data\logs\jardim.log -Wait -Encoding UTF8
```

Não inicie o script e os terminais ao mesmo tempo: uma segunda instância fica em
espera na fila. O script herda as variáveis de ambiente definidas no terminal pai.

### 4. Abrir o menu em outro terminal

```powershell
.\.venv\Scripts\python.exe -m cidade_inteligente menu
```

Use as opções 1 a 8 para publicar e a opção 9 para consultar o estado. Também existe:

```powershell
.\.venv\Scripts\python.exe -m cidade_inteligente estado
```

### 5. Encerrar e retomar

Saia do menu com `0`, encerre consumidores com Ctrl+C (ou seus PIDs) e execute:

```powershell
docker compose stop rabbitmq
```

Para retomar, repita os passos 2 a 4. O banco em `data/cidade.db` guarda configurações,
últimas leituras, estados e histórico completo de eventos processados; o volume
`rabbitmq_data` guarda a configuração do broker e as mensagens pendentes.
`docker compose down` também preserva volumes. A opção `-v` remove os volumes e seus
dados; use somente quando quiser apagar a simulação Docker.

## Alternativa: executar também os processos Python no Docker

Não exige Python local. Os consumidores compartilham um volume SQLite; o menu usa
o mesmo volume. Execute:

```powershell
docker compose --profile apps up -d --build jardim semaforo iluminacao limpador
docker compose --profile apps run --rm menu
```

Para acompanhar decisões e consultar os estados:

```powershell
docker compose logs -f jardim semaforo iluminacao limpador
docker compose --profile apps run --rm menu python -m cidade_inteligente estado
```

Para parar os serviços: `docker compose --profile apps down`. No modo Docker, SQLite
fica no volume `cidade_data`, separado do `data/cidade.db` do modo Python local.
Escolha um modo por demonstração para que a consulta reflita os consumidores usados.

## Configuração opcional

| Variável | Padrão local | Finalidade |
| --- | --- | --- |
| `RABBITMQ_HOST` | `localhost` | Endereço do broker |
| `RABBITMQ_PORT` | `5672` | Porta AMQP |
| `RABBITMQ_USER` | `cidade` | Usuário |
| `RABBITMQ_PASSWORD` | `cidade123` | Senha |
| `RABBITMQ_VHOST` | `/` | Virtual host |
| `CIDADE_DB` | `data/cidade.db` no projeto | Caminho SQLite |

Exemplo PowerShell: `$env:CIDADE_DB = 'C:\caminho\cidade.db'`. Repita a configuração
nos terminais dos consumidores e do menu. Não há carregamento automático de `.env`
pelo Python. O Compose já fornece as variáveis aos containers.

## Exemplos e roteiro para apresentar a atividade

| Etapa no menu | Resultado esperado no consumidor |
| --- | --- |
| 1 → `60`; 2 → `30`; 3 → `sim` | Jardim ativado; informa chuva, umidade e referência |
| 3 → `não` | Jardim não realiza ajuste |
| 3 → `sim`; 2 → `60` ou `80` | Umidade igual/acima da referência: nenhum ajuste |
| 4 → `99` | Lâmpada ativada |
| 4 → `100` e depois `150` | Nenhum ajuste; lâmpada continua acesa |
| 5 → `Norte` | Só Norte AVANÇAR; Sul, Leste e Oeste ESPERAR |
| 6 | Retorna a Norte/Sul AVANÇAR, Leste/Oeste ESPERAR |
| 7 | Limpador inicia a limpeza |
| 8 | Limpador interrompe a limpeza |
| 9 | Mostra os estados que os consumidores realmente processaram |
| Reiniciar os processos e usar 9 | Referência, leituras e estados preservados |

Para mostrar o desacoplamento, pare o consumidor do limpador e publique a opção 7.
A opção 9 deve manter o último estado processado. A fila `cidade.limpador` acumula
uma mensagem Ready; ao reiniciar o consumidor, ele processa a mensagem e atualiza o
estado. O broker não precisa que o assinante esteja online na hora da publicação.

## Como inspecionar o painel do RabbitMQ

1. Entre em [localhost:15672](http://localhost:15672) com `cidade` / `cidade123`.
2. Após iniciar ao menos um consumidor ou publicar um evento, em **Exchanges**, abra
   `cidade.eventos` e veja o tipo `topic` e os bindings.
3. Em **Queues and Streams**, abra as quatro filas. Verifique Consumers, Ready
   (aguardando consumo), Unacked (entregues sem confirmação) e taxas de publicação/ACK.
4. Com consumidores ativos é normal Ready e Unacked voltarem rapidamente a zero.
   Pare um consumidor para observar o acúmulo. **Get messages** pode retirar/reentregar
   mensagens; para apenas observar, prefira contadores e bindings.
5. Para demonstrar um segundo assinante, crie uma fila `cidade.auditoria` e adicione
   um binding de `cidade.eventos` com routing key `sensor.#`. Publique um sensor:
   o consumidor original processa sua cópia e a auditoria mantém outra. Remova a
   fila de auditoria ao terminar. Esta etapa é opcional e também é coberta pelo teste
   de integração em um vhost temporário.
6. `cidade.mensagens_invalidas` recebe JSON inválido e eventos fora do contrato.

## Testes automatizados

Validação realizada em 07/11/2026:

- Windows com Python 3.13: **67 testes passaram**, incluindo integração com RabbitMQ
  real e quatro processos consumidores independentes.
- Imagem Docker com Python 3.12/Linux: **66 testes passaram**, sem a integração
  (executada separadamente no Windows).
- Imagens construídas, quatro serviços consumidores iniciados e consulta ao SQLite
  compartilhado validada pelo Docker Compose; `pip check` sem dependências quebradas.

Testes sem RabbitMQ (regras, persistência, validação, contrato de publicação,
deduplicação, ACK/NACK, reconexão e menu):

```powershell
.\.venv\Scripts\python.exe -m pytest -q -m "not integration"
```

Teste de integração com RabbitMQ iniciado pelo Compose:

```powershell
$env:RUN_RABBITMQ_TESTS = '1'
.\.venv\Scripts\python.exe -m pytest -q
Remove-Item Env:\RUN_RABBITMQ_TESTS
```

O teste cria e remove um vhost temporário pela API de gerenciamento, sem tocar nas
filas da demonstração. Requer usuário com permissão para gerenciar vhosts (o usuário
inicial do Compose possui essa permissão). A API usa por padrão
`http://localhost:15672/api`; pode ser alterada com `RABBITMQ_MANAGEMENT_URL`.
O teste publica com consumidores desligados, inicia os quatro consumidores como
processos reais e verifica estados, fila adicional de assinatura, deduplicação e
mensagem inválida encaminhada à fila de rejeitados.

Também é possível testar sem Python local, dentro da imagem:

```powershell
docker compose --profile apps run --rm menu python -m pytest -q -m "not integration"
```

## Problemas comuns

- **Conexão recusada:** confirme Docker Desktop ativo, `docker compose ps` saudável
  e porta 5672 livre. O consumidor tenta reconectar automaticamente.
- **Estado não muda:** confira o consumidor responsável, o caminho `CIDADE_DB`,
  as credenciais/vhost e as mensagens Ready no painel. A publicação confirmada não
  significa que o consumidor já concluiu o evento.
- **Senha recusada após editar Compose:** credenciais iniciais só são criadas ao
  inicializar um volume RabbitMQ novo. Use as credenciais já existentes no volume.
- **Caracteres no PowerShell:** use um terminal UTF-8. Se necessário defina
  `$env:PYTHONIOENCODING = 'utf-8'` antes de iniciar os processos.

O projeto é uma simulação local para apresentação acadêmica; não possui frontend
web, serviços HTTP de aplicação ou integração com dispositivos físicos.
