# Plataforma de Monitoramento Ambiental Distribuída (TCC)

> **Nota para Assistentes de IA / Desenvolvimento:** Este documento define a arquitetura oficial, os limites de responsabilidade de cada serviço e as regras de abstração do projeto. **Não acople a lógica de negócio a nenhum broker específico.** Toda comunicação entre microsserviços deve passar por uma camada de abstração/interface (_Adapter Pattern_) que permita alternar entre **Redis Streams**, **RabbitMQ** e **Apache Kafka** via configuração (`EVENT_BUS_DRIVER`).

---

## 1. Objetivo do Projeto

Sistema distribuído orientado a microsserviços e dirigido a eventos (_Event-Driven Architecture_) para ingestão, processamento, armazenamento e atuação sobre dados de sensores ambientais em tempo real.

O propósito experimental deste repositório (TCC — Bacharelado em Sistemas de Informação, UFBA) é realizar um **benchmark comparativo controlado** entre três tecnologias de barramento de eventos:

1. **Redis Streams** (`EVENT_BUS_DRIVER=redis`)
2. **RabbitMQ** (`EVENT_BUS_DRIVER=rabbitmq`)
3. **Apache Kafka** (`EVENT_BUS_DRIVER=kafka`)

---

## 2. Topologia e Fluxo de Dados

```text
[Sensores IoT] ──(MQTT)──► [Ingest Service] ──► [[ Barramento de Eventos ]]
                                                          │
          ┌───────────────────────────────────────────────┼───────────────────────────────────────────────┐
          ▼                                               ▼                                               ▼
[Persistence Service]                         [Notification Service]                            [Command Service]
   │             │                                        │                                               │
   ▼             ▼                                        └──────────(Evento de Comando)──────────────────┘
[InfluxDB]  [PostgreSQL]                                                                                  │
   │                                                                                                      ▼
   ▼                                                                                          (MQTT) ──► [Atuadores]
[Grafana]
```

### Regras Arquiteturais Invioláveis

- **Isolamento de Broker:** Os serviços (`ingest`, `persistence`, `notification`, `command`) não importam bibliotecas clientes (`ioredis`, `amqplib`, `kafkajs`) diretamente nas regras de negócio. Consomem as interfaces comuns `EventBusPublisher` e `EventBusConsumer` de `@tcc/event-bus`.
- **Comunicação Externa vs. Interna:** O protocolo **MQTT** é usado _exclusivamente_ na borda (comunicação com sensores e atuadores via Mosquitto). Toda comunicação inter-serviços ocorre _exclusivamente_ pelo barramento de eventos.
- **Persistência Poliglota:** Dados de telemetria (séries temporais de alta frequência) vão para o **InfluxDB v2**. Dados cadastrais, metadados de sensores, regras de alerta e logs de comandos vão para o **PostgreSQL**.
- **Stack dos Microsserviços:** Todos os microsserviços são desenvolvidos em **NestJS (TypeScript)**.

---

## 3. Responsabilidades dos Microsserviços

### 3.1. `ingest-service` (Implementado)
- **Entrada:** Assina tópicos MQTT de telemetria (`sensors/+/telemetry`, QoS 1).
- **Processamento:**
  - Valida o schema da mensagem (`TelemetryPayloadDto`).
  - Realiza **deduplicação** em memória com TTL (`sensor_id` + `message_id` ou `sensor_id` + `timestamp`).
  - Injeta `ingested_at` em UTC com precisão de microssegundos para cálculo de latência no benchmark.
- **Saída:** Publica o evento normalizado no tópico/stream `telemetry.raw` via `EventBusPublisher`.
- **Healthcheck:** `GET http://localhost:3001/health`.

### 3.2. `persistence-service` (Implementado)
- **Entrada:** Consome eventos de `telemetry.raw` via `EventBusConsumer` (grupo `persistence-service`).
- **Processamento:**
  - Agrupa mensagens em lotes configuráveis (`BATCH_SIZE`, `BATCH_FLUSH_INTERVAL_MS`).
  - Registra `persisted_at` em UTC com precisão de microssegundos e calcula `e2e_latency_ms`.
  - Envia `ACK` manual ao broker **somente após** confirmar a gravação nos dois bancos (ou `NACK` em caso de erro).
- **Saída:**
  - Grava a measurement `telemetry` no **InfluxDB v2**.
  - Atualiza o estado mais recente do sensor na tabela `sensors` do **PostgreSQL**.
- **Healthcheck:** `GET http://localhost:3002/health`.

### 3.3. `notification-service` (Planejado)
- Consome `telemetry.raw`, avalia regras em `alert_rules` (PostgreSQL) com janela de histerese/cooldown e publica comandos em `commands.requested`.

### 3.4. `command-service` (Planejado)
- Consome `commands.requested`, envia comandos MQTT (`actuators/{device_id}/cmd`), assina confirmações (`actuators/+/ack`) e atualiza `command_logs` no PostgreSQL.

---

## 4. Contratos de Eventos (Schemas Base)

### Evento: `telemetry.raw`
```json
{
  "event_id": "uuid-v4",
  "sensor_id": "sensor-01",
  "type": "temperature",
  "value": 26.4,
  "unit": "C",
  "timestamps": {
    "sensor_created_at": "2026-11-24T15:00:00.120000Z",
    "ingested_at": "2026-11-24T15:00:00.125432Z"
  }
}
```

### Evento: `commands.requested`
```json
{
  "command_id": "uuid-v4",
  "actuator_id": "actuator-cooling-01",
  "action": "TURN_ON",
  "reason": "TEMP_THRESHOLD_EXCEEDED",
  "triggered_by_event_id": "uuid-v4",
  "created_at": "2026-11-24T15:00:00.140000Z"
}
```

---

## 5. Estrutura do Repositório

```text
├── docker-compose.base.yml       # Mosquitto, PostgreSQL, InfluxDB v2, Grafana
├── docker-compose.redis.yml      # Redis Streams + Microsserviços (EVENT_BUS_DRIVER=redis)
├── docker-compose.rabbitmq.yml   # RabbitMQ + Microsserviços (EVENT_BUS_DRIVER=rabbitmq)
├── docker-compose.kafka.yml      # Apache Kafka KRaft + Microsserviços (EVENT_BUS_DRIVER=kafka)
├── shared/
│   └── event-bus/                # @tcc/event-bus: interfaces, contratos e adapters (Redis, RabbitMQ, Kafka)
├── services/
│   ├── ingest-service/           # Microsserviço NestJS de ingestão MQTT
│   └── persistence-service/      # Microsserviço NestJS de persistência (InfluxDB + PostgreSQL)
├── infra/
│   ├── mosquitto/config/         # Configuração do broker MQTT
│   ├── postgres/                 # 01_init.sql (tabelas sensors, actuators, alert_rules, command_logs)
│   └── grafana/provisioning/     # Datasource InfluxDB e dashboard de benchmark
└── benchmark/                    # Gerador de carga MQTT e coletor de métricas (InfluxDB + docker stats)
```

---

## 6. Como Rodar a Aplicação

### Pré-requisitos
- **Docker** e **Docker Compose v2**
- **Node.js 22+** e **npm** (para rodar testes unitários localmente e o gerador de benchmark)

### Subindo a infraestrutura + canal de eventos escolhido

Os 3 barramentos são **mutuamente exclusivos por execução** e possuem os mesmos limites de recursos (`cpus: "1.0"`, `memory: 1G` nos brokers; `cpus: "1.0"`, `memory: 512M` nos microsserviços) e persistência em disco habilitada para garantir paridade no benchmark.

```powershell
# Opção 1: Redis Streams
docker compose -f docker-compose.base.yml -f docker-compose.redis.yml up -d --build

# Opção 2: RabbitMQ
docker compose -f docker-compose.base.yml -f docker-compose.rabbitmq.yml up -d --build

# Opção 3: Apache Kafka (KRaft)
docker compose -f docker-compose.base.yml -f docker-compose.kafka.yml up -d --build
```

Para trocar de broker entre rodadas, derrube o stack atual antes de subir o próximo:

```powershell
docker compose -f docker-compose.base.yml -f docker-compose.redis.yml down
```

### Portas e Credenciais Padrão

| Serviço | URL / Porta | Credenciais Padrão |
| :--- | :--- | :--- |
| **Grafana** | `http://localhost:3000` | `admin` / `admin` |
| **InfluxDB v2** | `http://localhost:8086` | `tcc` / `tcc_password` (org: `tcc-ufba`, bucket: `telemetry`) |
| **PostgreSQL** | `localhost:5432` | `tcc` / `tcc` (db: `tcc_monitoring`) |
| **Mosquitto (MQTT)** | `mqtt://localhost:1883` | Anônimo habilitado |
| **RabbitMQ UI** | `http://localhost:15672` | `tcc` / `tcc` |
| **ingest-service** | `http://localhost:3001/health` | — |
| **persistence-service** | `http://localhost:3002/health` | — |

---

## 7. Como Rodar os Testes Unitários

Antes de compilar ou testar os microsserviços localmente pela primeira vez, instale e compile o pacote compartilhado `@tcc/event-bus`:

```powershell
cd shared/event-bus
npm install
npm run build
```

Depois, execute o build e os testes Jest em cada serviço:

```powershell
# ingest-service
cd ../../services/ingest-service
npm install
npm run build
npm test

# persistence-service
cd ../persistence-service
npm install
npm run build
npm test
```

---

## 8. Como Rodar os Testes de Carga (Benchmark)

### Preparação (primeira vez)
```powershell
cd benchmark
npm install
npm run build
```

### Executando uma rodada
```powershell
npm run benchmark -- --driver redis --sensors 100 --connections 10 --qos 1 --rate 500 --duration 30 --settle 3
```

### Teste de stress extremo (saturação / perda de pacotes)
Use múltiplas conexões MQTT (`--connections`) e `--qos 0` (*fire-and-forget*) para eliminar o gargalo do gerador:

```powershell
npm run benchmark -- --driver redis --sensors 1000 --connections 30 --qos 0 --rate 5000 --duration 60 --settle 3
```

### Parâmetros disponíveis
| Parâmetro | Padrão | Descrição |
| :--- | :--- | :--- |
| `--driver` | `redis` | Rótulo do barramento nos relatórios (`redis`, `rabbitmq` ou `kafka`). |
| `--sensors` | `20` | Quantidade de sensores simulados (`sensor-001` ... `sensor-NNN`). |
| `--connections` | `10` | Quantidade de conexões MQTT paralelas abertas pelo gerador. |
| `--qos` | `1` | Qualidade de serviço MQTT (`0` = *fire-and-forget*, `1` = aguarda `PUBACK`). |
| `--rate` | `200` | Taxa alvo total de envio em **mensagens/segundo**. |
| `--duration` | `20` | Duração da carga em **segundos**. |
| `--settle` | `3` | Espera (em **segundos**) após o fim dos envios antes de consultar o InfluxDB. |
| `--mqtt-url` | `mqtt://localhost:1883` | Endereço do broker MQTT. |
| `--influx-url` | `http://localhost:8086` | URL do InfluxDB para coleta das métricas de latência. |

### Resultados e Monitoramento
- **Terminal e Arquivos:** Ao final de cada execução, o resumo (`published`, `persisted`, `lossPct`, `min/mean/p50/p95/p99/max` em ms e consumo médio/pico de CPU/RAM via `docker stats`) é exibido no terminal, salvo em `benchmark/results/<driver>-<timestamp>.json` e acumulado em `benchmark/results/summary.csv`.
- **Grafana:** Acesse `http://localhost:3000`, vá em **Dashboards → TCC Monitoramento → TCC - Benchmark Barramento de Eventos** para acompanhar vazão (`msgs/s`) e latência (`P50 / P95 / P99`) em tempo real.
"# tcc-event-driven-benchmark" 
