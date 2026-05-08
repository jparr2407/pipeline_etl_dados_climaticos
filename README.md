# Pipeline ETL — Dados Meteorológicos do Rio de Janeiro ☀️🌧️

Pipeline ETL orquestrado com **Apache Airflow** sobre **Docker Compose**, que coleta dados meteorológicos da API OpenWeatherMap para a cidade do Rio de Janeiro a cada hora e os armazena em um banco PostgreSQL.

---

## 🏗️ Arquitetura

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Extract    │────▶│  Transform   │────▶│    Load      │
│ (OpenWeather)│     │  (Pandas)    │     │ (PostgreSQL) │
└──────────────┘     └──────────────┘     └──────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
   weather_data.json    temp_data.parquet     weather_data ⬢
```

### Infraestrutura Docker

| Serviço         | Container                        | Descrição                        |
|-----------------|----------------------------------|----------------------------------|
| **Airflow**     | apiserver, scheduler, worker     | Orquestração (CeleryExecutor)    |
| **PostgreSQL**  | postgres                         | Backend do Airflow + dados       |
| **Redis**       | redis                            | Broker Celery                    |
| **DAG Processor** | dag-processor                  | Parsing e serialização das DAGs  |
| **Triggerer**   | triggerer                        | Deferrable operators             |

---

## 📦 Stack

- **Orquestração:** Apache Airflow 3.1.7 com CeleryExecutor
- **Extração:** Requests + OpenWeatherMap API
- **Transformação:** Pandas
- **Banco de dados:** PostgreSQL 16 + SQLAlchemy
- **Containerização:** Docker Compose
- **Python:** 3.12

---

## 📁 Estrutura do Projeto

```
pipeline_weather/
├── dags/
│   └── weather_dag.py          # Definição da DAG (extract → transform → load)
├── src/
│   ├── extract_data.py         # Requisição à API OpenWeatherMap
│   ├── transform_data.py       # Normalização e limpeza dos dados
│   └── load_data.py            # Carga no PostgreSQL via SQLAlchemy
├── config/
│   └── airflow.cfg             # Configuração do Airflow
│   └── .env                    # Credenciais (gitignored)
├── data/
│   ├── weather_data.json       # Dados brutos extraídos
│   └── temp_data.parquet       # Dados transformados (intermediário)
├── notebooks/
│   └── analysis_data.ipynb     # Análise exploratória dos dados
├── docker-compose.yaml         # Stack completa (Airflow + Postgres + Redis)
├── pyproject.toml              # Dependências do projeto
└── .env                        # AIRFLOW_UID (host UID)
```

---

## 🚀 Setup

### Pré-requisitos

- [Docker](https://docs.docker.com/engine/install/) e [Docker Compose](https://docs.docker.com/compose/install/)
- UID do host = 1000 (verifique com `id -u`)

### 1. Clone o repositório

```bash
git clone <seu-repo-url>
cd pipeline_weather
```

### 2. Configure as credenciais

Crie `config/.env` com suas credenciais (este arquivo é gitignored):

```env
API_KEY=<sua-chave-openweathermap>
database=weather_data
user=<seu-usuario-postgres>
password=<sua-senha-postgres>
```

O `config/.env` é montado no container em `/opt/airflow/config/.env` e carregado pela DAG via `python-dotenv`.

> **Importante:** A `API_KEY` é obtida gratuitamente em [openweathermap.org](https://openweathermap.org/api).

### 3. Suba os containers

```bash
# Ajuste o AIRFLOW_UID no .env raiz (opcional, padrão é 1000)
echo "AIRFLOW_UID=$(id -u)" > .env

# Inicialize o banco e crie o usuário admin
docker compose up airflow-init

# Suba todos os serviços
docker compose up -d
```

Na primeira execução, o container `airflow-init` executa as migrações do banco (`airflow db migrate`) e cria o usuário admin (`airflow` / `airflow`).

### 4. Crie o banco de dados da aplicação

```bash
docker exec -it pipeline_weather-postgres-1 psql -U airflow -c \
  "CREATE DATABASE weather_data;"
```

> O Airflow usa um banco `airflow` separado para metadados. Os dados meteorológicos vão para o banco `weather_data`.

### 5. Acesse o Airflow

Abra `http://localhost:8080` no navegador.

- **Usuário:** `admin`
- **Senha:** `admin`

A DAG `weather_pipeline` aparecerá na lista. Ative-a com o toggle e ela executará a cada hora.

---

## 🔄 Como funciona

### Extract (`extract_data.py`)
Faz uma requisição GET à API OpenWeatherMap para Rio de Janeiro, BR:
```
https://api.openweathermap.org/data/2.5/weather?q=Rio+de+Janeiro,BR&units=metric&appid={API_KEY}
```
O JSON bruto é salvo em `/opt/airflow/data/weather_data.json`.

### Transform (`transform_data.py`)
1. **Normaliza** o JSON nested (`weather`, `coord`, `main`, `wind`, etc.) em colunas planas
2. **Remove** colunas irrelevantes (`weather`, `base`, `weather_icon`, `sys.type`)
3. **Renomeia** colunas para português (ex: `main.temp` → `temperature`, `sys.sunrise` → `sunrise`)
4. **Converte** timestamps Unix para datetime com timezone de São Paulo
5. **Salva** o resultado como Parquet em `/opt/airflow/data/temp_data.parquet`

### Load (`load_data.py`)
1. Lê o Parquet intermediário
2. Conecta ao PostgreSQL via SQLAlchemy com `psycopg2`
3. Insere os registros na tabela `weather_data` (modo `append`)

---

## 📊 Esquema da Tabela

| Coluna        | Tipo              | Descrição                      |
|---------------|-------------------|--------------------------------|
| temperature   | float             | Temperatura atual (°C)         |
| feels_like    | float             | Sensação térmica (°C)          |
| temp_min      | float             | Temperatura mínima             |
| temp_max      | float             | Temperatura máxima             |
| pressure      | integer           | Pressão atmosférica (hPa)      |
| humidity      | integer           | Umidade (%)                    |
| sea_level     | integer           | Pressão ao nível do mar        |
| grnd_level    | integer           | Pressão ao nível do solo       |
| visibility    | integer           | Visibilidade (m)               |
| wind_speed    | float             | Velocidade do vento (m/s)      |
| wind_deg      | integer           | Direção do vento (°)           |
| wind_gust     | float             | Rajadas de vento               |
| clouds        | integer           | Nebulosidade (%)               |
| weather_id    | integer           | Código da condição climática   |
| weather_main  | text              | Condição principal (Clear, Rain) |
| weather_description | text         | Descrição (clear sky, etc.)    |
| datetime      | timestamptz       | Momento da medição             |
| sunrise       | timestamptz       | Nascer do sol                  |
| sunset        | timestamptz       | Pôr do sol                     |
| timezone      | integer           | Offset UTC (segundos)          |
| city_id       | integer           | ID da cidade na API            |
| city_name     | text              | Nome da cidade                 |
| code          | integer           | Código HTTP da resposta        |
| longitude     | float             | Longitude                      |
| latitude      | float             | Latitude                       |
| sys_id        | integer           | ID interno do sistema          |
| country       | text              | País                           |

---

## 🔍 Consultas úteis

Conecte-se ao banco para visualizar os dados:

```bash
docker exec -it pipeline_weather-postgres-1 psql -U airflow -d weather_data
```

```sql
-- Últimas medições
SELECT datetime, temperature, humidity, weather_main
FROM weather_data
ORDER BY datetime DESC
LIMIT 10;

-- Total de registros
SELECT COUNT(*) FROM weather_data;

-- Temperatura máxima por dia
SELECT DATE(datetime) AS dia, MAX(temperature) AS temp_max
FROM weather_data
GROUP BY dia
ORDER BY dia DESC;
```

---

## 🛑 Comandos úteis

```bash
# Ver logs de um serviço
docker compose logs airflow-worker

# Ver status dos containers
docker compose ps

# Resetar o Airflow (apaga banco)
docker compose down -v
docker compose up airflow-init
docker compose up -d

# Rodar a pipeline localmente (sem Airflow)
uv run python main.py
```

---

## ⚙️ Troubleshooting

| Problema                           | Solução                                               |
|------------------------------------|-------------------------------------------------------|
| `EACCES: permission denied`        | Verifique se `AIRFLOW_UID` no `.env` = `id -u`        |
| `ModuleNotFoundError: extract_data` | Confira volumes do `src/` no docker-compose           |
| `password` / `database` = `None`   | `config/.env` está configurado? Não versionado?       |
| Conexão recusada ao PostgreSQL     | Host deve ser `postgres` (nome do serviço), não `localhost` |
| DAG não aparece                    | Aguarde o DAG processor (~30s) ou reinicie os containers |

---
