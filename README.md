# Flight Data Platform

Plataforma de dados de voos construída no Databricks. O projeto consome APIs públicas (OpenSky Network e OurAirports), transforma os dados em Spark DataFrames e os persiste em tabelas Delta no Unity Catalog.

## Arquitetura

```
API / CSV  →  Pandas DataFrame  →  Spark DataFrame  →  Tabela Delta ( Bronze Layer )
```

O pipeline segue uma arquitetura medallion com as camadas **bronze** (dados brutos), **silver** (limpeza e transformação) e **gold** (agregações e KPIs) implementadas. As tabelas gold são exportadas para o Google BigQuery via REST API (load jobs com NDJSON inline).

## Fontes de dados

| Fonte | Endpoint | Tabela Delta | Descrição |
| --- | --- | --- | --- |
| OpenSky — States | `https://opensky-network.org/api/states/all` | `workspace.default.bronze_opensky_states` | Estado atual de todas as aeronaves rastreadas (posição, velocidade, altitude, etc.) |
| OurAirports — Airports | `https://raw.githubusercontent.com/.../airports.csv` | `workspace.default.dim_airports` | Dimensão de aeroportos (IATA, ICAO, coordenadas, país, região) |
| OpenSky — Departures | `https://opensky-network.org/api/flights/departure` | `workspace.default.bronze_opensky_departures` | Voos de partida do aeroporto SBGR (Guarulhos) nas últimas 2 horas |

## Notebooks

### Ingest (camada bronze)

Consome as APIs, transforma as respostas em Spark DataFrames e persiste em tabelas Delta com `overwriteSchema=true`.

### Transform (camada silver)

Lê as tabelas Delta da camada bronze, aplica limpeza, transformações e enriquecimento, e persiste as tabelas silver:

| Etapa | Descrição |
| --- | --- |
| Leitura | Carrega as 3 tabelas bronze via `spark.table()` |
| Análise de nulos | Conta nulos por coluna e calcula a porcentagem |
| Limpeza | Remove colunas com 100% de valores nulos |
| Trim de strings | Remove espaços em `callsign`, `ident`, `name`, `municipality`, `icao_code`, `iata_code` |
| Conversão de timestamps | `time_position` e `last_contact` (Unix epoch) → `timestamp` legível |
| Desduplicação | Mantém apenas o registro mais recente por `icao24` em states (`row_number` + `Window`) |
| Colunas derivadas | `altitude_ft`, `velocity_kmh`, `is_airborne`, `heading_category` (N/NE/E/SE/S/SW/W/NW) |
| Flags de qualidade | `is_position_complete` indica se lat/long/altitude estão preenchidos |
| Filtragem de dim_airports | Mantém apenas aeroportos com serviço regular ou de grande/médio porte |
| Enriquecimento | Join de departures com silver_airports (ICAO) para trazer nome, cidade e país |
| Persistência | Salva as 3 tabelas silver em Delta com `overwriteSchema=true` |

### Load (camada gold)

Lê as tabelas silver e constrói agregações e KPIs prontos para análise. Exporta as tabelas gold para o BigQuery (GCP) via BigQuery REST API.

| KPI | Tabela Gold | Descrição |
| --- | --- | --- |
| Aeronaves por país | `gold_aircraft_by_country` | Total, airborne, on_ground, avg altitude/velocity por país de origem |
| Distribuição de heading | `gold_heading_distribution` | Aircraft count, avg speed/altitude por direção (N/NE/E/...) |
| Partidas por aeroporto/linha | `gold_departures_summary` | Departure count por aeroporto + prefixo da companhia aérea |
| Estatísticas de aeroportos | `gold_airport_stats` | Airport count, avg/min/max elevation por país + tipo |

### Exportação para o BigQuery (Reverse ETL)

O notebook Load exporta as 4 tabelas gold para o BigQuery no projeto sandbox `engdta` (sem billing) usando a **BigQuery REST API** com **load jobs** (multipart upload de NDJSON inline).

#### Por que REST API e não o Spark BigQuery Connector?

* O Spark BigQuery Connector não é suportado para escrita em compute serverless (`UNSUPPORTED_DATA_SOURCE_WRITE`).
* O sandbox sem billing bloqueia streaming insert (`tabledata.insertAll`) e DML (`INSERT INTO ... VALUES`).
* Load jobs com NDJSON inline funcionam no sandbox sem billing e em compute serverless.
* Bibliotecas necessárias (`google-auth` + `requests`) já estão disponíveis no Databricks Runtime.

#### Pré-requisitos

1. Criar GCP Service Account com role `BigQuery Data Editor` no projeto `engdta`
2. Armazenar a key JSON em Databricks Secrets (`gcp-secrets` / `service-account-key`)
3. Criar dataset `flight_data` (location US) no BigQuery
4. Arquivo `.env` na raiz do projeto com as configurações

#### Tabelas gold exportadas para `engdta.flight_data`

| Tabela | Descrição | Linhas |
| --- | --- | --- |
| `gold_aircraft_by_country` | Aeronaves por país: total, airborne, on_ground, avg altitude/velocity | 112 |
| `gold_heading_distribution` | Distribuição por heading: aircraft count, avg speed/altitude | 8 |
| `gold_departures_summary` | Partidas por aeroporto + linha aérea | 6 |
| `gold_airport_stats` | Aeroportos por país + tipo: count, avg/min/max elevation | 518 |

> **Sandbox**: As tabelas no `flight_data` expiram em 60 dias (comportamento esperado do sandbox sem billing).

#### Migração para projeto com billing

Ao migrar para um projeto GCP com billing ativo e cluster clássico:
1. Instalar o Spark BigQuery Connector Maven (`com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1`)
2. Criar um bucket GCS temporário e adicionar `GCS_TEMP_BUCKET` no `.env`
3. Substituir a célula de exportação para usar `.format("bigquery").option("temporaryGcsBucket", GCS_TEMP_BUCKET)` (modo indireto)

## Estrutura do projeto

```
Flight-Data-Platform/
├── Ingest          # Notebook de ingestão (camada bronze)
├── Transform       # Notebook de transformação (camada silver)
├── Load            # Notebook de agregações e export BigQuery (camada gold)
├── .env            # Configuração do BigQuery (GCP_PROJECT_ID, BQ_DATASET, etc.)
├── .gitignore      # Ignora .env, __pycache__, .ipynb_checkpoints, etc.
└── README.md
```

## Como executar

1. Abra o notebook **Ingest** no Databricks Workspace.
2. Execute todas as células em ordem (Run All).
3. As tabelas Delta serão criadas/atualizadas em `workspace.default`.
4. Abra o notebook **Transform** e execute em ordem para ler, transformar e persistir as tabelas silver.
5. Abra o notebook **Load** e execute em ordem para gerar as tabelas gold e exportar para o BigQuery.

## Tabelas silver

| Tabela | Origem | Descrição |
| --- | --- | --- |
| `workspace.default.silver_opensky_states` | `bronze_opensky_states` | Aeronaves desduplicadas com timestamps legíveis, altitude em pés, velocidade em km/h, categoria de heading e flag de qualidade |
| `workspace.default.silver_dim_airports` | `dim_airports` | Aeroportos com serviço regular ou de grande/médio porte, strings padronizadas |
| `workspace.default.silver_opensky_departures` | `bronze_opensky_departures` | Partidas enriquecidas com nome, cidade, país e elevação do aeroporto de origem |

## Próximos passos

- [x] Persistir os DataFrames transformados em tabelas silver no Delta
- [x] Enriquecer departures com `dim_airports` (join por ICAO)
- [x] Camada Gold: agregações e KPIs
- [x] Exportação para o BigQuery (REST API + load jobs, sandbox sem billing)
- [ ] Dashboard de visualização
- [ ] Agendamento automático da ingestão
