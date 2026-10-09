# Flight Data Platform

Plataforma de dados de voos construída no Databricks. O projeto consome APIs públicas (OpenSky Network e OurAirports), transforma os dados em Spark DataFrames e os persiste em tabelas Delta no Unity Catalog.

## Arquitetura

```
API / CSV  →  Pandas DataFrame  →  Spark DataFrame  →  Tabela Delta ( Bronze Layer )
```

O pipeline segue uma arquitetura medallion simplificada com as camadas **bronze** (dados brutos) e **silver** (limpeza e transformação) implementadas.

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

Lê as tabelas Delta da camada bronze e aplica as primeiras transformações:

| Etapa | Descrição |
| --- | --- |
| Leitura | Carrega as 3 tabelas bronze via `spark.table()` |
| Análise de nulos | Conta nulos por coluna e calcula a porcentagem |
| Limpeza | Remove colunas com 100% de valores nulos |
| Visualização | Exibe amostras dos DataFrames limpos |

## Estrutura do projeto

```
Flight-Data-Platform/
├── Ingest          # Notebook de ingestão (camada bronze)
├── Transform       # Notebook de transformação (camada silver)
└── README.md
```

## Como executar

1. Abra o notebook **Ingest** no Databricks Workspace.
2. Execute todas as células em ordem (Run All).
3. As tabelas Delta serão criadas/atualizadas em `workspace.default`.
4. Abra o notebook **Transform** e execute em ordem para ler e limpar os dados bronze.

## Próximos passos

- [ ] Persistir os DataFrames limpos em tabelas silver no Delta
- [ ] Enriquecer departures com `dim_airports` (join por ICAO)
- [ ] Camada Gold: agregações e KPIs (voos por hora, rotas mais frequentes)
- [ ] Dashboard de visualização
- [ ] Agendamento automático da ingestão
