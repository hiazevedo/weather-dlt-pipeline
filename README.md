# Weather DLT Pipeline — Birigui-SP

> Pipeline meteorológico end-to-end com Delta Live Tables coletando 87 anos de dados históricos e previsão em tempo real para Birigui-SP

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Live Tables](https://img.shields.io/badge/Delta_Live_Tables-003366?style=for-the-badge&logo=delta&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-0194E2?style=for-the-badge&logo=databricks&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

---

## 📌 Sobre o projeto

Pipeline meteorológico completo que coleta dados da **Open-Meteo API** para Birigui-SP (-21.2878, -50.3409), processa via **Delta Live Tables** com validações automáticas de qualidade, e alimenta um dashboard em tempo real com previsão do tempo atualizada a cada 6 horas via **Databricks Workflows**.

---

## Arquitetura

```
Open-Meteo API (histórico 1940→2026 + previsão 48h)
           │
           ▼
    [UC Volume — JSON]
    /Volumes/weather_pipeline/bronze/raw_json
           │
           │  Delta Live Tables
           ▼
┌─────────────────────┐
│  @dlt.table         │  weather_raw
│  BRONZE             │  87 arquivos JSON
└──────────┬──────────┘  @dlt.expect validações
           │
           ▼
┌─────────────────────┐
│  @dlt.table         │  weather_clean
│  SILVER             │  756K registros horários
└──────────┬──────────┘  temp_class, rain_class, features temporais
           │
     ┌─────┴─────────────┐
     ▼                   ▼
┌─────────┐   ┌──────────────────┐
│  GOLD   │   │      GOLD        │
│ daily   │   │    monthly       │
│ 31K dias│   │   1K meses       │
└─────────┘   └──────────────────┘
     │
     ▼
┌─────────────────┐
│  GOLD forecast  │
│  168h previsão  │
└─────────────────┘
           │
           ▼
  [Dashboard SQL + Workflow 6h]
```

---

## Tabelas Delta Live Tables

| Tabela | Camada | Registros | Descrição |
|--------|--------|-----------|-----------|
| `weather_raw` | Bronze | 87 arquivos | JSON raw com validações |
| `weather_clean` | Silver | 756K horas | Dados limpos + classificações |
| `weather_daily` | Gold | 31K dias | Resumo diário histórico |
| `weather_monthly` | Gold | 1K meses | Climatologia mensal |
| `weather_forecast` | Gold | 168h | Previsão próximas 48h |

---

## Qualidade DLT — Expectations

```python
@dlt.expect("temperatura_valida",     "temperature_2m BETWEEN -10 AND 50")
@dlt.expect("sensacao_valida",        "apparent_temperature BETWEEN -15 AND 60")
@dlt.expect_or_drop("data_nao_nula",  "observation_time IS NOT NULL")
@dlt.expect_or_fail("chuva_positiva", "precipitation >= 0")
@dlt.expect("ano_valido",             "year >= 1940 AND year <= 2100")
@dlt.expect("mes_valido",             "month BETWEEN 1 AND 12")
@dlt.expect("hora_valida",            "hour BETWEEN 0 AND 23")
```

---

## Estrutura do projeto

```
weather-dlt-pipeline/
├── config.py                # Constantes centralizadas (LAT, LON, paths, timeouts)
├── databricks.yml           # Databricks Asset Bundle — Workflow completo (6h)
├── 00_setup.py              # Catalog, schemas, volumes
├── 01_collect_history.py    # 87 anos histórico — 755k registros
├── 02_dlt_pipeline.py       # Pipeline DLT (Bronze→Silver→Gold)
├── 03_visualizacoes.py      # 4 gráficos históricos de tendência climática
├── 04_collect_forecast.py   # Coleta previsão atualizada (6h)
├── 05_gold_today.py         # gold.weather_today + gold.rain_alert
├── dashboard.py             # Notebook de dashboard Databricks SQL
└── analyze_tables.py        # Inspeção e exploração das tabelas Delta
```

---

## Workflow automatizado

Pipeline roda automaticamente **4x por dia** via Databricks Workflows, configurado no `databricks.yml`:

```
Weather Birigui - Previsao + ML (6h)
  Schedule: 00h, 06h, 12h, 18h (Brasília)

  coleta_previsao  ← coleta API Open-Meteo (+ 7 dias histórico recente)
       │
  dlt_pipeline     ← executa DLT Bronze→Silver→Gold
       │
  gold_today       ← atualiza weather_today e rain_alert
       │
  update_features  ← atualiza feature store ML
       │
  ml_inference     ← predições modelo RandomForest 24h
```

Para fazer o deploy:

```bash
databricks bundle deploy
```

---

## Insights históricos (1940–2026)

- 🌡️ **+1.41°C** em 86 anos — aquecimento confirmado (+0.0166°C/ano)
- 🌧️ **-3.9mm/ano** de precipitação — Birigui está ficando mais seca
- **Jan/Fev:** pico de chuva — 37-38% das horas chove
- **Jul/Ago:** seca intensa — apenas 4.5% das horas
- **Chuva máxima** registrada: 34.9mm em uma hora
- **Temperatura máxima** registrada: 41.8°C

---

## Dashboard — Previsão do Tempo Birigui-SP

Dashboard Databricks SQL atualizado automaticamente com:

| Widget | Tipo | Dados |
|--------|------|-------|
| Vai Chover Hoje? | Counter | gold.rain_alert |
| Temperatura Máx/Mín | Counter | gold.rain_alert |
| Temperatura do Dia | Line chart | gold.weather_today |
| Precipitação + Umidade | Bar chart | gold.rain_alert |
| Umidade Relativa | Line chart | gold.weather_today |
| Probabilidade ML | Line chart | gold.rain_forecast_ml |

---

## Stack técnica

| Tecnologia | Uso |
|------------|-----|
| **Databricks Free Edition** | Ambiente Serverless AWS |
| **Delta Live Tables** | Pipeline declarativo com expectations |
| **Unity Catalog** | Governança + Volumes |
| **Databricks Workflows** | Orquestração 4x/dia |
| **Databricks Asset Bundles** | Pipeline e workflow como código |
| **Databricks SQL** | Dashboard em tempo real |
| **Open-Meteo API** | Dados meteorológicos (gratuita) |
| **Matplotlib/Seaborn** | Visualizações históricas |

---

## Como reproduzir

### Pré-requisitos
- Conta no [Databricks Free Edition](https://www.databricks.com/try-databricks)
- Acesso à internet para a API Open-Meteo (gratuita, sem autenticação)
- Databricks CLI instalado e configurado

### Passo a passo

```bash
# 1. Clone o repositório
git clone https://github.com/hiazevedo/weather-dlt-pipeline.git
cd weather-dlt-pipeline

# 2. Deploy via Asset Bundle (cria o Workflow automaticamente)
databricks bundle deploy
```

Ou execute os notebooks manualmente na ordem:

```
00_setup.py              # Cria catalog weather_pipeline
01_collect_history.py    # Coleta 87 anos (~5 min)
02_dlt_pipeline.py       # Criar e rodar DLT Pipeline no UI
03_visualizacoes.py      # Gera gráficos históricos
04_collect_forecast.py   # Coleta previsão inicial
05_gold_today.py         # Cria tabelas do dia
```

### Unity Catalog

```
Catalog : weather_pipeline
Schemas : bronze | silver | gold
Volume  : /Volumes/weather_pipeline/bronze/raw_json
```

---

## Decisões técnicas

**Por que `is_forecast` é classificado por tempo e não pelo nome do arquivo?**
Os arquivos `forecast_*.json` coletados pela API incluem os últimos 7 dias de observações reais junto com a previsão futura. Classificar pelo nome do arquivo marcava dados históricos recentes como previsão, criando um gap no histórico. A solução correta é: `observation_time > current_timestamp()` — horas passadas são observações reais, horas futuras são previsão.

**Por que `past_days=7` na coleta de forecast?**
Garante que cada execução do Workflow recoleta os últimos 7 dias de dados observados, preenchendo automaticamente qualquer gap caso o pipeline fique offline por alguns dias.

**Por que `trigger(availableNow=True)` no DLT?**
O Databricks Free Edition não mantém clusters ativos indefinidamente. O modo `Triggered` processa todos os dados pendentes e encerra — compatível com Serverless.

**Por que corrigir dados de 1940?**
Dados históricos antigos têm valores `NaN` em `precipitation` — sensores antigos não registravam chuva. O filtro antes do `@dlt.expect_or_fail` garante que o pipeline não falhe por dados incompletos de décadas passadas.

---

## Portfólio

Este projeto faz parte do [Databricks Data Engineering Portfolio](https://github.com/hiazevedo/databricks-portfolio), uma série de projetos práticos cobrindo o ciclo completo de engenharia de dados com Databricks.

| # | Projeto | Tema |
|---|---------|------|
| 1 | [fuel-price-pipeline-br](https://github.com/hiazevedo/fuel-price-pipeline-br) | Batch · Medallion · ANP |
| 2 | [earthquake-streaming-pipeline](https://github.com/hiazevedo/earthquake-streaming-pipeline) | Streaming · Auto Loader · USGS |
| 3 | [earthquake-ml-pipeline](https://github.com/hiazevedo/earthquake-ml-pipeline) | ML · MLflow · Spark ML |
| 4 | **weather-dlt-pipeline** ← você está aqui | DLT · Workflows · Open-Meteo |
| 5 | [weather-ml-rain-forecast](https://github.com/hiazevedo/weather-ml-rain-forecast) | ML Avançado · Previsão de Chuva |
