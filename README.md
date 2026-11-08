# Weather DLT Pipeline — Birigui-SP

> Pipeline meteorológico end-to-end com Delta Live Tables coletando 87 anos de dados históricos e previsão em tempo real para Birigui-SP

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Live Tables](https://img.shields.io/badge/Delta_Live_Tables-003366?style=for-the-badge&logo=delta&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-0194E2?style=for-the-badge&logo=databricks&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

---

## 📌 Sobre o projeto

Pipeline meteorológico completo que coleta dados da **Open-Meteo API** para Birigui-SP (-21.2878, -50.3409), processa via **Delta Live Tables** com validações automáticas de qualidade, e alimenta um dashboard em tempo real com previsão do tempo atualizada a cada hora via **Databricks Workflows**.

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
│  streaming table    │  weather_raw
│  BRONZE             │  Auto Loader (só arquivos novos)
└──────────┬──────────┘
           │  view weather_hourly_stage: explode + @dlt.expect
           ▼
┌─────────────────────┐
│  apply_changes      │  weather_clean
│  SILVER             │  761K horas, uma linha por hora
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
  [Dashboard SQL + Workflow de hora em hora]
```

---

## Tabelas Delta Live Tables

| Tabela | Camada | Registros | Descrição |
|--------|--------|-----------|-----------|
| `weather_raw` | Bronze | ~860 arquivos | JSON raw (streaming table, Auto Loader) |
| `weather_clean` | Silver | 761K horas | Dados limpos + classificações; uma linha por hora (`apply_changes`) |
| `weather_daily` | Gold | 31K dias | Resumo diário histórico |
| `weather_monthly` | Gold | 1K meses | Climatologia mensal |
| `weather_forecast` | Gold | 168h | Previsão dos próximos 7 dias (inclui chance de chuva e rajadas) |
| `forecast_emitida` | Silver | ~120K linhas | Cada previsão coletada (`emitida_em`) para cada hora futura (`valida_em`), com `antecedencia_h` — base para avaliar e treinar por antecedência (streaming, append) |

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
├── config.py                # Constantes centralizadas (LAT, LON, paths, variáveis horárias, modelo do Archive)
├── databricks.yml           # Databricks Asset Bundle — Workflows (1h, semanal)
├── 00_setup.py              # Catalog, schemas, volumes
├── 01_collect_history.py    # 87 anos histórico — 755k registros
├── 02_dlt_pipeline.py       # Pipeline DLT (Bronze→Silver→Gold)
├── 03_visualizacoes.py      # 4 gráficos históricos de tendência climática
├── 04_collect_forecast.py   # Coleta previsão atualizada (a cada hora)
├── 05_gold_today.py         # gold.weather_today + gold.rain_alert
├── 06_backfill_archive.py   # Backfill semanal com o Archive (ERA5)
├── 07_collect_enso.py       # Índice ENSO (ONI/NOAA) → silver.enso_oni
├── 08_collect_gauges.py     # Pluviômetros históricos (GHCN-Daily) → silver.gauge_daily
├── 09_collect_previous_runs.py # Previsões arquivadas com antecedência → silver.forecast_previous_runs
├── 10_ingest_inmet.py       # Estações automáticas do INMET (CSV no Volume) → silver.inmet_hourly
├── 11_ingest_avisos.py      # Avisos do INMET (JSON no Volume) → silver.inmet_avisos + views gold
├── 12_enso_impacto.py       # ENSO × clima de Birigui → gold.enso_impacto, enso_previsibilidade, estacao_chuvosa + views estacao_vs_analogos e acuracia_por_enso
├── 13_collect_ana.py        # ANA: postos (leitura diária) e telemetria → silver.ana_* + view gold.ana_telemetria_qc
├── 14_vigia.py              # Vigia: falha (→ e-mail) quando o job de 1h, a coleta, o celular ou o backfill param
├── 15_collect_merge.py      # MERGE/CPTEC horário nos pontos de interesse → silver.merge_hourly (sob demanda)
├── scripts/inmet_sync.py    # Coleta local do INMET (o INMET bloqueia o Databricks)
├── scripts/inmet_avisos_sync.py # Coleta local dos avisos do INMET (só envia quando mudam)
├── scripts/dbx.py           # Cliente mínimo da API REST do Databricks (envio e disparo de jobs)
├── scripts/termux/          # Instalação e agendamento no celular Android (Termux + cron)
├── dashboard.py             # Notebook de dashboard Databricks SQL
├── dashboards/              # Definição do painel Lakeview (export do publicado)
└── analyze_tables.py        # Inspeção e exploração das tabelas Delta
```

---

## Workflow automatizado

Pipeline roda automaticamente **a cada hora** via Databricks Workflows, configurado no `databricks.yml`. O DLT é incremental: o Auto Loader só lê os arquivos novos, a silver é atualizada por `apply_changes` e as golds não fazem recálculo completo, então o custo não cresce com o tempo:

```
[weather] Pipeline Completo (1h)
  Schedule: a cada hora cheia (Brasília)

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

Um segundo job roda **semanalmente** e mantém a série consistente:

```
[weather] Semanal — Archive ERA5, ENSO e previsões arquivadas
  Schedule: domingo 04h30 (Brasília; no meio da hora, longe do job de 1h)

  collect_enso           ← índice ONI da NOAA (mensal; independente)
  collect_previous_runs  ← previsões arquivadas d0/d1/d2 (base para MOS; independente)
  collect_ana            ← postos e telemetria da ANA (independente)
  ingest_inmet           ← CSVs do INMET enviados pelo script local (independente)

  backfill_archive  ← regrava historical_<ano>.json até hoje − 5 dias (atraso do Archive)
       │
  dlt_pipeline      ← a silver prioriza historical_* sobre forecast_* na mesma hora
       │
  rebuild_features  ← reconstrói gold.rain_features (bundle de ML)

  enso_impacto      ← estudo do ENSO e estação chuvosa (depois de collect_enso, collect_ana, ingest_inmet e dlt_pipeline)
```

Ele fecha sozinho buracos deixados por falhas do job de hora em hora e troca, nas horas
recentes, o "passado" da API de previsão pelos dados do Archive — a mesma fonte
usada no treino do modelo.

### Alertas

- **Falha** de qualquer job: e-mail do Databricks (`email_notifications.on_failure`).
- **Job de 1h lento** (> 30 min; o normal é ~6 min): e-mail de duração (`health`).
- **Algo parou** sem falhar: o job `[weather] Vigia (a cada 3 h)` (`14_vigia`) falha,
  e o e-mail de falha sai, quando há mais de 2,5 h sem previsão nova da Open-Meteo,
  sem execução completa do job de 1h ou sem sinal do celular, mais de 7 h sem a
  previsão dos 6 modelos (MOS, bundle de ML) ou mais de 8 dias sem o backfill semanal. Cada problema alerta uma vez e repete a cada 12 h
  (`gold.vigia_alertas`). Para testar o e-mail: rode o job com o parâmetro
  `simular=<texto>`.
- **Limite:** se o Databricks bloquear todas as execuções do workspace (como de 02 a
  07/11/2026), o vigia também não roda e nada avisa.

### Fontes de dados

| Fonte | O que traz | Tabela |
|---|---|---|
| Open-Meteo Archive (ERA5/IFS) | Série horária desde 1940 — temperatura, chuva, umidade, vento + orvalho, pressão, nuvens por camada, direção do vento, vapor d'água na coluna e camada limite | `silver.weather_clean` |
| Open-Meteo Forecast | Previsão de 7 dias (mesmas variáveis + chance de chuva e rajadas) a cada hora | `silver.weather_forecast`, `silver.forecast_emitida` |
| Open-Meteo Previous Runs | O que cada rodada previu 0, 1 e 2 dias antes (desde 2024) | `silver.forecast_previous_runs` |
| NOAA CPC | Índice ONI (El Niño / La Niña), global | `silver.enso_oni` |
| NOAA GHCN-Daily | Chuva medida em pluviômetros num raio de 20 km (em Birigui, 1939–1997); são os mesmos postos da ANA, que vão até 2024 | `silver.gauge_daily` |
| ANA (postos pluviométricos) | Chuva medida por dia em 27 postos num raio de 20 km; em Birigui, desde 1939 (chega à ANA com ~2 anos de atraso) | `silver.ana_diaria`, `silver.ana_estacoes` |
| ANA (estações telemétricas) | Chuva a cada 10–30 min em estações num raio de 30 km, com qualidade irregular (ver abaixo) | `silver.ana_chuva_horaria`, `gold.ana_telemetria_qc` |
| INMET (avisos meteorológicos) | Avisos oficiais repassados pela Defesa Civil (tempestade, chuvas intensas…) com municípios (IBGE), severidade, início/fim, riscos e polígono — coletados a cada hora | `silver.inmet_avisos`, `gold.avisos_cidade`, `gold.avisos_vs_modelo` |
| INMET (estações automáticas) | Chuva, temperatura, orvalho, pressão, umidade e vento **medidos**, por hora, nas estações num raio de 80 km (José Bonifácio 49 km, Valparaíso 61 km, Tupã 73 km, Lins 75 km), desde 2006 | `silver.inmet_hourly` |
| MERGE/CPTEC (INPE) | Chuva horária em grade de 10 km (satélite GPM-IMERG + pluviômetros), nos pontos de Birigui e das estações do INMET, desde jun/2023. **Não serve de rótulo para Birigui** (ver abaixo); coletado só sob demanda (`15_collect_merge`, sem agendamento) | `silver.merge_hourly` |

#### Chuva medida em Birigui: o que existe

A cidade não tem pluviômetro automático. Em nov/2026:

- **CEMADEN:** nenhuma estação em Birigui nem nas cidades vizinhas (a de Lins está
  parada desde 2016).
- **INMET:** as estações ficam a 49–75 km, e José Bonifácio e Valparaíso estão sem
  chuva válida desde jul/2026.
- **ANA:** 5 postos a até 6 km, mas leitura diária e com ~2 anos de atraso (dados
  até jan/2024). Servem para validar, não para operar.
- **MERGE/CPTEC:** nos pontos das estações do INMET, bate com elas hora a hora
  (correlação 0,77), porque as usa. Contra os 5 postos da ANA em Birigui, que são
  independentes (243 dias, jun/2023–jan/2024), fica pior que o Open-Meteo:
  correlação diária 0,37 (Open-Meteo 0,57), capta 45% do volume (Open-Meteo 80%) e
  marca chuva em 14% dos dias (postos 29%, Open-Meteo 30%). Longe de pluviômetro,
  ele é praticamente satélite. Detalhes em `gold.merge_validacao` (experimento 15
  do bundle de ML).

Conclusão: no **dia**, o Open-Meteo acompanha bem os postos (dias com chuva 30%
contra 29%); o exagero dele é na **hora** (chuva em 12% das horas, contra 3–5% nos
pluviômetros), ao espalhar a chuva do dia em muitas horas fracas.

#### ANA: chuva medida em Birigui (postos e telemetria)

O `13_collect_ana` (job semanal) usa o web service público da ANA, que responde de
dentro do Databricks:

- **Postos pluviométricos** num raio de 20 km (`silver.ana_diaria`): 27 postos, 23
  com dados. O de Birigui (`02150015`, DAEE/SP Águas) mede desde 1939, e outros oito
  seguem medindo até 2023–24. São leituras diárias, brutas, que chegam à ANA com
  cerca de 2 anos de atraso. Os pluviômetros do GHCN (`silver.gauge_daily`) são estes
  mesmos postos, só até 1997 (99,95% dos dias iguais).
- **Estações telemétricas** num raio de 30 km (`silver.ana_chuva_horaria`): chuva a
  cada 10 a 30 min, somada por hora. Birigui Urbano mandou dados de 2013 a 2021;
  Coroados (5 km) manda desde mar/2025 e Buritama (24 km) desde 2016.

O que os dados mostram:

- **O Open-Meteo acerta o volume do mês em Birigui:** contra o posto da cidade, a
  correlação mensal é 0,83 a 0,85 desde 1979, e o total bate (razão 1,01 a 1,06).
- **Mas marca chuva em horas demais:** 13–14% das horas, contra 3,5–4,5% nas estações
  telemétricas, como já tinha aparecido no INMET.
- **A telemetria da região falha:** Coroados mandou 0 mm em meses de chuva (jan, mar e
  jun/2026), e Buritama registrou 659 mm num único dia (13/nov/2025), quase 4 vezes a
  chuva de 100 anos. Horas acima da chuva de 100 anos da IDF de Birigui (114 mm em
  1 h, 174 mm em 24 h) ficam marcadas como `suspeita`, e a view `gold.ana_telemetria_qc`
  mostra a situação de cada estação por mês. Por isso a telemetria ainda não alimenta
  nenhuma análise nem o painel.

#### INMET: por que um coletor local (celular Android)

O INMET recusa conexões vindas de IPs de nuvem (o Databricks não consegue
conectar) e de clientes sem User-Agent de navegador. Por isso a coleta roda num
celular Android na tomada, no Wi-Fi de casa, com Termux + cron — instalação em
[`scripts/termux/README.md`](scripts/termux/README.md). O envio ao Databricks
usa a API REST (`scripts/dbx.py`).

- `scripts/inmet_sync.py` baixa os pacotes anuais de
  [portal.inmet.gov.br/dadoshistoricos](https://portal.inmet.gov.br/dadoshistoricos)
  (atualizados ~1x por mês), extrai as estações do raio e envia os CSVs para o
  Volume `bronze.inmet`; só reprocessa um ano quando o pacote muda.
- O cron do celular roda o `inmet_sync.py --ingerir` diariamente (só baixa quando
  o pacote muda).
- `10_ingest_inmet` (task `ingest_inmet` do job semanal) gera `silver.inmet_hourly`
  em hora local.
- Avisos: `scripts/inmet_avisos_sync.py` (cron do celular, a cada hora) baixa os
  avisos ativos e futuros da UF, envia
  um snapshot ao Volume `bronze.inmet_avisos` quando algo muda e dispara o
  `11_ingest_avisos`, que também roda no job semanal como reforço.

Dados horários quase em tempo real exigiriam a API do INMET com token
(solicitado por e-mail a cadastro.act@inmet.gov.br), também a partir da rede local.

As variáveis horárias e o modelo do Archive ficam em `config.py`
(`HOURLY_VARS`, `ARCHIVE_MODEL`).

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

## ENSO em Birigui — o que o El Niño muda aqui?

O experimento 08 do bundle de ML já tinha mostrado que o ONI não ajuda a prever
a próxima hora: o ENSO age na escala de meses. O `12_enso_impacto` mede o efeito
nessa escala, em três fontes independentes:

- modelo (ERA5, 1950→hoje)
- postos pluviométricos da ANA num raio de 20 km (1950–2024; os do GHCN são os mesmos, só até 1997)
- 4 estações do INMET (2007→hoje)

**Como**

- **Variáveis por trimestre móvel** (DJF…NDJ, mesma chave do ONI): chuva, dias com
  chuva, dias com ≥ 20 mm, maior sequência seca, média das máximas e dias com máxima
  ≥ 35 °C.
- **Anomalias sem a tendência de longo prazo:** o aquecimento e a secagem não viram
  "efeito do ENSO".
- **Cada fase × demais anos:** intervalo de confiança de 90% (bootstrap), p-valor
  por permutação e q-valor de Benjamini–Hochberg, porque são centenas de testes.
- **Estação chuvosa:** início e fim pelo método de Liebmann et al. (2007), a partir
  da chuva acumulada acima da média diária.
- **Previsibilidade:** o ONI já publicado antes do trimestre (0 a 6 meses) prevê o
  tercil (abaixo, normal ou acima)? Teste retroativo deixando cada ano de fora,
  medido pelo RPSS contra a climatologia.

**Resultados** (out/2026)

| Pergunta | Resposta |
|---|---|
| El Niño muda a chuva do verão (nov–mar)? | Não: −6% a +3% nas três fontes |
| E a da estação seca (abr–set)? | Chove mais com El Niño: +17% a +39% no modelo, +20% a +28% nos pluviômetros e +33% a +73% no INMET (só 3 anos). Em mm é pouco: +16 a +54 mm por trimestre |
| E a La Niña? | Fim da estação seca e primavera (jul–nov) um pouco mais secos: −8% a −21% no modelo e nos pluviômetros |
| Calor? | El Niño moderado ou forte: máximas +0,3 a +0,4 °C e cerca de 1 dia a mais com ≥ 35 °C por trimestre, de set a jan; dentro do ruído |
| Estação chuvosa? | O início não muda com o ENSO. No fim e na duração, modelo e pluviômetros discordam até no sinal |
| Algum resultado sobrevive à correção? | Não: das 745 comparações, 54 têm p < 0,05, um pouco mais do que o acaso daria (~37), mas nenhuma tem q < 0,10 |
| Dá para prever o trimestre pelo ONI? | Não: 0 de 490 combinações (trimestre × variável × antecedência) batem a climatologia; o melhor RPSS é 0,07 |
| A previsão erra mais com El Niño? | Até agora, não. Na API Open-Meteo, o F1 é 0,49 com El Niño e 0,50 sem, na rodada de 1 dia antes; na de 2 dias antes, 0,46 e 0,49. A comparação usa os 10 meses do calendário com dados nas duas situações (El Niño em jan–abr/2024 e mai–out/2026). O champion ainda não tem meses fora do El Niño |

Por isso o painel não traz uma "previsão sazonal pelo ENSO". Ele mostra a estação
em curso contra a normal e contra os anos de ONI parecido, com a ressalva de que o
efeito aqui é pequeno. Birigui fica na faixa de transição entre o Sul do Brasil,
onde o El Niño traz mais chuva, e o Norte/Nordeste, onde traz menos.

> 📌 **Lembrete para a expansão a outras cidades:** "o ENSO muda pouco o clima" vale
> para Birigui, não em geral. No Sul (mais chuva com El Niño) e no Norte/Nordeste
> (seca com El Niño) o sinal é forte, e lá a previsão sazonal pelo ONI, descartada
> aqui, pode passar no teste. Ao adicionar uma cidade:
>
> - rodar o `12_enso_impacto` para ela; o notebook usa a cidade do `config.py`, e as
>   saídas têm a coluna `cidade`;
> - olhar a `gold.enso_previsibilidade` antes de decidir se o painel dessa cidade
>   ganha uma perspectiva sazonal.
>
> Hoje o notebook regrava as tabelas inteiras, como o resto do pipeline. Com várias
> cidades, a gravação passa a ser por cidade (`replaceWhere`).

**Acompanhamento da estação** (`gold.estacao_vs_analogos`): view com a chuva e os
dias com ≥ 35 °C acumulados desde 1/jul. A estação atual é comparada com:

- a normal de 1991–2020 (os 10% mais secos, a mediana e os 10% mais chuvosos)
- os 5 anos cujo ONI, no mesmo trimestre, foi o mais próximo do atual

Como é uma view, acompanha a `silver.weather_daily` a cada hora. O início da estação
chuvosa (`gold.estacao_chuvosa`) é recalculado no job semanal. Na estação em curso
ele é provisório e só aparece quando a chuva acumulada já passou 50 mm acima da média
desde o mínimo.

Em out/2026 havia um El Niño muito forte: o ONI de JAS chegou a +2,16, o maior para
esse trimestre desde 1950. Os anos mais parecidos (1997–98 e 2015–16) estão bem
abaixo, então a comparação com eles é extrapolação.

**A previsão erra mais com El Niño?** (`gold.acuracia_por_enso`): view com o acerto
mensal de "vai chover na hora" (rótulo Open-Meteo) e a fase do ENSO de cada mês.
Cobre a API Open-Meteo (previsões arquivadas desde 2024, rodadas de 1 e 2 dias
antes) e o champion (histórico de inferência). A rodada do próprio dia fica de fora,
porque é a mesma série do rótulo observado.

A comparação só usa meses do calendário que têm dados com e sem El Niño: o acerto
muda muito entre a estação seca e a chuvosa. **Rever em mar/2027:** até lá, o
champion acumula o primeiro El Niño forte do histórico, e a comparação dele
aparece no painel assim que houver meses sem El Niño.

**Cuidados**

- **Estação chuvosa ano a ano:** o início calculado concorda pouco entre as fontes
  (correlação 0,35 do modelo com os postos da ANA e ~0,3 com o INMET). O valor de cada ano
  é indicativo.
- **INMET em 2026:** José Bonifácio e Valparaíso não têm chuva válida desde jul/2026,
  e Lins quase não tem. Por isso a estação em curso usa só o modelo, e o INMET entra
  apenas em anos com dados quase completos.

---

## Dashboard — Previsão de Chuva · Birigui

Painel Lakeview (AI/BI) com 3 páginas. A definição fica versionada em
[`dashboards/previsao_chuva_birigui.lvdash.json`](dashboards/previsao_chuva_birigui.lvdash.json)
(export do painel publicado; ao mudar o painel na interface, exporte de novo).

| Página | O que mostra |
|---|---|
| **Agora** | Chance de chuva da Open-Meteo (próxima hora e próximas 6 h), champion e regional no limiar de cada um, amanhã com chuva medida pela combinação de 6 modelos (MOS), máx/mín, avisos do INMET, status do pipeline e do celular, chuva no mês e em 7 dias, próximas 48 h (mm e chance), próximos 7 dias (máx, mín, chuva, chance, rajada), temperatura de hoje × normal e a estação chuvosa |
| **Modelo** | Modelo × realidade (−6 h a +18 h), desempenho em produção, no teste e contra chuva medida, acurácia por antecedência, a tabela hora a hora e as versões do MOS multimodelo |
| **Clima e ENSO** | ONI, estação atual × normal × análogos, o estudo do ENSO e a climatologia 1991–2020 (dia com chuva = ≥ 1 mm) |

O status do celular vem do sinal de vida que o `inmet_avisos_sync.py` grava a cada
hora em `/Volumes/weather_pipeline/bronze/coletores/celular.json`: 🔴 depois de
2,5 h sem contato.

---

## Stack técnica

| Tecnologia | Uso |
|------------|-----|
| **Databricks Free Edition** | Ambiente Serverless AWS |
| **Delta Live Tables** | Pipeline declarativo com expectations |
| **Unity Catalog** | Governança + Volumes |
| **Databricks Workflows** | Orquestração de hora em hora + semanal |
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

**Por que a `weather_clean` não tem coluna `is_forecast`?**
Os `forecast_*.json` trazem os últimos 7 dias junto com a previsão, então "é previsão" depende da hora atual, e não do arquivo. Uma coluna calculada com `current_timestamp()` tornava a silver não determinística e obrigava o DLT a recalcular tudo a cada hora. Quem consome filtra na consulta: `observation_time <= from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')` para o observado e `>` para a previsão (`observation_time` é hora local, sem fuso).

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
