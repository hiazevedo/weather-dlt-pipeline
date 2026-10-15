# Databricks notebook source
# Índice ENSO (El Niño / La Niña) — ONI da NOAA CPC
#
# ONI = anomalia da TSM no Niño 3.4 (média móvel de 3 meses, base climatológica
# móvel). Fonte: https://www.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt
#
# É um índice global (não depende da cidade): serve como feature sazonal para
# todas as cidades e para o painel. Tabela: weather_pipeline.silver.enso_oni

# COMMAND ----------

import io
import requests
import pandas as pd

ONI_URL   = "https://www.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt"
ENSO_TABLE = "weather_pipeline.silver.enso_oni"

# Estação de 3 meses → mês central (o ano YR do arquivo é o do mês central:
# "DJF 1950" = dez/49–fev/50, centro jan/50; "NDJ 1950" = nov/50–jan/51, centro dez/50)
CENTRO = {"DJF": 1, "JFM": 2, "FMA": 3, "MAM": 4, "AMJ": 5, "MJJ": 6,
          "JJA": 7, "JAS": 8, "ASO": 9, "SON": 10, "OND": 11, "NDJ": 12}

# COMMAND ----------

resp = requests.get(ONI_URL, timeout=60)
resp.raise_for_status()
df = pd.read_csv(io.StringIO(resp.text), sep=r"\s+")
df.columns = ["season", "year", "sst_total", "oni"]
assert set(df["season"]) <= set(CENTRO), f"estação desconhecida: {set(df['season']) - set(CENTRO)}"

centro = pd.to_datetime(dict(year=df["year"], month=df["season"].map(CENTRO), day=1))
df["center_month"]    = centro.dt.date
df["end_month"]       = (centro + pd.DateOffset(months=1)).dt.date
# Disponível a partir do mês seguinte ao último mês da estação (a CPC publica
# no início do mês seguinte). Usar este campo para juntar com dados horários
# evita usar um índice que ainda não existia na época (vazamento).
df["available_month"] = (centro + pd.DateOffset(months=2)).dt.date

# COMMAND ----------

# Fase por estação (limiar ±0,5 °C) e intensidade (convenção usual)
def fase(x):
    return "El Niño" if x >= 0.5 else "La Niña" if x <= -0.5 else "Neutro"

def intensidade(x):
    a = abs(x)
    if a < 0.5:  return None
    if a < 1.0:  return "fraco"
    if a < 1.5:  return "moderado"
    if a < 2.0:  return "forte"
    return "muito forte"

df["fase"]        = df["oni"].apply(fase)
df["intensidade"] = df["oni"].apply(intensidade)

# Episódio oficial: ao menos 5 estações consecutivas na mesma fase
run_id = (df["fase"] != df["fase"].shift()).cumsum()
run_len = df.groupby(run_id)["fase"].transform("size")
df["episodio"] = (df["fase"] != "Neutro") & (run_len >= 5)
# A sequência em curso ainda pode completar 5 estações: marca como "em curso"
ultimo = run_id == run_id.iloc[-1]
df["em_curso"] = ultimo & (df["fase"] != "Neutro")

cols = ["season", "year", "center_month", "end_month", "available_month",
        "sst_total", "oni", "fase", "intensidade", "episodio", "em_curso"]
(spark.createDataFrame(df[cols])
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(ENSO_TABLE))

ult = df.iloc[-1]
print(f"{ENSO_TABLE}: {len(df)} estações ({df['season'].iloc[0]} {df['year'].iloc[0]} → "
      f"{ult['season']} {ult['year']})")
print(f"Atual: {ult['season']}/{ult['year']} ONI {ult['oni']:+.2f} — {ult['fase']} "
      f"{ult['intensidade'] or ''}")
