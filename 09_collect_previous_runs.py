# Databricks notebook source
# Previsões arquivadas com antecedência — Open-Meteo Previous Runs API
#
# Para cada hora, o valor que o modelo previu na rodada mais recente (day0)
# e nas rodadas de 1 e 2 dias antes (day1, day2). É a base para treinar um
# modelo que corrige a previsão (MOS) com o mesmo tipo de dado usado em
# produção — hoje o modelo é treinado com dados observados e aplicado sobre
# previsões.
#
# Obs.: a Historical Forecast API NÃO serve para isso — ela junta só as
# primeiras horas de cada rodada (é quase uma análise, sem antecedência).
#
# Tabela: weather_pipeline.silver.forecast_previous_runs

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import json, time
from datetime import date, timedelta
import pandas as pd
import requests
from pyspark.sql import functions as F

VARS  = ["precipitation", "temperature_2m", "apparent_temperature", "relative_humidity_2m",
         "dew_point_2m", "pressure_msl", "cloud_cover", "wind_speed_10m", "wind_direction_10m"]
LEADS = [0, 1, 2]
INICIO = date(2024, 1, 1)   # a maioria dos modelos é arquivada a partir de 2024
spark.sql("CREATE VOLUME IF NOT EXISTS weather_pipeline.bronze.previous_runs")
RAW   = "/Volumes/weather_pipeline/bronze/previous_runs"
TABLE = "weather_pipeline.silver.forecast_previous_runs"

hourly = [v if l == 0 else f"{v}_previous_day{l}" for v in VARS for l in LEADS]

def fetch(start, end, tentativas=6):
    for i in range(tentativas):
        r = requests.get("https://previous-runs-api.open-meteo.com/v1/forecast", params={
            "latitude": LAT, "longitude": LON, "start_date": start, "end_date": end,
            "hourly": ",".join(hourly), "timezone": TIMEZONE}, timeout=180)
        if r.status_code == 429:
            time.sleep(60 * (i + 1)); continue
        r.raise_for_status()
        return r.json()["hourly"]
    raise RuntimeError(f"Previous Runs recusou {start}..{end}")

# COMMAND ----------

# Um arquivo por mês; meses fechados já baixados são reaproveitados
existentes = {f.name for f in dbutils.fs.ls(RAW)}
ontem = date.today() - timedelta(days=1)
mes = INICIO
while mes <= ontem:
    prox = (mes.replace(day=28) + timedelta(days=4)).replace(day=1)
    nome = f"prev_runs_{mes:%Y_%m}.json"
    fechado = prox <= ontem
    if not (nome in existentes and fechado):
        h = fetch(mes.isoformat(), min(prox - timedelta(days=1), ontem).isoformat())
        dbutils.fs.put(f"{RAW}/{nome}", json.dumps(h), overwrite=True)
        time.sleep(2)
    mes = prox

# COMMAND ----------

frames = [pd.DataFrame(json.loads(dbutils.fs.head(f.path, 50_000_000))) for f in dbutils.fs.ls(RAW)]
df = pd.concat(frames, ignore_index=True).drop_duplicates("time")
# nomes curtos: <variável>_d<antecedência em dias>
df = df.rename(columns={c: (f"{c}_d0" if c in VARS else
                            c.replace("_previous_day", "_d")) for c in df.columns if c != "time"})
# descarta o início sem arquivo (antes de a rodada de 1 dia antes existir)
df = df[df["precipitation_d1"].notna()]

(spark.createDataFrame(df)
    .withColumn("observation_time", F.to_timestamp("time")).drop("time")
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE))

print(f"{TABLE}: {len(df):,} horas ({df['time'].min()} → {df['time'].max()})")
print("nulos por coluna (%):", (df.isna().mean() * 100).round(1).to_dict())
