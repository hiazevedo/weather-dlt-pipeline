# Databricks notebook source
# Backfill semanal com a Open-Meteo Archive API (ERA5)
#
# Regrava historical_<ano>.json do ano corrente (e do anterior, na virada do
# ano) até hoje − 5 dias (atraso do Archive). Efeitos:
#   - fecha sozinho buracos deixados por falhas do job de 6 h;
#   - troca, nas horas recentes, o "passado" da API de previsão pelos dados
#     do Archive (ERA5) — a mesma fonte usada no treino do modelo.
# Na silver, weather_clean dá prioridade a arquivos historical_* sobre forecast_*.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import requests
import json
from datetime import datetime, timezone, date, timedelta

ARCHIVE_DELAY_DIAS = 5   # o Archive publica com ~5 dias de atraso
JANELA_DIAS        = 35  # garante cobrir o mês anterior inteiro na virada do ano

fim    = date.today() - timedelta(days=ARCHIVE_DELAY_DIAS)
anos   = sorted({(fim - timedelta(days=JANELA_DIAS)).year, fim.year})

print(f"Backfill Archive — {CIDADE}")
print(f"   Até   : {fim}")
print(f"   Anos  : {anos}\n")

# COMMAND ----------

def fetch_archive(start: str, end: str) -> dict:
    """Dados horários da Open-Meteo Archive API (mesmas variáveis do 01_collect_history)."""
    resp = requests.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude":   LAT,
            "longitude":  LON,
            "start_date": start,
            "end_date":   end,
            "hourly":     ",".join([
                "temperature_2m",
                "apparent_temperature",
                "precipitation",
                "rain",
                "relative_humidity_2m",
                "windspeed_10m"
            ]),
            "timezone":   TIMEZONE
        },
        timeout=API_TIMEOUT * 4
    )
    resp.raise_for_status()
    return resp.json()


for ano in anos:
    start = f"{ano}-01-01"
    end   = min(date(ano, 12, 31), fim).isoformat()
    data  = fetch_archive(start, end)

    horas = data["hourly"]["time"]
    nulos = sum(p is None for p in data["hourly"]["precipitation"])
    # Não sobrescreve um arquivo bom com uma resposta vazia ou incompleta
    if not horas or nulos > 24:
        raise RuntimeError(f"Resposta inválida do Archive para {ano}: "
                           f"{len(horas)} horas, {nulos} precipitações nulas")

    payload = {
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "cidade":       CIDADE,
        "latitude":     LAT,
        "longitude":    LON,
        "data":         data
    }
    dbutils.fs.put(f"{RAW_PATH}/historical_{ano}.json", json.dumps(payload), overwrite=True)
    print(f"   historical_{ano}.json: {len(horas):,} horas ({horas[0]} → {horas[-1]})")

print("\nBackfill concluído — próximas tasks: DLT e reconstrução das features")
