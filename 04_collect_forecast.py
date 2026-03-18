# Databricks notebook source

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import requests
import json
from datetime import datetime, timezone

# COMMAND ----------

# Coleta previsão atualizada (roda a cada 6h pelo Workflow)
print(f"Coletando previsão atualizada — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")

try:
    resp = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude":      LAT,
            "longitude":     LON,
            "hourly":        ",".join([
                "temperature_2m",
                "apparent_temperature",
                "precipitation",
                "rain",
                "relative_humidity_2m",
                "windspeed_10m"
            ]),
            "daily": ",".join([
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
                "precipitation_probability_max"
            ]),
            "forecast_days": FORECAST_DAYS,
            "past_days":     7,
            "timezone":      TIMEZONE
        },
        timeout=API_TIMEOUT
    )
    resp.raise_for_status()
    data = resp.json()

    # Salvar com timestamp para não sobrescrever (Auto Loader detecta novo arquivo)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename  = f"forecast_{timestamp}.json"
    payload   = {
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "cidade":       CIDADE,
        "latitude":     LAT,
        "longitude":    LON,
        "data":         data
    }
    dbutils.fs.put(f"{RAW_PATH}/{filename}", json.dumps(payload), overwrite=True)

    horas = len(data["hourly"]["time"])
    print(f"Previsão coletada: {horas}h ({filename})")
    print(f"   Temp agora : {data['hourly']['temperature_2m'][0]}°C")
    print(f"   Chuva hoje : {data['daily']['precipitation_sum'][0]}mm previsto")
    print(f"   Prob chuva : {data['daily']['precipitation_probability_max'][0]}%")

except Exception as e:
    print(f"❌ Erro ao coletar previsão: {type(e).__name__}: {e}")
    raise
