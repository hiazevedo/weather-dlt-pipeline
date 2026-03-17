# Databricks notebook source
import requests
import json
from datetime import datetime, timezone

# COMMAND ----------

LAT      = -21.2878
LON      = -50.3409
CIDADE   = "Birigui-SP"
TIMEZONE = "America/Sao_Paulo"
RAW_PATH = "/Volumes/weather_pipeline/bronze/raw_json"

# COMMAND ----------

# Coleta previsão atualizada (roda a cada 6h pelo Workflow)
print(f"Coletando previsão atualizada — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n")

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
            "windspeed_10m",
            "precipitation_probability",  # novo — prob de chuva %
            "weathercode"                 # novo — código de condição
        ]),
        "daily": ",".join([
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "precipitation_probability_max"
        ]),
        "forecast_days": 2,   # hoje + amanhã
        "timezone":      TIMEZONE
    },
    timeout=30
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