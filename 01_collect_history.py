# Databricks notebook source

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import requests
import json
from datetime import datetime, timezone, date, timedelta
import time

print(f"   Coleta meteorológica — {CIDADE}")
print(f"   Lat/Lon : {LAT}, {LON}")
print(f"   Volume  : {RAW_PATH}")

# COMMAND ----------

# Funções de coleta

def fetch_historical(start: str, end: str) -> dict:
    """Coleta dados históricos horários da Open-Meteo Archive API."""
    resp = requests.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude":             LAT,
            "longitude":            LON,
            "start_date":           start,
            "end_date":             end,
            "hourly":               ",".join([
                "temperature_2m",
                "apparent_temperature",
                "precipitation",
                "rain",
                "relative_humidity_2m",
                "windspeed_10m"
            ]),
            "timezone":             TIMEZONE
        },
        timeout=API_TIMEOUT
    )
    resp.raise_for_status()
    return resp.json()


def fetch_forecast() -> dict:
    """Coleta previsão dos próximos 7 dias."""
    resp = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude":       LAT,
            "longitude":      LON,
            "hourly":         ",".join([
                "temperature_2m",
                "apparent_temperature",
                "precipitation",
                "rain",
                "relative_humidity_2m",
                "windspeed_10m"
            ]),
            "daily":          ",".join([
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
                "rain_sum"
            ]),
            "forecast_days":  FORECAST_DAYS,
            "timezone":       TIMEZONE
        },
        timeout=API_TIMEOUT
    )
    resp.raise_for_status()
    return resp.json()


def save_json(data: dict, filename: str) -> None:
    """Salva um dict como JSON no Volume UC."""
    payload = {
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "cidade":       CIDADE,
        "latitude":     LAT,
        "longitude":    LON,
        "data":         data
    }
    filepath = f"{RAW_PATH}/{filename}"
    dbutils.fs.put(filepath, json.dumps(payload), overwrite=True)

print("Funções definidas")

# COMMAND ----------

# Coletar TODO o histórico desde 1940 (janelas anuais)

print("Coletando histórico completo 1940→2026...\n")

hoje         = date.today()
ANO_INICIO   = HIST_ANO_INICIO
ANO_FIM      = hoje.year

# Gerar janelas anuais
janelas = []
for ano in range(ANO_INICIO, ANO_FIM + 1):
    start = f"{ano}-01-01"
    # Último ano: até hoje - 5 dias (delay do Archive)
    if ano == ANO_FIM:
        end = (hoje - timedelta(days=5)).isoformat()
    else:
        end = f"{ano}-12-31"
    janelas.append((start, end))

print(f"   Janelas : {len(janelas)} anos ({ANO_INICIO}→{ANO_FIM})")
print(f"   Delay   : {API_DELAY_SEG}s entre chamadas")
print(f"   Tempo   : ~{len(janelas) * API_DELAY_SEG / 60:.0f} minutos\n")

total_horas = 0
erros       = 0

for i, (start, end) in enumerate(janelas, 1):
    try:
        print(f"   [{i:02d}/{len(janelas)}] {start[:4]} ...", end=" ")

        data     = fetch_historical(start, end)
        horas    = len(data["hourly"]["time"])
        filename = f"historical_{start[:4]}.json"
        save_json(data, filename)

        total_horas += horas
        print(f"✅ {horas:,} horas")

        time.sleep(API_DELAY_SEG)

    except Exception as e:
        erros += 1
        print(f"❌ ERRO: {type(e).__name__}: {e}")
        time.sleep(5)  # esperar mais em caso de erro

print(f"""
COLETA HISTÓRICA CONCLUÍDA
 - Anos coletados  : {len(janelas) - erros:<5} / {len(janelas):<5}
 - Horas totais    : {total_horas:>10,}
 - Erros           : {erros:<5}
""")

# COMMAND ----------

# Coletar previsão dos próximos 7 dias
print("Coletando previsão 7 dias...\n")

try:
    data     = fetch_forecast()
    filename = f"forecast_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    save_json(data, filename)
    dias = len(data["daily"]["time"])
    print(f"Previsão salva: {dias} dias")
    print(f"   Arquivo: {filename}")
except Exception as e:
    print(f"❌ Erro ao coletar previsão: {type(e).__name__}: {e}")

# COMMAND ----------

# Verificar arquivos coletados
print("Arquivos no Volume:\n")

files      = dbutils.fs.ls(RAW_PATH)
total_mb   = sum(f.size for f in files) / 1024 / 1024
historicos = [f for f in files if "historical" in f.name]
forecasts  = [f for f in files if "forecast"   in f.name]

for f in sorted(files, key=lambda x: x.name):
    tipo = "histórico" if "historical" in f.name else "🔮 previsão"
    print(f"   {tipo} | {f.name:<45} {f.size/1024:>8.1f} KB")

print(f"""
COLETA CONCLUÍDA
 - Arquivos históricos : {len(historicos):<5}
 - Arquivos previsão   : {len(forecasts):<5}
 - Tamanho total       : {total_mb:<6.2f} MB
""")
