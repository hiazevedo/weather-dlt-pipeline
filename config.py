# Databricks notebook source
# Configurações globais — Weather Pipeline Birigui-SP
# Execute com: %run ./config

# COMMAND ----------

# Localização
LAT     = -21.2878
LON     = -50.3409
CIDADE  = "Birigui-SP"
TIMEZONE = "America/Sao_Paulo"

# Paths
RAW_PATH = "/Volumes/weather_pipeline/bronze/raw_json"
CATALOG  = "weather_pipeline"

# COMMAND ----------

# Janela climatológica — Normal Climatológica WMO
CLIMA_ANO_INICIO = 1991
CLIMA_ANO_FIM    = 2020

# Série histórica
HIST_ANO_INICIO  = 1940

# COMMAND ----------

# Thresholds de temperatura (°C)
TEMP_MUITO_QUENTE = 35
TEMP_QUENTE       = 28
TEMP_AGRADAVEL    = 20
TEMP_FRESCO       = 12

# Thresholds de chuva (mm/h)  — INMET/WMO
CHUVA_FORTE     = 10.0
CHUVA_MODERADA  =  2.5
CHUVA_FRACA     =  0.1

# COMMAND ----------

# Thresholds de alerta de chuva
# Base: umidade > 85% + temp > 25°C indica condição favorável à convecção
HUMIDITY_ALERT = 85
TEMP_ALERT     = 25.0

# COMMAND ----------

# Configurações de API
API_TIMEOUT   = 30   # segundos
API_DELAY_SEG =  2   # delay entre chamadas (rate limit Open-Meteo)
FORECAST_DAYS =  7   # janela de previsão

# COMMAND ----------

print(f"config carregado — {CIDADE} ({LAT}, {LON})")
