# Databricks notebook source
# Configurações globais — Weather Pipeline Birigui-SP
# Execute com: %run ./config

# COMMAND ----------

# Localização
LAT     = -21.2878
LON     = -50.3409
CIDADE  = "Birigui-SP"
CIDADE_IBGE = "3506508"   # código IBGE do município (avisos do INMET são por município)
UF      = "SP"
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

# Variáveis horárias coletadas (Archive e Forecast usam os mesmos nomes).
# As extras (orvalho, pressão, nuvens por camada, direção do vento, vapor
# d'água na coluna, camada limite) melhoraram o modelo — experimento
# 08_experiment_extra_features do bundle de ML.
HOURLY_VARS_BASE  = ["temperature_2m", "apparent_temperature", "precipitation", "rain",
                     "relative_humidity_2m", "windspeed_10m"]
HOURLY_VARS_EXTRA = ["dew_point_2m", "pressure_msl", "cloud_cover", "cloud_cover_low",
                     "cloud_cover_mid", "cloud_cover_high", "wind_direction_10m",
                     "total_column_integrated_water_vapour", "boundary_layer_height"]
HOURLY_VARS = HOURLY_VARS_BASE + HOURLY_VARS_EXTRA

# Modelo do Archive para a série histórica: "best_match" (ERA5 até 2017 e
# IFS a partir de 2018) ou "era5" (fonte única) — ver 10_experiment_fonte_historica
ARCHIVE_MODEL = "best_match"

# COMMAND ----------

# Configurações de API
API_TIMEOUT   = 30   # segundos
API_DELAY_SEG =  2   # delay entre chamadas (rate limit Open-Meteo)
FORECAST_DAYS =  7   # janela de previsão

# COMMAND ----------

print(f"config carregado — {CIDADE} ({LAT}, {LON})")
