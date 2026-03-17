# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime
import pytz

# COMMAND ----------

# Criar tabela gold.weather_today para o dashboard

print("Construindo gold.weather_today...\n")

TZ_BR    = pytz.timezone("America/Sao_Paulo")
hoje     = datetime.now(TZ_BR).strftime("%Y-%m-%d")
agora_h  = datetime.now(TZ_BR).hour

print(f"   Data hoje  : {hoje}")
print(f"   Hora atual : {agora_h}h (Brasília)")

# Pegar forecast mais recente (último arquivo coletado)
df_forecast = spark.sql("""
    SELECT
        observation_time,
        temperature_2m,
        apparent_temperature,
        precipitation,
        rain,
        relative_humidity,
        hour,
        temp_class,
        rain_class,
        _ingested_at
    FROM weather_pipeline.silver.weather_forecast
    WHERE DATE(observation_time) = CURRENT_DATE()
       OR DATE(observation_time) = DATE_ADD(CURRENT_DATE(), 1)
""")

# Pegar dados históricos de hoje dos anos anteriores (climatologia)
df_clima_hoje = spark.sql(f"""
    SELECT
        hour,
        ROUND(AVG(temperature_2m),  2) AS temp_climatologica,
        ROUND(AVG(precipitation),   2) AS precip_climatologica,
        ROUND(AVG(relative_humidity),2) AS humidity_climatologica
    FROM weather_pipeline.silver.weather_clean
    WHERE month = MONTH(CURRENT_DATE())
      AND day   = DAY(CURRENT_DATE())
      AND year BETWEEN 1991 AND 2020
      AND is_forecast = false
    GROUP BY hour
    ORDER BY hour
""")

# Juntar previsão com climatologia histórica
df_today = (
    df_forecast
    .join(df_clima_hoje, on="hour", how="left")
    .withColumn("is_past",
        F.when(F.col("hour") < agora_h, True).otherwise(False))
    .withColumn("acima_climatologia",
        F.when(
            F.col("temperature_2m") > F.col("temp_climatologica"),
            True).otherwise(False))
    .orderBy("observation_time")
)

# Salvar como tabela Gold
df_today.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("weather_pipeline.gold.weather_today")

total = df_today.count()
print(f"gold.weather_today: {total} registros")

# COMMAND ----------

# Criar tabela gold.rain_alert (responde: vai chover?)
print("Calculando alertas de chuva...\n")

spark.sql("""
    CREATE OR REPLACE TABLE weather_pipeline.gold.rain_alert AS
    SELECT
        DATE(observation_time)              AS data,
        HOUR(observation_time)              AS hora,
        observation_time,
        temperature_2m,
        apparent_temperature,
        precipitation,
        relative_humidity,
        rain_class,

        -- Probabilidade de chuva por faixa de umidade + precipitação
        CASE
            WHEN precipitation >= 10  THEN 'CHUVA FORTE 🌧️'
            WHEN precipitation >= 2.5 THEN 'CHUVA MODERADA 🌦️'
            WHEN precipitation >= 0.1 THEN 'CHUVA FRACA 🌂'
            WHEN relative_humidity > 85
             AND temperature_2m > 25  THEN 'POSSÍVEL CHUVA ⚠️'
            ELSE                           'SEM CHUVA ☀️'
        END AS previsao_chuva,

        CASE
            WHEN precipitation >= 2.5            THEN 3
            WHEN precipitation >= 0.1            THEN 2
            WHEN relative_humidity > 85
             AND temperature_2m > 25             THEN 1
            ELSE                                      0
        END AS nivel_alerta,  -- 0=ok, 1=atenção, 2=chuva, 3=chuva forte

        _ingested_at AS ultima_atualizacao

    FROM weather_pipeline.silver.weather_forecast
    WHERE DATE(observation_time) = CURRENT_DATE()
    ORDER BY observation_time
""")

# Resumo do dia
resumo = spark.sql("""
    SELECT
        COUNT(*)                                    AS total_horas,
        SUM(CASE WHEN nivel_alerta >= 2 THEN 1 ELSE 0 END)
                                                    AS horas_com_chuva,
        ROUND(SUM(precipitation), 2)                AS precip_total_prevista,
        MAX(temperature_2m)                         AS temp_max_prevista,
        MIN(temperature_2m)                         AS temp_min_prevista,
        MAX(CASE WHEN nivel_alerta >= 2
            THEN hora END)                          AS ultima_hora_chuva
    FROM weather_pipeline.gold.rain_alert
""").collect()[0]

vai_chover = resumo["horas_com_chuva"] > 0

print(f"""
PREVISÃO DO DIA — BIRIGUI-SP                                      
VAI CHOVER HOJE? {'SIM' if vai_chover else 'NÃO'}          
---------------------------------
Precipitação prevista : {resumo['precip_total_prevista']:>6.1f} mm
Horas com chuva       : {resumo['horas_com_chuva']:>6} h          
Temperatura máx       : {resumo['temp_max_prevista']:>6.1f} °C    
Temperatura mín       : {resumo['temp_min_prevista']:>6.1f} °C    
""")