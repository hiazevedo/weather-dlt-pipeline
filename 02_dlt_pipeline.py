# Databricks notebook source
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, ArrayType, FloatType
)

# COMMAND ----------

# BRONZE: Leitura raw dos JSONs do Volume
RAW_PATH = "/Volumes/weather_pipeline/bronze/raw_json"

hourly_schema = StructType([
    StructField("time",                  ArrayType(StringType()),  True),
    StructField("temperature_2m",        ArrayType(FloatType()),   True),
    StructField("apparent_temperature",  ArrayType(FloatType()),   True),
    StructField("precipitation",         ArrayType(FloatType()),   True),
    StructField("rain",                  ArrayType(FloatType()),   True),
    StructField("relative_humidity_2m",  ArrayType(FloatType()),   True),
    StructField("windspeed_10m",         ArrayType(FloatType()),   True),
])
data_schema = StructType([
    StructField("hourly", hourly_schema, True),
])
root_schema = StructType([
    StructField("collected_at", StringType(), True),
    StructField("cidade",       StringType(), True),
    StructField("latitude",     DoubleType(), True),
    StructField("longitude",    DoubleType(), True),
    StructField("data",         data_schema,  True),
])

@dlt.table(
    name    = "weather_raw",
    comment = "Dados meteorológicos raw — Birigui-SP 1940→presente",
    table_properties = {
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def weather_raw():
    return (
        spark.read
        .format("json")
        .schema(root_schema)
        .load(RAW_PATH)
        .withColumn("_source_file",  F.col("_metadata.file_path"))
        .withColumn("_ingested_at",  F.current_timestamp())
        .withColumn("is_forecast",
            F.col("_source_file").contains("forecast").cast("boolean"))
    )

# COMMAND ----------

# SILVER: Explode + limpeza + validações DLT
@dlt.table(
    name    = "weather_clean",
    comment = "Dados horários limpos com validações de qualidade",
    table_properties = {"quality": "silver"}
)
@dlt.expect("temperatura_valida",      "temperature_2m BETWEEN -10 AND 50")
@dlt.expect("sensacao_valida",         "apparent_temperature BETWEEN -15 AND 60")
@dlt.expect_or_drop("data_nao_nula",   "observation_time IS NOT NULL")
@dlt.expect_or_fail("chuva_positiva",  "precipitation >= 0")
def weather_clean():
    return (
        dlt.read("weather_raw")

        # Explodir arrays em linhas individuais (uma por hora)
        .withColumn("idx",
            F.explode(F.sequence(
                F.lit(0),
                F.expr("size(data.hourly.time) - 1")
            )))

        # Extrair cada variável pelo índice
        .withColumn("observation_time",
            F.to_timestamp(F.col("data.hourly.time")[F.col("idx")]))
        .withColumn("temperature_2m",
            F.col("data.hourly.temperature_2m")[F.col("idx")])
        .withColumn("apparent_temperature",
            F.col("data.hourly.apparent_temperature")[F.col("idx")])
        .withColumn("precipitation",
            F.col("data.hourly.precipitation")[F.col("idx")])
        .withColumn("rain",
            F.col("data.hourly.rain")[F.col("idx")])
        .withColumn("relative_humidity",
            F.col("data.hourly.relative_humidity_2m")[F.col("idx")])
        .withColumn("windspeed",
            F.col("data.hourly.windspeed_10m")[F.col("idx")])

        # Filtra linhas sem precipitation válido
        .filter(~F.isnan(F.col("precipitation")) & F.col("precipitation").isNotNull())

        # Features temporais
        .withColumn("year",    F.year("observation_time"))
        .withColumn("month",   F.month("observation_time"))
        .withColumn("day",     F.dayofmonth("observation_time"))
        .withColumn("hour",    F.hour("observation_time"))
        .withColumn("weekday", F.dayofweek("observation_time"))
        .withColumn("quarter", F.quarter("observation_time"))

        # Classificações
        .withColumn("temp_class",
            F.when(F.col("temperature_2m") >= 35, "Muito Quente")
             .when(F.col("temperature_2m") >= 28, "Quente")
             .when(F.col("temperature_2m") >= 20, "Agradável")
             .when(F.col("temperature_2m") >= 12, "Fresco")
             .otherwise("Frio"))
        .withColumn("rain_class",
            F.when(F.col("precipitation") == 0,    "Sem chuva")
             .when(F.col("precipitation") <  2.5,  "Chuva fraca")
             .when(F.col("precipitation") <  10,   "Chuva moderada")
             .when(F.col("precipitation") <  50,   "Chuva forte")
             .otherwise("Chuva muito forte"))
        .withColumn("cidade",     F.col("cidade"))
        .withColumn("latitude",   F.col("latitude"))
        .withColumn("longitude",  F.col("longitude"))
        .withColumn("is_forecast",F.col("is_forecast"))

        # Selecionar colunas finais
        .select(
            "observation_time", "cidade", "latitude", "longitude",
            "temperature_2m", "apparent_temperature",
            "precipitation", "rain", "relative_humidity", "windspeed",
            "temp_class", "rain_class", "is_forecast",
            "year", "month", "day", "hour", "weekday", "quarter",
            "_source_file", "_ingested_at"
        )
    )

# COMMAND ----------

# GOLD: Resumo diário histórico
@dlt.table(
    name    = "weather_daily",
    comment = "Resumo diário — temperatura, chuva e umidade",
    table_properties = {"quality": "gold"}
)
def weather_daily():
    return (
        dlt.read("weather_clean")
        .filter(F.col("is_forecast") == False)
        .groupBy("cidade", "year", "month", "day",
                 F.to_date("observation_time").alias("date"))
        .agg(
            F.round(F.max("temperature_2m"),   2).alias("temp_max"),
            F.round(F.min("temperature_2m"),   2).alias("temp_min"),
            F.round(F.avg("temperature_2m"),   2).alias("temp_avg"),
            F.round(F.avg("apparent_temperature"), 2).alias("sensacao_avg"),
            F.round(F.sum("precipitation"),    2).alias("precip_total"),
            F.round(F.sum("rain"),             2).alias("rain_total"),
            F.round(F.avg("relative_humidity"),2).alias("humidity_avg"),
            F.round(F.avg("windspeed"),        2).alias("wind_avg"),
            F.max("rain_class").alias("rain_class_dominante"),
            F.count("*").alias("horas_registradas")
        )
        .withColumn("teve_chuva",
            F.when(F.col("precip_total") > 0, True).otherwise(False))
        .orderBy("date")
    )

# COMMAND ----------

# GOLD: Resumo mensal para análise de tendências
@dlt.table(
    name    = "weather_monthly",
    comment = "Climatologia mensal — médias e totais por mês/ano",
    table_properties = {"quality": "gold"}
)
def weather_monthly():
    return (
        dlt.read("weather_daily")
        .groupBy("cidade", "year", "month")
        .agg(
            F.round(F.avg("temp_max"),      2).alias("temp_max_media"),
            F.round(F.avg("temp_min"),      2).alias("temp_min_media"),
            F.round(F.avg("temp_avg"),      2).alias("temp_media"),
            F.round(F.sum("precip_total"),  2).alias("precip_mensal"),
            F.round(F.avg("humidity_avg"),  2).alias("humidity_media"),
            F.sum(F.col("teve_chuva").cast("int")).alias("dias_com_chuva"),
            F.count("*").alias("dias_registrados")
        )
        .withColumn("precip_media_diaria",
            F.round(F.col("precip_mensal") / F.col("dias_registrados"), 2))
        .orderBy("year", "month")
    )

# COMMAND ----------

# GOLD: Previsão próximos 7 dias
@dlt.table(
    name    = "weather_forecast",
    comment = "Previsão horária dos próximos 7 dias",
    table_properties = {"quality": "gold"}
)
def weather_forecast():
    from pyspark.sql import Window as W

    # Window para pegar o registro mais recente por hora
    window = W.partitionBy("observation_time") \
               .orderBy(F.desc("_ingested_at"))

    return (
        dlt.read("weather_clean")
        .filter(F.col("is_forecast") == True)
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(
            "observation_time", "cidade",
            "temperature_2m", "apparent_temperature",
            "precipitation", "rain", "relative_humidity",
            "temp_class", "rain_class",
            "year", "month", "day", "hour",
            "_ingested_at"
        )
        .orderBy("observation_time")
    )