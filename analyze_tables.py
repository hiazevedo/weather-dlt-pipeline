# Databricks notebook source
from pyspark.sql import functions as F

print("=" * 60)
print("  ANÁLISE DE TABELAS — weather + ml")
print("=" * 60)

# COMMAND ----------

tabelas = [
    "weather_pipeline.gold.weather_daily",
    "weather_pipeline.gold.weather_monthly",
    "weather_pipeline.gold.weather_forecast",
    "weather_pipeline.gold.rain_features",
]

for tabela in tabelas:
    try:
        df = spark.table(tabela)
        print(f"\n{'='*50}")
        print(f"  {tabela}")
        print(f"  {df.count():,} registros | {len(df.columns)} colunas")
        print(f"  Colunas: {df.columns}")
        df.limit(2).show(truncate=False)
    except Exception as e:
        print(f"\n  ERRO {tabela}: {e}")

# COMMAND ----------

print("\n--- RANGE de datas (weather_daily) ---")
spark.table("weather_pipeline.gold.weather_daily").agg(
    F.min("date"), F.max("date"), F.count("*").alias("total_dias")
).show()

print("\n--- Estatísticas temperatura ---")
spark.table("weather_pipeline.gold.weather_daily").agg(
    F.round(F.avg("temp_max"),2).alias("temp_max_media"),
    F.round(F.avg("temp_min"),2).alias("temp_min_media"),
    F.round(F.avg("temp_avg"),2).alias("temp_media"),
    F.round(F.max("temp_max"),2).alias("temp_record_max"),
    F.round(F.min("temp_min"),2).alias("temp_record_min"),
    F.round(F.sum("precip_total"),1).alias("precip_total_mm"),
    F.sum(F.col("teve_chuva").cast("int")).alias("total_dias_com_chuva"),
).show(truncate=False)

print("\n--- Distribuição rain_class ---")
spark.table("weather_pipeline.silver.weather_clean").groupBy("rain_class").count().orderBy(F.desc("count")).show()

print("\n--- Distribuição temp_class ---")
spark.table("weather_pipeline.silver.weather_clean").groupBy("temp_class").count().orderBy(F.desc("count")).show()

print("\n--- ML: distribuição target_will_rain ---")
spark.table("weather_pipeline.gold.rain_features").groupBy("target_will_rain").agg(
    F.count("*").alias("total"),
    F.round(F.count("*") * 100.0 / spark.table("weather_pipeline.gold.rain_features").count(), 1).alias("pct")
).orderBy("target_will_rain").show()

print("\n--- Anos disponíveis weather_monthly ---")
spark.table("weather_pipeline.gold.weather_monthly").select("year").distinct().orderBy("year").show(20, False)
