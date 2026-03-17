# Databricks notebook source
# Deleta catalog, schemas e volume antes de criar novamente
print("Removendo ambiente...\n")

# Drop volume se existir
spark.sql("""
    DROP VOLUME IF EXISTS weather_pipeline.bronze.raw_json
""")

# Drop schemas se existirem
for schema in ["bronze", "silver", "gold", "checkpoints"]:
    spark.sql(f"DROP SCHEMA IF EXISTS weather_pipeline.{schema} CASCADE")
    print(f"Schema removido: {schema}")

# Drop catalog se existir
spark.sql("DROP CATALOG IF EXISTS weather_pipeline CASCADE")

# COMMAND ----------

print("Configurando ambiente...\n")

# Catalog
spark.sql("CREATE CATALOG IF NOT EXISTS weather_pipeline")
spark.sql("USE CATALOG weather_pipeline")

# Schemas
for schema in ["bronze", "silver", "gold", "checkpoints"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS weather_pipeline.{schema}")
    print(f"Schema: {schema}")

# Volume para JSONs
spark.sql("""
    CREATE VOLUME IF NOT EXISTS weather_pipeline.bronze.raw_json
""")
print("Volume: bronze/raw_json")

print(f"""
AMBIENTE CONFIGURADO
 - Catalog : weather_pipeline
 - Schemas : bronze | silver | gold | checkpoints
 - Volume  : /Volumes/weather_pipeline/bronze/raw_json
""")