# Databricks notebook source
# MAGIC %md
# MAGIC # Dashboard — Previsão de Chuva · Birigui SP
# MAGIC > Série histórica INMET 1940–2026 + ML model `rain-forecast-birigui` + API Open-Meteo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Temperatura Histórica por Mês (sazonalidade)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   month,
# MAGIC   ROUND(AVG(temp_max), 1)  AS temp_max_media,
# MAGIC   ROUND(AVG(temp_avg), 1)  AS temp_media,
# MAGIC   ROUND(AVG(temp_min), 1)  AS temp_min_media,
# MAGIC   ROUND(MIN(temp_min), 1)  AS temp_record_min,
# MAGIC   ROUND(MAX(temp_max), 1)  AS temp_record_max
# MAGIC FROM weather_pipeline.silver.weather_daily
# MAGIC GROUP BY month
# MAGIC ORDER BY month

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Precipitação Média por Mês (estação seca vs chuvosa)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   month,
# MAGIC   ROUND(AVG(precip_total), 1)                          AS precip_media_diaria,
# MAGIC   ROUND(SUM(precip_total) / COUNT(DISTINCT year), 1)  AS precip_media_mensal_acum,
# MAGIC   ROUND(AVG(CAST(teve_chuva AS INT)) * 100, 1)        AS pct_dias_com_chuva
# MAGIC FROM weather_pipeline.silver.weather_daily
# MAGIC GROUP BY month
# MAGIC ORDER BY month

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tendência Anual de Temperatura (1940–2026)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   year,
# MAGIC   ROUND(AVG(temp_max), 1) AS temp_max_anual,
# MAGIC   ROUND(AVG(temp_avg), 1) AS temp_media_anual,
# MAGIC   ROUND(AVG(temp_min), 1) AS temp_min_anual
# MAGIC FROM weather_pipeline.silver.weather_daily
# MAGIC WHERE year >= 1940
# MAGIC GROUP BY year
# MAGIC ORDER BY year

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Distribuição de Dias com Chuva por Classe

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   rain_class_dominante            AS classe_chuva,
# MAGIC   COUNT(*)                        AS total_dias,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
# MAGIC FROM weather_pipeline.silver.weather_daily
# MAGIC GROUP BY rain_class_dominante
# MAGIC ORDER BY total_dias DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Comparação: ML vs API — Probabilidade × Chuva Real (últimas observações)
# MAGIC > `prob_chuva` = probabilidade predita pelo modelo | `precipitation` = chuva real medida pela API

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ra.observation_time,
# MAGIC   ra.data                                          AS data,
# MAGIC   ra.hora,
# MAGIC   ra.temperature_2m                                AS temperatura,
# MAGIC   ra.relative_humidity                             AS umidade,
# MAGIC   ra.precipitation                                 AS chuva_real_mm,
# MAGIC   ra.rain_class                                    AS classe_real,
# MAGIC   ROUND(ml.prob_chuva, 3)                          AS prob_chuva_modelo,
# MAGIC   ml.pred_vai_chover                               AS pred_vai_chover,
# MAGIC   ml.alerta_chuva                                  AS alerta_modelo,
# MAGIC   ml.threshold_usado                               AS threshold,
# MAGIC   CASE
# MAGIC     WHEN ra.precipitation > 0 AND ml.pred_vai_chover = 1 THEN 'Verdadeiro Positivo'
# MAGIC     WHEN ra.precipitation = 0 AND ml.pred_vai_chover = 0 THEN 'Verdadeiro Negativo'
# MAGIC     WHEN ra.precipitation > 0 AND ml.pred_vai_chover = 0 THEN 'Falso Negativo'
# MAGIC     WHEN ra.precipitation = 0 AND ml.pred_vai_chover = 1 THEN 'Falso Positivo'
# MAGIC   END                                              AS resultado_classificacao
# MAGIC FROM weather_pipeline.gold.rain_alert      ra
# MAGIC JOIN weather_pipeline.gold.rain_forecast_ml ml
# MAGIC   ON ra.observation_time = ml.observation_time
# MAGIC ORDER BY ra.observation_time

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Previsão do Modelo — Próximas Horas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   observation_time,
# MAGIC   hour                              AS hora,
# MAGIC   ROUND(temperature_2m, 1)          AS temperatura,
# MAGIC   ROUND(relative_humidity, 0)       AS umidade_pct,
# MAGIC   ROUND(prob_chuva * 100, 1)        AS prob_chuva_pct,
# MAGIC   pred_vai_chover,
# MAGIC   alerta_chuva,
# MAGIC   threshold_usado
# MAGIC FROM weather_pipeline.gold.rain_forecast_ml
# MAGIC ORDER BY observation_time

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Resumo de Alertas — Nível por Data e Hora

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   data,
# MAGIC   hora,
# MAGIC   ROUND(temperature_2m, 1)  AS temperatura,
# MAGIC   ROUND(precipitation, 1)   AS chuva_mm,
# MAGIC   rain_class                AS classe_real,
# MAGIC   previsao_chuva,
# MAGIC   nivel_alerta
# MAGIC FROM weather_pipeline.gold.rain_alert
# MAGIC ORDER BY data DESC, hora DESC
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Acurácia do Modelo (matriz de confusão simplificada)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   resultado_classificacao,
# MAGIC   COUNT(*) AS total,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     CASE
# MAGIC       WHEN ra.precipitation > 0 AND ml.pred_vai_chover = 1 THEN 'Verdadeiro Positivo'
# MAGIC       WHEN ra.precipitation = 0 AND ml.pred_vai_chover = 0 THEN 'Verdadeiro Negativo'
# MAGIC       WHEN ra.precipitation > 0 AND ml.pred_vai_chover = 0 THEN 'Falso Negativo'
# MAGIC       WHEN ra.precipitation = 0 AND ml.pred_vai_chover = 1 THEN 'Falso Positivo'
# MAGIC     END AS resultado_classificacao
# MAGIC   FROM weather_pipeline.gold.rain_alert      ra
# MAGIC   JOIN weather_pipeline.gold.rain_forecast_ml ml
# MAGIC     ON ra.observation_time = ml.observation_time
# MAGIC )
# MAGIC GROUP BY resultado_classificacao
# MAGIC ORDER BY total DESC
