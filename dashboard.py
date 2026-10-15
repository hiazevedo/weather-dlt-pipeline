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
# MAGIC ## 5. Comparação: Modelo vs chuva observada (últimas 24 h)
# MAGIC > `prob_chuva` = probabilidade do modelo de chover na hora-alvo (`target_time`) | `chuva_real_mm` = chuva **observada** nessa hora.
# MAGIC > O modelo recebe as features da hora t e prevê a hora t+1; a comparação usa a última previsão feita *antes* da hora-alvo (histórico em `rain_forecast_ml_history`).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   target_time                          AS observation_time,
# MAGIC   DATE(target_time)                    AS data,
# MAGIC   HOUR(target_time)                    AS hora,
# MAGIC   chuva_real_mm,
# MAGIC   prob_chuva                           AS prob_chuva_modelo,
# MAGIC   pred_vai_chover,
# MAGIC   threshold_usado                      AS threshold,
# MAGIC   resultado_classificacao
# MAGIC FROM weather_pipeline.gold.rain_model_accuracy_hourly
# MAGIC WHERE target_time > (SELECT MAX(target_time) FROM weather_pipeline.gold.rain_model_accuracy_hourly) - INTERVAL 24 HOURS
# MAGIC ORDER BY target_time

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Previsão do Modelo — Próximas Horas

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   target_time                       AS observation_time,
# MAGIC   HOUR(target_time)                 AS hora,
# MAGIC   ROUND(temperature_2m, 1)          AS temperatura,
# MAGIC   ROUND(relative_humidity, 0)       AS umidade_pct,
# MAGIC   ROUND(prob_chuva * 100, 1)        AS prob_chuva_pct,
# MAGIC   pred_vai_chover,
# MAGIC   alerta_chuva,
# MAGIC   threshold_usado
# MAGIC FROM weather_pipeline.gold.rain_forecast_ml
# MAGIC ORDER BY target_time

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
# MAGIC ## 8. Desempenho do Modelo — produção vs validação
# MAGIC > **Produção**: previsões passadas comparadas com a chuva observada (`rain_model_accuracy_hourly`).
# MAGIC > **Validação**: teste dos últimos 24 meses do champion (`rain_model_versions`).
# MAGIC > Com poucas horas observadas, a amostra é pequena e o número de produção ainda não é conclusivo.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   resultado_classificacao,
# MAGIC   COUNT(*) AS total,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
# MAGIC FROM weather_pipeline.gold.rain_model_accuracy_hourly
# MAGIC GROUP BY resultado_classificacao
# MAGIC ORDER BY total DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT version, ROUND(accuracy * 100, 1) AS acuracia_pct, ROUND(f1, 3) AS f1,
# MAGIC        ROUND(`precision` * 100, 1) AS precisao_pct, ROUND(recall * 100, 1) AS recall_pct,
# MAGIC        test_size, promoted, created_at
# MAGIC FROM weather_pipeline.gold.rain_model_versions
# MAGIC ORDER BY version DESC
