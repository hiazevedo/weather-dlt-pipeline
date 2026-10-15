# Databricks notebook source
# MAGIC %md
# MAGIC # Dashboard — Previsão de Chuva · Birigui SP
# MAGIC > Espelho das queries do dashboard Lakeview **Previsão de Chuva · Birigui**.
# MAGIC > Série histórica 1940→hoje (Open-Meteo/ERA5) + modelo `workspace.default.rain-forecast-birigui@champion` + API Open-Meteo.
# MAGIC > `observation_time`/`target_time` estão na hora local (America/Sao_Paulo) sem fuso; por isso "agora" é `from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Painel do momento

# COMMAND ----------

# MAGIC %md
# MAGIC #### Vai chover na próxima hora? (modelo)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CONCAT(
# MAGIC   CASE WHEN prob_chuva >= 0.85 THEN '🔴 '
# MAGIC        WHEN prob_chuva >= 0.65 THEN '🟠 '
# MAGIC        WHEN prob_chuva >= 0.40 THEN '🟡 '
# MAGIC        ELSE '🟢 ' END,
# MAGIC   CAST(ROUND(prob_chuva * 100) AS INT), '% às ', LPAD(CAST(HOUR(target_time) AS STRING), 2, '0'), 'h') AS `Próxima Hora`
# MAGIC FROM weather_pipeline.gold.rain_forecast_ml
# MAGIC WHERE target_time = (
# MAGIC   SELECT MIN(target_time) FROM weather_pipeline.gold.rain_forecast_ml
# MAGIC   WHERE target_time > from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Chuva prevista — próxima hora (API, mm)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ROUND(CAST(precipitation AS DOUBLE), 1) AS `Chuva Prevista (mm)`
# MAGIC FROM weather_pipeline.gold.weather_today
# MAGIC WHERE observation_time = (
# MAGIC   SELECT MIN(observation_time)
# MAGIC   FROM weather_pipeline.gold.weather_today
# MAGIC   WHERE is_past = false
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Status do pipeline — 🔴 se a última previsão tiver mais de 7 h

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN (unix_timestamp(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')) - unix_timestamp(MAX(inferencia_local))) / 60 <= 420
# MAGIC       THEN CONCAT('🟢 Atualizado às ', DATE_FORMAT(MAX(inferencia_local), 'HH:mm'))
# MAGIC     ELSE CONCAT('🔴 Atrasado · desde ', DATE_FORMAT(MAX(inferencia_local), 'dd/MM HH:mm'))
# MAGIC   END AS `Status`
# MAGIC FROM weather_pipeline.gold.rain_forecast_ml

# COMMAND ----------

# MAGIC %md
# MAGIC #### Chuva acumulada (mês e 7 dias) vs média 1991–2020

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Chuva observada vs média 1991–2020 do mesmo período do calendário
# MAGIC WITH hoje AS (SELECT DATE(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')) AS d),
# MAGIC dias7 AS (SELECT explode(sequence(date_sub(d, 6), d)) AS dd FROM hoje),
# MAGIC mtd AS (
# MAGIC   SELECT COALESCE(SUM(o.precip_total), 0) AS v
# MAGIC   FROM weather_pipeline.silver.weather_daily o, hoje
# MAGIC   WHERE o.date BETWEEN trunc(hoje.d, 'MM') AND hoje.d),
# MAGIC mtd_n AS (
# MAGIC   SELECT SUM(o.precip_total) / 30 AS v
# MAGIC   FROM weather_pipeline.silver.weather_daily o, hoje
# MAGIC   WHERE o.year BETWEEN 1991 AND 2020 AND o.month = MONTH(hoje.d) AND o.day <= DAY(hoje.d)),
# MAGIC w7 AS (
# MAGIC   SELECT COALESCE(SUM(o.precip_total), 0) AS v
# MAGIC   FROM weather_pipeline.silver.weather_daily o JOIN dias7 ON o.date = dias7.dd),
# MAGIC w7_n AS (
# MAGIC   SELECT SUM(o.precip_total) / 30 AS v
# MAGIC   FROM weather_pipeline.silver.weather_daily o
# MAGIC   JOIN dias7 ON o.month = MONTH(dias7.dd) AND o.day = DAY(dias7.dd)
# MAGIC   WHERE o.year BETWEEN 1991 AND 2020)
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN mtd_n.v < 0.5 THEN CONCAT(REPLACE(CASE WHEN mtd.v = 0 THEN '0' WHEN mtd.v < 10 THEN CAST(ROUND(mtd.v, 1) AS STRING) ELSE CAST(CAST(ROUND(mtd.v) AS INT) AS STRING) END, '.', ','), ' mm · média: ', REPLACE(CASE WHEN mtd_n.v = 0 THEN '0' WHEN mtd_n.v < 10 THEN CAST(ROUND(mtd_n.v, 1) AS STRING) ELSE CAST(CAST(ROUND(mtd_n.v) AS INT) AS STRING) END, '.', ','), ' mm')
# MAGIC     WHEN ABS((mtd.v / mtd_n.v - 1) * 100) < 5 THEN CONCAT(REPLACE(CASE WHEN mtd.v = 0 THEN '0' WHEN mtd.v < 10 THEN CAST(ROUND(mtd.v, 1) AS STRING) ELSE CAST(CAST(ROUND(mtd.v) AS INT) AS STRING) END, '.', ','), ' mm · na média')
# MAGIC     WHEN (mtd.v / mtd_n.v - 1) * 100 > 0 THEN CONCAT(REPLACE(CASE WHEN mtd.v = 0 THEN '0' WHEN mtd.v < 10 THEN CAST(ROUND(mtd.v, 1) AS STRING) ELSE CAST(CAST(ROUND(mtd.v) AS INT) AS STRING) END, '.', ','), ' mm · ', CAST(ROUND((mtd.v / mtd_n.v - 1) * 100) AS INT), '% acima da média')
# MAGIC     ELSE CONCAT(REPLACE(CASE WHEN mtd.v = 0 THEN '0' WHEN mtd.v < 10 THEN CAST(ROUND(mtd.v, 1) AS STRING) ELSE CAST(CAST(ROUND(mtd.v) AS INT) AS STRING) END, '.', ','), ' mm · ', CAST(ROUND(-(mtd.v / mtd_n.v - 1) * 100) AS INT), '% abaixo da média')
# MAGIC   END AS `Chuva no mês`,
# MAGIC   CASE
# MAGIC     WHEN w7_n.v < 0.5 THEN CONCAT(REPLACE(CASE WHEN w7.v = 0 THEN '0' WHEN w7.v < 10 THEN CAST(ROUND(w7.v, 1) AS STRING) ELSE CAST(CAST(ROUND(w7.v) AS INT) AS STRING) END, '.', ','), ' mm · média: ', REPLACE(CASE WHEN w7_n.v = 0 THEN '0' WHEN w7_n.v < 10 THEN CAST(ROUND(w7_n.v, 1) AS STRING) ELSE CAST(CAST(ROUND(w7_n.v) AS INT) AS STRING) END, '.', ','), ' mm')
# MAGIC     WHEN ABS((w7.v / w7_n.v - 1) * 100) < 5 THEN CONCAT(REPLACE(CASE WHEN w7.v = 0 THEN '0' WHEN w7.v < 10 THEN CAST(ROUND(w7.v, 1) AS STRING) ELSE CAST(CAST(ROUND(w7.v) AS INT) AS STRING) END, '.', ','), ' mm · na média')
# MAGIC     WHEN (w7.v / w7_n.v - 1) * 100 > 0 THEN CONCAT(REPLACE(CASE WHEN w7.v = 0 THEN '0' WHEN w7.v < 10 THEN CAST(ROUND(w7.v, 1) AS STRING) ELSE CAST(CAST(ROUND(w7.v) AS INT) AS STRING) END, '.', ','), ' mm · ', CAST(ROUND((w7.v / w7_n.v - 1) * 100) AS INT), '% acima da média')
# MAGIC     ELSE CONCAT(REPLACE(CASE WHEN w7.v = 0 THEN '0' WHEN w7.v < 10 THEN CAST(ROUND(w7.v, 1) AS STRING) ELSE CAST(CAST(ROUND(w7.v) AS INT) AS STRING) END, '.', ','), ' mm · ', CAST(ROUND(-(w7.v / w7_n.v - 1) * 100) AS INT), '% abaixo da média')
# MAGIC   END AS `Chuva 7 dias`
# MAGIC FROM mtd, mtd_n, w7, w7_n

# COMMAND ----------

# MAGIC %md
# MAGIC #### Temperatura máxima do dia

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ROUND(MAX(CAST(temperature_2m AS DOUBLE)), 1) AS `Temp Máxima (°C)`
# MAGIC FROM weather_pipeline.gold.weather_today
# MAGIC WHERE DATE(observation_time) = DATE(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Temperatura mínima do dia

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ROUND(MIN(CAST(temperature_2m AS DOUBLE)), 1) AS `Temp Mínima (°C)`
# MAGIC FROM weather_pipeline.gold.weather_today
# MAGIC WHERE DATE(observation_time) = DATE(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Umidade atual

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ROUND(CAST(relative_humidity AS DOUBLE), 0) AS `Umidade (%)`
# MAGIC FROM weather_pipeline.gold.weather_today
# MAGIC WHERE observation_time = (
# MAGIC   SELECT MAX(observation_time)
# MAGIC   FROM weather_pipeline.gold.weather_today
# MAGIC   WHERE is_past = true
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Modelo vs Realidade (−6h a +18h)
# MAGIC > Probabilidade do modelo para cada hora-alvo × chuva observada. `horas_rel` = horas em relação à hora atual; o modelo usa as features da hora t para prever t+1.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ultima AS (
# MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY target_time ORDER BY inferencia_local DESC) AS rn
# MAGIC   FROM weather_pipeline.gold.rain_forecast_ml_history
# MAGIC   WHERE target_time >  date_trunc('hour', from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')) - INTERVAL 6 HOURS
# MAGIC     AND target_time <= date_trunc('hour', from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')) + INTERVAL 18 HOURS
# MAGIC )
# MAGIC SELECT
# MAGIC   u.target_time                                          AS observation_time,
# MAGIC   CAST((unix_timestamp(u.target_time) - unix_timestamp(date_trunc('hour', from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')))) / 3600 AS INT) AS horas_rel,
# MAGIC   HOUR(u.target_time)                                    AS hora,
# MAGIC   ROUND(u.prob_chuva * 100, 1)                           AS prob_modelo_pct,
# MAGIC   u.threshold_usado * 100                                AS threshold_pct,
# MAGIC   CASE WHEN o.precipitation > 0 THEN 100
# MAGIC        WHEN o.precipitation = 0 THEN 0 END               AS choveu_real,
# MAGIC   ROUND(CAST(o.precipitation AS DOUBLE), 1)              AS chuva_real_mm
# MAGIC FROM ultima u
# MAGIC LEFT JOIN weather_pipeline.silver.weather_clean o
# MAGIC   ON o.observation_time = u.target_time AND o.is_forecast = false
# MAGIC WHERE u.rn = 1
# MAGIC ORDER BY u.target_time

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Temperatura de hoje vs média histórica
# MAGIC > Por hora: observado + previsão de hoje × média 1991–2020 numa janela de ±7 dias em torno da data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   hour                                          AS hora,
# MAGIC   ROUND(CAST(temperature_2m AS DOUBLE), 1)      AS temperatura,
# MAGIC   ROUND(temp_climatologica, 1)                  AS normal
# MAGIC FROM weather_pipeline.gold.weather_today
# MAGIC WHERE DATE(observation_time) = DATE(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo'))
# MAGIC ORDER BY hour

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Desempenho — modelo vs API vs teste
# MAGIC > **Produção**: horas com chuva observada em que existem previsão do modelo e da API (mesmas horas). **API**: baseline "a Open-Meteo prevê > 0 mm". **Teste**: últimos 12 meses fora do treino, no limiar do champion (os 12 meses anteriores calibram o limiar). Com menos de 48 h observadas a amostra é pequena.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Produção: horas com chuva observada em que há previsão do modelo E da API
# MAGIC -- (mesmas horas para os dois). Teste: últimos 12 meses fora do treino, no limiar do champion.
# MAGIC WITH base AS (
# MAGIC   SELECT pred_vai_chover, api_vai_chover, choveu FROM weather_pipeline.gold.rain_model_accuracy_hourly
# MAGIC   WHERE api_precip_alvo IS NOT NULL
# MAGIC ),
# MAGIC conf AS (
# MAGIC   SELECT 'modelo' AS src, pred_vai_chover AS p, choveu AS c FROM base
# MAGIC   UNION ALL
# MAGIC   SELECT 'api', api_vai_chover, choveu FROM base
# MAGIC ),
# MAGIC agg AS (
# MAGIC   SELECT src, COUNT(*) AS n,
# MAGIC     SUM(CASE WHEN p = 1 AND c = 1 THEN 1 ELSE 0 END) AS vp,
# MAGIC     SUM(CASE WHEN p = 0 AND c = 0 THEN 1 ELSE 0 END) AS vn,
# MAGIC     SUM(CASE WHEN p = 1 AND c = 0 THEN 1 ELSE 0 END) AS fp,
# MAGIC     SUM(CASE WHEN p = 0 AND c = 1 THEN 1 ELSE 0 END) AS fn
# MAGIC   FROM conf GROUP BY src
# MAGIC ),
# MAGIC met AS (
# MAGIC   SELECT s.src,
# MAGIC     COALESCE(a.n, 0) AS n, COALESCE(a.vp, 0) AS vp, COALESCE(a.vn, 0) AS vn,
# MAGIC     COALESCE(a.fp, 0) AS fp, COALESCE(a.fn, 0) AS fn,
# MAGIC     (a.vp + a.vn) / NULLIF(a.n, 0)          AS acc,
# MAGIC     a.vp / NULLIF(a.vp + a.fp, 0)           AS prec,
# MAGIC     a.vp / NULLIF(a.vp + a.fn, 0)           AS rec,
# MAGIC     2 * a.vp / NULLIF(2 * a.vp + a.fp + a.fn, 0) AS f1
# MAGIC   FROM (VALUES ('modelo'), ('api')) AS s(src)
# MAGIC   LEFT JOIN agg a ON a.src = s.src
# MAGIC ),
# MAGIC champ AS (
# MAGIC   SELECT * FROM weather_pipeline.gold.rain_model_versions
# MAGIC   WHERE promoted ORDER BY version DESC LIMIT 1
# MAGIC ),
# MAGIC f AS (
# MAGIC   SELECT
# MAGIC     m.n, m.vp AS m_vp, m.vn AS m_vn, m.fp AS m_fp, m.fn AS m_fn,
# MAGIC     m.acc AS m_acc, m.prec AS m_prec, m.rec AS m_rec, m.f1 AS m_f1,
# MAGIC     a.vp AS a_vp, a.vn AS a_vn, a.fp AS a_fp, a.fn AS a_fn,
# MAGIC     a.acc AS a_acc, a.prec AS a_prec, a.rec AS a_rec, a.f1 AS a_f1,
# MAGIC     c.version AS ver, c.threshold AS v_thr, c.test_size AS v_n,
# MAGIC     c.accuracy AS v_acc, c.precision AS v_prec, c.recall AS v_rec, c.f1 AS v_f1
# MAGIC   FROM met m
# MAGIC   JOIN met a ON m.src = 'modelo' AND a.src = 'api'
# MAGIC   CROSS JOIN champ c
# MAGIC )
# MAGIC SELECT 1 AS ord, 'Observações (horas)' AS `Métrica`,
# MAGIC        CAST(n AS STRING) AS `Modelo (produção)`,
# MAGIC        CAST(n AS STRING) AS `API Open-Meteo (produção)`,
# MAGIC        CAST(v_n AS STRING) AS `Modelo (teste 12 meses)` FROM f
# MAGIC UNION ALL SELECT 2, 'Acurácia', CASE WHEN m_acc IS NULL THEN '—' ELSE CONCAT(ROUND(m_acc * 100, 1), '%') END, CASE WHEN a_acc IS NULL THEN '—' ELSE CONCAT(ROUND(a_acc * 100, 1), '%') END, CASE WHEN v_acc IS NULL THEN '—' ELSE CONCAT(ROUND(v_acc * 100, 1), '%') END FROM f
# MAGIC UNION ALL SELECT 3, 'Precisão', CASE WHEN m_prec IS NULL THEN '—' ELSE CONCAT(ROUND(m_prec * 100, 1), '%') END, CASE WHEN a_prec IS NULL THEN '—' ELSE CONCAT(ROUND(a_prec * 100, 1), '%') END, CASE WHEN v_prec IS NULL THEN '—' ELSE CONCAT(ROUND(v_prec * 100, 1), '%') END FROM f
# MAGIC UNION ALL SELECT 4, 'Recall',   CASE WHEN m_rec IS NULL THEN '—' ELSE CONCAT(ROUND(m_rec * 100, 1), '%') END,  CASE WHEN a_rec IS NULL THEN '—' ELSE CONCAT(ROUND(a_rec * 100, 1), '%') END,  CASE WHEN v_rec IS NULL THEN '—' ELSE CONCAT(ROUND(v_rec * 100, 1), '%') END  FROM f
# MAGIC UNION ALL SELECT 5, 'F1',       CASE WHEN m_f1 IS NULL THEN '—' ELSE CAST(ROUND(m_f1, 3) AS STRING) END,   CASE WHEN a_f1 IS NULL THEN '—' ELSE CAST(ROUND(a_f1, 3) AS STRING) END,   CASE WHEN v_f1 IS NULL THEN '—' ELSE CAST(ROUND(v_f1, 3) AS STRING) END   FROM f
# MAGIC UNION ALL SELECT 6, 'VP / VN / FP / FN',
# MAGIC        CONCAT(m_vp, ' / ', m_vn, ' / ', m_fp, ' / ', m_fn),
# MAGIC        CONCAT(a_vp, ' / ', a_vn, ' / ', a_fp, ' / ', a_fn), '—' FROM f
# MAGIC UNION ALL SELECT 7, 'Diagnóstico',
# MAGIC        CASE
# MAGIC          WHEN n < 48 THEN CONCAT('⏳ Amostra pequena (', n, ' h)')
# MAGIC          WHEN m_f1 >= v_f1 - 0.05 THEN '✅ Como na validação'
# MAGIC          WHEN m_f1 >= v_f1 - 0.15 THEN '⚠️ Abaixo da validação — monitorar'
# MAGIC          ELSE '🔴 Retreinamento recomendado'
# MAGIC        END,
# MAGIC        CASE
# MAGIC          WHEN n < 48 THEN '⏳ Aguardando amostra'
# MAGIC          WHEN COALESCE(m_f1, 0) > COALESCE(a_f1, 0) + 0.02 THEN '✅ Modelo supera a API'
# MAGIC          WHEN COALESCE(m_f1, 0) < COALESCE(a_f1, 0) - 0.02 THEN '⚠️ API supera o modelo'
# MAGIC          ELSE '≈ Empate com a API'
# MAGIC        END,
# MAGIC        CONCAT('v', ver, ' · limiar ', REPLACE(CAST(v_thr AS STRING), '.', ',')) FROM f
# MAGIC ORDER BY ord

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resumo de Alertas — hoje e amanhã
# MAGIC > Uma linha por hora (observado + previsão). **Diferença p/ média histórica**: temperatura da hora − média 1991–2020 da mesma hora (janela de ±7 dias em torno da data; 🔴 ≥ +2 °C, 🔵 ≤ −2 °C).
# MAGIC > **Faixa de temperatura** é a classe da temperatura do ar. **Modelo**: última previsão feita antes da hora (histórico em `rain_forecast_ml_history`).

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH modelo AS (
# MAGIC   -- última previsão feita antes de cada hora-alvo (mesma regra da view de acurácia)
# MAGIC   SELECT target_time, prob_chuva,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY target_time ORDER BY inferencia_local DESC) AS rn
# MAGIC   FROM weather_pipeline.gold.rain_forecast_ml_history
# MAGIC   WHERE inferencia_local < target_time
# MAGIC )
# MAGIC SELECT
# MAGIC   CONCAT(DATE_FORMAT(wt.observation_time, 'dd/MM'), ' ',
# MAGIC          LPAD(CAST(wt.hour AS STRING), 2, '0'), 'h')               AS `Quando`,
# MAGIC   CASE WHEN wt.is_past THEN '✅ Observado' ELSE '🔮 Previsão' END  AS `Tipo`,
# MAGIC   ROUND(CAST(wt.temperature_2m AS DOUBLE), 1)                       AS `Temp (°C)`,
# MAGIC   ROUND(CAST(wt.apparent_temperature AS DOUBLE), 1)                 AS `Sensação (°C)`,
# MAGIC   CASE
# MAGIC     WHEN wt.temp_climatologica IS NULL THEN '—'
# MAGIC     ELSE CONCAT(
# MAGIC       CASE WHEN wt.temperature_2m - wt.temp_climatologica >=  2 THEN '🔴 '
# MAGIC            WHEN wt.temperature_2m - wt.temp_climatologica <= -2 THEN '🔵 '
# MAGIC            ELSE '⚪ ' END,
# MAGIC       CASE WHEN wt.temperature_2m >= wt.temp_climatologica THEN '+' ELSE '' END,
# MAGIC       ROUND(wt.temperature_2m - wt.temp_climatologica, 1))
# MAGIC   END                                                               AS `Diferença p/ média histórica (°C)`,
# MAGIC   wt.temp_class                                                     AS `Faixa de temperatura`,
# MAGIC   CASE
# MAGIC     WHEN wt.precipitation IS NULL THEN '—'
# MAGIC     WHEN wt.precipitation = 0    THEN 'Sem chuva'
# MAGIC     ELSE CONCAT(ROUND(CAST(wt.precipitation AS DOUBLE), 1), ' mm · ', wt.rain_class)
# MAGIC   END                                                               AS `Chuva`,
# MAGIC   ROUND(CAST(wt.relative_humidity AS DOUBLE), 0)                    AS `Umidade (%)`,
# MAGIC   CASE
# MAGIC     WHEN m.prob_chuva IS NULL THEN '—'
# MAGIC     ELSE CONCAT(
# MAGIC       CASE WHEN m.prob_chuva >= 0.85 THEN '🔴 '
# MAGIC            WHEN m.prob_chuva >= 0.65 THEN '🟠 '
# MAGIC            WHEN m.prob_chuva >= 0.40 THEN '🟡 '
# MAGIC            ELSE '🟢 ' END,
# MAGIC       CAST(ROUND(m.prob_chuva * 100) AS INT), '%')
# MAGIC   END                                                               AS `Modelo (chance de chuva)`
# MAGIC FROM weather_pipeline.gold.weather_today wt
# MAGIC LEFT JOIN modelo m
# MAGIC   ON m.target_time = wt.observation_time AND m.rn = 1
# MAGIC ORDER BY wt.observation_time

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Precipitação por Mês (média 1991–2020)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   month,
# MAGIC     CASE month
# MAGIC     WHEN 1  THEN 'Jan' 
# MAGIC     WHEN 2  THEN 'Fev' 
# MAGIC     WHEN 3  THEN 'Mar'
# MAGIC     WHEN 4  THEN 'Abr' 
# MAGIC     WHEN 5  THEN 'Mai' 
# MAGIC     WHEN 6  THEN 'Jun'
# MAGIC     WHEN 7  THEN 'Jul' 
# MAGIC     WHEN 8  THEN 'Ago'
# MAGIC     WHEN 9  THEN 'Set'
# MAGIC     WHEN 10 THEN 'Out' 
# MAGIC     WHEN 11 THEN 'Nov' 
# MAGIC     WHEN 12 THEN 'Dez'
# MAGIC     END                  AS mes_nome,
# MAGIC   ROUND(AVG(precip_total), 1)                          AS precip_media_diaria,
# MAGIC   ROUND(SUM(precip_total) / COUNT(DISTINCT year), 1)  AS precip_media_mensal_acum,
# MAGIC   ROUND(AVG(CAST(teve_chuva AS INT)) * 100, 1)        AS pct_dias_com_chuva
# MAGIC FROM weather_pipeline.silver.weather_daily
# MAGIC WHERE year BETWEEN 1991 AND 2020   -- mesma referência das demais comparações
# MAGIC GROUP BY month
# MAGIC ORDER BY month

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Temperatura por Mês (média 1991–2020)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   month,
# MAGIC   CASE month
# MAGIC     WHEN 1  THEN 'Jan' 
# MAGIC     WHEN 2  THEN 'Fev' 
# MAGIC     WHEN 3  THEN 'Mar'
# MAGIC     WHEN 4  THEN 'Abr' 
# MAGIC     WHEN 5  THEN 'Mai' 
# MAGIC     WHEN 6  THEN 'Jun'
# MAGIC     WHEN 7  THEN 'Jul' 
# MAGIC     WHEN 8  THEN 'Ago'
# MAGIC     WHEN 9  THEN 'Set'
# MAGIC     WHEN 10 THEN 'Out' 
# MAGIC     WHEN 11 THEN 'Nov' 
# MAGIC     WHEN 12 THEN 'Dez'
# MAGIC   END                  AS mes_nome,
# MAGIC   ROUND(AVG(temp_max), 1)  AS temp_max_media,
# MAGIC   ROUND(AVG(temp_avg), 1)  AS temp_media,
# MAGIC   ROUND(AVG(temp_min), 1)  AS temp_min_media
# MAGIC FROM weather_pipeline.silver.weather_daily
# MAGIC WHERE year BETWEEN 1991 AND 2020   -- mesma referência das demais comparações
# MAGIC GROUP BY month
# MAGIC ORDER BY month

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Tendência Anual de Temperatura (anos completos)
# MAGIC > O ano corrente fica de fora: sem nov/dez, ele puxaria a média para baixo.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   year,
# MAGIC   ROUND(AVG(temp_max), 1) AS temp_max_anual,
# MAGIC   ROUND(AVG(temp_avg), 1) AS temp_media_anual,
# MAGIC   ROUND(AVG(temp_min), 1) AS temp_min_anual
# MAGIC FROM weather_pipeline.silver.weather_daily
# MAGIC WHERE year >= 1940
# MAGIC   AND year < YEAR(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo'))   -- só anos completos (o ano corrente puxaria a média para baixo)
# MAGIC GROUP BY year
# MAGIC ORDER BY year
