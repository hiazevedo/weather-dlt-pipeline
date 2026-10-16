# Databricks notebook source
# MAGIC %md
# MAGIC # Dashboard — Previsão de Chuva · Birigui SP
# MAGIC > Espelho das queries do dashboard Lakeview **Previsão de Chuva · Birigui**.
# MAGIC > Série histórica 1940→hoje (Open-Meteo) + champion `workspace.default.rain-forecast-birigui@champion` (rótulo Open-Meteo) + regional `workspace.default.rain-forecast-regional-inmet@champion` (chuva medida pelo INMET) + API Open-Meteo + avisos do INMET + ENSO (NOAA).
# MAGIC > `observation_time`/`target_time` estão na hora local (America/Sao_Paulo) sem fuso; por isso "agora" é `from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Painel do momento

# COMMAND ----------

# MAGIC %md
# MAGIC #### Vai chover na próxima hora? — champion (prevê o rótulo do Open-Meteo)

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
# MAGIC #### Chuva medida na próxima hora? — modelo regional (treinado com o pluviômetro do INMET)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CONCAT(
# MAGIC   CASE WHEN prob_chuva_medida IS NULL THEN '⚪ '
# MAGIC        WHEN prob_chuva_medida >= 0.9 THEN '🔴 '
# MAGIC        WHEN prob_chuva_medida >= threshold_regional THEN '🟠 '
# MAGIC        WHEN prob_chuva_medida >= threshold_regional - 0.2 THEN '🟡 '
# MAGIC        ELSE '🟢 ' END,
# MAGIC   COALESCE(CONCAT(CAST(ROUND(prob_chuva_medida * 100) AS INT), '% às ',
# MAGIC                   LPAD(CAST(HOUR(target_time) AS STRING), 2, '0'), 'h'), 'indisponível')) AS `Chuva medida`
# MAGIC FROM weather_pipeline.gold.rain_forecast_ml
# MAGIC WHERE target_time = (
# MAGIC   SELECT MIN(target_time) FROM weather_pipeline.gold.rain_forecast_ml
# MAGIC   WHERE target_time > from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Chuva prevista — próxima hora (API Open-Meteo, mm)

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
# MAGIC #### Temperatura máxima / mínima do dia

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CONCAT(
# MAGIC   REPLACE(CAST(ROUND(MAX(CAST(temperature_2m AS DOUBLE)), 1) AS STRING), '.', ','), '° / ',
# MAGIC   REPLACE(CAST(ROUND(MIN(CAST(temperature_2m AS DOUBLE)), 1) AS STRING), '.', ','), '°') AS `Máx / Mín`
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
# MAGIC #### Aviso oficial mais severo ativo (INMET — repassado pela Defesa Civil)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aviso oficial mais severo ativo para a cidade (INMET — repassado pela Defesa Civil)
# MAGIC WITH a AS (SELECT * FROM weather_pipeline.gold.avisos_cidade),
# MAGIC ativos AS (SELECT * FROM a WHERE situacao = 'ativo'),
# MAGIC topo AS (SELECT * FROM ativos ORDER BY nivel DESC, fim DESC LIMIT 1),
# MAGIC prox AS (SELECT * FROM a WHERE situacao = 'futuro' ORDER BY inicio, nivel DESC LIMIT 1)
# MAGIC SELECT CASE
# MAGIC   WHEN (SELECT count(*) FROM ativos) > 0 THEN (
# MAGIC     SELECT CONCAT(CASE nivel WHEN 3 THEN '🔴 ' WHEN 2 THEN '🟠 ' ELSE '🟡 ' END,
# MAGIC                   descricao, ' — ', severidade, ' · até ',
# MAGIC                   DATE_FORMAT(fim, CASE WHEN DATE(fim) = DATE(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')) THEN 'HH:mm' ELSE 'dd/MM HH:mm' END),
# MAGIC                   CASE WHEN (SELECT count(*) FROM ativos) > 1
# MAGIC                        THEN CONCAT(' (+', (SELECT count(*) FROM ativos) - 1, ')') ELSE '' END)
# MAGIC     FROM topo)
# MAGIC   WHEN (SELECT count(*) FROM prox) > 0 THEN (
# MAGIC     SELECT CONCAT('🟢 Nenhum ativo · próximo: ', descricao, ' (', severidade, ') em ', DATE_FORMAT(inicio, 'dd/MM'))
# MAGIC     FROM prox)
# MAGIC   ELSE '🟢 Nenhum aviso ativo'
# MAGIC END AS `Avisos`

# COMMAND ----------

# MAGIC %md
# MAGIC #### Status do pipeline — 🔴 se a última previsão tiver mais de 2 h (pipeline de hora em hora)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN (unix_timestamp(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')) - unix_timestamp(MAX(inferencia_local))) / 60 <= 120   -- pipeline roda a cada hora: 🔴 com 2 h sem atualização
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
# MAGIC ## 2. Avisos oficiais para a cidade (INMET · Defesa Civil)
# MAGIC > Ativos, próximos e encerrados nos últimos 7 dias, com a chuva observada e as horas em que champion e regional deram alerta dentro da janela do aviso. Views `gold.avisos_cidade` e `gold.avisos_vs_modelo`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Avisos da cidade (ativos, próximos e encerrados nos últimos 7 dias) × chuva observada × modelos
# MAGIC SELECT
# MAGIC   CASE a.situacao WHEN 'ativo' THEN '⚠️ Ativo' WHEN 'futuro' THEN '🔜 Próximo' ELSE '✔️ Encerrado' END AS `Situação`,
# MAGIC   CONCAT(CASE a.nivel WHEN 3 THEN '🔴 ' WHEN 2 THEN '🟠 ' ELSE '🟡 ' END, a.descricao)          AS `Aviso`,
# MAGIC   a.severidade                                                                                AS `Severidade`,
# MAGIC   DATE_FORMAT(a.inicio, 'dd/MM HH:mm')                                                        AS `Início`,
# MAGIC   DATE_FORMAT(a.fim, 'dd/MM HH:mm')                                                           AS `Fim`,
# MAGIC   COALESCE(CONCAT(REPLACE(CAST(m.chuva_obs_mm AS STRING), '.', ','), ' mm em ', m.horas_observadas, ' h'), '—') AS `Chuva observada`,
# MAGIC   CASE WHEN m.horas_com_previsao > 0
# MAGIC        THEN CONCAT(m.horas_alerta_champion, ' de ', m.horas_com_previsao, ' h') ELSE '—' END   AS `Champion: horas com alerta`,
# MAGIC   CASE WHEN m.horas_com_previsao > 0 AND m.horas_alerta_regional IS NOT NULL
# MAGIC        THEN CONCAT(m.horas_alerta_regional, ' de ', m.horas_com_previsao, ' h') ELSE '—' END   AS `Regional: horas com alerta`,
# MAGIC   a.riscos                                                                                    AS `Riscos`
# MAGIC FROM weather_pipeline.gold.avisos_cidade a
# MAGIC LEFT JOIN weather_pipeline.gold.avisos_vs_modelo m ON m.id = a.id
# MAGIC WHERE a.situacao IN ('ativo', 'futuro') OR a.fim >= from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') - INTERVAL 7 DAYS
# MAGIC ORDER BY CASE a.situacao WHEN 'ativo' THEN 0 WHEN 'futuro' THEN 1 ELSE 2 END, a.inicio, a.nivel DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Modelo vs Realidade (−6h a +18h)
# MAGIC > Probabilidade do champion e do regional para cada hora-alvo × chuva observada. `horas_rel` = horas em relação à hora atual; os modelos usam as features da hora t para prever t+1.

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
# MAGIC   ROUND(u.prob_chuva_medida * 100, 1)                    AS prob_regional_pct,
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
# MAGIC ## 4. Desempenho
# MAGIC > **Champion** (rótulo Open-Meteo): produção (horas observadas, mesmas horas da API), API Open-Meteo como baseline e teste de 12 meses no limiar do champion. **Chuva medida (INMET)**: champion nas cidades das estações (validação semanal) e regional avaliado deixando cada estação de fora (como Birigui, que não tem estação), com a persistência do pluviômetro como referência.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Produção: horas com chuva observada em que há previsão do modelo E da API
# MAGIC -- (mesmas horas para os dois). Teste: últimos 12 meses fora do treino, no limiar do champion.
# MAGIC -- INMET: champion contra chuva medida nas cidades das estações (validação semanal).
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
# MAGIC inmet AS (
# MAGIC   -- última validação semanal do champion contra a chuva MEDIDA (INMET),
# MAGIC   -- média das cidades das estações, últimos 12 meses com dado
# MAGIC   SELECT avg(CASE WHEN variante LIKE '%_chuva_medida_inmet' THEN f1 END)            AS i_f1,
# MAGIC          avg(CASE WHEN variante LIKE '%_chuva_medida_inmet' THEN `precision` END)   AS i_prec,
# MAGIC          avg(CASE WHEN variante LIKE '%_chuva_medida_inmet' THEN recall END)        AS i_rec,
# MAGIC          avg(CASE WHEN variante LIKE '%_chuva_medida_inmet' THEN accuracy END)      AS i_acc,
# MAGIC          sum(CASE WHEN variante LIKE '%_chuva_medida_inmet' THEN test_size END)     AS i_n,
# MAGIC          count(CASE WHEN variante LIKE '%_chuva_medida_inmet' THEN 1 END)          AS i_cidades,
# MAGIC          avg(CASE WHEN variante LIKE '%_persistencia_inmet%' THEN f1 END)           AS p_f1
# MAGIC   FROM weather_pipeline.gold.ml_experiments
# MAGIC   WHERE experimento = 'inmet_validacao'
# MAGIC     AND created_at = (SELECT max(created_at) FROM weather_pipeline.gold.ml_experiments
# MAGIC                       WHERE experimento = 'inmet_validacao')
# MAGIC ),
# MAGIC reg AS (
# MAGIC   -- champion regional (rótulo = chuva medida): avaliação deixando cada estação de fora
# MAGIC   SELECT version AS r_ver, threshold AS r_thr, f1 AS r_f1, `precision` AS r_prec, recall AS r_rec,
# MAGIC          accuracy AS r_acc, test_size AS r_n
# MAGIC   FROM weather_pipeline.gold.rain_regional_versions
# MAGIC   WHERE promoted ORDER BY version DESC LIMIT 1
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
# MAGIC   , i.*, r.*
# MAGIC   FROM met m
# MAGIC   JOIN met a ON m.src = 'modelo' AND a.src = 'api'
# MAGIC   CROSS JOIN champ c
# MAGIC   CROSS JOIN inmet i
# MAGIC   CROSS JOIN reg r
# MAGIC )
# MAGIC SELECT 1 AS ord, 'Observações (horas)' AS `Métrica`,
# MAGIC        CAST(n AS STRING) AS `Modelo (produção)`,
# MAGIC        CAST(n AS STRING) AS `API Open-Meteo (produção)`,
# MAGIC        CAST(v_n AS STRING) AS `Modelo (teste 12 meses)`,
# MAGIC        CONCAT(i_n, ' (', i_cidades, ' cidades)') AS `Modelo vs chuva medida (INMET)`,
# MAGIC        CONCAT(r_n, ' (estação fora do treino)') AS `Regional vs chuva medida (INMET)` FROM f
# MAGIC UNION ALL SELECT 2, 'Acurácia', CASE WHEN m_acc IS NULL THEN '—' ELSE CONCAT(ROUND(m_acc * 100, 1), '%') END, CASE WHEN a_acc IS NULL THEN '—' ELSE CONCAT(ROUND(a_acc * 100, 1), '%') END, CASE WHEN v_acc IS NULL THEN '—' ELSE CONCAT(ROUND(v_acc * 100, 1), '%') END, CASE WHEN i_acc IS NULL THEN '—' ELSE CONCAT(ROUND(i_acc * 100, 1), '%') END, CASE WHEN r_acc IS NULL THEN '—' ELSE CONCAT(ROUND(r_acc * 100, 1), '%') END FROM f
# MAGIC UNION ALL SELECT 3, 'Precisão', CASE WHEN m_prec IS NULL THEN '—' ELSE CONCAT(ROUND(m_prec * 100, 1), '%') END, CASE WHEN a_prec IS NULL THEN '—' ELSE CONCAT(ROUND(a_prec * 100, 1), '%') END, CASE WHEN v_prec IS NULL THEN '—' ELSE CONCAT(ROUND(v_prec * 100, 1), '%') END, CASE WHEN i_prec IS NULL THEN '—' ELSE CONCAT(ROUND(i_prec * 100, 1), '%') END, CASE WHEN r_prec IS NULL THEN '—' ELSE CONCAT(ROUND(r_prec * 100, 1), '%') END FROM f
# MAGIC UNION ALL SELECT 4, 'Recall',   CASE WHEN m_rec IS NULL THEN '—' ELSE CONCAT(ROUND(m_rec * 100, 1), '%') END,  CASE WHEN a_rec IS NULL THEN '—' ELSE CONCAT(ROUND(a_rec * 100, 1), '%') END,  CASE WHEN v_rec IS NULL THEN '—' ELSE CONCAT(ROUND(v_rec * 100, 1), '%') END , CASE WHEN i_rec IS NULL THEN '—' ELSE CONCAT(ROUND(i_rec * 100, 1), '%') END, CASE WHEN r_rec IS NULL THEN '—' ELSE CONCAT(ROUND(r_rec * 100, 1), '%') END FROM f
# MAGIC UNION ALL SELECT 5, 'F1',       CASE WHEN m_f1 IS NULL THEN '—' ELSE CAST(ROUND(m_f1, 3) AS STRING) END,   CASE WHEN a_f1 IS NULL THEN '—' ELSE CAST(ROUND(a_f1, 3) AS STRING) END,   CASE WHEN v_f1 IS NULL THEN '—' ELSE CAST(ROUND(v_f1, 3) AS STRING) END  , CASE WHEN i_f1 IS NULL THEN '—' ELSE CAST(ROUND(i_f1, 3) AS STRING) END, CASE WHEN r_f1 IS NULL THEN '—' ELSE CAST(ROUND(r_f1, 3) AS STRING) END FROM f
# MAGIC UNION ALL SELECT 6, 'VP / VN / FP / FN',
# MAGIC        CONCAT(m_vp, ' / ', m_vn, ' / ', m_fp, ' / ', m_fn),
# MAGIC        CONCAT(a_vp, ' / ', a_vn, ' / ', a_fp, ' / ', a_fn), '—', '—', '—' FROM f
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
# MAGIC        CONCAT('v', ver, ' · limiar ', REPLACE(CAST(v_thr AS STRING), '.', ',')),
# MAGIC        CASE
# MAGIC          WHEN i_f1 IS NULL THEN '—'
# MAGIC          WHEN i_f1 >= p_f1 THEN CONCAT('✅ Supera a persistência do pluviômetro (F1 ', ROUND(p_f1, 2), ')')
# MAGIC          ELSE CONCAT('⚠️ Abaixo da persistência do pluviômetro (F1 ', ROUND(p_f1, 2), ')')
# MAGIC        END,
# MAGIC        CASE
# MAGIC          WHEN r_f1 IS NULL THEN '—'
# MAGIC          WHEN i_f1 IS NULL THEN CONCAT('v', r_ver, ' · limiar ', REPLACE(CAST(r_thr AS STRING), '.', ','))
# MAGIC          ELSE CONCAT(CASE WHEN r_f1 >= i_f1 THEN '✅ +' ELSE '⚠️ ' END,
# MAGIC                      ROUND(r_f1 - i_f1, 2), ' de F1 vs champion · v', r_ver,
# MAGIC                      ' · limiar ', REPLACE(CAST(r_thr AS STRING), '.', ','))
# MAGIC        END FROM f
# MAGIC ORDER BY ord

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Acurácia por antecedência (produção)
# MAGIC > Todas as previsões do histórico (cada hora-alvo é prevista por até 4 rodadas) × chuva observada, só do champion atual. View `gold.rain_model_accuracy_by_lead`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Acurácia por antecedência (produção, champion atual): todas as previsões do
# MAGIC -- histórico × chuva observada; ⏳ enquanto a faixa tiver menos de 48 horas avaliadas
# MAGIC WITH atual AS (
# MAGIC   SELECT max_by(modelo, inferencia_local) AS modelo FROM weather_pipeline.gold.rain_forecast_ml_history
# MAGIC ),
# MAGIC b AS (
# MAGIC   SELECT l.* FROM weather_pipeline.gold.rain_model_accuracy_by_lead l JOIN atual a ON l.modelo = a.modelo
# MAGIC ),
# MAGIC faixas AS (SELECT explode(array('01–06 h', '07–12 h', '13–18 h', '19–24 h')) AS faixa_antecedencia),
# MAGIC agg AS (
# MAGIC   SELECT faixa_antecedencia, count(*) AS n,
# MAGIC     sum(CASE WHEN pred_vai_chover = 1 AND choveu = 1 THEN 1 ELSE 0 END) AS vp,
# MAGIC     sum(CASE WHEN pred_vai_chover = 1 AND choveu = 0 THEN 1 ELSE 0 END) AS fp,
# MAGIC     sum(CASE WHEN pred_vai_chover = 0 AND choveu = 1 THEN 1 ELSE 0 END) AS fn,
# MAGIC     sum(CASE WHEN api_vai_chover = 1 AND choveu = 1 THEN 1 ELSE 0 END)  AS avp,
# MAGIC     sum(CASE WHEN api_vai_chover = 1 AND choveu = 0 THEN 1 ELSE 0 END)  AS afp,
# MAGIC     sum(CASE WHEN api_vai_chover = 0 AND choveu = 1 THEN 1 ELSE 0 END)  AS afn
# MAGIC   FROM b GROUP BY faixa_antecedencia
# MAGIC )
# MAGIC SELECT
# MAGIC   f.faixa_antecedencia                                              AS `Antecedência`,
# MAGIC   COALESCE(n, 0)                                                    AS `Horas avaliadas`,
# MAGIC   CASE WHEN COALESCE(n, 0) < 48 THEN '⏳'
# MAGIC        ELSE COALESCE(CAST(ROUND(2 * vp / NULLIF(2 * vp + fp + fn, 0), 3) AS STRING), '—') END   AS `F1 modelo`,
# MAGIC   CASE WHEN COALESCE(n, 0) < 48 THEN '⏳'
# MAGIC        ELSE COALESCE(CAST(ROUND(2 * avp / NULLIF(2 * avp + afp + afn, 0), 3) AS STRING), '—') END AS `F1 API Open-Meteo`,
# MAGIC   CASE WHEN COALESCE(n, 0) < 48 THEN '⏳'
# MAGIC        ELSE COALESCE(CONCAT(ROUND(vp * 100 / NULLIF(vp + fp, 0), 1), '%'), '—') END             AS `Precisão modelo`,
# MAGIC   CASE WHEN COALESCE(n, 0) < 48 THEN '⏳'
# MAGIC        ELSE COALESCE(CONCAT(ROUND(vp * 100 / NULLIF(vp + fn, 0), 1), '%'), '—') END             AS `Recall modelo`,
# MAGIC   (SELECT modelo FROM atual)                                        AS `Modelo`
# MAGIC FROM faixas f
# MAGIC LEFT JOIN agg a ON a.faixa_antecedencia = f.faixa_antecedencia
# MAGIC ORDER BY f.faixa_antecedencia

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Temperatura de hoje vs média histórica
# MAGIC > Por hora: observado + previsão de hoje × média 1991–2020 numa janela de ±7 dias em torno da data.

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
# MAGIC ## 7. Resumo de Alertas — hoje e amanhã
# MAGIC > Uma linha por hora (observado + previsão). **Diferença p/ média histórica**: temperatura da hora − média 1991–2020 da mesma hora (±7 dias; 🔴 ≥ +2 °C, 🔵 ≤ −2 °C). **Modelo** e **Regional**: última previsão feita antes da hora (histórico em `rain_forecast_ml_history`).

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH modelo AS (
# MAGIC   -- última previsão feita antes de cada hora-alvo (mesma regra da view de acurácia)
# MAGIC   SELECT target_time, prob_chuva, prob_chuva_medida, threshold_regional,
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
# MAGIC   END                                                               AS `Modelo (chance de chuva)`,
# MAGIC   CASE
# MAGIC     WHEN m.prob_chuva_medida IS NULL THEN '—'
# MAGIC     ELSE CONCAT(
# MAGIC       CASE WHEN m.prob_chuva_medida >= 0.9 THEN '🔴 '
# MAGIC            WHEN m.prob_chuva_medida >= m.threshold_regional THEN '🟠 '
# MAGIC            WHEN m.prob_chuva_medida >= m.threshold_regional - 0.2 THEN '🟡 '
# MAGIC            ELSE '🟢 ' END,
# MAGIC       CAST(ROUND(m.prob_chuva_medida * 100) AS INT), '%')
# MAGIC   END                                                               AS `Regional (chuva medida)`
# MAGIC FROM weather_pipeline.gold.weather_today wt
# MAGIC LEFT JOIN modelo m
# MAGIC   ON m.target_time = wt.observation_time AND m.rn = 1
# MAGIC ORDER BY wt.observation_time

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. ENSO — El Niño / La Niña (NOAA CPC)
# MAGIC > Índice ONI: anomalia da temperatura do mar no Niño 3.4 (média de 3 meses). Fase pelo limiar ±0,5 °C; intensidade: fraco < 1,0 ≤ moderado < 1,5 ≤ forte < 2,0 ≤ muito forte. Índice global (vale para qualquer cidade).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ENSO atual (último trimestre publicado pela NOAA CPC) e tendência
# MAGIC WITH u AS (
# MAGIC   SELECT season, year, oni, fase, intensidade, center_month,
# MAGIC          oni - LAG(oni) OVER (ORDER BY center_month) AS variacao
# MAGIC   FROM weather_pipeline.silver.enso_oni
# MAGIC )
# MAGIC SELECT
# MAGIC   CONCAT(
# MAGIC     CASE fase WHEN 'El Niño' THEN '🔴 ' WHEN 'La Niña' THEN '🔵 ' ELSE '⚪ ' END,
# MAGIC     fase,
# MAGIC     CASE WHEN intensidade IS NOT NULL THEN CONCAT(' ', intensidade) ELSE '' END,
# MAGIC     ' (', CASE WHEN oni >= 0 THEN '+' ELSE '' END,
# MAGIC     REPLACE(CAST(ROUND(oni, 1) AS STRING), '.', ','), ')')        AS `ENSO`,
# MAGIC   CONCAT(
# MAGIC     CASE WHEN variacao >  0.1 THEN '📈 Aquecendo'
# MAGIC          WHEN variacao < -0.1 THEN '📉 Esfriando'
# MAGIC          ELSE '➡️ Estável' END,
# MAGIC     ' · ', season, '/', year)                                    AS `Tendência`
# MAGIC FROM u
# MAGIC ORDER BY center_month DESC
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ONI dos últimos 15 anos; limiares de El Niño (+0,5) e La Niña (−0,5)
# MAGIC SELECT
# MAGIC   year + (MONTH(center_month) - 1) / 12.0  AS ano,
# MAGIC   oni,
# MAGIC   0.5                                       AS limiar_el_nino,
# MAGIC   -0.5                                      AS limiar_la_nina
# MAGIC FROM weather_pipeline.silver.enso_oni
# MAGIC WHERE center_month >= add_months(current_date(), -12 * 15)
# MAGIC ORDER BY center_month

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Precipitação por Mês (média 1991–2020)

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
# MAGIC ## 10. Temperatura por Mês (média 1991–2020)

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
# MAGIC ## 11. Tendência Anual de Temperatura (anos completos)
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
