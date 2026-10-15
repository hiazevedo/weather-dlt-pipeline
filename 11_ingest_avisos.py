# Databricks notebook source
# Avisos meteorológicos oficiais do INMET (os que a Defesa Civil repassa)
#
# Snapshots JSON enviados a cada 3 h pelo script local scripts/inmet_avisos_sync.py
# (o INMET bloqueia o Databricks) → weather_pipeline.bronze.inmet_avisos
#
# Saídas:
#   silver.inmet_avisos     — um registro por aviso (última versão vista), com
#                             municípios da UF (IBGE), polígono e histórico de coleta
#   gold.avisos_cidade      — VIEW: avisos que incluem a cidade do config, com a
#                             situação (ativo / futuro / encerrado) calculada na consulta
#   gold.avisos_vs_modelo   — VIEW: cada aviso de chuva/tempestade da cidade × chuva
#                             observada × o que champion e regional previram na janela

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import ast, json
import pandas as pd

AVISOS_PATH  = "/Volumes/weather_pipeline/bronze/inmet_avisos"
AVISOS_TABLE = "weather_pipeline.silver.inmet_avisos"
spark.sql("CREATE VOLUME IF NOT EXISTS weather_pipeline.bronze.inmet_avisos")

def listar(caminho):
    for f in dbutils.fs.ls(caminho):
        if f.isDir():
            yield from listar(f.path)
        elif f.name.endswith(".json"):
            yield f.path

def lista(v):
    """Riscos/instruções vêm como texto de lista Python ("['a', 'b']")."""
    if isinstance(v, list):
        return [str(x).strip() for x in v]
    try:
        r = ast.literal_eval(str(v))
        return [str(x).strip() for x in r] if isinstance(r, (list, tuple)) else [str(r).strip()]
    except (ValueError, SyntaxError):
        return [str(v).strip()] if v else []

# COMMAND ----------

linhas = []
for caminho in listar(AVISOS_PATH):
    snap = json.loads(dbutils.fs.head(caminho, 50_000_000))
    for grupo in ("hoje", "futuro"):
        for a in snap.get(grupo, []):
            linhas.append({**a, "coletado_em": snap["coletado_em"]})
if not linhas:
    dbutils.notebook.exit("sem snapshots — rode scripts/inmet_avisos_sync.py na máquina local")

df = pd.DataFrame(linhas)
df["id"] = df["id"].astype(str)
df["coletado_em"] = pd.to_datetime(df["coletado_em"])
hist = df.groupby("id")["coletado_em"].agg(primeiro_visto="min", ultimo_visto="max", n_snapshots="size")
df = (df.sort_values(["coletado_em", "updated_at"]).drop_duplicates("id", keep="last")
        .set_index("id").join(hist).reset_index())

for c in ("inicio", "fim"):                        # hora local de Brasília ("2026-10-15 00:01")
    df[c] = pd.to_datetime(df[c], format="%Y-%m-%d %H:%M", errors="coerce")
for c in ("created_at", "updated_at"):
    df[c] = pd.to_datetime(df[c], errors="coerce", utc=True).dt.tz_convert(TIMEZONE).dt.tz_localize(None)
for c in ("riscos", "instrucoes"):
    df[c] = df[c].apply(lista)
for c in ("geocodes", "municipios"):
    df[c] = df[c].apply(lambda v: v if isinstance(v, list) else [])
df["nivel"] = df["severidade"].map({"Perigo Potencial": 1, "Perigo": 2, "Grande Perigo": 3}).fillna(0).astype(int)
txt = ["id", "id_aviso", "codigo", "descricao", "severidade", "aviso_cor", "estados", "poligono",
       "encerrado", "alterado", "id_severidade", "id_condicao_severa", "referencia"]
for c in txt:
    df[c] = df[c].astype(str).where(df[c].notna(), None) if c in df.columns else None

cols = ["id", "id_aviso", "codigo", "descricao", "severidade", "nivel", "aviso_cor", "inicio", "fim",
        "riscos", "instrucoes", "estados", "geocodes", "municipios", "poligono", "encerrado", "alterado",
        "created_at", "updated_at", "primeiro_visto", "ultimo_visto", "n_snapshots"]
(spark.createDataFrame(df[cols])
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(AVISOS_TABLE))
print(f"{AVISOS_TABLE}: {len(df)} avisos (de {df['primeiro_visto'].min()} a {df['ultimo_visto'].max()})")

# COMMAND ----------

NOW = "from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo')"
spark.sql(f"""
    CREATE OR REPLACE VIEW weather_pipeline.gold.avisos_cidade AS
    SELECT
        id, descricao, severidade, nivel, aviso_cor, inicio, fim,
        CASE
            WHEN lower(encerrado) IN ('true', '1') THEN 'encerrado'
            WHEN {NOW} < inicio THEN 'futuro'
            WHEN {NOW} <= fim   THEN 'ativo'
            ELSE 'encerrado'
        END                                       AS situacao,
        array_join(riscos, ' ')                   AS riscos,
        array_join(instrucoes, ' ')               AS instrucoes,
        primeiro_visto, ultimo_visto, poligono,
        '{CIDADE}'                                AS cidade
    FROM {AVISOS_TABLE}
    WHERE array_contains(geocodes, '{CIDADE_IBGE}')
""")

# Avisos de chuva/tempestade da cidade × chuva observada × previsões (última
# previsão feita antes de cada hora da janela). Enche conforme o histórico cresce.
spark.sql(f"""
    CREATE OR REPLACE VIEW weather_pipeline.gold.avisos_vs_modelo AS
    WITH av AS (
        SELECT * FROM weather_pipeline.gold.avisos_cidade
        WHERE lower(descricao) RLIKE 'chuva|tempestade|temporal'
    ),
    prev AS (
        SELECT target_time, prob_chuva, pred_vai_chover, prob_chuva_medida, pred_chuva_medida,
               ROW_NUMBER() OVER (PARTITION BY target_time ORDER BY inferencia_local DESC) AS rn
        FROM weather_pipeline.gold.rain_forecast_ml_history
        WHERE inferencia_local < target_time
    ),
    obs AS (
        SELECT observation_time, precipitation
        FROM weather_pipeline.silver.weather_clean WHERE NOT is_forecast
    ),
    obs_agg AS (
        SELECT av.id,
               count(o.observation_time)            AS horas_observadas,
               round(sum(o.precipitation), 1)       AS chuva_obs_mm,
               round(max(o.precipitation), 1)       AS chuva_max_hora_mm
        FROM av LEFT JOIN obs o ON o.observation_time BETWEEN av.inicio AND av.fim
        GROUP BY av.id
    ),
    prev_agg AS (
        SELECT av.id,
               count(p.target_time)                 AS horas_com_previsao,
               round(max(p.prob_chuva), 3)          AS prob_max_champion,
               sum(p.pred_vai_chover)               AS horas_alerta_champion,
               round(max(p.prob_chuva_medida), 3)   AS prob_max_regional,
               CAST(sum(p.pred_chuva_medida) AS INT) AS horas_alerta_regional
        FROM av LEFT JOIN prev p ON p.rn = 1 AND p.target_time BETWEEN av.inicio AND av.fim
        GROUP BY av.id
    )
    SELECT
        av.id, av.descricao, av.severidade, av.inicio, av.fim, av.situacao,
        CAST((unix_timestamp(av.fim) - unix_timestamp(av.inicio)) / 3600 AS INT) + 1 AS horas_janela,
        o.horas_observadas, o.chuva_obs_mm, o.chuva_max_hora_mm,
        p.horas_com_previsao, p.prob_max_champion, p.horas_alerta_champion,
        p.prob_max_regional, p.horas_alerta_regional
    FROM av
    JOIN obs_agg o USING (id)
    JOIN prev_agg p USING (id)
""")
display(spark.sql("SELECT descricao, severidade, situacao, inicio, fim FROM weather_pipeline.gold.avisos_cidade ORDER BY inicio DESC"))
