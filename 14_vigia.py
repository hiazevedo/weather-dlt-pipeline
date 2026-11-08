# Databricks notebook source
# Vigia — confere se as coletas e o pipeline continuam vivos e FALHA quando
# algo parou, para o Databricks mandar o e-mail de falha do job
# "[weather] Vigia" (email_notifications.on_failure no databricks.yml).
#
# Por que existe: o e-mail de falha do job de 1h só sai quando uma execução
# falha. Se ela nem começa, ou se o celular para de coletar, nada avisa — de
# 02 a 07/11/2026 o celular e o job ficaram 5 dias parados sem ninguém saber.
#
# Limite: se o Databricks bloquear TODAS as execuções do workspace (foi o que
# aconteceu naquela semana: "Triggering new runs ... disabled temporarily"),
# o vigia também não roda e não há e-mail.
#
# Para não mandar o mesmo e-mail a cada execução, cada problema fica aberto em
# gold.vigia_alertas e só volta a alertar depois de REPETIR_H horas.

# COMMAND ----------

import json
from datetime import datetime, timedelta
import pytz

TZ = pytz.timezone("America/Sao_Paulo")
AGORA = datetime.now(TZ).replace(tzinfo=None)      # hora local sem fuso, como nas tabelas
REPETIR_H = 12
ESTADO = "weather_pipeline.gold.vigia_alertas"
SINAL_CELULAR = "/Volumes/weather_pipeline/bronze/coletores/celular.json"


def ha(t):
    """'há 3 h 20 min' a partir de um datetime local."""
    m = int((AGORA - t).total_seconds() // 60)
    return f"há {m // 60} h {m % 60:02d} min" if m >= 60 else f"há {m} min"


def ultimo(sql):
    return spark.sql(sql).collect()[0][0]

# COMMAND ----------

problemas = {}     # id → descrição

# Teste do e-mail: rode o job com o parâmetro simular=<texto>
dbutils.widgets.text("simular", "")
if dbutils.widgets.get("simular"):
    problemas["simulado"] = f"Teste do vigia: {dbutils.widgets.get('simular')}"

# 1) Job de 1h: a inferência é a última task, então a previsão mais recente
#    mostra a última execução completa
t = ultimo("SELECT max(inferencia_local) FROM weather_pipeline.gold.rain_forecast_ml")
if t is None or AGORA - t > timedelta(hours=2, minutes=30):
    problemas["job_1h"] = (f"Job de 1h sem execução completa {ha(t)} (última previsão em {t:%d/%m %H:%M})"
                           if t else "Job de 1h: gold.rain_forecast_ml vazia")

# 2) Coleta da previsão (primeira task do job de 1h): falha aqui para o DLT sem dado novo
t = ultimo("""SELECT max(from_utc_timestamp(to_timestamp(collected_at), 'America/Sao_Paulo'))
              FROM weather_pipeline.silver.weather_raw WHERE NOT is_historical""")
if t is None or AGORA - t > timedelta(hours=2, minutes=30):
    problemas["coleta_previsao"] = f"Nenhuma previsão nova da Open-Meteo {ha(t)} (última em {t:%d/%m %H:%M})"

# 3) Celular (coletor do INMET): sinal de vida gravado a cada hora pelo inmet_avisos_sync.py
try:
    s = spark.sql(f"SELECT to_timestamp(enviado_em) AS t, resultado "
                  f"FROM read_files('{SINAL_CELULAR}', format => 'json')").collect()[0]
    if AGORA - s.t > timedelta(hours=2, minutes=30):
        problemas["celular"] = f"Celular sem contato {ha(s.t)} (último sinal em {s.t:%d/%m %H:%M})"
    elif s.resultado.startswith("erro"):
        problemas["celular_erro"] = f"Celular com erro na coleta dos avisos às {s.t:%H:%M}: {s.resultado}"
except Exception as e:  # arquivo ausente ou ilegível também é problema
    problemas["celular"] = f"Sinal de vida do celular ilegível: {type(e).__name__}: {str(e)[:200]}"

# 4) Job semanal: o backfill regrava historical_<ano>.json até hoje − 5 dias
t = ultimo("""SELECT max(_ingested_at) FROM weather_pipeline.silver.weather_raw WHERE is_historical""")
t = t and t.replace(tzinfo=pytz.utc).astimezone(TZ).replace(tzinfo=None)
if t is None or AGORA - t > timedelta(days=8):
    problemas["job_semanal"] = f"Backfill semanal (Archive) sem rodar {ha(t)} (último em {t:%d/%m %H:%M})"

# 5) MOS multimodelo (bundle de ML, a cada 3 h): chance de chuva medida dos 6 modelos
try:
    t = ultimo("SELECT max(inferencia_local) FROM weather_pipeline.gold.rain_forecast_mos")
    if t is None or AGORA - t > timedelta(hours=7):
        problemas["mos_multimodelo"] = f"Previsão dos 6 modelos (MOS) sem atualizar {ha(t)} (última em {t:%d/%m %H:%M})"
except Exception as e:
    problemas["mos_multimodelo"] = f"gold.rain_forecast_mos ilegível: {type(e).__name__}: {str(e)[:200]}"

print(f"{AGORA:%d/%m %H:%M} — {len(problemas)} problema(s)")
for k, v in problemas.items():
    print(f"  - [{k}] {v}")

# COMMAND ----------

# Estado: abre/atualiza/resolve os problemas e decide se este é um alerta novo
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {ESTADO} (
        problema STRING, descricao STRING, aberto_em TIMESTAMP,
        ultimo_alerta TIMESTAMP, resolvido_em TIMESTAMP)
""")
abertos = {r.problema: r for r in spark.table(ESTADO).where("resolvido_em IS NULL").collect()}

# Alerta quando o problema é novo ou o último alerta tem REPETIR_H horas.
# "Alertado há menos de 30 min" também falha: o serverless repete a task que
# falhou (o max_retries: 0 do bundle é omitido no deploy), e sem isso a
# repetição veria o alerta já registrado, terminaria com sucesso e o job não
# mandaria o e-mail.
alertar = []
for k, desc in problemas.items():
    r = abertos.get(k)
    if (r is None or AGORA - r.ultimo_alerta >= timedelta(hours=REPETIR_H)
            or AGORA - r.ultimo_alerta < timedelta(minutes=30)):
        alertar.append(desc)

linhas = [(k, desc, (abertos[k].aberto_em if k in abertos else AGORA),
           (AGORA if desc in alertar else abertos[k].ultimo_alerta), None)
          for k, desc in problemas.items()]
resolvidos = [k for k in abertos if k not in problemas]

df = spark.createDataFrame(linhas, "problema string, descricao string, aberto_em timestamp, "
                                   "ultimo_alerta timestamp, resolvido_em timestamp")
df.createOrReplaceTempView("_vigia_atual")
spark.sql(f"""
    MERGE INTO {ESTADO} e
    USING _vigia_atual a ON e.problema = a.problema AND e.resolvido_em IS NULL
    WHEN MATCHED THEN UPDATE SET e.descricao = a.descricao, e.ultimo_alerta = a.ultimo_alerta
    WHEN NOT MATCHED THEN INSERT *
""")
if resolvidos:
    lista = ", ".join(f"'{k}'" for k in resolvidos)
    spark.sql(f"UPDATE {ESTADO} SET resolvido_em = timestamp'{AGORA:%Y-%m-%d %H:%M:%S}' "
              f"WHERE resolvido_em IS NULL AND problema IN ({lista})")
    print("resolvidos:", ", ".join(resolvidos))

# COMMAND ----------

# Falhar = e-mail do Databricks. A mensagem do erro vai no e-mail.
if alertar:
    raise RuntimeError("VIGIA — " + " | ".join(alertar))
dbutils.notebook.exit(json.dumps({"problemas": list(problemas), "alertados": len(alertar)}))
