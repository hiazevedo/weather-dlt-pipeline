# Databricks notebook source
# MERGE/CPTEC (INPE) — chuva horária "medida" em grade de 10 km
#
# Por que: Birigui não tem pluviômetro automático (o CEMADEN não tem estação na
# região; as do INMET mais próximas ficam a 49–75 km e várias pararam em 2026).
# O MERGE junta a estimativa de satélite (GPM-IMERG) com os pluviômetros de
# superfície, hora a hora, para a América do Sul — é o rótulo de "chuva medida"
# possível para a cidade.
#
# Fonte: https://ftp.cptec.inpe.br/modelos/tempo/MERGE/GPM/HOURLY/AAAA/MM/DD/
#        MERGE_CPTEC_AAAAMMDDHH.grib2 (UTC; ~1 dia de atraso). Variáveis: PREC
#        (mm/h) e NEST (pluviômetros no ponto de grade; vazio = nenhum).
# Tabela: weather_pipeline.silver.merge_hourly — um registro por ponto e hora,
#         nos pontos de Birigui e das estações do INMET (para validação).
# Incremental: só baixa as horas que faltam desde INICIO.

# COMMAND ----------

# MAGIC %pip install -q eccodes
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import eccodes
import requests

BASE = "https://ftp.cptec.inpe.br/modelos/tempo/MERGE/GPM/HOURLY"
INICIO = datetime(2023, 6, 1)          # sobreposição com os postos da ANA em Birigui (até jan/2024)
TABLE = "weather_pipeline.silver.merge_hourly"
LOTE = 1500                            # horas por gravação (o progresso fica salvo se cair)
THREADS = 12

est = spark.sql("""SELECT station_id, first(lat) AS lat, first(lon) AS lon
                   FROM weather_pipeline.silver.inmet_hourly GROUP BY station_id""").collect()
PONTOS = {"birigui": (LAT, LON), **{r.station_id: (r.lat, r.lon) for r in est}}

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        ponto STRING, lat DOUBLE, lon DOUBLE,
        hora_utc TIMESTAMP COMMENT 'hora do arquivo do MERGE (UTC)',
        observation_time TIMESTAMP COMMENT 'hora_utc − 3 h (hora local, como na silver)',
        prec_mm DOUBLE, n_estacoes INT, coletado_em TIMESTAMP)
    COMMENT 'MERGE/CPTEC horário (satélite GPM-IMERG + pluviômetros) nos pontos de interesse'
""")

# COMMAND ----------

def ler_grib(conteudo):
    """{'rdp' ou 'prmsl': {ponto: valor}} — no MERGE horário, rdp = PREC e prmsl = NEST."""
    with tempfile.NamedTemporaryFile(suffix=".grib2") as tmp:
        tmp.write(conteudo); tmp.flush()
        out = {}
        with open(tmp.name, "rb") as f:
            while (g := eccodes.codes_grib_new_from_file(f)) is not None:
                try:
                    ni, nj = eccodes.codes_get(g, "Ni"), eccodes.codes_get(g, "Nj")
                    lat0 = eccodes.codes_get(g, "latitudeOfFirstGridPointInDegrees")
                    lon0 = eccodes.codes_get(g, "longitudeOfFirstGridPointInDegrees")
                    dx = eccodes.codes_get(g, "iDirectionIncrementInDegrees")
                    dy = eccodes.codes_get(g, "jDirectionIncrementInDegrees")
                    jpos = eccodes.codes_get(g, "jScansPositively")
                    miss = eccodes.codes_get(g, "missingValue")
                    v = eccodes.codes_get_values(g).reshape(nj, ni)
                    nome = eccodes.codes_get(g, "shortName")
                    for p, (la, lo) in PONTOS.items():
                        i = int(round(((lo % 360) - (lon0 % 360)) / dx))
                        j = int(round((la - lat0) / dy)) if jpos else int(round((lat0 - la) / dy))
                        x = float(v[j, i])
                        out.setdefault(nome, {})[p] = None if x == miss else x
                finally:
                    eccodes.codes_release(g)
    return out

def baixar(t, sessao, tentativas=4):
    url = f"{BASE}/{t:%Y}/{t:%m}/{t:%d}/MERGE_CPTEC_{t:%Y%m%d%H}.grib2"
    for i in range(tentativas):
        try:
            r = sessao.get(url, timeout=60)
            if r.status_code == 404:
                return t, None                     # ainda não publicado (ou falha do INPE)
            r.raise_for_status()
            return t, r.content
        except requests.RequestException:
            time.sleep(5 * (i + 1))
    return t, None

# COMMAND ----------

# Horas que faltam (por Birigui: todos os pontos saem do mesmo arquivo)
feitas = {r[0] for r in spark.sql(f"SELECT DISTINCT hora_utc FROM {TABLE} WHERE ponto = 'birigui'").collect()}
agora = datetime.now(timezone.utc).replace(tzinfo=None, minute=0, second=0, microsecond=0)
faltam, t = [], INICIO
while t < agora - timedelta(hours=2):
    if t not in feitas:
        faltam.append(t)
    t += timedelta(hours=1)
print(f"{len(feitas):,} horas já na tabela; {len(faltam):,} para baixar")

sessao = requests.Session()
sessao.headers["User-Agent"] = "weather-dlt-pipeline (coleta MERGE horario)"
total, sem_arquivo = 0, 0
for k in range(0, len(faltam), LOTE):
    lote, linhas = faltam[k:k + LOTE], []
    with ThreadPoolExecutor(THREADS) as ex:
        for fut in as_completed([ex.submit(baixar, t, sessao) for t in lote]):
            t, conteudo = fut.result()
            if conteudo is None:
                sem_arquivo += 1; continue
            v = ler_grib(conteudo)            # decodifica na thread principal (eccodes)
            for p, (la, lo) in PONTOS.items():
                n = v.get("prmsl", {}).get(p)
                linhas.append((p, float(la), float(lo), t, t - timedelta(hours=3),
                               v.get("rdp", {}).get(p), None if n is None else int(n), datetime.utcnow()))
    if linhas:
        spark.createDataFrame(linhas, "ponto string, lat double, lon double, hora_utc timestamp, "
                              "observation_time timestamp, prec_mm double, n_estacoes int, coletado_em timestamp"
                              ).write.mode("append").saveAsTable(TABLE)
    total += len(linhas) // len(PONTOS)
    print(f"  lote {k // LOTE + 1}: {total:,} horas gravadas, {sem_arquivo} sem arquivo")
print(f"concluído — {total:,} horas novas; {sem_arquivo} horas sem arquivo (tentadas de novo na próxima execução)")
