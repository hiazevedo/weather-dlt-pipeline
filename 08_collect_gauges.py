# Databricks notebook source
# Pluviômetros (chuva MEDIDA em solo) — NOAA GHCN-Daily
#
# Tudo o que o pipeline chama de "observado" vem de modelo (reanálise ERA5 ou
# o passado da API de previsão). Estes postos dão a chuva medida, para medir
# o viés dos rótulos do ERA5.
#
# Limitações conhecidas:
#   - os postos próximos de Birigui no GHCN-Daily vão só até 1997 (validação
#     histórica, não tempo real);
#   - são diários (leitura manual, em geral às 7h), não horários;
#   - INMET (horário, atual) recusa conexões dos nossos ambientes — quando
#     houver acesso, entra aqui como outra fonte.
#
# Tabela: weather_pipeline.silver.gauge_daily

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import io, gzip, math
import requests
import pandas as pd

GHCN       = "https://www.ncei.noaa.gov/pub/data/ghcn/daily"
RAIO_KM    = 20
GAUGE_TABLE = "weather_pipeline.silver.gauge_daily"

def km(lat1, lon1, lat2, lon2):
    p = math.radians
    h = (math.sin(p(lat2 - lat1) / 2) ** 2 +
         math.cos(p(lat1)) * math.cos(p(lat2)) * math.sin(p(lon2 - lon1) / 2) ** 2)
    return 12742 * math.asin(math.sqrt(h))

# COMMAND ----------

# Postos com precipitação (PRCP) num raio de RAIO_KM do ponto da cidade
inv = requests.get(f"{GHCN}/ghcnd-inventory.txt", timeout=300).text
postos = {}
for l in inv.splitlines():
    if l[31:35] != "PRCP":
        continue
    lat, lon = float(l[12:20]), float(l[21:30])
    d = km(LAT, LON, lat, lon)
    if d <= RAIO_KM:
        postos[l[:11]] = dict(lat=lat, lon=lon, dist_km=round(d, 1),
                              ano_ini=int(l[36:40]), ano_fim=int(l[41:45]))

nomes = {}
for l in requests.get(f"{GHCN}/ghcnd-stations.txt", timeout=300).text.splitlines():
    if l[:11] in postos:
        nomes[l[:11]] = l[41:71].strip()

print(f"{len(postos)} postos com PRCP até {RAIO_KM} km de {CIDADE}:")
for sid, p in sorted(postos.items(), key=lambda kv: kv[1]["dist_km"]):
    print(f"   {p['dist_km']:5.1f} km  {sid}  {nomes.get(sid, '?'):<30} {p['ano_ini']}–{p['ano_fim']}")

# COMMAND ----------

frames = []
for sid, p in postos.items():
    r = requests.get(f"{GHCN}/by_station/{sid}.csv.gz", timeout=300)
    r.raise_for_status()
    d = pd.read_csv(io.BytesIO(gzip.decompress(r.content)), header=None,
                    names=["station_id", "date", "element", "value",
                           "mflag", "qflag", "sflag", "obs_time"],
                    dtype={"date": str, "mflag": str, "qflag": str, "sflag": str, "obs_time": str})
    d = d[(d["element"] == "PRCP") & (d["qflag"].isna())]   # descarta valores reprovados no controle de qualidade
    d["date"]    = pd.to_datetime(d["date"], format="%Y%m%d").dt.date
    d["prcp_mm"] = d["value"] / 10.0                          # GHCN guarda em décimos de mm
    d["name"], d["lat"], d["lon"], d["dist_km"] = nomes.get(sid), p["lat"], p["lon"], p["dist_km"]
    frames.append(d[["station_id", "name", "lat", "lon", "dist_km", "date", "prcp_mm", "mflag", "sflag"]])

gauges = pd.concat(frames, ignore_index=True)
gauges["cidade"] = CIDADE
gauges[["mflag", "sflag"]] = gauges[["mflag", "sflag"]].fillna("")
(spark.createDataFrame(gauges)
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(GAUGE_TABLE))

print(f"\n{GAUGE_TABLE}: {len(gauges):,} dias-posto "
      f"({gauges['date'].min()} → {gauges['date'].max()})")
