# Databricks notebook source
# INMET — estações automáticas (chuva MEDIDA, horária) → silver.inmet_hourly
#
# Os CSVs chegam ao Volume weather_pipeline.bronze.inmet pelo script local
# scripts/inmet_sync.py (o INMET bloqueia conexões vindas do Databricks).
# Layout: /Volumes/weather_pipeline/bronze/inmet/<ano>/<código>.csv
#
# Observação: não há estação automática do INMET em Birigui; as mais
# próximas ficam a 49–75 km (José Bonifácio, Valparaíso, Tupã, Lins).

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import io, math
import pandas as pd

INMET_PATH  = "/Volumes/weather_pipeline/bronze/inmet"
INMET_TABLE = "weather_pipeline.silver.inmet_hourly"
spark.sql("CREATE VOLUME IF NOT EXISTS weather_pipeline.bronze.inmet")

# As colunas têm a mesma ordem em todos os anos (os nomes mudam um pouco),
# então a leitura é por posição.
COLUNAS = ["data", "hora", "precip_mm", "pressure_hpa", "pressure_max_hpa", "pressure_min_hpa",
           "radiation_kjm2", "temp_c", "dewpoint_c", "temp_max_c", "temp_min_c",
           "dewpoint_max_c", "dewpoint_min_c", "rh_max_pct", "rh_min_pct", "rh_pct",
           "wind_dir_deg", "wind_gust_ms", "wind_speed_ms"]

def km(lat1, lon1, lat2, lon2):
    p = math.radians
    h = (math.sin(p(lat2 - lat1) / 2) ** 2 +
         math.cos(p(lat1)) * math.cos(p(lat2)) * math.sin(p(lon2 - lon1) / 2) ** 2)
    return 12742 * math.asin(math.sqrt(h))

def ler(caminho):
    texto = open(caminho, "rb").read().decode("latin-1")
    linhas = texto.splitlines()
    # Campos do cabeçalho identificados pelo início do nome: a grafia varia entre
    # anos ("ESTACAO", "ESTAÇÃO" e, em 2019, "ESTAC?O" — acentos viraram "?")
    PREFIXOS = {"REGI": "REGIAO", "UF": "UF", "ESTA": "ESTACAO", "COD": "CODIGO",
                "LAT": "LATITUDE", "LON": "LONGITUDE", "ALT": "ALTITUDE"}
    cab = {}
    for l in linhas[:8]:
        if ":;" not in l:
            continue
        k, v = l.rstrip(";").split(":;", 1)
        for pre, nome in PREFIXOS.items():
            if k.strip().upper().startswith(pre):
                cab[nome] = v.strip()
    df = pd.read_csv(io.StringIO("\n".join(linhas[9:])), sep=";", header=None,
                     dtype=str, keep_default_na=False).iloc[:, :len(COLUNAS)]
    df.columns = COLUNAS[:df.shape[1]]
    num = [c for c in COLUNAS[2:] if c in df.columns]
    df[num] = (df[num].apply(lambda s: s.str.replace(",", ".", regex=False))
                      .replace({"": None, "-9999": None})
                      .apply(pd.to_numeric, errors="coerce"))
    hora = df["hora"].str.extract(r"(\d{2})")[0]
    utc = pd.to_datetime(df["data"].str.replace("/", "-", regex=False) + " " + hora + ":00",
                         format="%Y-%m-%d %H:%M", errors="coerce")
    df["observation_time_utc"] = utc
    # hora local sem fuso, como o observation_time da silver (Open-Meteo, America/Sao_Paulo)
    df["observation_time"] = (utc.dt.tz_localize("UTC").dt.tz_convert(TIMEZONE).dt.tz_localize(None))
    lat = float(cab["LATITUDE"].replace(",", "."))
    lon = float(cab["LONGITUDE"].replace(",", "."))
    df["station_id"]   = cab.get("CODIGO", caminho.rsplit("/", 1)[-1].split(".")[0])
    df["station_name"] = cab.get("ESTACAO", "")
    df["uf"]           = cab.get("UF", "")
    df["lat"], df["lon"] = lat, lon
    df["altitude_m"]   = pd.to_numeric(cab.get("ALTITUDE", "").replace(",", "."), errors="coerce")
    df["dist_km"]      = round(km(LAT, LON, lat, lon), 1)
    return df.drop(columns=["data", "hora"]).dropna(subset=["observation_time_utc"])

# COMMAND ----------

arquivos = [f.path.replace("dbfs:", "") for ano in dbutils.fs.ls(INMET_PATH) for f in dbutils.fs.ls(ano.path)
            if f.name.lower().endswith(".csv")]
if not arquivos:
    dbutils.notebook.exit("Volume vazio — rode scripts/inmet_sync.py na máquina local")

df = pd.concat([ler(a) for a in arquivos], ignore_index=True)
# um mesmo ano/estação pode aparecer em mais de um arquivo: fica a última leitura
df = df.drop_duplicates(["station_id", "observation_time_utc"], keep="last")
# Metadados únicos por estação (a precisão das coordenadas muda entre anos):
# nome mais frequente; coordenadas, altitude e distância do arquivo mais recente
meta = (df.sort_values("observation_time_utc").groupby("station_id")
          .agg(station_name=("station_name", lambda s: s[s != ""].mode().iat[0] if (s != "").any() else ""),
               uf=("uf", "last"), lat=("lat", "last"), lon=("lon", "last"),
               altitude_m=("altitude_m", "last"), dist_km=("dist_km", "last")))
df = df.drop(columns=meta.columns).join(meta, on="station_id")
df["cidade"] = CIDADE

(spark.createDataFrame(df)
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(INMET_TABLE))

resumo = (df.assign(valida=df["precip_mm"].notna())
            .groupby(["station_id", "station_name", "dist_km"])
            .agg(horas=("valida", "size"), chuva_valida=("valida", "mean"),
                 inicio=("observation_time", "min"), fim=("observation_time", "max"))
            .reset_index().sort_values("dist_km"))
print(f"{INMET_TABLE}: {len(df):,} horas-estação de {df['station_id'].nunique()} estações")
print(resumo.to_string(index=False))
