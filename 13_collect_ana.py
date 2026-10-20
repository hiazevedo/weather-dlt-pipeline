# Databricks notebook source
# ANA — chuva MEDIDA perto da cidade: postos (leitura diária) e estações
# telemétricas (a cada 10–30 min)
#
# Fonte: web service público da ANA (telemetriaws1.ana.gov.br/ServiceANA.asmx),
# que responde de dentro do Databricks (o INMET não).
#
#   postos pluviométricos num raio de RAIO_POSTOS_KM → silver.ana_diaria
#     Leitura manual diária (DAEE/SP Águas e outros). Em Birigui, o posto
#     02150015 funciona desde 1939; os dados chegam à ANA com ~2 anos de atraso
#     e são brutos (nível de consistência 1). Os postos "BR00C7-…" do GHCN
#     (silver.gauge_daily) são estes mesmos, só até 1997.
#   estações telemétricas ativas num raio de RAIO_TELEMETRIA_KM → silver.ana_chuva_horaria
#     Chuva somada por hora, em hora local: a hora HH guarda a chuva depois de
#     HH−1 até HH, como o Open-Meteo e o INMET. `completa`: chegaram todos os
#     registros da hora; `suspeita`: hora ou dia acima da chuva de 100 anos.
#     Os sensores da região falham (meses zerados, dias impossíveis): a view
#     gold.ana_telemetria_qc mostra a situação de cada estação por mês.
#   inventário dos dois tipos, com o período que tem dado → silver.ana_estacoes
#
# Telemetria ano a ano: um arquivo por estação e ano no Volume bronze.ana; anos
# fechados não são baixados de novo (para baixar de novo, apague o arquivo).

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import json, math, time
import xml.etree.ElementTree as ET
from datetime import date

import pandas as pd
import requests

ANA = "http://telemetriaws1.ana.gov.br/ServiceANA.asmx"
RAIO_POSTOS_KM     = 20     # o mesmo raio dos pluviômetros do GHCN
RAIO_TELEMETRIA_KM = 30
TELEMETRIA_INICIO  = 2013   # primeiro ano com telemetria na região (Birigui Urbano)
# Chuva de 100 anos de tempo de retorno na equação IDF de Birigui (SGB-CPRM, 2023):
# acima disso, quase certamente é defeito do sensor
CHUVA_1H_MAX  = 114
CHUVA_24H_MAX = 174

RAW        = "/Volumes/weather_pipeline/bronze/ana"
T_ESTACOES = f"{CATALOG}.silver.ana_estacoes"
T_DIARIA   = f"{CATALOG}.silver.ana_diaria"
T_HORARIA  = f"{CATALOG}.silver.ana_chuva_horaria"
spark.sql("CREATE VOLUME IF NOT EXISTS weather_pipeline.bronze.ana")

hoje = pd.Timestamp.now(tz=TIMEZONE).tz_localize(None).normalize()

# Status dos valores diários (códigos da ANA)
STATUS = {"0": "branco", "1": "real", "2": "estimado", "3": "duvidoso", "4": "acumulado"}

def km(lat1, lon1, lat2, lon2):
    p = math.radians
    h = (math.sin(p(lat2 - lat1) / 2) ** 2 +
         math.cos(p(lat1)) * math.cos(p(lat2)) * math.sin(p(lon2 - lon1) / 2) ** 2)
    return 12742 * math.asin(math.sqrt(h))

def consulta(metodo, linha, tentativas=3, **params):
    """Linhas `linha` da resposta (lista de dicts). O serviço é instável: às vezes
    responde "sem dados" por engano ou dá erro 500, então os dois são repetidos.
    Erro persistente levanta exceção; vazio persistente devolve []."""
    for i in range(tentativas):
        try:
            r = requests.get(f"{ANA}/{metodo}", params=params, timeout=300,
                             headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
        except requests.RequestException:
            if i == tentativas - 1:
                raise
            time.sleep(5 * (i + 1))
            continue
        linhas = [{c.tag.split("}")[-1]: (c.text or "").strip() for c in t}
                  for t in ET.fromstring(r.content).iter() if t.tag.split("}")[-1] == linha]
        if linhas:
            return linhas
        time.sleep(i + 1)
    return []

# COMMAND ----------

# Inventário: postos pluviométricos (o código da ANA começa pela quadrícula de
# 1° — 0 + latitude + longitude, ex.: 02150015 = 21°S, 50°W) e estações telemétricas ativas

def quadriculas(raio_km):
    dlat = raio_km / 111.0
    dlon = raio_km / (111.0 * math.cos(math.radians(LAT)))
    return sorted({(int(abs(la)), int(abs(lo)))
                   for la in (LAT - dlat, LAT, LAT + dlat) for lo in (LON - dlon, LON, LON + dlon)})

postos = []
for la, lo in quadriculas(RAIO_POSTOS_KM):
    pre = f"0{la:02d}{lo:02d}"
    for e in consulta("HidroInventario", "Table", codEstDE=pre + "000", codEstATE=pre + "999", tpEst="2",
                      nmEst="", nmRio="", codSubBacia="", codBacia="", nmMunicipio="", nmEstado="",
                      sgResp="", sgOper="", telemetrica=""):
        d = km(LAT, LON, float(e["Latitude"]), float(e["Longitude"]))
        if d <= RAIO_POSTOS_KM:
            postos.append(dict(station_id=e["Codigo"].zfill(8), nome=e["Nome"], tipo="posto",
                               municipio=e["nmMunicipio"], operadora=e["OperadoraSigla"],
                               lat=float(e["Latitude"]), lon=float(e["Longitude"]), dist_km=round(d, 1)))

tel = []
for e in consulta("ListaEstacoesTelemetricas", "Table", statusEstacoes="0", origem=""):
    try:
        d = km(LAT, LON, float(e["Latitude"]), float(e["Longitude"]))
    except ValueError:
        continue
    if d <= RAIO_TELEMETRIA_KM:
        op = e["Operadora"].split(" - ")
        tel.append(dict(station_id=e["CodEstacao"], nome=e["NomeEstacao"], tipo="telemétrica",
                        municipio=e["Municipio-UF"], operadora=op[1] if len(op) > 1 else e["Operadora"],
                        lat=float(e["Latitude"]), lon=float(e["Longitude"]), dist_km=round(d, 1)))

estacoes = pd.DataFrame(postos + tel)
print(f"{len(postos)} postos até {RAIO_POSTOS_KM} km e {len(tel)} telemétricas até {RAIO_TELEMETRIA_KM} km")

# COMMAND ----------

# Postos: série diária completa (valores consistidos, nível 2, prevalecem sobre os brutos)

def serie_diaria(codigo):
    dias = {}
    for g in consulta("HidroSerieHistorica", "SerieHistorica", codEstacao=codigo, dataInicio="01/01/1900",
                      dataFim=f"{hoje:%d/%m/%Y}", tipoDados="2", nivelConsistencia=""):
        mes = pd.Timestamp(g["DataHora"][:10])
        nivel = int(g.get("NivelConsistencia") or 1)
        for d in range(1, mes.days_in_month + 1):
            v = g.get(f"Chuva{d:02d}")
            if not v:
                continue
            dia = (mes + pd.Timedelta(days=d - 1)).date()
            if dia in dias and dias[dia]["nivel_consistencia"] > nivel:
                continue
            dias[dia] = dict(date=dia, chuva_mm=float(v), nivel_consistencia=nivel,
                             status=STATUS.get(g.get(f"Chuva{d:02d}Status") or "", None))
    return pd.DataFrame(list(dias.values()))

frames = []
for e in postos:
    try:
        s = serie_diaria(e["station_id"])
    except requests.RequestException as err:        # fica de fora só nesta semana
        print(f"aviso: posto {e['station_id']} sem resposta ({err})")
        continue
    if len(s):
        frames.append(s.assign(station_id=e["station_id"], nome=e["nome"], dist_km=e["dist_km"]))
diaria = pd.concat(frames, ignore_index=True)
diaria["cidade"] = CIDADE
diaria = diaria[["cidade", "station_id", "nome", "dist_km", "date", "chuva_mm", "status", "nivel_consistencia"]]

# COMMAND ----------

# Telemetria: um arquivo por estação e ano; anos fechados já baixados são reaproveitados

def telemetria(codigo, inicio, fim):
    """Registros (DataHora, Chuva) de uma estação telemétrica entre duas datas (inclusive)."""
    regs = consulta("DadosHidrometeorologicos", "DadosHidrometereologicos", codEstacao=codigo,
                    dataInicio=f"{inicio:%d/%m/%Y}", dataFim=f"{fim:%d/%m/%Y}")
    return [[r["DataHora"], r["Chuva"]] for r in regs if r.get("Chuva")]

existentes = {f.name for f in dbutils.fs.ls(RAW)}
for e in tel:
    for ano in range(TELEMETRIA_INICIO, hoje.year + 1):
        nome = f"tel_{e['station_id']}_{ano}.json"
        if nome in existentes and ano < hoje.year:
            continue
        try:
            regs = telemetria(e["station_id"], date(ano, 1, 1), min(date(ano, 12, 31), hoje.date()))
        except requests.RequestException as err:    # sem arquivo: tenta de novo na próxima semana
            print(f"aviso: {nome} sem resposta ({err})")
            continue
        dbutils.fs.put(f"{RAW}/{nome}", json.dumps(regs), overwrite=True)
        existentes.add(nome)

frames = []
for e in tel:
    lista = [r for ano in range(TELEMETRIA_INICIO, hoje.year + 1)
             if (arq := f"tel_{e['station_id']}_{ano}.json") in existentes
             for r in json.loads(dbutils.fs.head(f"{RAW}/{arq}", 100_000_000))]
    df = pd.DataFrame(lista, columns=["t", "chuva_mm"])
    df["t"] = pd.to_datetime(df["t"])
    df["chuva_mm"] = pd.to_numeric(df["chuva_mm"], errors="coerce")
    df = df[df["chuva_mm"] >= 0].drop_duplicates("t").sort_values("t")
    if df.empty:
        continue
    intervalo = int(df["t"].diff().dt.total_seconds().div(60).mode().iat[0]) if len(df) > 1 else 60
    df["observation_time"] = df["t"].dt.ceil("h")             # a hora HH soma depois de HH−1 até HH
    h = (df.groupby("observation_time")
           .agg(chuva_mm=("chuva_mm", "sum"), n_registros=("t", "size")).reset_index())
    dia = (h["observation_time"] - pd.Timedelta(hours=1)).dt.date
    h["suspeita"] = (h["chuva_mm"] > CHUVA_1H_MAX) | (h.groupby(dia)["chuva_mm"].transform("sum") > CHUVA_24H_MAX)
    h["completa"] = h["n_registros"] >= 60 // intervalo
    frames.append(h.assign(station_id=e["station_id"], nome=e["nome"], dist_km=e["dist_km"],
                           intervalo_min=intervalo))
hor = pd.concat(frames, ignore_index=True)
hor["chuva_mm"] = hor["chuva_mm"].round(1)
hor["cidade"] = CIDADE
hor = hor[["cidade", "station_id", "nome", "dist_km", "observation_time", "chuva_mm",
           "n_registros", "intervalo_min", "completa", "suspeita"]]

# COMMAND ----------

# Período com dado de cada estação (posto: dias; telemétrica: horas)
per = pd.concat([
    diaria.groupby("station_id")["date"].agg(dados_inicio="min", dados_fim="max"),
    hor.groupby("station_id")["observation_time"].agg(dados_inicio="min", dados_fim="max"),
])
estacoes = estacoes.merge(per.astype("datetime64[ns]").reset_index(), on="station_id", how="left")
estacoes["cidade"] = CIDADE

def grava(df, tabela):
    (spark.createDataFrame(df).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela))
    print(f"{tabela}: {len(df):,} linhas")

grava(estacoes, T_ESTACOES)
grava(diaria, T_DIARIA)
grava(hor, T_HORARIA)

# COMMAND ----------

# Qualidade da telemetria por estação e mês, comparada com o Open-Meteo. Só pega
# falhas grosseiras (o modelo também erra): sensor zerado num mês em que choveu,
# volume muito acima do modelo ou horas suspeitas. A chuva do mês exclui as horas suspeitas.

spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.gold.ana_telemetria_qc AS
    WITH t AS (
        SELECT station_id, nome, dist_km,
               to_date(date_trunc('MONTH', observation_time - INTERVAL 1 HOUR))  AS mes,
               count(*)                                                       AS horas,
               count_if(completa)                                             AS horas_completas,
               count_if(suspeita)                                             AS horas_suspeitas,
               round(sum(CASE WHEN NOT suspeita THEN chuva_mm ELSE 0 END), 1)  AS chuva_mm
        FROM {T_HORARIA}
        WHERE cidade = '{CIDADE}'
        GROUP BY 1, 2, 3, 4
    ),
    om AS (
        SELECT to_date(date_trunc('MONTH', date)) AS mes, round(sum(precip_total), 1) AS chuva_openmeteo_mm
        FROM {CATALOG}.silver.weather_daily
        WHERE cidade = '{CIDADE}'
        GROUP BY 1
    )
    SELECT t.*, om.chuva_openmeteo_mm,
           round(t.chuva_mm / nullif(om.chuva_openmeteo_mm, 0), 2) AS razao_openmeteo,
           CASE
               WHEN t.horas_completas < 0.9 * 24 * day(last_day(t.mes))                   THEN 'incompleta'
               WHEN om.chuva_openmeteo_mm >= 50 AND t.chuva_mm < 0.2 * om.chuva_openmeteo_mm THEN 'zerada?'
               WHEN t.chuva_mm >= 50 AND t.chuva_mm > 3 * om.chuva_openmeteo_mm              THEN 'excesso?'
               WHEN t.horas_suspeitas > 0                                                    THEN 'horas suspeitas'
               ELSE 'ok'
           END AS situacao
    FROM t LEFT JOIN om USING (mes)
""")

# COMMAND ----------

print(estacoes.sort_values(["tipo", "dist_km"])[["tipo", "station_id", "nome", "dist_km", "operadora",
      "dados_inicio", "dados_fim"]].to_string(index=False))
print("\nTelemetria — situação dos últimos 12 meses:")
display(spark.sql(f"""
    SELECT nome, situacao, count(*) AS meses FROM {CATALOG}.gold.ana_telemetria_qc
    WHERE mes >= add_months(trunc(current_date(), 'MM'), -12)
    GROUP BY 1, 2 ORDER BY 1, 3 DESC
"""))
