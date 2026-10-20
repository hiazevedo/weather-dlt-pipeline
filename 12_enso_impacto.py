# Databricks notebook source
# ENSO × clima de Birigui — o que o El Niño / La Niña muda aqui?
#
# O ENSO age na escala de meses: o experimento 08 do bundle de ML não achou
# ganho na previsão horária. Aqui o efeito é medido por trimestre e na estação
# chuvosa, em três fontes, para não depender só do modelo:
#   ERA5    silver.weather_daily   1950→hoje   reanálise (Open-Meteo Archive)
#   ANA     silver.ana_diaria      1950→2024   chuva medida, postos até 20 km (leitura diária)
#   INMET   silver.inmet_hourly    2007→hoje   medido, 4 estações a 49–75 km
# (os pluviômetros do GHCN, silver.gauge_daily, são os mesmos postos da ANA, só até
# 1997 — entrariam em dobro)
#
# Roda para a cidade do config.py, como o resto do pipeline; as saídas levam a
# coluna `cidade`.
#
# Saídas:
#   gold.enso_impacto          anomalia média por fase do ENSO no mesmo período,
#                              com IC de 90% (bootstrap), p-valor (permutação) e
#                              q-valor (Benjamini–Hochberg: são centenas de testes)
#   gold.enso_previsibilidade  o ONI já publicado antes do trimestre prevê o
#                              tercil (abaixo / normal / acima)? Teste retroativo
#                              deixando cada ano de fora, contra a climatologia
#   gold.estacao_chuvosa       início, fim e chuva de cada estação chuvosa
#                              (Liebmann et al., 2007) + ONI
#   gold.estacao_vs_analogos   VIEW: chuva e dias ≥ 35 °C acumulados desde 1/jul
#                              na estação atual × normal 1991–2020 × os 5 anos com
#                              o ONI mais parecido com o atual
#   gold.acuracia_por_enso     VIEW: acerto mensal da API Open-Meteo (1 e 2 dias
#                              antes) e do champion × fase do ENSO no mês

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import numpy as np
import pandas as pd

ANO_INI    = 1950        # o ONI começa em DJF/1950
MIN_ANOS   = 10          # anos mínimos de uma unidade (posto/estação) num trimestre
N_PERM     = 5000        # permutações do teste de diferença entre fases
N_PERM_RPS = 1000        # permutações dos testes da previsibilidade (RPSS e correlação)
N_BOOT     = 2000
SUBIDA_MIN = 50          # mm: quanto A precisa subir desde o mínimo para aceitar um início provisório
rng  = np.random.default_rng(42)
hoje = pd.Timestamp.now(tz=TIMEZONE).tz_localize(None).normalize()

SIGLA = {1: "DJF", 2: "JFM", 3: "FMA", 4: "MAM", 5: "AMJ", 6: "MJJ",
         7: "JJA", 8: "JAS", 9: "ASO", 10: "SON", 11: "OND", 12: "NDJ"}   # mês central → trimestre
MES = "jan fev mar abr mai jun jul ago set out nov dez".split()
MESES = {c: f"{MES[(c - 2) % 12]}–{MES[c % 12]}" for c in SIGLA}         # 1 → "dez–fev"
ESTACAO = "estação chuvosa"

METRICAS = {
    "chuva_mm":         "chuva total (mm)",
    "dias_chuva":       "dias com chuva ≥ 1 mm",
    "dias_r20":         "dias com chuva ≥ 20 mm (R20mm)",
    "veranico_dias":    "maior sequência de dias com < 1 mm (CDD)",
    "tmax_c":           "média das máximas (°C)",
    "dias_35":          "dias com máxima ≥ 35 °C",
    "inicio_dia":       "início da estação chuvosa (dias após 1/jul)",
    "fim_dia":          "fim da estação chuvosa (dias após 1/jul)",
    "duracao_dias":     "duração da estação chuvosa (dias)",
    "chuva_estacao_mm": "chuva entre o início e o fim da estação (mm)",
}
TRI_METRICAS = list(METRICAS)[:6]
EST_METRICAS = list(METRICAS)[6:]
PCT = {"chuva_mm", "dias_chuva", "dias_r20", "chuva_estacao_mm"}   # anomalia também em % da normal

# COMMAND ----------

# Dados: ONI (por mês central) e chuva / máxima diárias das três fontes

oni = spark.table(f"{CATALOG}.silver.enso_oni").select("center_month", "oni").toPandas()
oni = (oni.assign(centro=pd.to_datetime(oni["center_month"]).dt.to_period("M"))
          .set_index("centro")["oni"].astype(float).sort_index())

corte = f"{hoje:%Y-%m-%d}"     # o dia de hoje ainda está incompleto
era5 = spark.sql(f"""
    SELECT 'ERA5' AS fonte, 'ERA5' AS unidade, date, precip_total AS chuva_mm, temp_max AS tmax_c
    FROM {CATALOG}.silver.weather_daily
    WHERE cidade = '{CIDADE}' AND horas_registradas >= 23 AND year >= {ANO_INI} AND date < '{corte}'
""").toPandas()
ana = spark.sql(f"""
    SELECT 'ANA' AS fonte, station_id AS unidade, date, chuva_mm, CAST(NULL AS DOUBLE) AS tmax_c
    FROM {CATALOG}.silver.ana_diaria
    WHERE cidade = '{CIDADE}' AND year(date) >= {ANO_INI} AND date < '{corte}'
      AND coalesce(status, '') NOT IN ('duvidoso', 'acumulado')
""").toPandas()
# INMET: dia local com quase todas as horas (o horário de verão, até 2019, dá dias de 23 e 25 h)
inmet = spark.sql(f"""
    SELECT 'INMET' AS fonte, station_id AS unidade, to_date(observation_time) AS date,
           CASE WHEN count(precip_mm)  >= 22 THEN sum(precip_mm)  END AS chuva_mm,
           CASE WHEN count(temp_max_c) >= 20 THEN max(temp_max_c) END AS tmax_c
    FROM {CATALOG}.silver.inmet_hourly
    WHERE cidade = '{CIDADE}' AND to_date(observation_time) < '{corte}'
    GROUP BY 1, 2, 3
""").toPandas()

diario = pd.concat([era5, ana, inmet], ignore_index=True).drop_duplicates(["fonte", "unidade", "date"])
diario["date"] = pd.to_datetime(diario["date"])
diario[["chuva_mm", "tmax_c"]] = diario[["chuva_mm", "tmax_c"]].astype(float)
print(diario.groupby("fonte").agg(unidades=("unidade", "nunique"), dias=("date", "size"),
                                  inicio=("date", "min"), fim=("date", "max")).to_string())

# COMMAND ----------

# Métricas por trimestre móvel (DJF, JFM, …), com a mesma chave do ONI: o mês central

def trimestres(d):
    """Uma linha por unidade e trimestre. Trimestre válido com ≥ 90% dos dias
    (chuva e temperatura avaliadas separadamente)."""
    mes = d["date"].dt.to_period("M")
    # cada dia entra nos trimestres centrados no mês anterior, no próprio e no seguinte
    t = pd.concat([d.assign(centro=mes + k) for k in (-1, 0, 1)], ignore_index=True)
    t = t.sort_values(["fonte", "unidade", "centro", "date"], ignore_index=True)
    t["seco"] = t["chuva_mm"] < 1                      # dia sem dado não é seco: interrompe a sequência
    t["seq"]  = (~t["seco"]).cumsum()
    veranico = (t.groupby(["fonte", "unidade", "centro", "seq"])["seco"].sum()
                 .groupby(level=["fonte", "unidade", "centro"]).max().rename("veranico_dias"))
    t["dias_chuva"] = (t["chuva_mm"] >= 1).astype(float)
    t["dias_r20"]   = (t["chuva_mm"] >= 20).astype(float)
    t["dias_35"]    = (t["tmax_c"] >= 35).astype(float)
    r = (t.groupby(["fonte", "unidade", "centro"])
          .agg(n_chuva=("chuva_mm", "count"), n_tmax=("tmax_c", "count"),
               chuva_mm=("chuva_mm", "sum"), dias_chuva=("dias_chuva", "sum"),
               dias_r20=("dias_r20", "sum"), tmax_c=("tmax_c", "mean"), dias_35=("dias_35", "sum"))
          .join(veranico).reset_index())
    dias = ((r["centro"] + 2).dt.start_time - (r["centro"] - 1).dt.start_time).dt.days
    r.loc[r["n_chuva"] < 0.9 * dias, ["chuva_mm", "dias_chuva", "dias_r20", "veranico_dias"]] = np.nan
    r.loc[r["n_tmax"]  < 0.9 * dias, ["tmax_c", "dias_35"]] = np.nan
    r["ano"], r["mes_oni"] = r["centro"].dt.year, r["centro"].dt.month
    r["trimestre"] = r["mes_oni"].map(SIGLA)
    return r.drop(columns=["centro", "n_chuva", "n_tmax"])

tri = trimestres(diario)

# COMMAND ----------

# Estação chuvosa por ano hidrológico (1/jul → 30/jun) — Liebmann et al. (2007):
# A(t) = Σ (chuva − média diária climatológica) desde 1/jul. O início é o dia
# seguinte ao mínimo de A (procurado até 31/jan) e o fim, o dia do máximo depois
# do início. Na estação em curso, o início é provisório e só aparece quando A já
# subiu SUBIDA_MIN mm desde o mínimo; o fim fica em aberto.

def estacoes(d):
    s = d.groupby("date")["chuva_mm"].mean().asfreq("D")   # várias unidades: média das que mediram no dia
    media = s.mean()
    ontem = hoje - pd.Timedelta(days=1)
    linhas = []
    for ano in range(s.index.min().year - 1, s.index.max().year + 1):
        dias = pd.date_range(f"{ano}-07-01", f"{ano + 1}-06-30")
        x = s.reindex(dias)
        corrente = dias[0] <= ontem < dias[-1]
        completa = (not corrente) and x.notna().mean() >= 0.95
        # estação em curso: só com dados em dia (≥ 90% dos dias até ontem) e já 2 meses andados
        em_dia = corrente and x[:ontem].notna().mean() >= 0.9 and (ontem - dias[0]).days >= 60
        if not (completa or em_dia):
            continue
        a = (x - media).fillna(0).cumsum()                  # dia sem dado não muda A
        busca = a[: min(pd.Timestamp(f"{ano + 1}-01-31"), ontem)]
        i_min = busca.idxmin()
        inicio = fim = None
        if completa:
            inicio, fim = i_min + pd.Timedelta(days=1), a[i_min:].idxmax()
        elif a[ontem] - a[i_min] >= SUBIDA_MIN:
            inicio = i_min + pd.Timedelta(days=1)
        linhas.append(dict(
            ano_estacao=ano, estacao=f"{ano}–{(ano + 1) % 100:02d}",
            inicio=inicio, fim=fim,
            inicio_dia=(inicio - dias[0]).days if inicio is not None else None,
            fim_dia=(fim - dias[0]).days if fim is not None else None,
            duracao_dias=(fim - inicio).days if fim is not None else None,
            chuva_estacao_mm=round(float(x[inicio:fim].sum()), 1) if fim is not None else None,
            chuva_total_mm=round(float(x.sum()), 1) if completa else None,
            chuva_ate_ontem_mm=round(float(x[:ontem].sum()), 1) if corrente else None,
            completa=completa, provisorio=corrente, media_diaria_mm=round(float(media), 2)))
    return pd.DataFrame(linhas)

est = pd.concat([estacoes(g).assign(fonte=f) for f, g in diario.groupby("fonte")], ignore_index=True)
est["oni_jas"] = [oni.get(pd.Period(year=a, month=8, freq="M")) for a in est["ano_estacao"]]
est["oni_ond"] = [oni.get(pd.Period(year=a, month=11, freq="M")) for a in est["ano_estacao"]]
est["fase_ond"] = np.select([est["oni_ond"] >= 0.5, est["oni_ond"] <= -0.5, est["oni_ond"].notna()],
                            ["El Niño", "La Niña", "Neutro"], None)
for c in ("inicio", "fim"):
    est[c] = [d.date() if pd.notna(d) else None for d in pd.to_datetime(est[c])]
est = est.astype({c: "float" for c in EST_METRICAS + ["chuva_total_mm", "chuva_ate_ontem_mm", "oni_jas", "oni_ond"]})

# COMMAND ----------

# Anomalias: cada unidade em relação à própria reta de tendência (séries com
# ≥ 30 anos: o aquecimento e a secagem não viram "efeito do ENSO") ou à própria
# média (séries curtas, em que a reta seria ruído); depois, a média das unidades.

def anomalias(df, metricas):
    longo = (df.melt(id_vars=["fonte", "unidade", "ano", "trimestre", "mes_oni"], value_vars=metricas,
                     var_name="metrica", value_name="valor").dropna(subset=["valor"]))
    grupo = ["fonte", "unidade", "metrica", "trimestre"]
    g = longo.groupby(grupo)
    n = g["ano"].transform("nunique")
    longo = longo[n >= MIN_ANOS].copy()
    g = longo.groupby(grupo)
    dx = longo["ano"] - g["ano"].transform("mean")
    dy = longo["valor"] - g["valor"].transform("mean")
    longo["_cov"], longo["_var"] = dx * dy, dx ** 2
    incl = (longo.groupby(grupo)["_cov"].transform("sum") / longo.groupby(grupo)["_var"].transform("sum"))
    incl = incl.where(g["ano"].transform("nunique") >= 30, 0.0)
    longo["anomalia"] = dy - incl * dx
    longo["normal"]   = g["valor"].transform("mean")
    return (longo.groupby(["fonte", "metrica", "trimestre", "mes_oni", "ano"])
                 .agg(valor=("valor", "mean"), normal=("normal", "mean"),
                      anomalia=("anomalia", "mean"), n_unidades=("unidade", "nunique"))
                 .reset_index())

est_longo = est[est["completa"]].assign(unidade=est["fonte"], ano=est["ano_estacao"],
                                        trimestre=ESTACAO, mes_oni=11)   # fase do ENSO pelo ONI de OND
anom = pd.concat([anomalias(tri, TRI_METRICAS), anomalias(est_longo, EST_METRICAS)], ignore_index=True)
anom["oni"] = oni.reindex(
    pd.to_datetime(dict(year=anom["ano"], month=anom["mes_oni"], day=1)).dt.to_period("M")).to_numpy()
anom = anom.dropna(subset=["oni"])

# COMMAND ----------

# Impacto: anomalia média de cada fase × demais anos, no mesmo período do ONI

GRUPOS = {
    "La Niña":            lambda o: o <= -0.5,
    "Neutro":             lambda o: np.abs(o) < 0.5,
    "El Niño":            lambda o: o >= 0.5,
    "El Niño ≥ moderado": lambda o: o >= 1.0,
}

def bh(p):
    """q-valores de Benjamini–Hochberg."""
    p = np.asarray(p, float)
    o = np.argsort(p)
    q = p[o] * len(p) / np.arange(1, len(p) + 1)
    q = np.minimum.accumulate(q[::-1])[::-1]
    out = np.empty(len(p))
    out[o] = np.minimum(q, 1)
    return out

def compara(x, o):
    """Para cada fase: anomalia média, IC de 90% (bootstrap) e p-valor bicaudal
    da diferença para os demais anos (permutação dos anos entre os grupos)."""
    if np.ptp(x) == 0:                                    # métrica sempre igual (ex.: dias ≥ 35 °C no inverno)
        return
    xp = x[rng.permuted(np.tile(np.arange(len(x)), (N_PERM, 1)), axis=1)]
    for nome, regra in GRUPOS.items():
        m = regra(o)
        n = int(m.sum())
        if n < 3 or len(x) - n < 3:
            continue
        dif  = x[m].mean() - x[~m].mean()
        difp = xp[:, m].mean(1) - xp[:, ~m].mean(1)
        boot = rng.choice(x[m], size=(N_BOOT, n)).mean(1)
        yield dict(grupo=nome, n_anos=n, anomalia=x[m].mean(), diferenca=dif,
                   ic90_inf=np.quantile(boot, 0.05), ic90_sup=np.quantile(boot, 0.95),
                   p_valor=((np.abs(difp) >= abs(dif)).sum() + 1) / (N_PERM + 1))

linhas = []
for (fonte, trimestre, metrica), s in anom.groupby(["fonte", "trimestre", "metrica"]):
    ordem = 13 if trimestre == ESTACAO else int(s["mes_oni"].iat[0])
    base = dict(fonte=fonte, trimestre=trimestre, meses=MESES.get(ordem, "jul–jun"), ordem=ordem,
                metrica=metrica, descricao=METRICAS[metrica],
                oni_referencia="OND" if trimestre == ESTACAO else trimestre,
                ano_ini=int(s["ano"].min()), ano_fim=int(s["ano"].max()), normal=s["normal"].mean())
    for r in compara(s["anomalia"].to_numpy(), s["oni"].to_numpy()):
        linhas.append({**base, **r})

impacto = pd.DataFrame(linhas)
impacto["anomalia_pct"] = np.where(impacto["metrica"].isin(PCT),
                                   100 * impacto["anomalia"] / impacto["normal"], np.nan)
impacto["q_valor"] = bh(impacto["p_valor"])
impacto["significativo"] = impacto["q_valor"] < 0.10
for c in ("normal", "anomalia", "diferenca", "ic90_inf", "ic90_sup", "anomalia_pct"):
    impacto[c] = impacto[c].round(2)
impacto = impacto.sort_values(["fonte", "ordem", "metrica", "grupo"], ignore_index=True)

print(f"{len(impacto)} comparações; p < 0,05 em {(impacto['p_valor'] < 0.05).sum()} "
      f"(o acaso daria ~{0.05 * len(impacto):.0f}); q < 0,10 em {impacto['significativo'].sum()}")

# COMMAND ----------

# Previsibilidade (ERA5, série mais longa): o ONI publicado `antecedencia` meses
# antes do início do trimestre prevê o tercil? Antecedência 0 = o último ONI
# disponível quando o trimestre começa (ex.: JAS, publicado em outubro, para OND).
#
# Previsão por fase do ONI (La Niña / Neutro / El Niño): probabilidade de cada
# tercil = frequência entre os DEMAIS anos da mesma fase (+1 em cada tercil, para
# amostras pequenas). RPSS > 0 = melhor que a climatologia (1/3 para cada tercil).
# Os limites dos tercis usam todos os anos (a diferença de tirar um ano é desprezível).

def rpss(cat, fase):
    cont = np.zeros((3, 3))
    np.add.at(cont, (fase, cat), 1)
    c = cont[fase] - np.eye(3)[cat]                       # tira o próprio ano
    p = (c + 1) / (c.sum(1, keepdims=True) + 3)
    obs = np.eye(3)[cat].cumsum(1)
    rps   = ((p.cumsum(1) - obs) ** 2).sum(1).mean()
    rps_c = ((np.array([1 / 3, 2 / 3, 1]) - obs) ** 2).sum(1).mean()
    vence = p == p.max(1, keepdims=True)                  # empate: acerto fracionado
    return 1 - rps / rps_c, (vence[np.arange(len(cat)), cat] / vence.sum(1)).mean()

linhas = []
era = anom[(anom["fonte"] == "ERA5") & (anom["trimestre"] != ESTACAO)]
for (metrica, trimestre), s in era.groupby(["metrica", "trimestre"]):
    centro = pd.PeriodIndex(pd.to_datetime(dict(year=s["ano"], month=s["mes_oni"], day=1)).dt.to_period("M"))
    for k in range(7):
        pred = oni.reindex(centro - (3 + k)).to_numpy()
        ok = ~np.isnan(pred)
        x, o = s["anomalia"].to_numpy()[ok], pred[ok]
        cat  = np.digitize(x, np.quantile(x, [1 / 3, 2 / 3]))
        if np.bincount(cat, minlength=3).min() < 0.2 * len(x):   # valores repetidos demais para ter tercis
            continue
        fase = np.where(o <= -0.5, 0, np.where(o >= 0.5, 2, 1))
        sk, acerto = rpss(cat, fase)
        nulos = np.array([rpss(cat, f)[0] for f in rng.permuted(np.tile(fase, (N_PERM_RPS, 1)), axis=1)])
        zx, zo = (x - x.mean()) / x.std(), (o - o.mean()) / o.std()
        r = (zx * zo).mean()
        r_nulos = (rng.permuted(np.tile(zo, (N_PERM_RPS, 1)), axis=1) * zx).mean(1)
        p_r = ((np.abs(r_nulos) >= abs(r)).sum() + 1) / (N_PERM_RPS + 1)
        linhas.append(dict(
            metrica=metrica, descricao=METRICAS[metrica], trimestre=trimestre,
            ordem=int(s["mes_oni"].iat[0]), antecedencia_meses=k,
            trimestre_oni=SIGLA[(centro[0] - (3 + k)).month], n_anos=int(ok.sum()),
            r=round(float(r), 3), p_valor_r=float(p_r),
            rpss=round(float(sk), 3), acerto_tercil=round(float(acerto), 3),
            p_valor_rpss=((nulos >= sk).sum() + 1) / (N_PERM_RPS + 1)))

prev = pd.DataFrame(linhas)
prev["q_valor_rpss"] = bh(prev["p_valor_rpss"])
prev["tem_habilidade"] = (prev["rpss"] > 0) & (prev["q_valor_rpss"] < 0.10)
prev = prev.sort_values(["metrica", "ordem", "antecedencia_meses"], ignore_index=True)

# COMMAND ----------

def grava(df, nome):
    df = df.copy()
    df.insert(0, "cidade", CIDADE)
    (spark.createDataFrame(df).write.mode("overwrite").option("overwriteSchema", "true")
         .saveAsTable(f"{CATALOG}.gold.{nome}"))
    print(f"{CATALOG}.gold.{nome}: {len(df)} linhas")

grava(impacto, "enso_impacto")
grava(prev, "enso_previsibilidade")
grava(est[["fonte", "ano_estacao", "estacao", "inicio", "fim", "inicio_dia", "fim_dia", "duracao_dias",
           "chuva_estacao_mm", "chuva_total_mm", "chuva_ate_ontem_mm", "completa", "provisorio",
           "media_diaria_mm", "oni_jas", "oni_ond", "fase_ond"]], "estacao_chuvosa")

# COMMAND ----------

# Acompanhamento da estação em curso: chuva e dias ≥ 35 °C acumulados desde 1/jul.
# Análogos: os 5 anos em que o último trimestre do ONI publicado (o mesmo
# trimestre, no mesmo ponto do ano hidrológico) teve o valor mais próximo do atual.
# Todas as séries ficam no calendário da estação atual (coluna `data`); `tipo`
# identifica a série: atual, normal_p10 / normal_p50 / normal_p90, analogos
# (mediana dos 5) e analogo (cada um, com `ano_estacao`).
# É uma view: acompanha a silver.weather_daily a cada hora, sem depender deste notebook.

spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.gold.estacao_vs_analogos AS
    WITH dia AS (
        SELECT date, precip_total, temp_max,
               CASE WHEN month >= 7 THEN year ELSE year - 1 END AS ano_estacao
        FROM {CATALOG}.silver.weather_daily
        WHERE cidade = '{CIDADE}' AND horas_registradas >= 23
          AND date < to_date(from_utc_timestamp(current_timestamp(), '{TIMEZONE}'))
    ),
    acum AS (
        SELECT ano_estacao, date,
               datediff(date, make_date(ano_estacao, 7, 1))              AS dia_estacao,
               sum(precip_total) OVER w                                  AS chuva_acum_mm,
               sum(CASE WHEN temp_max >= 35 THEN 1 ELSE 0 END) OVER w    AS dias_35_acum
        FROM dia
        WINDOW w AS (PARTITION BY ano_estacao ORDER BY date)
    ),
    atual AS (SELECT max(ano_estacao) AS ano FROM dia),
    ult AS (                     -- último ONI publicado e o deslocamento até o ano da estação atual
        SELECT o.season, o.oni, o.year - atual.ano AS desloc
        FROM {CATALOG}.silver.enso_oni o, atual
        ORDER BY o.center_month DESC LIMIT 1
    ),
    analogos AS (
        SELECT o.year - ult.desloc AS ano_estacao, o.oni
        FROM {CATALOG}.silver.enso_oni o, ult, atual
        WHERE o.season = ult.season AND o.year - ult.desloc < atual.ano
        ORDER BY abs(o.oni - ult.oni) LIMIT 5
    ),
    clima AS (                   -- 30 estações: jul/1990 → jun/2020
        SELECT dia_estacao,
               percentile(chuva_acum_mm, 0.1) AS chuva_p10, percentile(chuva_acum_mm, 0.5) AS chuva_p50,
               percentile(chuva_acum_mm, 0.9) AS chuva_p90, percentile(dias_35_acum, 0.5)  AS d35_p50
        FROM acum WHERE ano_estacao BETWEEN 1990 AND 2019
        GROUP BY dia_estacao
    ),
    series AS (
        SELECT a.dia_estacao, 'atual' AS tipo, a.ano_estacao,
               concat(a.ano_estacao, '–', lpad(CAST((a.ano_estacao + 1) % 100 AS STRING), 2, '0'), ' (atual)') AS serie,
               a.chuva_acum_mm, a.dias_35_acum, (SELECT oni FROM ult) AS oni
        FROM acum a, atual WHERE a.ano_estacao = atual.ano
        UNION ALL
        SELECT dia_estacao, 'normal_p10', NULL, 'Normal 1991–2020 — 10% mais secas', chuva_p10, NULL, NULL FROM clima
        UNION ALL
        SELECT dia_estacao, 'normal_p50', NULL, 'Normal 1991–2020 — mediana', chuva_p50, d35_p50, NULL FROM clima
        UNION ALL
        SELECT dia_estacao, 'normal_p90', NULL, 'Normal 1991–2020 — 10% mais chuvosas', chuva_p90, NULL, NULL FROM clima
        UNION ALL
        SELECT a.dia_estacao, 'analogos', NULL, 'Análogos do ENSO — mediana',
               percentile(a.chuva_acum_mm, 0.5), percentile(a.dias_35_acum, 0.5), NULL
        FROM acum a JOIN analogos n USING (ano_estacao)
        GROUP BY a.dia_estacao
        UNION ALL
        SELECT a.dia_estacao, 'analogo', a.ano_estacao,
               concat(a.ano_estacao, '–', lpad(CAST((a.ano_estacao + 1) % 100 AS STRING), 2, '0')),
               a.chuva_acum_mm, a.dias_35_acum, n.oni
        FROM acum a JOIN analogos n USING (ano_estacao)
    )
    SELECT '{CIDADE}' AS cidade, date_add(make_date(atual.ano, 7, 1), s.dia_estacao) AS data, s.*,
           (SELECT season FROM ult)                               AS trimestre_oni
    FROM series s, atual
""")

# COMMAND ----------

# A previsão erra mais com El Niño? Acerto mensal de "vai chover na hora" (rótulo
# Open-Meteo, chuva > 0) da API (previsões arquivadas, desde 2024) e do champion
# (histórico de inferência), com a fase do ENSO do mês. Para comparar fases, use
# os mesmos meses do calendário: o acerto muda muito entre estação seca e chuvosa.

spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.gold.acuracia_por_enso AS
    WITH obs AS (
        SELECT observation_time, precipitation AS chuva_obs, temperature_2m AS temp_obs
        FROM {CATALOG}.silver.weather_clean
        WHERE NOT is_forecast AND cidade = '{CIDADE}'
    ),
    api AS (                     -- previsões arquivadas: rodadas de 1 e de 2 dias antes (a do próprio dia, d0,
                                 -- é a mesma série do rótulo observado e daria acerto perfeito)
        SELECT observation_time,
               stack(2, 'd1 (1 dia antes)',  precipitation_d1, temperature_2m_d1,
                        'd2 (2 dias antes)', precipitation_d2, temperature_2m_d2) AS (antecedencia, chuva_prev, temp_prev)
        FROM {CATALOG}.silver.forecast_previous_runs
    ),
    prev AS (
        SELECT observation_time, 'API Open-Meteo' AS fonte, antecedencia, chuva_prev, temp_prev FROM api
        UNION ALL                -- champion: todas as previsões feitas antes da hora (até 24 h)
        SELECT target_time, 'champion', '1–24 h', CAST(pred_vai_chover AS DOUBLE), CAST(NULL AS DOUBLE)
        FROM {CATALOG}.gold.rain_forecast_ml_history
        WHERE inferencia_local < target_time
    ),
    agg AS (
        SELECT p.fonte, p.antecedencia, to_date(date_trunc('MONTH', p.observation_time)) AS mes,
               count(*)                                                                AS horas,
               sum(CASE WHEN p.chuva_prev > 0 AND o.chuva_obs > 0 THEN 1 ELSE 0 END)  AS acertos,
               sum(CASE WHEN p.chuva_prev > 0 AND o.chuva_obs = 0 THEN 1 ELSE 0 END)  AS falsos_alarmes,
               sum(CASE WHEN p.chuva_prev = 0 AND o.chuva_obs > 0 THEN 1 ELSE 0 END)  AS perdidas,
               sum(p.chuva_prev)                                                       AS chuva_prev_mm,
               sum(o.chuva_obs)                                                        AS chuva_obs_mm,
               avg(abs(p.temp_prev - o.temp_obs))                                      AS mae_temp
        FROM prev p JOIN obs o USING (observation_time)
        WHERE p.chuva_prev IS NOT NULL
        GROUP BY 1, 2, 3
    ),
    ref AS (                     -- ONI do trimestre centrado no mês; sem publicação ainda, o último publicado
        SELECT m.mes, max(e.center_month) AS cm
        FROM (SELECT DISTINCT mes FROM agg) m JOIN {CATALOG}.silver.enso_oni e ON e.center_month <= m.mes
        GROUP BY m.mes
    )
    SELECT
        '{CIDADE}' AS cidade, a.fonte, a.antecedencia, a.mes, month(a.mes) AS mes_do_ano,
        e.oni,
        CASE WHEN e.oni >= 0.5 THEN 'El Niño' WHEN e.oni <= -0.5 THEN 'La Niña' ELSE 'Neutro' END AS fase,
        e.center_month <> a.mes                                                         AS oni_provisorio,
        a.horas, a.acertos, a.falsos_alarmes, a.perdidas,
        round(2 * a.acertos / nullif(2 * a.acertos + a.falsos_alarmes + a.perdidas, 0), 3) AS f1,
        round(a.acertos / nullif(a.acertos + a.perdidas, 0), 3)                         AS pod,
        round(a.falsos_alarmes / nullif(a.acertos + a.falsos_alarmes, 0), 3)            AS far,
        round((a.acertos + a.falsos_alarmes) / nullif(a.acertos + a.perdidas, 0), 2)    AS vies_frequencia,
        CASE WHEN a.fonte <> 'champion' THEN round(a.chuva_prev_mm / nullif(a.chuva_obs_mm, 0), 2) END AS vies_volume,
        round(a.mae_temp, 2)                                                            AS mae_temp_c
    FROM agg a
    JOIN ref r USING (mes)
    JOIN {CATALOG}.silver.enso_oni e ON e.center_month = r.cm
""")

# COMMAND ----------

# Resumo

atual = est[(est["fonte"] == "ERA5") & est["provisorio"]]
if len(atual):
    a = atual.iloc[0]
    ini = f"{a['inicio']:%d/%m} (provisório)" if pd.notna(a["inicio"]) else "ainda não começou"
    print(f"Estação {a['estacao']}: {a['chuva_ate_ontem_mm']:.0f} mm desde 1/jul · início da estação chuvosa: {ini}")
print("\nÚltimo ONI:", oni.index[-1], f"{oni.iat[-1]:+.2f}")

print("\nEl Niño × demais anos — chuva por trimestre (% da normal), nas três fontes:")
chuva = impacto[(impacto["metrica"] == "chuva_mm") & (impacto["grupo"] == "El Niño")]
print(chuva.pivot_table(index=["ordem", "trimestre"], columns="fonte", values="anomalia_pct").round(0).to_string())

print("\nResultados com q < 0,10:")
print(impacto[impacto["significativo"]][["fonte", "trimestre", "metrica", "grupo", "n_anos",
      "anomalia", "anomalia_pct", "p_valor", "q_valor"]].to_string(index=False))

print("\nPrevisibilidade (antecedência 0) — melhores RPSS:")
print(prev[prev["antecedencia_meses"] == 0].nlargest(10, "rpss")[
      ["metrica", "trimestre", "trimestre_oni", "r", "rpss", "acerto_tercil", "p_valor_rpss", "q_valor_rpss"]
      ].to_string(index=False))
print(f"\nCombinações com habilidade (RPSS > 0 e q < 0,10): {prev['tem_habilidade'].sum()} de {len(prev)}")
