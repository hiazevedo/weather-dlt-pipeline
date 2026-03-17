# Databricks notebook source
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import seaborn as sns
import pandas as pd
import numpy as np



# COMMAND ----------

plt.rcParams.update({
    "figure.facecolor": "#0d1117", "axes.facecolor":  "#161b22",
    "axes.edgecolor":   "#30363d", "axes.labelcolor": "#c9d1d9",
    "axes.titlecolor":  "#ffffff", "xtick.color":     "#8b949e",
    "ytick.color":      "#8b949e", "text.color":      "#c9d1d9",
    "grid.color":       "#21262d", "grid.linestyle":  "--",
    "grid.alpha":       0.5,       "font.family":     "monospace",
})

# COMMAND ----------

# Gráfico 1: Temperatura histórica anual (1940→2026)

df_monthly = spark.sql("""
    SELECT year, month, temp_media, temp_max_media,
           temp_min_media, precip_mensal, dias_com_chuva
    FROM weather_pipeline.silver.weather_monthly
    WHERE year >= 1940
    ORDER BY year, month
""").toPandas()

df_monthly["date"] = pd.to_datetime(
    df_monthly[["year","month"]].assign(day=1)
)

# Média móvel de 12 meses para suavizar
df_monthly["temp_ma12"] = df_monthly["temp_media"].rolling(12).mean()

fig, axes = plt.subplots(2, 1, figsize=(18, 10), sharex=True)

# Temperatura
axes[0].fill_between(df_monthly["date"],
    df_monthly["temp_min_media"], df_monthly["temp_max_media"],
    alpha=0.2, color="#58a6ff", label="Range min-max")
axes[0].plot(df_monthly["date"], df_monthly["temp_media"],
    color="#58a6ff", linewidth=0.5, alpha=0.5)
axes[0].plot(df_monthly["date"], df_monthly["temp_ma12"],
    color="#ffa657", linewidth=2, label="Média móvel 12 meses")
axes[0].set_title("Temperatura Média Mensal — Birigui-SP (1940→2026)",
    fontweight="bold", fontsize=13)
axes[0].set_ylabel("Temperatura (°C)")
axes[0].legend(fontsize=9, framealpha=0.2)
axes[0].grid(True)

# Precipitação
axes[1].bar(df_monthly["date"], df_monthly["precip_mensal"],
    width=25, color="#3fb950", alpha=0.7, label="Precipitação mensal")
precip_ma12 = df_monthly["precip_mensal"].rolling(12).mean()
axes[1].plot(df_monthly["date"], precip_ma12,
    color="#ffa657", linewidth=2, label="Média móvel 12 meses")
axes[1].set_title("Precipitação Mensal — Birigui-SP (1940→2026)",
    fontweight="bold", fontsize=13)
axes[1].set_ylabel("Precipitação (mm)")
axes[1].set_xlabel("Ano")
axes[1].legend(fontsize=9, framealpha=0.2)
axes[1].grid(True, axis="y")

plt.tight_layout()
plt.show()

# COMMAND ----------

# Gráfico 2: Climatologia mensal (sazonalidade)

df_clima = spark.sql("""
    SELECT month,
           ROUND(AVG(temp_media),      2) AS temp_media,
           ROUND(AVG(temp_max_media),  2) AS temp_max,
           ROUND(AVG(temp_min_media),  2) AS temp_min,
           ROUND(AVG(precip_mensal),   2) AS precip_media,
           ROUND(AVG(dias_com_chuva),  1) AS dias_chuva_media
    FROM weather_pipeline.silver.weather_monthly
    WHERE year BETWEEN 1991 AND 2020  -- Padrão climatológico WMO
    GROUP BY month
    ORDER BY month
""").toPandas()

meses = ["Jan","Fev","Mar","Abr","Mai","Jun",
         "Jul","Ago","Set","Out","Nov","Dez"]
df_clima["mes_nome"] = meses

fig, ax1 = plt.subplots(figsize=(14, 6))
ax2 = ax1.twinx()

# Barras de precipitação
bars = ax1.bar(df_clima["mes_nome"], df_clima["precip_media"],
    color="#3fb950", alpha=0.6, label="Precipitação média (mm)", zorder=2)
ax1.set_ylabel("Precipitação (mm)", color="#3fb950")
ax1.tick_params(axis="y", labelcolor="#3fb950")

# Linhas de temperatura
ax2.plot(df_clima["mes_nome"], df_clima["temp_max"],
    color="#f78166", linewidth=2.5, marker="o", markersize=5,
    label="Temp máx média")
ax2.plot(df_clima["mes_nome"], df_clima["temp_media"],
    color="#ffa657", linewidth=2.5, marker="o", markersize=5,
    label="Temp média")
ax2.plot(df_clima["mes_nome"], df_clima["temp_min"],
    color="#58a6ff", linewidth=2.5, marker="o", markersize=5,
    label="Temp mín média")
ax2.set_ylabel("Temperatura (°C)", color="#c9d1d9")

ax1.set_title("Climatologia Mensal — Birigui-SP (1991–2020)\nNormal Climatológica WMO",
    fontweight="bold", fontsize=13)
ax1.grid(True, axis="y", alpha=0.3, zorder=1)

lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2,
    loc="upper right", fontsize=9, framealpha=0.2)

plt.tight_layout()
plt.show()

# COMMAND ----------

# Gráfico 3: Tendência de aquecimento (1940→2026)

df_anual = spark.sql("""
    SELECT year,
           ROUND(AVG(temp_media),     2) AS temp_anual,
           ROUND(SUM(precip_mensal),  2) AS precip_anual,
           ROUND(SUM(dias_com_chuva), 0) AS dias_chuva_ano
    FROM weather_pipeline.silver.weather_monthly
    WHERE year BETWEEN 1940 AND 2025
      AND year != 2026
    GROUP BY year
    ORDER BY year
""").toPandas()

# Regressão linear para tendência
z_temp   = np.polyfit(df_anual["year"], df_anual["temp_anual"],   1)
z_precip = np.polyfit(df_anual["year"], df_anual["precip_anual"], 1)
p_temp   = np.poly1d(z_temp)
p_precip = np.poly1d(z_precip)

fig, axes = plt.subplots(1, 2, figsize=(18, 6))

# Tendência de temperatura
axes[0].scatter(df_anual["year"], df_anual["temp_anual"],
    color="#58a6ff", s=20, alpha=0.7, label="Temperatura anual")
axes[0].plot(df_anual["year"], p_temp(df_anual["year"]),
    color="#f78166", linewidth=2.5, linestyle="--",
    label=f"Tendência: {z_temp[0]:+.4f}°C/ano")
delta_temp = p_temp(2025) - p_temp(1940)
axes[0].set_title(
    f"Tendência de Temperatura — Birigui-SP\n"
    f"Variação 1940→2025: {delta_temp:+.2f}°C",
    fontweight="bold")
axes[0].set_xlabel("Ano")
axes[0].set_ylabel("Temperatura Média Anual (°C)")
axes[0].legend(fontsize=9, framealpha=0.2)
axes[0].grid(True)

# Tendência de precipitação
axes[1].bar(df_anual["year"], df_anual["precip_anual"],
    color="#3fb950", alpha=0.6, width=0.8)
axes[1].plot(df_anual["year"], p_precip(df_anual["year"]),
    color="#ffa657", linewidth=2.5, linestyle="--",
    label=f"Tendência: {z_precip[0]:+.1f}mm/ano")
axes[1].set_title("Tendência de Precipitação Anual — Birigui-SP",
    fontweight="bold")
axes[1].set_xlabel("Ano")
axes[1].set_ylabel("Precipitação Total Anual (mm)")
axes[1].legend(fontsize=9, framealpha=0.2)
axes[1].grid(True, axis="y")

plt.tight_layout()
plt.show()

# COMMAND ----------

# Gráfico 4: Previsão próximos 7 dias
df_prev = spark.sql("""
    SELECT observation_time, temperature_2m, apparent_temperature,
           precipitation, rain, relative_humidity,
           temp_class, rain_class, day, month, hour
    FROM weather_pipeline.silver.weather_forecast
    ORDER BY observation_time
""").toPandas()

df_prev["observation_time"] = pd.to_datetime(df_prev["observation_time"])

fig, axes = plt.subplots(3, 1, figsize=(16, 12), sharex=True)

# Temperatura e sensação térmica
axes[0].fill_between(df_prev["observation_time"],
    df_prev["apparent_temperature"], df_prev["temperature_2m"],
    alpha=0.2, color="#ffa657", label="Diferença sensação")
axes[0].plot(df_prev["observation_time"], df_prev["temperature_2m"],
    color="#58a6ff", linewidth=2, label="Temperatura (°C)")
axes[0].plot(df_prev["observation_time"], df_prev["apparent_temperature"],
    color="#ffa657", linewidth=2, linestyle="--", label="Sensação térmica (°C)")
axes[0].set_title("Previsão 7 Dias — Birigui-SP", fontweight="bold", fontsize=13)
axes[0].set_ylabel("Temperatura (°C)")
axes[0].legend(fontsize=8, framealpha=0.2)
axes[0].grid(True)

# Precipitação prevista
axes[1].bar(df_prev["observation_time"], df_prev["precipitation"],
    width=0.04, color="#3fb950", alpha=0.8, label="Precipitação (mm)")
axes[1].set_ylabel("Precipitação (mm)")
axes[1].legend(fontsize=8, framealpha=0.2)
axes[1].grid(True, axis="y")

# Umidade relativa
axes[2].fill_between(df_prev["observation_time"],
    df_prev["relative_humidity"],
    alpha=0.3, color="#d2a8ff")
axes[2].plot(df_prev["observation_time"], df_prev["relative_humidity"],
    color="#d2a8ff", linewidth=2, label="Umidade (%)")
axes[2].axhline(70, color="#ffa657", linestyle="--",
    linewidth=1, label="Limite 70%")
axes[2].set_ylabel("Umidade (%)")
axes[2].set_xlabel("Data")
axes[2].legend(fontsize=8, framealpha=0.2)
axes[2].grid(True)

plt.tight_layout()
plt.show()

# COMMAND ----------

# Relatório final
total_registros = spark.sql(
    "SELECT COUNT(*) FROM weather_pipeline.silver.weather_clean"
).collect()[0][0]

print(f"""
WEATHER DLT PIPELINE — BIRIGUI-SP
PIPELINE DLT                                          
  weather_raw      : 87 arquivos JSON                 
  weather_clean    : {total_registros:>10,} registros horários
  weather_daily    : 31.000 dias históricos           
  weather_monthly  : 1.032 meses históricos           
  weather_forecast : 168h previsão (7 dias)           
                                                
COBERTURA HISTÓRICA                                   
  Período : 1940 → 2026 (86 anos)                     
  Cidade  : Birigui-SP (-21.2878, -50.3409)           
                                                
QUALIDADE DLT                                         
  @dlt.expect           : temperatura_valida          
  @dlt.expect           : sensacao_valida             
  @dlt.expect_or_drop   : data_nao_nula               
  @dlt.expect_or_fail   : chuva_positiva              
""")