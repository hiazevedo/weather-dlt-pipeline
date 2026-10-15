#!/usr/bin/env python3
"""Sincroniza os dados horários das estações automáticas do INMET com o Databricks.

Por que roda FORA do Databricks: o INMET recusa conexões vindas de IPs de
nuvem (o Databricks não consegue nem abrir a conexão) e de clientes sem
User-Agent de navegador. Da rede brasileira, com o cabeçalho, funciona.
Agendado no celular Android (Termux + cron) — ver scripts/termux/.

Fonte: os pacotes anuais em portal.inmet.gov.br/dadoshistoricos (todas as
estações automáticas, um zip por ano, atualizado mensalmente; o do ano
corrente traz dados até o fim do mês anterior).

O que faz, para cada ano pedido:
  1. confere o Last-Modified do zip — se não mudou desde a última execução, pula;
  2. baixa o zip, extrai só as estações num raio de --raio-km da cidade
     (LAT/LON do config.py) e mantém a versão com a data final mais recente;
  3. envia os CSVs para o Volume weather_pipeline.bronze.inmet (API REST, scripts/dbx.py);
  4. com --ingerir e se algo foi enviado, dispara o notebook 10_ingest_inmet
     (silver.inmet_hourly).

Uso:
  python3 scripts/inmet_sync.py                  # ano anterior e corrente (uso mensal)
  python3 scripts/inmet_sync.py --desde 2007     # carga inicial
  python3 scripts/inmet_sync.py --ingerir        # também atualiza a tabela no Databricks
"""
import argparse
import contextlib
import datetime as dt
import importlib.util
import io
import json
import logging
import math
import os
import pathlib
import shutil
import tempfile
import urllib.request
import zipfile

import dbx

REPO = pathlib.Path(__file__).resolve().parents[1]
CACHE = pathlib.Path(os.environ.get("INMET_SYNC_CACHE", pathlib.Path.home() / ".cache" / "inmet_sync"))
URL = "https://portal.inmet.gov.br/uploads/dadoshistoricos/{ano}.zip"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/129.0 Safari/537.36")
VOLUME = "/Volumes/weather_pipeline/bronze/inmet"
BUNDLE_FILES = "/Workspace/Users/{user}/.bundle/weather-dlt-pipeline-birigui/dev/files"

log = logging.getLogger("inmet_sync")


def carregar_config():
    """Lê LAT/LON/CIDADE do config.py do pipeline (é um notebook, mas Python puro)."""
    spec = importlib.util.spec_from_file_location("config", REPO / "config.py")
    cfg = importlib.util.module_from_spec(spec)
    with contextlib.redirect_stdout(io.StringIO()):
        spec.loader.exec_module(cfg)
    return cfg


def http(url, metodo="GET"):
    return urllib.request.urlopen(urllib.request.Request(url, method=metodo, headers={"User-Agent": UA}),
                                  timeout=120)


def km(lat1, lon1, lat2, lon2):
    p = math.radians
    h = (math.sin(p(lat2 - lat1) / 2) ** 2 +
         math.cos(p(lat1)) * math.cos(p(lat2)) * math.sin(p(lon2 - lon1) / 2) ** 2)
    return 12742 * math.asin(math.sqrt(h))


def data_final(nome):
    """'INMET_SE_SP_A727_LINS_01-01-2026_A_30-09-2026.CSV' → '20260930'."""
    d = nome.rsplit("_", 1)[-1].split(".")[0]          # 30-09-2026
    return d[6:] + d[3:5] + d[:2]


def extrair(zip_path, destino, lat, lon, raio_km):
    """Extrai as estações do raio; se houver várias versões, fica com a mais recente."""
    escolhidos = {}
    with zipfile.ZipFile(zip_path) as z:
        for nome in z.namelist():
            if not nome.upper().endswith(".CSV"):
                continue
            with z.open(nome) as f:
                cab = dict(l.decode("latin-1").strip().rstrip(";").split(":;", 1)
                           for l in [f.readline() for _ in range(8)] if b":;" in l)
            try:
                la = float(cab["LATITUDE"].replace(",", "."))
                lo = float(cab["LONGITUDE"].replace(",", "."))
            except (KeyError, ValueError):
                continue
            if km(lat, lon, la, lo) > raio_km:
                continue
            cod = cab.get("CODIGO (WMO)", nome.split("_")[3])
            if cod not in escolhidos or data_final(nome) > data_final(escolhidos[cod]):
                escolhidos[cod] = nome
        destino.mkdir(parents=True, exist_ok=True)
        for cod, nome in escolhidos.items():
            (destino / f"{cod}.csv").write_bytes(z.read(nome))
    return {cod: data_final(n) for cod, n in escolhidos.items()}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    hoje = dt.date.today()
    ap.add_argument("--desde", type=int, default=hoje.year - 1, help="primeiro ano (padrão: ano anterior)")
    ap.add_argument("--ate", type=int, default=hoje.year)
    ap.add_argument("--raio-km", type=float, default=80, help="raio em torno da cidade (padrão: 80 km)")
    ap.add_argument("--forcar", action="store_true", help="processa mesmo se o zip não mudou")
    ap.add_argument("--ingerir", action="store_true", help="dispara o notebook 10_ingest_inmet no final")
    a = ap.parse_args()

    CACHE.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(), logging.FileHandler(CACHE / "inmet_sync.log")])
    cfg = carregar_config()
    estado_path = CACHE / "estado.json"
    estado = json.loads(estado_path.read_text()) if estado_path.exists() else {}
    log.info("INMET → Databricks | %s (%.4f, %.4f) | raio %.0f km | anos %d–%d",
             cfg.CIDADE, cfg.LAT, cfg.LON, a.raio_km, a.desde, a.ate)

    dbx.criar_volume("weather_pipeline", "bronze", "inmet")
    enviados = 0
    for ano in range(a.desde, a.ate + 1):
        try:
            lm = http(URL.format(ano=ano), "HEAD").headers.get("Last-Modified")
        except Exception as e:                       # ano ainda sem pacote, por exemplo
            log.warning("%d: pacote indisponível (%s)", ano, e)
            continue
        if not a.forcar and estado.get(str(ano)) == lm:
            log.info("%d: sem mudanças desde %s", ano, lm)
            continue
        with tempfile.TemporaryDirectory(dir=CACHE) as tmp:
            zip_path = pathlib.Path(tmp) / f"{ano}.zip"
            with http(URL.format(ano=ano)) as r, open(zip_path, "wb") as f:
                shutil.copyfileobj(r, f, 1 << 20)
            saida = pathlib.Path(tmp) / str(ano)
            est = extrair(zip_path, saida, cfg.LAT, cfg.LON, a.raio_km)
            for cod in est:
                dbx.enviar_arquivo(saida / f"{cod}.csv", f"{VOLUME}/{ano}/{cod}.csv")
            enviados += len(est)
        estado[str(ano)] = lm
        estado_path.write_text(json.dumps(estado, indent=1))
        log.info("%d: %d estações enviadas (dados até %s)", ano, len(est),
                 ", ".join(f"{c}:{d[6:]}/{d[4:6]}" for c, d in sorted(est.items())))

    if a.ingerir and enviados == 0:
        log.info("nada novo — ingestão não disparada (o job semanal também reprocessa o Volume)")
    elif a.ingerir:
        nb = BUNDLE_FILES.format(user=dbx.usuario_atual()) + "/10_ingest_inmet"
        run_id = dbx.disparar_notebook("INMET — ingestão (inmet_sync)", nb)
        log.info("ingestão disparada no Databricks (run_id %s)", run_id)
    log.info("concluído — %d arquivos de estação enviados", enviados)


if __name__ == "__main__":
    main()
