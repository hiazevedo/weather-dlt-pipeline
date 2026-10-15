#!/usr/bin/env python3
"""Coleta os avisos meteorológicos oficiais do INMET e envia ao Databricks.

São os avisos que a Defesa Civil repassa à população (tempestade, chuvas
intensas, vendaval, baixa umidade…), com municípios afetados (código IBGE),
severidade, início/fim, riscos, instruções e polígono.

Roda FORA do Databricks pelo mesmo motivo do inmet_sync.py: o INMET recusa
conexões de IPs de nuvem e de clientes sem User-Agent de navegador.

O que faz:
  1. baixa os avisos ativos e futuros (apiprevmet3.inmet.gov.br/avisos/ativos);
  2. mantém só os que tocam a UF do config.py (e só os municípios dessa UF);
  3. se algo mudou desde a última coleta, envia um snapshot JSON ao Volume
     weather_pipeline.bronze.inmet_avisos e dispara o notebook 11_ingest_avisos.

Uso:
  python3 scripts/inmet_avisos_sync.py            # agendado a cada 3 h
  python3 scripts/inmet_avisos_sync.py --forcar   # envia mesmo sem mudança
"""
import argparse
import datetime as dt
import hashlib
import json
import logging
import tempfile
import pathlib
import urllib.request

# reaproveita CLI do Databricks, config e cabeçalho do coletor do INMET
from inmet_sync import CACHE, UA, BUNDLE_FILES, carregar_config, databricks

URL = "https://apiprevmet3.inmet.gov.br/avisos/ativos"
VOLUME = "dbfs:/Volumes/weather_pipeline/bronze/inmet_avisos"
CAMPOS = ["id", "id_aviso", "codigo", "descricao", "severidade", "id_severidade", "id_condicao_severa",
          "aviso_cor", "inicio", "fim", "data_inicio", "data_fim", "hora_inicio", "hora_fim",
          "riscos", "instrucoes", "estados", "regioes", "mesorregioes", "microrregioes",
          "created_at", "updated_at", "encerrado", "alterado", "id_sequencia", "referencia", "poligono"]
UF_NOME = {"SP": "São Paulo"}   # nome usado no campo "estados" do INMET

log = logging.getLogger("inmet_avisos_sync")


def filtrar(avisos, uf, prefixo_ibge):
    """Só avisos que tocam a UF; geocodes/municípios reduzidos aos dessa UF."""
    saida = []
    for a in avisos:
        geocodes = [g for g in str(a.get("geocodes", "")).split(",") if g.strip().startswith(prefixo_ibge)]
        if not geocodes:
            continue
        municipios = [m.strip() for m in str(a.get("municipios", "")).split(",") if f"- {uf} (" in m]
        r = {k: a.get(k) for k in CAMPOS}
        r["geocodes"], r["municipios"] = geocodes, municipios
        saida.append(r)
    return sorted(saida, key=lambda r: (str(r["id"]), str(r["updated_at"])))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--forcar", action="store_true", help="envia mesmo se nada mudou")
    a = ap.parse_args()

    CACHE.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(), logging.FileHandler(CACHE / "inmet_avisos.log")])
    cfg = carregar_config()
    uf = getattr(cfg, "UF", "SP")
    prefixo = str(getattr(cfg, "CIDADE_IBGE", "35"))[:2]          # 2 primeiros dígitos = UF

    req = urllib.request.Request(URL, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=120) as r:
        bruto = json.load(r)

    snapshot = {grupo: filtrar(bruto.get(grupo, []), uf, prefixo) for grupo in ("hoje", "futuro")}
    cidade = [x for g in snapshot.values() for x in g if str(cfg.CIDADE_IBGE) in x["geocodes"]]
    assinatura = hashlib.sha256(json.dumps(snapshot, sort_keys=True, ensure_ascii=False).encode()).hexdigest()

    estado_path = CACHE / "avisos_estado.json"
    estado = json.loads(estado_path.read_text()) if estado_path.exists() else {}
    log.info("avisos em %s: %d hoje, %d futuros | %s: %d (%s)", uf, len(snapshot["hoje"]), len(snapshot["futuro"]),
             cfg.CIDADE, len(cidade), "; ".join(f"{x['descricao']} – {x['severidade']}" for x in cidade) or "nenhum")
    if not a.forcar and estado.get("assinatura") == assinatura:
        log.info("sem mudanças desde %s — nada enviado", estado.get("enviado_em"))
        return

    agora = dt.datetime.now()
    payload = {"coletado_em": agora.isoformat(timespec="seconds"), "fonte": URL, "uf": uf, **snapshot}
    destino = f"{VOLUME}/{agora:%Y}/{agora:%m}/avisos_{agora:%Y%m%d_%H%M%S}.json"
    with tempfile.TemporaryDirectory(dir=CACHE) as tmp:
        arq = pathlib.Path(tmp) / "avisos.json"
        arq.write_text(json.dumps(payload, ensure_ascii=False))
        databricks("volumes", "create", "weather_pipeline", "bronze", "inmet_avisos", "MANAGED", check=False)
        databricks("fs", "mkdir", destino.rsplit("/", 1)[0])     # cp de arquivo não cria pastas
        databricks("fs", "cp", "--overwrite", str(arq), destino)

    user = json.loads(databricks("current-user", "me", "-o", "json").stdout)["userName"]
    run = json.loads(databricks("jobs", "submit", "--no-wait", "-o", "json", "--json", json.dumps(
        {"run_name": "INMET — ingestão de avisos (inmet_avisos_sync)",
         "tasks": [{"task_key": "ingest", "notebook_task": {
             "notebook_path": BUNDLE_FILES.format(user=user) + "/11_ingest_avisos"}}]})).stdout)
    estado_path.write_text(json.dumps({"assinatura": assinatura, "enviado_em": agora.isoformat(timespec="seconds")}))
    log.info("snapshot enviado (%s) e ingestão disparada (run_id %s)", destino.rsplit("/", 1)[-1], run["run_id"])


if __name__ == "__main__":
    main()
