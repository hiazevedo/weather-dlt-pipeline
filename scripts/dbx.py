"""Cliente mínimo da API REST do Databricks — só biblioteca padrão do Python.

Usado pelos coletores locais (inmet_sync.py, inmet_avisos_sync.py), que rodam
fora do Databricks: no celular Android (Termux) ou no PC. Substitui a CLI do
Databricks, que tem problema de DNS no Android.

Credencial (nesta ordem):
  1. variáveis de ambiente DATABRICKS_HOST e DATABRICKS_TOKEN;
  2. ~/.databrickscfg, perfil $DATABRICKS_CONFIG_PROFILE (padrão: DEFAULT), chaves host e token.
"""
import configparser
import json
import os
import pathlib
import urllib.error
import urllib.parse
import urllib.request


def _credencial():
    host, token = os.environ.get("DATABRICKS_HOST"), os.environ.get("DATABRICKS_TOKEN")
    if not (host and token):
        cfg = configparser.ConfigParser()
        cfg.read(pathlib.Path.home() / ".databrickscfg")
        perfil = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
        sec = cfg[perfil] if perfil in cfg else {}
        host, token = host or sec.get("host"), token or sec.get("token")
    if not (host and token):
        raise RuntimeError("Credencial do Databricks não encontrada: defina DATABRICKS_HOST/"
                           "DATABRICKS_TOKEN ou host/token em ~/.databrickscfg")
    return host.rstrip("/"), token


def chamar(metodo, caminho, corpo=None, dados=None, aceitar=()):
    """Faz a chamada e devolve o JSON da resposta (ou {}).

    `corpo`: dict enviado como JSON; `dados`: bytes brutos (upload de arquivo);
    `aceitar`: códigos HTTP de erro tratados como sucesso (ex.: 409 = já existe).
    """
    host, token = _credencial()
    headers = {"Authorization": f"Bearer {token}"}
    if corpo is not None:
        dados, headers["Content-Type"] = json.dumps(corpo).encode(), "application/json"
    elif dados is not None:
        headers["Content-Type"] = "application/octet-stream"
    req = urllib.request.Request(host + caminho, data=dados, method=metodo, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=300) as r:
            txt = r.read().decode() or "{}"
            return json.loads(txt) if txt.lstrip().startswith(("{", "[")) else {}
    except urllib.error.HTTPError as e:
        msg = e.read().decode(errors="replace")
        if e.code in aceitar or ("RESOURCE_ALREADY_EXISTS" in msg and 409 in aceitar):
            return {}
        raise RuntimeError(f"Databricks {metodo} {caminho} → HTTP {e.code}: {msg[:300]}") from None


def _vol(caminho):
    """'/Volumes/cat/sch/vol/a b.json' → caminho codificado para a Files API."""
    return urllib.parse.quote(caminho)


def criar_volume(catalogo, schema, volume):
    chamar("POST", "/api/2.1/unity-catalog/volumes",
           {"catalog_name": catalogo, "schema_name": schema, "name": volume, "volume_type": "MANAGED"},
           aceitar=(400, 409))


def criar_pasta(caminho_volume):
    chamar("PUT", f"/api/2.0/fs/directories{_vol(caminho_volume)}", aceitar=(409,))


def enviar_arquivo(local, caminho_volume):
    """Envia um arquivo local para um Volume (sobrescreve). Cria a pasta de destino."""
    criar_pasta(caminho_volume.rsplit("/", 1)[0])
    chamar("PUT", f"/api/2.0/fs/files{_vol(caminho_volume)}?overwrite=true",
           dados=pathlib.Path(local).read_bytes())


def usuario_atual():
    return chamar("GET", "/api/2.0/preview/scim/v2/Me")["userName"]


def disparar_notebook(nome_execucao, caminho_notebook):
    """Execução avulsa (serverless) de um notebook; devolve o run_id."""
    r = chamar("POST", "/api/2.1/jobs/runs/submit", {
        "run_name": nome_execucao,
        "tasks": [{"task_key": "t", "notebook_task": {"notebook_path": caminho_notebook}}]})
    return r["run_id"]
