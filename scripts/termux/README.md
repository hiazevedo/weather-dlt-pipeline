# Coletor do INMET no celular Android (Termux)

O INMET recusa conexões vindas de IPs de nuvem (o Databricks não consegue
conectar) e de clientes sem User-Agent de navegador. Por isso dois coletores
rodam fora do Databricks, num celular Android na tomada, ligado ao Wi-Fi de casa:

| Script | O que faz | Agenda (cron) |
|---|---|---|
| `scripts/inmet_avisos_sync.py` | Avisos meteorológicos oficiais (Defesa Civil) da UF; envia só quando mudam | a cada hora |
| `scripts/inmet_sync.py --ingerir` | Pacotes anuais das estações automáticas num raio de 80 km; baixa só quando o pacote muda | diário, 10h |

O envio ao Databricks usa a API REST (`scripts/dbx.py`, só biblioteca padrão do
Python) — a CLI do Databricks tem problema de DNS no Android.

## Instalação

1. Instale pelo **F-Droid** (a versão da Play Store está desatualizada):
   [Termux](https://f-droid.org/packages/com.termux/) e
   [Termux:Boot](https://f-droid.org/packages/com.termux.boot/). Abra o Termux:Boot uma vez.
2. Em *Configurações → Apps*, desative a **otimização de bateria** do Termux e do
   Termux:Boot. Deixe o celular **na tomada**.
3. No Databricks, crie um token só para o celular (*Settings → Developer → Access
   tokens*, com validade).
4. No Termux:

   ```bash
   pkg install -y curl
   curl -fsSL https://raw.githubusercontent.com/hiazevedo/weather-dlt-pipeline/main/scripts/termux/setup.sh | bash
   ```

   O script instala Python, git e cron, clona o repositório, pede o host e o token,
   faz um envio de teste dos avisos, grava o crontab e o script de boot.
5. Feche e abra o Termux uma vez e confira: `sv status crond` (deve dizer `run`).

## Operação

- Logs: `~/.cache/inmet_sync/` (`cron.log`, `inmet_avisos.log`, `inmet_sync.log`).
- Agenda: `crontab -l`. Os scripts se atualizam sozinhos (`git pull` diário às 4h30).
- Token expirado: edite `~/.databrickscfg` com um token novo.
- Rodar na mão: `cd ~/weather-dlt-pipeline && python scripts/inmet_avisos_sync.py --forcar`.

## Acesso remoto (SSH via Tailscale)

Com o app do Tailscale conectado no celular ("VPN sempre ativa", sem otimização de
bateria), rode no Termux:

```bash
curl -fsSL https://raw.githubusercontent.com/hiazevedo/weather-dlt-pipeline/main/scripts/termux/acesso_remoto.sh | bash
```

Instala o OpenSSH (porta 8022), autoriza só a chave do PC de desenvolvimento,
desliga login por senha e sobe o `sshd` como serviço (inclusive no boot). Do PC,
que também está no Tailscale: `ssh -p 8022 <usuário>@<nome-do-celular>`.
