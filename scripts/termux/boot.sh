#!/data/data/com.termux/files/usr/bin/sh
# Executado pelo Termux:Boot quando o celular liga (copiado para ~/.termux/boot/ pelo setup.sh).
# Mantém a CPU acordada e sobe os serviços: cron (coletas) e sshd (acesso remoto via Tailscale).
termux-wake-lock
. "$PREFIX/etc/profile.d/start-services.sh"
sv-enable crond
sv-enable sshd 2>/dev/null || true

# Recupera o que o cron não pega: se o celular ficou desligado/sem rede, roda os coletores
# assim que a rede voltar (ambos só enviam ao Databricks quando há algo novo).
(
  REPO="$HOME/weather-dlt-pipeline"; LOGS="$HOME/.cache/inmet_sync"; mkdir -p "$LOGS"
  for i in $(seq 1 30); do   # espera até ~5 min pela rede/DNS
    python -c "import socket; socket.gethostbyname('apiprevmet3.inmet.gov.br')" 2>/dev/null && break
    sleep 10
  done
  cd "$REPO" || exit 0
  git pull -q
  python scripts/inmet_avisos_sync.py >> "$LOGS/cron.log" 2>&1
  python scripts/inmet_sync.py --ingerir >> "$LOGS/cron.log" 2>&1
) &
