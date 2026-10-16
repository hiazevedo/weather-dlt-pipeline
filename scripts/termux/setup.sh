#!/data/data/com.termux/files/usr/bin/bash
# Configura o celular Android (Termux) como coletor do INMET para o weather-dlt-pipeline.
#
# Pré-requisitos (instalar pelo F-Droid, NÃO pela Play Store):
#   - Termux        https://f-droid.org/packages/com.termux/
#   - Termux:Boot   https://f-droid.org/packages/com.termux.boot/  (abra o app uma vez)
# No Android: desative a otimização de bateria para Termux e Termux:Boot e deixe o
# celular na tomada.
#
# Uso, dentro do Termux:
#   curl -fsSL https://raw.githubusercontent.com/hiazevedo/weather-dlt-pipeline/main/scripts/termux/setup.sh | bash
# (ou, com o repositório já clonado: bash ~/weather-dlt-pipeline/scripts/termux/setup.sh)
set -euo pipefail

REPO_URL="https://github.com/hiazevedo/weather-dlt-pipeline.git"
REPO="$HOME/weather-dlt-pipeline"
LOGS="$HOME/.cache/inmet_sync"

echo "==> Pacotes (python, git, cron e serviços do Termux)"
pkg update -y
pkg install -y python git cronie termux-services

echo "==> Código"
if [ -d "$REPO/.git" ]; then git -C "$REPO" pull -q; else git clone -q "$REPO_URL" "$REPO"; fi

echo "==> Credencial do Databricks (~/.databrickscfg)"
if [ ! -f "$HOME/.databrickscfg" ]; then
  echo "Crie um token só para o celular em Databricks > Settings > Developer > Access tokens"
  echo "(com validade, ex.: 90 dias)."
  read -r -p "Host do workspace [https://dbc-4755d9a9-d11b.cloud.databricks.com]: " HOST </dev/tty
  HOST="${HOST:-https://dbc-4755d9a9-d11b.cloud.databricks.com}"
  read -r -s -p "Token (não aparece ao digitar): " TOKEN </dev/tty; echo
  umask 077
  printf '[DEFAULT]\nhost  = %s\ntoken = %s\n' "$HOST" "$TOKEN" > "$HOME/.databrickscfg"
else
  echo "    já existe — mantido"
fi

echo "==> Teste: avisos do INMET → Databricks"
mkdir -p "$LOGS"
python "$REPO/scripts/inmet_avisos_sync.py" --forcar

echo "==> Agendamento (cron)"
INI="# >>> weather-dlt-pipeline"
FIM="# <<< weather-dlt-pipeline"
# reescreve só o bloco deste projeto (mantém outras linhas do crontab)
{ (crontab -l 2>/dev/null || true) | sed "/^$INI/,/^$FIM/d"
  cat <<EOF
$INI (gerado por scripts/termux/setup.sh)
# avisos do INMET a cada hora (só envia ao Databricks quando algum aviso muda)
0 * * * * cd $REPO && python scripts/inmet_avisos_sync.py >> $LOGS/cron.log 2>&1
# pacotes anuais das estações do INMET (só baixa quando o pacote muda; diário porque o cron não recupera execuções perdidas)
0 10 * * * cd $REPO && python scripts/inmet_sync.py --ingerir >> $LOGS/cron.log 2>&1
# mantém os scripts atualizados com o GitHub
30 4 * * * git -C $REPO pull -q >> $LOGS/cron.log 2>&1
$FIM
EOF
} | crontab -
crontab -l | sed -n "/^$INI/,/^$FIM/p"

echo "==> Inicialização automática (Termux:Boot)"
mkdir -p "$HOME/.termux/boot"
cp "$REPO/scripts/termux/boot.sh" "$HOME/.termux/boot/weather-dlt-pipeline.sh"
chmod +x "$HOME/.termux/boot/weather-dlt-pipeline.sh"

echo "==> Serviço do cron"
# termux-services só fica ativo depois de reabrir o Termux na primeira vez
source "$PREFIX/etc/profile.d/start-services.sh" 2>/dev/null || true
sv-enable crond 2>/dev/null || echo "    feche e reabra o Termux e rode: sv-enable crond"
termux-wake-lock 2>/dev/null || true

echo
echo "Pronto. Logs em $LOGS (cron.log, inmet_avisos.log, inmet_sync.log)."
echo "Para conferir o cron: sv status crond   |   crontab -l"
