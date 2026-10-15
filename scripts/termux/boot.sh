#!/data/data/com.termux/files/usr/bin/sh
# Executado pelo Termux:Boot quando o celular liga (copiado para ~/.termux/boot/ pelo setup.sh).
# Mantém a CPU acordada para o cron e sobe o serviço do cron.
termux-wake-lock
. "$PREFIX/etc/profile.d/start-services.sh"
sv-enable crond
