#!/data/data/com.termux/files/usr/bin/sh
# Executado pelo Termux:Boot quando o celular liga (copiado para ~/.termux/boot/ pelo setup.sh).
# Mantém a CPU acordada e sobe os serviços: cron (coletas) e sshd (acesso remoto via Tailscale).
termux-wake-lock
. "$PREFIX/etc/profile.d/start-services.sh"
sv-enable crond
sv-enable sshd 2>/dev/null || true
