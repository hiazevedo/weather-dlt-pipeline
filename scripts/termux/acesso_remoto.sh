#!/data/data/com.termux/files/usr/bin/bash
# Acesso remoto ao Termux por SSH, via Tailscale (sem abrir porta na internet).
#
# - instala o servidor SSH do Termux (porta 8022);
# - autoriza SOMENTE a chave do PC de desenvolvimento (WSL "desktop-tartaro");
# - desliga o login por senha;
# - sobe o sshd como serviço e inclui no boot (Termux:Boot).
#
# Uso, dentro do Termux:
#   curl -fsSL https://raw.githubusercontent.com/hiazevedo/weather-dlt-pipeline/main/scripts/termux/acesso_remoto.sh | bash
#
# No celular, deixe o app do Tailscale conectado com "VPN sempre ativa"
# (Configurações > Rede > VPN > Tailscale) e sem otimização de bateria.
set -euo pipefail

CHAVE="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIIxhTRJPZ8foTJjIFxxK8xmuVx+qGe0cIb3ig/ryKpIM wsl-desktop-tartaro"

echo "==> OpenSSH"
pkg install -y openssh termux-services

echo "==> Chave autorizada"
mkdir -p "$HOME/.ssh" && chmod 700 "$HOME/.ssh"
touch "$HOME/.ssh/authorized_keys" && chmod 600 "$HOME/.ssh/authorized_keys"
grep -qF "$(echo "$CHAVE" | awk '{print $2}')" "$HOME/.ssh/authorized_keys" \
  || echo "$CHAVE" >> "$HOME/.ssh/authorized_keys"

echo "==> Só login por chave (sem senha)"
CFG="$PREFIX/etc/ssh/sshd_config"
sed -i -E 's/^#?[[:space:]]*PasswordAuthentication[[:space:]].*/PasswordAuthentication no/' "$CFG"
grep -q '^PasswordAuthentication no' "$CFG" || echo 'PasswordAuthentication no' >> "$CFG"

echo "==> Serviço sshd"
source "$PREFIX/etc/profile.d/start-services.sh" 2>/dev/null || true
sv-enable sshd 2>/dev/null || echo "    feche e reabra o Termux e rode: sv-enable sshd"
sv restart sshd 2>/dev/null || true

echo "==> Boot"
REPO="$HOME/weather-dlt-pipeline"
if [ -f "$REPO/scripts/termux/boot.sh" ]; then
  git -C "$REPO" pull -q || true
  mkdir -p "$HOME/.termux/boot"
  cp "$REPO/scripts/termux/boot.sh" "$HOME/.termux/boot/weather-dlt-pipeline.sh"
  chmod +x "$HOME/.termux/boot/weather-dlt-pipeline.sh"
else
  echo "    repositório ainda não clonado — rode o setup.sh para o boot completo"
fi
termux-wake-lock 2>/dev/null || true

echo
echo "Pronto: usuário $(whoami), porta 8022."
echo "No PC (com Tailscale): ssh -p 8022 $(whoami)@<nome-do-celular-no-tailscale>"
