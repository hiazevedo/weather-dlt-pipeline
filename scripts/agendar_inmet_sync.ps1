# Agenda o scripts/inmet_sync.py no Agendador de Tarefas do Windows.
#
# Por que no Windows: o INMET só aceita conexões da rede local (o Databricks é
# bloqueado), e o WSL não fica rodando sozinho — o Agendador do Windows sobe o
# WSL na hora marcada.
#
# Frequência: semanal (segunda, 10h). O script só baixa um pacote anual quando
# o Last-Modified muda (o INMET atualiza ~1x por mês), então nas outras semanas
# ele faz apenas consultas HEAD. Se o PC estiver desligado no horário, a tarefa
# roda assim que ele for ligado (StartWhenAvailable).
#
# Uso (PowerShell, sem precisar de administrador):
#   powershell -ExecutionPolicy Bypass -File scripts\agendar_inmet_sync.ps1
# Para remover:
#   Unregister-ScheduledTask -TaskName "INMET sync (weather pipeline)" -Confirm:$false

param(
    [string]$Distro = "Ubuntu-24.04",
    [string]$Repo   = "/home/higor/workspaces/weather-dlt-pipeline"
)

$nome   = "INMET sync (weather pipeline)"
$acao   = New-ScheduledTaskAction -Execute "wsl.exe" `
            -Argument "-d $Distro -- python3 $Repo/scripts/inmet_sync.py --ingerir"
$quando = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday -At 10:00
$config = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries `
            -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Hours 2)

Register-ScheduledTask -TaskName $nome -Action $acao -Trigger $quando -Settings $config `
    -Description "Baixa os pacotes anuais do INMET, extrai as estações próximas e envia ao Databricks" -Force |
    Select-Object TaskName, State
