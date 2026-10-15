# Agenda o scripts/inmet_avisos_sync.py a cada 3 horas no Agendador de Tarefas do Windows.
#
# Avisos do INMET mudam ao longo do dia, por isso a frequência é maior que a do
# inmet_sync (semanal). O script só envia ao Databricks quando algum aviso muda.
# Roda sem abrir janela (conhost --headless). Se o PC estiver desligado no
# horário, roda assim que ele for ligado (StartWhenAvailable).
#
# Uso (PowerShell, sem precisar de administrador):
#   powershell -ExecutionPolicy Bypass -File "\\wsl.localhost\Ubuntu-24.04\home\higor\workspaces\weather-dlt-pipeline\scripts\agendar_inmet_avisos.ps1"
# Para remover:
#   Unregister-ScheduledTask -TaskName "INMET avisos (weather pipeline)" -Confirm:$false

param(
    [string]$Distro = "Ubuntu-24.04",
    [string]$Repo   = "/home/higor/workspaces/weather-dlt-pipeline"
)

$nome   = "INMET avisos (weather pipeline)"
$acao   = New-ScheduledTaskAction -Execute "conhost.exe" `
            -Argument "--headless wsl.exe -d $Distro -- python3 $Repo/scripts/inmet_avisos_sync.py"
# diário às 00:00, repetindo a cada 3 h ao longo do dia (00, 03, 06, ..., 21 h)
$quando = New-ScheduledTaskTrigger -Daily -At 00:00
$quando.Repetition = (New-ScheduledTaskTrigger -Once -At 00:00 `
            -RepetitionInterval (New-TimeSpan -Hours 3) -RepetitionDuration (New-TimeSpan -Days 1)).Repetition
$config = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries `
            -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Minutes 30)

Register-ScheduledTask -TaskName $nome -Action $acao -Trigger $quando -Settings $config `
    -Description "Coleta os avisos meteorológicos do INMET (Defesa Civil) e envia ao Databricks quando mudam" -Force |
    Select-Object TaskName, State
