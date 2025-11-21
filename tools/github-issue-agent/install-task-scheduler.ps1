# GitHub Issue Agent - Task Scheduler Installation
# Registers the agent to run automatically every 10 minutes
# MUST BE RUN AS ADMINISTRATOR

param(
    [int]$IntervalMinutes = 10,
    [string]$Mode = "draft",
    [switch]$Uninstall,
    [switch]$Help
)

if ($Help) {
    Write-Host @"
GitHub Issue Agent - Task Scheduler Setup

Usage (run as Administrator):
    .\install-task-scheduler.ps1                  # Install with defaults
    .\install-task-scheduler.ps1 -IntervalMinutes 5  # Custom interval
    .\install-task-scheduler.ps1 -Mode auto-post  # Auto-post mode
    .\install-task-scheduler.ps1 -Uninstall       # Remove from scheduler

Options:
    -IntervalMinutes    Polling interval (default: 10)
    -Mode              Operating mode: draft or auto-post (default: draft)
    -Uninstall         Remove scheduled task
    -Help              Show this help

"@
    exit 0
}

# Check if running as Administrator
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if (-not $isAdmin) {
    Write-Host "ERROR: This script must be run as Administrator!" -ForegroundColor Red
    Write-Host "Right-click PowerShell and select 'Run as Administrator'" -ForegroundColor Yellow
    exit 1
}

$TaskName = "GitHubIssueAgent"
$ScriptPath = Join-Path (Get-Location) "run-agent.ps1"

# Uninstall
if ($Uninstall) {
    Write-Host "Removing scheduled task..." -ForegroundColor Yellow

    $existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue

    if ($existingTask) {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
        Write-Host "✓ Scheduled task removed successfully" -ForegroundColor Green
    } else {
        Write-Host "Task not found: $TaskName" -ForegroundColor Gray
    }

    exit 0
}

# Install
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Installing Task Scheduler" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Task Name:       $TaskName" -ForegroundColor White
Write-Host "Interval:        Every $IntervalMinutes minutes" -ForegroundColor White
Write-Host "Mode:            $Mode" -ForegroundColor White
Write-Host "Script:          $ScriptPath" -ForegroundColor White
Write-Host ""

# Check if script exists
if (-not (Test-Path $ScriptPath)) {
    Write-Host "ERROR: run-agent.ps1 not found at: $ScriptPath" -ForegroundColor Red
    exit 1
}

# Remove existing task if present
$existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existingTask) {
    Write-Host "Removing existing task..." -ForegroundColor Yellow
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

# Create scheduled task
Write-Host "Creating scheduled task..." -ForegroundColor Yellow

# Action: Run PowerShell script
$action = New-ScheduledTaskAction `
    -Execute "PowerShell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`" -Mode $Mode -Log" `
    -WorkingDirectory (Get-Location)

# Trigger: Every N minutes, indefinitely
$trigger = New-ScheduledTaskTrigger `
    -Once `
    -At (Get-Date) `
    -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes) `
    -RepetitionDuration ([TimeSpan]::MaxValue)

# Settings
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -MultipleInstances IgnoreNew

# Principal: Run as current user
$principal = New-ScheduledTaskPrincipal `
    -UserId $env:USERNAME `
    -LogonType Interactive `
    -RunLevel Limited

# Register task
Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Description "GitHub Issue Agent - Automated issue monitoring and response generation" | Out-Null

Write-Host "✓ Scheduled task created successfully" -ForegroundColor Green
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Installation Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "The agent will now run automatically every $IntervalMinutes minutes." -ForegroundColor White
Write-Host ""
Write-Host "Useful commands:" -ForegroundColor Yellow
Write-Host "  View task:    Get-ScheduledTask -TaskName '$TaskName'" -ForegroundColor Gray
Write-Host "  Run now:      Start-ScheduledTask -TaskName '$TaskName'" -ForegroundColor Gray
Write-Host "  Disable:      Disable-ScheduledTask -TaskName '$TaskName'" -ForegroundColor Gray
Write-Host "  Enable:       Enable-ScheduledTask -TaskName '$TaskName'" -ForegroundColor Gray
Write-Host "  Uninstall:    .\install-task-scheduler.ps1 -Uninstall" -ForegroundColor Gray
Write-Host ""
Write-Host "Logs will be saved to: data/logs/" -ForegroundColor White
Write-Host ""

# Ask to run test
$runTest = Read-Host "Would you like to run a test now? (Y/n)"
if ($runTest -ne "n") {
    Write-Host ""
    Write-Host "Running test..." -ForegroundColor Yellow
    Start-ScheduledTask -TaskName $TaskName
    Write-Host "✓ Test started - Check Task Scheduler for results" -ForegroundColor Green
    Write-Host "  Or check logs: data/logs/" -ForegroundColor Gray
}
