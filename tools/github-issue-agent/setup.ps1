# GitHub Issue Agent - Setup Script for Windows
# Run this script to set up the agent on your Windows PC

param(
    [switch]$SkipVenv,
    [switch]$Help
)

if ($Help) {
    Write-Host @"
GitHub Issue Agent Setup

Usage:
    .\setup.ps1               # Full setup with virtual environment
    .\setup.ps1 -SkipVenv     # Setup without creating venv

This script will:
1. Check Python installation
2. Create virtual environment (optional)
3. Install dependencies
4. Create .env file from template
5. Test connections

"@
    exit 0
}

$ErrorActionPreference = "Stop"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  GitHub Issue Agent Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check Python
Write-Host "[1/6] Checking Python installation..." -ForegroundColor Yellow
try {
    $pythonVersion = python --version 2>&1
    Write-Host "  ✓ Found: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "  ✗ Python not found!" -ForegroundColor Red
    Write-Host "  Please install Python 3.9+ from https://www.python.org/" -ForegroundColor Red
    exit 1
}

# Create virtual environment
if (-not $SkipVenv) {
    Write-Host "[2/6] Creating virtual environment..." -ForegroundColor Yellow
    if (Test-Path "venv") {
        Write-Host "  Virtual environment already exists" -ForegroundColor Gray
    } else {
        python -m venv venv
        Write-Host "  ✓ Virtual environment created" -ForegroundColor Green
    }

    # Activate virtual environment
    Write-Host "[3/6] Activating virtual environment..." -ForegroundColor Yellow
    & ".\venv\Scripts\Activate.ps1"
    Write-Host "  ✓ Virtual environment activated" -ForegroundColor Green
} else {
    Write-Host "[2/6] Skipping virtual environment creation" -ForegroundColor Gray
    Write-Host "[3/6] Skipping virtual environment activation" -ForegroundColor Gray
}

# Install dependencies
Write-Host "[4/6] Installing Python dependencies..." -ForegroundColor Yellow
pip install --upgrade pip | Out-Null
pip install -r requirements.txt
Write-Host "  ✓ Dependencies installed" -ForegroundColor Green

# Create .env file
Write-Host "[5/6] Setting up environment variables..." -ForegroundColor Yellow
if (Test-Path ".env") {
    Write-Host "  .env file already exists" -ForegroundColor Gray
    $overwrite = Read-Host "  Overwrite? (y/N)"
    if ($overwrite -ne "y") {
        Write-Host "  Keeping existing .env file" -ForegroundColor Gray
    } else {
        Copy-Item ".env.example" ".env" -Force
        Write-Host "  ✓ Created new .env file" -ForegroundColor Green
    }
} else {
    Copy-Item ".env.example" ".env"
    Write-Host "  ✓ Created .env file from template" -ForegroundColor Green
}

# Create data directories
Write-Host "[6/6] Creating data directories..." -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path "data/logs" | Out-Null
New-Item -ItemType Directory -Force -Path "drafts" | Out-Null
Write-Host "  ✓ Data directories created" -ForegroundColor Green

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Setup Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "1. Edit .env file with your API keys:" -ForegroundColor White
Write-Host "   notepad .env" -ForegroundColor Gray
Write-Host ""
Write-Host "2. (Optional) Edit config.yaml:" -ForegroundColor White
Write-Host "   notepad config.yaml" -ForegroundColor Gray
Write-Host ""
Write-Host "3. Test the setup:" -ForegroundColor White
Write-Host "   python agent.py test" -ForegroundColor Gray
Write-Host ""
Write-Host "4. Run the agent:" -ForegroundColor White
Write-Host "   python agent.py run --mode draft" -ForegroundColor Gray
Write-Host ""
Write-Host "5. Install Task Scheduler (for automatic execution):" -ForegroundColor White
Write-Host "   .\install-task-scheduler.ps1" -ForegroundColor Gray
Write-Host ""
