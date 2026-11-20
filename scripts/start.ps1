# ==============================================================================
# RetailLake 360 - Quickstart Script (Windows PowerShell)
# ==============================================================================
Write-Host "===========================================================================" -ForegroundColor Cyan
Write-Host "   Starting RetailLake 360 Enterprise Data Lakehouse Platform" -ForegroundColor Cyan
Write-Host "   Database Engine: MySQL 8.4 LTS | Lakehouse Storage: Parquet" -ForegroundColor Cyan
Write-Host "===========================================================================" -ForegroundColor Cyan

if (-not (Test-Path ".env")) {
    Write-Host "Creating .env from .env.example. Set unique passwords, then run this script again." -ForegroundColor Yellow
    Copy-Item ".env.example" ".env"
    exit 1
}

Write-Host "Running automated platform acceptance verification suite..." -ForegroundColor Green
python scripts/verify_platform.py

Write-Host ""
Write-Host "Run the Streamlit Dashboard locally with:" -ForegroundColor White
Write-Host "streamlit run src/dashboard/app.py" -ForegroundColor Yellow
Write-Host ""
Write-Host "Or start the complete Docker cluster with:" -ForegroundColor White
Write-Host "docker compose up -d" -ForegroundColor Yellow
Write-Host "===========================================================================" -ForegroundColor Cyan
