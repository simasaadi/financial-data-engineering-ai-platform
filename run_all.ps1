# Run the full financial data engineering pipeline from PowerShell.

Write-Host "Starting full financial data engineering pipeline..." -ForegroundColor Cyan

if (-Not (Test-Path ".\.venv\Scripts\python.exe")) {
    Write-Host "Virtual environment not found. Creating .venv..." -ForegroundColor Yellow
    python -m venv .venv
    .\.venv\Scripts\python.exe -m pip install --upgrade pip
    .\.venv\Scripts\python.exe -m pip install -r requirements.txt
}

.\.venv\Scripts\python.exe src/orchestration/run_pipeline.py

Write-Host "Pipeline run completed." -ForegroundColor Green
