param(
    [string]$ConnectUrl = "http://localhost:8083"
)

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$envFile = Join-Path $projectRoot ".env"
$configFile = Join-Path $PSScriptRoot "sqlserver-connector.json"

if (-not (Test-Path -LiteralPath $envFile)) {
    throw "Missing $envFile. Copy .env.example to .env and set DB_PASSWORD."
}

$envValues = @{}
foreach ($line in Get-Content -LiteralPath $envFile) {
    if ($line -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$') {
        $envValues[$Matches[1]] = $Matches[2].Trim().Trim('"').Trim("'")
    }
}
$dbPassword = $envValues['DB_PASSWORD']
$dbUser = $envValues['DB_USER']
if ([string]::IsNullOrWhiteSpace($dbPassword)) {
    throw "DB_PASSWORD is missing from .env."
}
if ([string]::IsNullOrWhiteSpace($dbUser)) {
    $dbUser = "sa"
}

$document = Get-Content -Raw -LiteralPath $configFile | ConvertFrom-Json
$document.config.'database.password' = $dbPassword
$document.config.'database.user' = $dbUser
$connectorName = $document.name

# Wait for Kafka Connect REST API. SQL Server and sql-init are ordered by Compose.
$deadline = (Get-Date).AddMinutes(3)
do {
    try {
        $null = Invoke-RestMethod -Method Get -Uri "$ConnectUrl/connectors" -TimeoutSec 5
        break
    }
    catch {
        if ((Get-Date) -ge $deadline) { throw "Kafka Connect did not become ready at $ConnectUrl." }
        Start-Sleep -Seconds 3
    }
} while ($true)

$existing = Invoke-RestMethod -Method Get -Uri "$ConnectUrl/connectors" -TimeoutSec 10
if ($existing -contains $connectorName) {
    $body = $document.config | ConvertTo-Json -Depth 20
    Invoke-RestMethod -Method Put -Uri "$ConnectUrl/connectors/$connectorName/config" `
        -ContentType "application/json" -Body $body
    Write-Host "Updated connector '$connectorName'."
}
else {
    $body = $document | ConvertTo-Json -Depth 20
    Invoke-RestMethod -Method Post -Uri "$ConnectUrl/connectors" `
        -ContentType "application/json" -Body $body
    Write-Host "Registered connector '$connectorName'."
}
