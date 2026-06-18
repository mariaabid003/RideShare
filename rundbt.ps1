# Loads warehouse\.env, then runs dbt with the right project/profiles dirs.
# Usage from the project root:
#     .\rundbt.ps1 seed
#     .\rundbt.ps1 run
#     .\rundbt.ps1 test
#     .\rundbt.ps1 build      # seed + run + test in one go
param([Parameter(ValueFromRemainingArguments = $true)] $DbtArgs)

$envFile = Join-Path $PSScriptRoot "warehouse\.env"
if (-not (Test-Path $envFile)) {
    Write-Error "No .env found at $envFile. Copy warehouse\.env.example to warehouse\.env and fill it in."
    exit 1
}

# Parse each KEY=VALUE line (split on the FIRST '=' only; ignore blanks/comments)
$loaded = 0
Get-Content $envFile | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith('#') -and $line.Contains('=')) {
        $idx  = $line.IndexOf('=')
        $name = $line.Substring(0, $idx).Trim()
        $val  = $line.Substring($idx + 1).Trim()
        Set-Item -Path "Env:$name" -Value $val
        $loaded++
    }
}
Write-Host "Loaded $loaded vars from .env (account: $env:SNOWFLAKE_ACCOUNT)"

dbt @DbtArgs --project-dir (Join-Path $PSScriptRoot "dbt") --profiles-dir (Join-Path $PSScriptRoot "dbt")
