$OutputEncoding = [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$env:PYTHONIOENCODING = "utf-8"

# Alleen PowerShell 7+
$PSNativeCommandUseErrorActionPreference = $false

$stages = @(
    "aa_en_maas",
    "brabantse_delta",
    "de_dommel",
    "limburg",
    "rijn_en_ijssel",
    "vallei_en_veluwe",
    "vechtstromen",
    "drents_overijsselse_delta",
    "stichtse_rijnlanden",
    "hunze_en_aas",
    "noorderzijlvest"
)

$logfile = "dvc_run_vrij-afwaterend.log"

if (Test-Path $logfile) {
    Remove-Item $logfile
}

"Start run: $(Get-Date)" | Tee-Object -FilePath $logfile
$results = @()

foreach ($stage in $stages) {
    $header = "=== Running stage: $stage ==="
    Write-Host "`n$header" -ForegroundColor Cyan
    $header | Tee-Object -FilePath $logfile -Append | Out-Null

    & uv run dvc repro -k -f $stage *>&1 | Tee-Object -FilePath $logfile -Append
    $exitCode = $LASTEXITCODE

    if ($exitCode -eq 0) {
        $line = "SUCCESS: $stage"
        Write-Host $line -ForegroundColor Green
        $line | Tee-Object -FilePath $logfile -Append | Out-Null
        $results += [PSCustomObject]@{
            Stage = $stage
            Status = "SUCCESS"
            ExitCode = $exitCode
        }
    }
    else {
        $line = "FAILED: $stage (exit code $exitCode)"
        Write-Host $line -ForegroundColor Red
        $line | Tee-Object -FilePath $logfile -Append | Out-Null
        $results += [PSCustomObject]@{
            Stage = $stage
            Status = "FAILED"
            ExitCode = $exitCode
        }
    }
}

"`n=== SUMMARY ===" | Tee-Object -FilePath $logfile -Append

foreach ($r in $results) {
    $line = "$($r.Stage): $($r.Status) (exit code $($r.ExitCode))"
    if ($r.Status -eq "SUCCESS") {
        Write-Host $line -ForegroundColor Green
    }
    else {
        Write-Host $line -ForegroundColor Red
    }
    $line | Tee-Object -FilePath $logfile -Append | Out-Null
}

"End run: $(Get-Date)" | Tee-Object -FilePath $logfile -Append
