# Chaos test for crash recovery (ADR-009): start a long job, SIGKILL the worker running it,
# and measure how long until another worker starts attempt 2.
# Needs the stack running:  docker compose --profile apps up -d --build
# Usage:                    powershell -File scripts/chaos-kill-worker.ps1 [-Trials 3]
param(
    [string]$BaseUrl = "http://localhost:8080",
    [string]$ApiKey = "dev-key",
    [int]$Trials = 3,
    [int]$WaitSeconds = 150
)
# Not "Stop": docker compose writes progress to stderr, which PowerShell 5 would treat as an error.
$ErrorActionPreference = "Continue"
$headers = @{ "X-API-Key" = $ApiKey }

function Get-FirstRun([string]$jobId) {
    (Invoke-RestMethod -Uri "$BaseUrl/api/v1/jobs/$jobId/runs" -Headers $headers)[0]
}
function Get-DbNow {
    # Database clock, so kill time and attempt timestamps come from the same clock.
    $ms = (docker compose exec -T postgres psql -U jobs -d jobs -tAc "SELECT (extract(epoch FROM now()) * 1000)::bigint").Trim()
    [DateTimeOffset]::FromUnixTimeMilliseconds([long]$ms)
}

$results = @()
for ($t = 1; $t -le $Trials; $t++) {
    Write-Host "`n=== Trial $t / $Trials ==="
    docker compose --profile apps up -d --scale worker=2 --no-recreate 2>$null | Out-Null
    Start-Sleep -Seconds 20   # let the consumer group settle after the previous kill

    $jobId = (Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/jobs" -Headers $headers -ContentType "application/json" `
        -Body '{"type":"GENERATE_REPORT","payload":{"durationMs":60000},"timeoutSeconds":120,"maxRetries":3}').jobId

    do { Start-Sleep -Milliseconds 300; $run = Get-FirstRun $jobId } while ($run.status -ne "RUNNING")
    $owner = $run.leaseOwner
    $container = $owner.Split("-")[0]           # worker id = <container hostname>-<random>
    Write-Host "Job $jobId running on $owner; killing container $container"

    docker kill $container | Out-Null
    $killedAt = Get-DbNow

    $deadline = (Get-Date).AddSeconds($WaitSeconds)
    do { Start-Sleep -Milliseconds 500; $run = Get-FirstRun $jobId } while (@($run.attempts).Count -lt 2 -and (Get-Date) -lt $deadline)

    if (@($run.attempts).Count -lt 2) {
        Write-Host "No second attempt within $WaitSeconds s"
        $results += [pscustomobject]@{ Trial = $t; Recovered = $false }
    } else {
        $a1 = $run.attempts[0]; $a2 = $run.attempts[1]
        $reapedAt = [DateTimeOffset]::Parse($a1.finishedAt)
        $restartedAt = [DateTimeOffset]::Parse($a2.startedAt)
        $results += [pscustomobject]@{
            Trial          = $t
            Recovered      = $true
            KillToReap_s   = [math]::Round(($reapedAt - $killedAt).TotalSeconds, 1)
            ReapToRerun_s  = [math]::Round(($restartedAt - $reapedAt).TotalSeconds, 1)
            Total_s        = [math]::Round(($restartedAt - $killedAt).TotalSeconds, 1)
            Attempt1       = $a1.outcome
            RerunOn        = $a2.workerId
        }
    }
    Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/jobs/$jobId/cancel" -Headers $headers | Out-Null
}

docker compose --profile apps up -d --scale worker=2 --no-recreate 2>$null | Out-Null
Write-Host "`nResults (seconds, database clock; kill time accurate to ~1 s):"
$results | Format-Table -AutoSize
