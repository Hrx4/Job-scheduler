# End-to-end check of the full path: API -> Postgres -> scheduler -> Kafka -> workers -> Postgres.
# Needs the stack running:  docker compose --profile apps up -d --build
# Usage:                    powershell -File scripts/smoke-test.ps1 [-Count 100]
param(
    [string]$BaseUrl = "http://localhost:8080",
    [string]$ApiKey = "dev-key",
    [int]$Count = 100,
    [int]$TimeoutSeconds = 90
)
$ErrorActionPreference = "Stop"
$headers = @{ "X-API-Key" = $ApiKey }

function Submit-Job([string]$json) {
    (Invoke-RestMethod -Method Post -Uri "$BaseUrl/api/v1/jobs" -Headers $headers `
        -ContentType "application/json" -Body $json).jobId
}

Write-Host "Submitting $Count jobs + 1 job that fails once..."
$ids = 1..$Count | ForEach-Object { Submit-Job '{"type":"GENERATE_REPORT","payload":{"durationMs":200}}' }
$retryId = Submit-Job '{"type":"GENERATE_REPORT","payload":{"durationMs":50,"failUntilAttempt":1},"maxRetries":2}'
$all = @($ids) + $retryId

$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
do {
    Start-Sleep -Seconds 2
    $statuses = $all | ForEach-Object {
        (Invoke-RestMethod -Uri "$BaseUrl/api/v1/jobs/$_" -Headers $headers).latestRun.status
    }
    $done = @($statuses | Where-Object { $_ -eq "SUCCEEDED" }).Count
    Write-Host "  $done / $($all.Count) succeeded"
} while ($done -lt $all.Count -and (Get-Date) -lt $deadline)

# Which worker ran each attempt
$attempts = $all | ForEach-Object {
    (Invoke-RestMethod -Uri "$BaseUrl/api/v1/jobs/$_/runs" -Headers $headers)[0].attempts
}
Write-Host "`nAttempts per worker:"
$attempts | Group-Object workerId | Sort-Object Name | ForEach-Object { "  {0,-40} {1}" -f $_.Name, $_.Count }

$retryRun = (Invoke-RestMethod -Uri "$BaseUrl/api/v1/jobs/$retryId/runs" -Headers $headers)[0]
Write-Host "`nRetry job: status=$($retryRun.status) attempts=$($retryRun.attemptCount)"
$retryRun.attempts | ForEach-Object { "  attempt $($_.attemptNo): $($_.outcome) on $($_.workerId)" }

$extra = @($attempts).Count - $all.Count - 1   # the retry job has 2 attempts by design
Write-Host ""
if ($done -eq $all.Count -and $extra -eq 0 -and $retryRun.attemptCount -eq 2) {
    Write-Host "PASS: all $($all.Count) jobs succeeded, no duplicate attempts, retry went through the scheduler"
    exit 0
}
Write-Host "FAIL: succeeded=$done/$($all.Count), unexpected extra attempts=$extra, retry attempts=$($retryRun.attemptCount)"
exit 1
