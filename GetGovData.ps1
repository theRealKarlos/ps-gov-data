# GetGovData.ps1
#
# Downloads metadata for datasets from data.gov.uk and exports to CSV.
# Uses parallel execution in PowerShell 7+ for efficiency, with sequential fallback for older versions.
# Logs all API requests and errors for debugging and auditing.

# --- 1. Define API endpoints and output files ---
$baseurl = "https://data.gov.uk/api/action/"
$datasetListUrl = $baseurl + "package_list"
$datasetMetadataUrl = $baseurl + "package_show?id="
$csvFile = "DataGovUK_Datasets.csv"
$logFile = "DataGovUK_Log.txt"
$debugFile = "Debug_Log.txt"

# --- 2. Check PowerShell version for parallel support ---
if ($PSVersionTable.PSVersion.Major -lt 7) {
    Write-Warning "Parallel execution requires PowerShell 7 or higher. Running sequentially."
}

# --- 3. Fetch dataset list from data.gov.uk ---
try {
    $datasetIDs = (Invoke-RestMethod -Uri $datasetListUrl -TimeoutSec 30).result
}
catch {
    Write-Error "Failed to retrieve dataset list: $_"
    exit
}

# --- 4. Test Mode: Limit to a subset of datasets for testing ---
# Uncomment the following line to limit dataset retrieval to only a few datasets for testing
$datasetIDs = $datasetIDs[0..9]

# --- 5. Prepare thread-safe collections for results and logs ---
$datasetMetadata = [System.Collections.Concurrent.ConcurrentBag[object]]::new()
$logEntries = [System.Collections.Concurrent.ConcurrentBag[string]]::new()

# Clear log file at start to avoid mixing runs
Clear-Content -Path $logFile -ErrorAction SilentlyContinue

# --- 6. Log whether parallelism or sequential is used ---
if ($PSVersionTable.PSVersion.Major -ge 7) {
    $parallelModeMsg = "INFO: Using parallel processing (PowerShell $($PSVersionTable.PSVersion))"
} else {
    $parallelModeMsg = "INFO: Using sequential processing (PowerShell $($PSVersionTable.PSVersion))"
}
Write-Output $parallelModeMsg
$parallelModeMsg | Out-File -Append -FilePath $logFile

# --- 7. Helper: Retry logic for Invoke-RestMethod ---
function Retry-InvokeRestMethod {
    param(
        [string]$Uri,
        [int]$MaxRetries = 3,
        [int]$RetryDelay = 5,
        [ref]$Logs
    )
    $attempt = 0
    while ($attempt -lt $MaxRetries) {
        try {
            $Logs.Value += "Requesting URL: $Uri at $(Get-Date)"
            $response = Invoke-RestMethod -Uri $Uri -TimeoutSec 30
            $Logs.Value += "Fetched: $Uri at $(Get-Date)"
            return $response
        } catch {
            $attempt++
            $statusCode = $_.Exception.Response.StatusCode.Value__
            $errMessage = $_.Exception.Message
            $logEntry = "Error fetching {0} (HTTP {1}): {2} at {3}" -f $Uri, $statusCode, $errMessage, (Get-Date)
            $Logs.Value += $logEntry
            if ($attempt -lt $MaxRetries) {
                $Logs.Value += "Retrying $Uri (Attempt $attempt of $MaxRetries)..."
                Start-Sleep -Seconds $RetryDelay
            }
        }
    }
    return $null
}

# --- 8. Helper: Build metadata object from API result ---
function Get-MetadataObject {
    param($metadata)
    # --- Validate required fields before processing ---
    if ($null -eq $metadata) {
        return $null
    }
    $requiredFields = @('id', 'title', 'notes', 'license_title', 'organization', 'metadata_created', 'metadata_modified', 'resources')
    foreach ($field in $requiredFields) {
        if (-not $metadata.PSObject.Properties.Name -contains $field) {
            return $null
        }
    }
    if ($null -eq $metadata.organization -or -not $metadata.organization.PSObject.Properties.Name -contains 'title') {
        return $null
    }
    if ($null -eq $metadata.resources -or $metadata.resources.Count -eq 0) {
        return $null
    }
    # --- End validation ---
    $downloadURLs = @($metadata.resources | ForEach-Object { $_.url })
    $metadataObject = [PSCustomObject]@{
        ID           = $metadata.id
        Title        = $metadata.title
        Description  = ($metadata.notes -replace '<[^>]+>', '')  # Strip HTML
        License      = $metadata.license_title
        Organization = $metadata.organization.title
        Created      = $metadata.metadata_created
        Modified     = $metadata.metadata_modified
        Format       = ($metadata.resources | ForEach-Object { $_.format }) -join ", "
    }
    for ($i = 0; $i -lt $downloadURLs.Count; $i++) {
        $metadataObject | Add-Member -MemberType NoteProperty -Name "Download_URL_$($i+1)" -Value $downloadURLs[$i]
    }
    return $metadataObject
}

# --- 9. Helper: Process a single dataset (used in both parallel and sequential flows) ---
function Process-Dataset {
    param(
        [string]$datasetID,
        [string]$datasetMetadataUrl
    )
    $localLogs = @()
    $fullUrl = "$datasetMetadataUrl$datasetID"
    $response = Retry-InvokeRestMethod -Uri $fullUrl -Logs ([ref]$localLogs)
    if ($null -ne $response -and $response.PSObject.Properties.Name -contains 'result') {
        $metadataObject = Get-MetadataObject $response.result
        if ($null -eq $metadataObject) {
            $localLogs += "WARNING: Skipped dataset $datasetID due to missing or malformed required fields at $(Get-Date)"
        }
        return @{ Metadata = $metadataObject; Logs = $localLogs }
    } else {
        $localLogs += "WARNING: Skipped dataset $datasetID due to null or malformed API response at $(Get-Date)"
        return @{ Metadata = $null; Logs = $localLogs }
    }
}

# --- 10. Main processing: Parallel in PowerShell 7+, sequential fallback otherwise ---
if ($PSVersionTable.PSVersion.Major -ge 7) {
    # --- 10a. Parallel processing block ---
    $results = $datasetIDs | ForEach-Object -Parallel {
        $datasetID = $_
        # Import helper functions into parallel scope
        function Retry-InvokeRestMethod {
            param([string]$Uri, [int]$MaxRetries = 3, [int]$RetryDelay = 5, [ref]$Logs)
            $attempt = 0
            while ($attempt -lt $MaxRetries) {
                try {
                    $Logs.Value += "Requesting URL: $Uri at $(Get-Date)"
                    $response = Invoke-RestMethod -Uri $Uri -TimeoutSec 30
                    $Logs.Value += "Fetched: $Uri at $(Get-Date)"
                    return $response
                } catch {
                    $attempt++
                    $statusCode = $_.Exception.Response.StatusCode.Value__
                    $errMessage = $_.Exception.Message
                    $logEntry = "Error fetching {0} (HTTP {1}): {2} at {3}" -f $Uri, $statusCode, $errMessage, (Get-Date)
                    $Logs.Value += $logEntry
                    if ($attempt -lt $MaxRetries) {
                        $Logs.Value += "Retrying $Uri (Attempt $attempt of $MaxRetries)..."
                        Start-Sleep -Seconds $RetryDelay
                    }
                }
            }
            return $null
        }
        function Get-MetadataObject {
            param($metadata)
            # --- Validate required fields before processing ---
            if ($null -eq $metadata) {
                return $null
            }
            $requiredFields = @('id', 'title', 'notes', 'license_title', 'organization', 'metadata_created', 'metadata_modified', 'resources')
            foreach ($field in $requiredFields) {
                if (-not $metadata.PSObject.Properties.Name -contains $field) {
                    return $null
                }
            }
            if ($null -eq $metadata.organization -or -not $metadata.organization.PSObject.Properties.Name -contains 'title') {
                return $null
            }
            if ($null -eq $metadata.resources -or $metadata.resources.Count -eq 0) {
                return $null
            }
            # --- End validation ---
            $downloadURLs = @($metadata.resources | ForEach-Object { $_.url })
            $metadataObject = [PSCustomObject]@{
                ID           = $metadata.id
                Title        = $metadata.title
                Description  = ($metadata.notes -replace '<[^>]+>', '')
                License      = $metadata.license_title
                Organization = $metadata.organization.title
                Created      = $metadata.metadata_created
                Modified     = $metadata.metadata_modified
                Format       = ($metadata.resources | ForEach-Object { $_.format }) -join ", "
            }
            for ($i = 0; $i -lt $downloadURLs.Count; $i++) {
                $metadataObject | Add-Member -MemberType NoteProperty -Name "Download_URL_$($i+1)" -Value $downloadURLs[$i]
            }
            return $metadataObject
        }
        $localLogs = @()
        $fullUrl = "$using:datasetMetadataUrl$datasetID"
        $response = Retry-InvokeRestMethod -Uri $fullUrl -Logs ([ref]$localLogs)
        if ($null -ne $response -and $response.PSObject.Properties.Name -contains 'result') {
            $metadataObject = Get-MetadataObject $response.result
            if ($null -eq $metadataObject) {
                $localLogs += "WARNING: Skipped dataset $datasetID due to missing or malformed required fields at $(Get-Date)"
            }
            return @{ Metadata = $metadataObject; Logs = $localLogs }
        } else {
            $localLogs += "WARNING: Skipped dataset $datasetID due to null or malformed API response at $(Get-Date)"
            return @{ Metadata = $null; Logs = $localLogs }
        }
    } -ThrottleLimit 5
    # --- 10b. Collect results and logs from parallel jobs ---
    foreach ($result in $results) {
        if ($null -ne $result.Metadata) {
            $datasetMetadata.Add($result.Metadata)
        }
        foreach ($log in $result.Logs) {
            $logEntries.Add($log)
        }
    }
} else {
    # --- 10c. Sequential fallback for PowerShell <7 ---
    foreach ($datasetID in $datasetIDs) {
        $result = Process-Dataset -datasetID $datasetID -datasetMetadataUrl $datasetMetadataUrl
        if ($null -ne $result) {
            $datasetMetadata.Add($result.Metadata)
            foreach ($log in $result.Logs) {
                $logEntries.Add($log)
            }
        }
    }
}

# --- 11. Write logs to file ---
"Starting dataset retrieval at $(Get-Date)" | Out-File -FilePath $logFile -Append
$logEntries | Out-File -Append -FilePath $logFile

# --- 12. Debugging: Write all collected metadata to a debug log ---
$datasetMetadata | Out-File -FilePath $debugFile

# --- 13. Export results to CSV if any valid metadata was collected ---
if ($datasetMetadata.Count -gt 0) {
    $datasetMetadata | Where-Object { $_ -ne $null } | Export-Csv -Path $csvFile -NoTypeInformation
    Write-Output "Dataset metadata saved to $csvFile"
} else {
    Write-Output "No valid dataset metadata found, skipping CSV export."
}