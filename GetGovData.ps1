# GetGovData.ps1
#
# Downloads metadata for datasets from data.gov.uk and exports to CSV.
# Uses parallel execution in PowerShell 7+ for efficiency, with sequential fallback for older versions.
# Logs all API requests and errors for debugging and auditing.
#
# Functions are imported from the GetGovData.Functions.psm1 module for testability and to enable Pester's InModuleScope for mocking.
# All function names use approved PowerShell verbs for discoverability and compliance.

Import-Module "$PSScriptRoot/GetGovData.Functions.psm1"

if ($MyInvocation.InvocationName -ne '.') {
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
        $errMsg = "FATAL: Failed to retrieve dataset list: $_"
        Write-Error $errMsg
        $errMsg | Out-File -Append -FilePath $logFile
        Write-Output "EXIT CODE: 1 (fatal error fetching dataset list)"
        exit 1
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

    # --- 7. Main processing: Parallel in PowerShell 7+, sequential fallback otherwise ---
    if ($PSVersionTable.PSVersion.Major -ge 7) {
        # --- 7a. Parallel processing block ---
        $results = $datasetIDs | ForEach-Object -Parallel {
            $datasetID = $_
            # Import the module in each parallel job for function availability
            Import-Module "$using:PSScriptRoot/GetGovData.Functions.psm1"
            $localLogs = @()
            $fullUrl = "$using:datasetMetadataUrl$datasetID"
            $response = Invoke-RestMethodWithRetry -Uri $fullUrl -Logs ([ref]$localLogs)
            if ($null -ne $response -and $response.PSObject.Properties.Name -contains 'result') {
                $metadataObject = ConvertTo-MetadataObject $response.result
                if ($null -eq $metadataObject) {
                    $localLogs += "WARNING: Skipped dataset $datasetID due to missing or malformed required fields at $(Get-Date)"
                }
                return @{ Metadata = $metadataObject; Logs = $localLogs }
            } else {
                $localLogs += "WARNING: Skipped dataset $datasetID due to null or malformed API response at $(Get-Date)"
                return @{ Metadata = $null; Logs = $localLogs }
            }
        } -ThrottleLimit 5
        # --- 7b. Collect results and logs from parallel jobs ---
        foreach ($result in $results) {
            if ($null -ne $result.Metadata) {
                $datasetMetadata.Add($result.Metadata)
            }
            foreach ($log in $result.Logs) {
                $logEntries.Add($log)
            }
        }
    } else {
        # --- 7c. Sequential fallback for PowerShell <7 ---
        foreach ($datasetID in $datasetIDs) {
            $result = Invoke-DatasetProcessing -datasetID $datasetID -datasetMetadataUrl $datasetMetadataUrl
            if ($null -ne $result) {
                $datasetMetadata.Add($result.Metadata)
                foreach ($log in $result.Logs) {
                    $logEntries.Add($log)
                }
            }
        }
    }

    # --- 8. Write logs to file ---
    "Starting dataset retrieval at $(Get-Date)" | Out-File -FilePath $logFile -Append
    $logEntries | Out-File -Append -FilePath $logFile

    # --- 9. Debugging: Write all collected metadata to a debug log ---
    $datasetMetadata | Out-File -FilePath $debugFile

    # --- 10. Export results to CSV if any valid metadata was collected ---
    if ($datasetMetadata.Count -gt 0) {
        $datasetMetadata | Where-Object { $_ -ne $null } | Export-Csv -Path $csvFile -NoTypeInformation
        Write-Output "Dataset metadata saved to $csvFile"
        Write-Output "EXIT CODE: 0 (success)"
        exit 0
    } else {
        $errMsg = "No valid dataset metadata found, skipping CSV export."
        Write-Output $errMsg
        $errMsg | Out-File -Append -FilePath $logFile
        Write-Output "EXIT CODE: 2 (no valid metadata)"
        exit 2
    }
}