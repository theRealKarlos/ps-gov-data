# Define API endpoints
$datasetListUrl = "https://data.gov.uk/api/action/package_list"
$datasetMetadataUrl = "https://data.gov.uk/api/action/package_show?id="

# Fetch dataset list
try {
    $datasetIDs = (Invoke-RestMethod -Uri $datasetListUrl -TimeoutSec 30).result
}
catch {
    Write-Error "Failed to retrieve dataset list: $_"
    exit
}

# ---- Test Mode ----
# Uncomment the following line to limit dataset retrieval to only a few datasets for testing
#$datasetIDs = $datasetIDs[0..20]

# Set up CSV output
$csvFile = "DataGovUK_Datasets.csv"
$logFile = "DataGovUK_Log.txt"
$datasetMetadata = @()

# Debugging log to check API response
"Starting dataset retrieval at $(Get-Date)" | Out-File -Append -FilePath $logFile

# Function to fetch dataset metadata (without parallel processing)
function Get-DatasetMetadata {
    param ([string]$datasetID, [string]$datasetMetadataUrl)

    $maxRetries = 3
    $retryDelay = 5  # Seconds
    $attempt = 0
    $success = $false
    $metadata = $null

    while ($attempt -lt $maxRetries -and -not $success) {
        try {
            # Construct full request URL
            $fullUrl = "$datasetMetadataUrl$datasetID"

            # Log full request URL
            "Requesting URL: $fullUrl at $(Get-Date)" | Out-File -Append -FilePath $logFile

            # Make API call
            $response = Invoke-RestMethod -Uri $fullUrl -TimeoutSec 30
            $metadata = $response.result

            # Log successful retrieval
            "Fetched: $datasetID at $(Get-Date)" | Out-File -Append -FilePath $logFile
            $success = $true
        }
        catch {
            $attempt++
            $statusCode = $_.Exception.Response.StatusCode.Value__
            $errMessage = $_.Exception.Message

            # Log structured error with full request URL
            $logEntry = "Error fetching {0} (HTTP {1}): {2} at {3}" -f $fullUrl, $statusCode, $errMessage, (Get-Date)
            $logEntry | Out-File -Append -FilePath $logFile

            # Apply backoff delay before retrying
            if ($attempt -lt $maxRetries) {
                Write-Output "Retrying $datasetID (Attempt $attempt of $maxRetries)..."
                Start-Sleep -Seconds $retryDelay
            }
        }
    }

    if ($success) {
        # Extract and format download URLs into separate columns
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

        # Add URLs as separate columns dynamically
        for ($i = 0; $i -lt $downloadURLs.Count; $i++) {
            $metadataObject | Add-Member -MemberType NoteProperty -Name "Download_URL_$($i+1)" -Value $downloadURLs[$i]
        }

        return $metadataObject
    }
    else {
        return $null
    }
}

# Process dataset metadata without parallel execution
foreach ($datasetID in $datasetIDs) {
    $result = Get-DatasetMetadata -datasetID $datasetID -datasetMetadataUrl $datasetMetadataUrl
    if ($null -ne $result) {
        $datasetMetadata += $result
    }
}

# Debugging step before writing to CSV
$datasetMetadata | Out-File -FilePath "Debug_Log.txt"

# Ensure metadata is valid before exporting to CSV
if ($datasetMetadata.Count -gt 0) {
    $datasetMetadata | Where-Object { $_ -ne $null } | Export-Csv -Path $csvFile -NoTypeInformation
    Write-Output "Dataset metadata saved to $csvFile"
}
else {
    Write-Output "No valid dataset metadata found, skipping CSV export."
}