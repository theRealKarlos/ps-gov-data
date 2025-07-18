# GetGovData.Functions.psm1
#
# This module contains all reusable function definitions for the GetGovData project.
#
# Purpose:
#   - Provide robust, production-grade, and testable PowerShell functions for dataset metadata retrieval and processing from data.gov.uk.
#   - Enable reliable unit testing and mocking with Pester via InModuleScope.
#   - Use only approved PowerShell verbs for discoverability and compliance.
#
# Usage:
#   Import this module in your script or test with:
#       Import-Module ./GetGovData.Functions.psm1
#   All exported functions will be available for use and for mocking in Pester.
#
# Best Practices:
#   - All functions are exported explicitly via Export-ModuleMember.
#   - All functions use comment-based help for discoverability.
#   - All functions are designed for reusability and testability.

<#
.SYNOPSIS
Retries an HTTP REST request with backoff and logs all attempts.
.DESCRIPTION
Invokes Invoke-RestMethod with retry logic, logging each attempt and error. Returns the response or $null on repeated failure.
.PARAMETER Uri
The URI to request.
.PARAMETER MaxRetries
Maximum number of attempts (default: 3).
.PARAMETER RetryDelay
Delay in seconds between retries (default: 5).
.PARAMETER Logs
[ref] Array to collect log messages.
.EXAMPLE
$response = Invoke-RestMethodWithRetry -Uri $url -Logs ([ref]$logs)
#>
function Invoke-RestMethodWithRetry {
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

<#
.SYNOPSIS
Converts a raw metadata object from the API into a validated, flattened PowerShell object for CSV export.
.DESCRIPTION
Validates required fields and flattens nested properties. Returns $null if required fields are missing or malformed.
.PARAMETER metadata
The raw metadata object from the API.
.EXAMPLE
$metaObj = ConvertTo-MetadataObject $apiResult
#>
function ConvertTo-MetadataObject {
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

<#
.SYNOPSIS
Processes a single dataset ID: fetches metadata, validates, and logs.
.DESCRIPTION
Fetches metadata for a dataset ID, applies validation and flattening, and returns both the processed object and logs.
.PARAMETER datasetID
The dataset ID to process.
.PARAMETER datasetMetadataUrl
The base URL for metadata requests.
.EXAMPLE
$result = Invoke-DatasetProcessing -datasetID $id -datasetMetadataUrl $url
#>
function Invoke-DatasetProcessing {
    param(
        [string]$datasetID,
        [string]$datasetMetadataUrl
    )
    $localLogs = @()
    $fullUrl = "$datasetMetadataUrl$datasetID"
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
}

Export-ModuleMember -Function Invoke-RestMethodWithRetry, ConvertTo-MetadataObject, Invoke-DatasetProcessing