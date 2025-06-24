# Ensure Power BI module is imported
Import-Module MicrosoftPowerBIMgmt

# Log in to Power BI
Login-PowerBIServiceAccount

# Get the access token
$accessToken = Get-PowerBIAccessToken -AsString

# Define workspace (group) ID
$groupId = "ca18f04c-c227-4364-a8f6-33689f71bb1a"

# Function to get dataset IDs from a JSON response
function Get-DatasetIds {
    $datasetsUrl = "https://api.powerbi.com/v1.0/myorg/groups/$groupId/datasets"
    $jsonResponse = Invoke-PowerBIRestMethod -Method Get -Url $datasetsUrl

    # Convert JSON response to a PowerShell object
    $datasets = $jsonResponse | ConvertFrom-Json 

    # Extract dataset IDs
    $datasetIds = $datasets.value | ForEach-Object { $_.id }

    return $datasetIds
}

# Function to refresh dataset
function Process-Dataset {
    param (
        [string]$datasetId
    )

    # Generate correct refresh URL for the dataset
    $requestUrl = "https://api.powerbi.com/v1.0/myorg/datasets/$datasetId/refreshes"

    # Refresh dataset
    Write-Host "Triggering refresh for dataset ID: $datasetId"
    Invoke-PowerBIRestMethod -Url $requestUrl -Method Post
}

# Get all dataset IDs
$allDatasetIds = Get-DatasetIds

# Process each dataset (refresh)
foreach ($datasetId in $allDatasetIds) {
    Process-Dataset -datasetId $datasetId
    Write-Host "Refresh triggered for dataset -> $datasetId"
    Start-Sleep -Seconds 30  # Avoid API rate limiting
}