# Ensure Power BI module is imported
Import-Module MicrosoftPowerBIMgmt

# Log in to Power BI
Login-PowerBIServiceAccount

$accessToken = Get-PowerBIAccessToken -AsString
$groupId = "ca18f04c-c227-4364-a8f6-33689f71bb1a" # this is our production workspace id
$datasetsUrl = "https://api.powerbi.com/v1.0/myorg/groups/$groupId/datasets" # this url gets all of the datasets in the workspace
$requestUrl = "https://api.powerbi.com/v1.0/myorg/datasets/$datasetId/refreshes"

function Get-DatasetIds {

    $jsonResponse = Invoke-PowerBIRestMethod -Method Get -url $datasetsUrl

    # Convert JSON response to a PowerShell object
    $datasets = $jsonResponse | ConvertFrom-Json # pipe json variable into the covert from cmdlet

    # Extract dataset IDs
    $datasetIds = $datasets.value | ForEach-Object { $_.id } # pipe the json and take each id

    return $datasetIds
}

# Function to process each dataset ID (e.g., refresh dataset or perform another task)
function Process-Dataset {
    param (
        [string]$datasetId
    )

    # Example processing: Print the dataset ID or perform an action
    Write-Host "Processing dataset with ID: $datasetId"
    Invoke-PowerBIRestMethod -Url $requestUrl -Method Post
}

# Get all dataset IDs
$allDatasetIds = Get-DatasetIds

# Pipe each dataset ID into the Process-Dataset function
foreach ($datasetId in $allDatasetIds) {
    Process-Dataset -datasetId $datasetId
    Write-Host "Processed dataset -> $datasetId"
    Start-Sleep -Seconds 30
}
