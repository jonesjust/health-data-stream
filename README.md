# FHIR Data Stream App

A Python application for streaming synthetic FHIR resources from Azure Blob Storage to Azure Event Hubs.  
Designed for real-time healthcare analytics pipelines using FHIR data (e.g., Synthea-generated datasets).

## Features
- Reads `.ndjson` FHIR resources from Azure Blob Storage.
- Sends static resources (e.g., `Organization`, `Patient`) directly to Event Hubs.
- Streams dynamic resources (e.g., `Encounter`, `Condition`, `Observation`) **in chronological order** with a built-in delay.
- `test_fhir_stream.py` verifies data availability and processing logic **without sending to Event Hubs**.

## Requirements
- Python 3.9+
- Azure Blob Storage with `.ndjson` FHIR files
- Azure Event Hubs namespace and hubs created for each resource type
- `.env` file with credentials

## Install
```bash
git clone https://github.com/jonesjust/health-data-stream.git
cd health-data-stream
```
#### Create a virtual environment
- `python -m venv .venv`
- macOS/Linux `source .venv/bin/activate`
- Windows `source .venv\Scripts\activate`

#### Install dependencies
`pip install -r requirements.txt`


## Configure
Create a `.env` in the project root

#### Azure Blob Storage
- `STORAGE_ACCOUNT_NAME=your_storage_account`
- `STORAGE_ACCOUNT_KEY=your_storage_key`
- `CONTAINER_NAME=your_container`
- `FOLDER_PATH=path/to/fhir/files`

#### Azure Event Hubs
`EVENTHUB_CONNECTION_STRING=your_eventhub_namespace_connection_string`

## Run

#### Stream to Event Hubs
`python fhir_stream.py`
- Streams static resources first.
- Streams dynamic resources in chronological order (default ~5s delay per event).

#### Dry-run
`python test_fhir_stream.py`
- Lists available files.
- Prints resource previews, timestamps, and intended destinations.