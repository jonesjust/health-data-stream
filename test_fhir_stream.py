from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import json
import os
import time
from typing import Optional, Dict, List, Any, Tuple
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class FHIRStreamer:
    def __init__(self):
        # Azure credentials with type assertions
        self.storage_account_name: str = os.getenv('STORAGE_ACCOUNT_NAME', '')
        self.storage_account_key: str = os.getenv('STORAGE_ACCOUNT_KEY', '')
        self.container_name: str = os.getenv('CONTAINER_NAME', '')
        self.folder_path: str = os.getenv('FOLDER_PATH', '')

        # validate required environment variables
        if not all([self.storage_account_name, self.storage_account_key,
                   self.container_name, self.folder_path]):
            raise ValueError("Missing required environment variables")

        # configure which resources are static vs dynamic
        self.static_resources: Dict[str, str] = {
            'Organization': 'fhir-organization',
            'Location': 'fhir-location',
            'Practitioner': 'fhir-practitioner',
            'PractitionerRole': 'fhir-practitioner-role',
            'Patient': 'fhir-patient'
        }

        self.dynamic_resources: Dict[str, Tuple[str, str]] = {
            'Encounter': ('fhir-encounter', 'period.start'),
            'Condition': ('fhir-condition', 'onsetDateTime'),
            'Observation': ('fhir-observation', 'effectiveDateTime')
        }

        self.blob_service_client = self.get_blob_service_client()

    # create and return a blob service client
    def get_blob_service_client(self) -> BlobServiceClient:
        connect_str = f"DefaultEndpointsProtocol=https;AccountName={self.storage_account_name};AccountKey={self.storage_account_key};EndpointSuffix=core.windows.net"
        return BlobServiceClient.from_connection_string(connect_str)

    # convert timestamp string to datetime object
    def parse_timestamp(self, timestamp_str: str) -> datetime:
        try:
            # Remove timezone suffix if present and parse
            clean_timestamp = timestamp_str.replace('Z', '+00:00')
            return datetime.fromisoformat(clean_timestamp)
        except (ValueError, AttributeError) as e:
            print(f"Error parsing timestamp {timestamp_str}: {e}")
            # Return a far-future date if parsing fails
            return datetime.max

    # extract timestamp from a resource using dot notation path and convert to datetime
    def extract_timestamp(self, resource: Dict[str, Any], timestamp_path: str) -> Optional[datetime]:
        timestamp_str = None
        if '.' in timestamp_path:
            current: Any = resource
            for key in timestamp_path.split('.'):
                if isinstance(current, dict):
                    current = current.get(key, {})
                else:
                    return None
            timestamp_str = current if current else None
        else:
            timestamp_str = resource.get(timestamp_path)

        if timestamp_str:
            return self.parse_timestamp(timestamp_str)
        return None

    # print all available files in the container/folder
    def print_available_files(self) -> None:
        container_client = self.blob_service_client.get_container_client(self.container_name)
        all_blobs = list(container_client.list_blobs(name_starts_with=self.folder_path))

        print("\nAvailable files in container:")
        print("="*80)
        for blob in all_blobs:
            print(f"- {blob.name}")
        print("="*80 + "\n")

    # print resource details to console
    def print_resource(self, resource: Dict[str, Any], destination: str) -> None:
        resource_type = resource.get('resourceType', 'Unknown')
        resource_id = resource.get('id', 'Unknown')
        print("\n" + "="*80)
        print(f"Resource Type: {resource_type}")
        print(f"Resource ID: {resource_id}")
        print(f"Would be sent to: {destination}")

        # extract and print timestamp if it's a dynamic resource
        if resource_type in dict(self.dynamic_resources):
            timestamp_path = self.dynamic_resources[resource_type][1]
            timestamp = self.extract_timestamp(resource, timestamp_path)
            if timestamp:
                print(f"Timestamp: {timestamp.isoformat()}")

        print("\nResource Content Preview:")
        print(json.dumps(resource, indent=2)[:500] + "...")
        print("="*80 + "\n")

    # process and print static reference resources
    def process_static_resources(self) -> None:
        container_client = self.blob_service_client.get_container_client(self.container_name)
        all_blobs = list(container_client.list_blobs(name_starts_with=self.folder_path))

        for resource_type, hub_name in self.static_resources.items():
            try:
                # find matching files for this resource type
                matching_blobs = []
                for blob in all_blobs:
                    filename = blob.name.split('/')[-1]
                    if filename.startswith(f"{resource_type}.") and filename.endswith('.ndjson'):
                        matching_blobs.append(blob)

                if not matching_blobs:
                    print(f"\nWarning: No file found for {resource_type}")
                    continue

                # use the first matching file
                blob = matching_blobs[0]
                print(f"\nProcessing {blob.name} for {resource_type}")

                # process the file
                blob_client = container_client.get_blob_client(blob.name)
                stream = blob_client.download_blob()
                content = stream.readall().decode('utf-8')
                lines = [line for line in content.splitlines() if line.strip()]
                print(f"Found {len(lines)} {resource_type} resources")

                for line in lines:
                    resource = json.loads(line)
                    self.print_resource(resource, hub_name)
                    time.sleep(1)

            except Exception as e:
                print(f"\nError processing {resource_type}: {str(e)}")

    # process and print dynamic resources in chronological order
    def process_dynamic_resources(self) -> None:
        container_client = self.blob_service_client.get_container_client(self.container_name)
        all_blobs = list(container_client.list_blobs(name_starts_with=self.folder_path))

        # list to store all resources with their timestamps
        all_resources: List[Tuple[datetime, str, Dict[str, Any]]] = []

        print("\nCollecting dynamic resources...")
        for resource_type, (hub_name, timestamp_path) in self.dynamic_resources.items():
            try:
                # find matching files for this resource type
                matching_blobs = []
                for blob in all_blobs:
                    filename = blob.name.split('/')[-1]
                    if filename.startswith(f"{resource_type}.") and filename.endswith('.ndjson'):
                        matching_blobs.append(blob)

                if not matching_blobs:
                    print(f"\nWarning: No file found for {resource_type}")
                    continue

                blob = matching_blobs[0]
                print(f"\nReading {blob.name} for {resource_type}")

                # process the file
                blob_client = container_client.get_blob_client(blob.name)
                stream = blob_client.download_blob()
                content = stream.readall().decode('utf-8')
                lines = [line for line in content.splitlines() if line.strip()]
                print(f"Found {len(lines)} {resource_type} resources")

                # process all resources from this file
                for line in lines:
                    try:
                        resource = json.loads(line)
                        timestamp = self.extract_timestamp(resource, timestamp_path)
                        if timestamp:
                            all_resources.append((timestamp, resource_type, resource))
                    except json.JSONDecodeError as e:
                        print(f"Error parsing JSON: {e}")
                    except Exception as e:
                        print(f"Error processing resource: {e}")

            except Exception as e:
                print(f"\nError processing {resource_type}: {str(e)}")

        # sort all resources by timestamp
        print("\nSorting resources chronologically...")
        all_resources.sort(key=lambda x: x[0])
        total_resources = len(all_resources)
        print(f"Total resources to process: {total_resources}")

        # process resources in chronological order
        print("\nProcessing resources in chronological order...")
        for i, (timestamp, resource_type, resource) in enumerate(all_resources, 1):
            hub_name = self.dynamic_resources[resource_type][0]
            print(f"\nProcessing resource {i} of {total_resources}")
            print(f"Timestamp: {timestamp.isoformat()}")
            self.print_resource(resource, hub_name)
            time.sleep(2)

    # main method to process all resources
    def stream_all_resources(self) -> None:
        try:
            print("\nStarting FHIR resource processing...")

            print("\nChecking available files...")
            self.print_available_files()

            print("\nProcessing static resources...")
            self.process_static_resources()

            print("\nProcessing dynamic resources...")
            self.process_dynamic_resources()

            print("\nProcessing completed!")

        except Exception as e:
            print(f"\nError in main processing: {str(e)}")

if __name__ == '__main__':
    streamer = FHIRStreamer()
    streamer.stream_all_resources()
