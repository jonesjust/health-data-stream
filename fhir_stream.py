from azure.storage.blob import BlobServiceClient
from azure.eventhub import EventHubProducerClient, EventData
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
        self.eventhub_connection_string: str = os.getenv('EVENTHUB_CONNECTION_STRING', '')

        # validate required environment variables
        if not all([
            self.storage_account_name,
            self.storage_account_key,
            self.container_name,
            self.folder_path,
            self.eventhub_connection_string
        ]):
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
        self.producer_clients: Dict[str, EventHubProducerClient] = {}

    # create and return a blob service client
    def get_blob_service_client(self) -> BlobServiceClient:
        connect_str = f"DefaultEndpointsProtocol=https;AccountName={self.storage_account_name};AccountKey={self.storage_account_key};EndpointSuffix=core.windows.net"
        return BlobServiceClient.from_connection_string(connect_str)

    # get or create an Event Hub producer client
    def get_producer_client(self, hub_name: str) -> EventHubProducerClient:
        if hub_name not in self.producer_clients:
            self.producer_clients[hub_name] = EventHubProducerClient.from_connection_string(
                conn_str=self.eventhub_connection_string,
                eventhub_name=hub_name
            )
        return self.producer_clients[hub_name]

    # convert timestamp string to datetime object
    def parse_timestamp(self, timestamp_str: str) -> datetime:
        try:
            clean_timestamp = timestamp_str.replace('Z', '+00:00')
            return datetime.fromisoformat(clean_timestamp)
        except (ValueError, AttributeError) as e:
            logger.error(f"Error parsing timestamp {timestamp_str}: {e}")
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

    # send a resource to Event Hub
    def send_to_eventhub(self, resource: Dict[str, Any], hub_name: str) -> None:
        try:
            producer = self.get_producer_client(hub_name)
            event_data_batch = producer.create_batch()
            event_data = EventData(json.dumps(resource))
            event_data_batch.add(event_data)
            producer.send_batch(event_data_batch)
            logger.info(f"Sent {resource.get('resourceType')} {resource.get('id')} to {hub_name}")
        except Exception as e:
            logger.error(f"Error sending to Event Hub {hub_name}: {e}")

    # process and send static reference resources
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
                    logger.warning(f"No file found for {resource_type}")
                    continue

                blob = matching_blobs[0]
                logger.info(f"Processing {blob.name} for {resource_type}")

                blob_client = container_client.get_blob_client(blob.name)
                stream = blob_client.download_blob()
                content = stream.readall().decode('utf-8')
                lines = [line for line in content.splitlines() if line.strip()]
                logger.info(f"Found {len(lines)} {resource_type} resources")

                for line in lines:
                    resource = json.loads(line)
                    self.send_to_eventhub(resource, hub_name)

            except Exception as e:
                logger.error(f"Error processing {resource_type}: {str(e)}")

    # process and send dynamic resources in chronological order
    def process_dynamic_resources(self) -> None:
        container_client = self.blob_service_client.get_container_client(self.container_name)
        all_blobs = list(container_client.list_blobs(name_starts_with=self.folder_path))

        # list to store all resources with their timestamps
        all_resources: List[Tuple[datetime, str, Dict[str, Any]]] = []

        logger.info("Collecting dynamic resources...")
        for resource_type, (hub_name, timestamp_path) in self.dynamic_resources.items():
            try:
                # find matching files for this resource type
                matching_blobs = []
                for blob in all_blobs:
                    filename = blob.name.split('/')[-1]
                    if filename.startswith(f"{resource_type}.") and filename.endswith('.ndjson'):
                        matching_blobs.append(blob)

                if not matching_blobs:
                    logger.warning(f"No file found for {resource_type}")
                    continue

                blob = matching_blobs[0]
                logger.info(f"Reading {blob.name} for {resource_type}")

                blob_client = container_client.get_blob_client(blob.name)
                stream = blob_client.download_blob()
                content = stream.readall().decode('utf-8')
                lines = [line for line in content.splitlines() if line.strip()]
                logger.info(f"Found {len(lines)} {resource_type} resources")

                for line in lines:
                    try:
                        resource = json.loads(line)
                        timestamp = self.extract_timestamp(resource, timestamp_path)
                        if timestamp:
                            all_resources.append((timestamp, resource_type, resource))
                    except json.JSONDecodeError as e:
                        logger.error(f"Error parsing JSON: {e}")
                    except Exception as e:
                        logger.error(f"Error processing resource: {e}")

            except Exception as e:
                logger.error(f"Error processing {resource_type}: {str(e)}")

        # sort all resources by timestamp
        logger.info("Sorting resources chronologically...")
        all_resources.sort(key=lambda x: x[0])
        total_resources = len(all_resources)
        logger.info(f"Total resources to process: {total_resources}")

        # process resources in chronological order
        logger.info("Processing resources in chronological order...")
        for i, (timestamp, resource_type, resource) in enumerate(all_resources, 1):
            hub_name = self.dynamic_resources[resource_type][0]
            logger.info(f"Processing resource {i} of {total_resources} - {resource_type} from {timestamp.isoformat()}")
            self.send_to_eventhub(resource, hub_name)
            time.sleep(5)

    # close all Event Hub connections
    def close(self) -> None:
        for producer in self.producer_clients.values():
            producer.close()

    # main method to stream all resources
    def stream_all_resources(self) -> None:
        try:
            logger.info("Starting FHIR resource streaming...")

            logger.info("Processing static resources...")
            self.process_static_resources()

            logger.info("Processing dynamic resources...")
            self.process_dynamic_resources()

            logger.info("Streaming completed!")

        except Exception as e:
            logger.error(f"Error in main streaming process: {e}")
        finally:
            self.close()

if __name__ == '__main__':
    streamer = FHIRStreamer()
    streamer.stream_all_resources()
