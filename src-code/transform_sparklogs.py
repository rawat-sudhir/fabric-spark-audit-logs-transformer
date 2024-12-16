# Using Service Principal 

import os
import json
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.identity import ClientSecretCredential

global_user = None

# Define the extract_fields function

def extract_fields(json_content):
    try:
        spark_job_description = json_content.get("properties", {}).get("Properties", {}).get("spark.job.description")
        if spark_job_description is None:
            user = json_content.get("properties", {}).get("User", "")
            if user:  # If properties.User is not empty or None
                global global_user
                global_user = user
                print(global_user)
            return None  # Do not return anything if spark.job.description is null

        return {
            "timestamp": json_content.get("timestamp"),
            "category": json_content.get("category"),
            "fabricLivyId": json_content.get("fabricLivyId"),
            "applicationId": json_content.get("applicationId"),
            "applicationName": json_content.get("applicationName"),
            "executorId": json_content.get("executorId"),
            "fabricTenantId": json_content.get("fabricTenantId"),
            "capacityId": json_content.get("capacityId"),
            "artifactType": json_content.get("artifactType"),
            "artifactId": json_content.get("artifactId"),
            "fabricWorkspaceId": json_content.get("fabricWorkspaceId"),
            "fabricEnvId": json_content.get("fabricEnvId"),
            "properties.Properties.spark.job.description": json_content.get("properties", {}).get("Properties", {}).get("spark.job.description"),
            "user": global_user
        }
    except Exception as e:
        print(f"Error extracting fields: {e}")
        return {}



# Load configuration
with open("/lakehouse/default/Files/spnconfig.json", "r") as config_file:
    config = json.load(config_file)

# Service principal credentials
tenant_id = config["tenant_id"]
client_id = config["client_id"]
client_secret = config["client_secret"]
storage_account_name = config["storage_account_name"]
src_container_name = config["src_container_name"]
dest_container_name = config["dest_container_name"]

credential = ClientSecretCredential(tenant_id, client_id, client_secret)

# Construct the storage account URL
account_url = f"https://{storage_account_name}.blob.core.windows.net"

# Initialize Blob Service Client
blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
container_client = blob_service_client.get_container_client(src_container_name)

# List unique folders in the container
folders = set()
for blob in container_client.list_blobs():
    # Check for the presence of a "folder" structure in the blob name
    if "/" in blob.name:
        # Extract the full folder path (excluding the file name)
        folder_name = "/".join(blob.name.split("/")[:-1])
        folders.add(folder_name)

# Process each folder
for folder in folders:
    print(f"Processing folder: {folder}/")

    # List subfolders/files in the folder
    print(f"Checking blobs under: {folder}/")
    sub_blob_found = False
    for sub_blob in container_client.list_blobs(name_starts_with=f"{folder}/"):
        sub_blob_found = True
        subfolder_name = sub_blob.name
        print(f"Found blob: {subfolder_name}")

        # Check for driver folder and specific file
        if "driver/" in subfolder_name and subfolder_name.endswith("spark-events"):
            print(f"Found spark-events.json in: {subfolder_name}")

            # Download and read JSON file
            json_blob_client = container_client.get_blob_client(subfolder_name)
            json_data = json_blob_client.download_blob().readall()

            # Parse JSON and extract specific elements
            json_lines = json_data.decode("utf-8").splitlines()
            for line in json_lines:
                if line.strip() and line.lstrip().startswith("{\"timestamp\""):

                    json_content = json.loads(line)
                    extracted_fields = extract_fields(json_content)

                    if extracted_fields is not None:
                        print("")
                        print(json.dumps(extracted_fields, indent=4))
                        

    if not sub_blob_found:
        print(f"No blobs found under: {folder}/")
    