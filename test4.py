from azure.storage.blob import BlobServiceClient
import os
# Replace these with your Azure Storage account credentials
connection_string = "DefaultEndpointsProtocol=https;AccountName=egabistorage;AccountKey=gcXX6kMcy7XQqAd1MT2CfGqKiauMAdtvaA636hKA7f5LwfkV6yK8UofTax7wDFQrZKBMH8z9DaiB+AStAC/EfA==;EndpointSuffix=core.windows.net"
container_name = "test"
local_download_path = "downloaded_mp3_files"  # Directory to save downloaded MP3 files

# Create a BlobServiceClient to interact with the Azure Storage account
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

# Function to retrieve all MP3 files from the container
def list_mp3_files(container_client):
    mp3_files = []
    for blob in container_client.list_blobs():
        if blob.name.endswith('.mp3'):
            mp3_files.append(blob)
    return mp3_files

# Function to download an MP3 file locally
def download_mp3(blob_client, local_path):
    with open(local_path, "wb") as mp3_file:
        mp3_file.write(blob_client.download_blob().readall())

# Main function to download all MP3 files locally
def main():
    mp3_files = list_mp3_files(container_client)
    
    # Create the local download directory if it doesn't exist
    os.makedirs(local_download_path, exist_ok=True)
    
    for mp3_file in mp3_files:
        blob_client = container_client.get_blob_client(mp3_file.name)
        local_mp3_path = os.path.join(local_download_path, mp3_file.name)
        
        # Download the MP3 file locally
        download_mp3(blob_client, local_mp3_path)
        print(f"Downloaded MP3 file: {local_mp3_path}")

if __name__ == "__main__":
    main()