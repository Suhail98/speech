import os
import subprocess
from azure.storage.blob import BlobServiceClient
from SrtToTextgrid import srt_to_textGrid

# Azure storage account details
connection_string = "DefaultEndpointsProtocol=https;AccountName=egabistorage;AccountKey=gcXX6kMcy7XQqAd1MT2CfGqKiauMAdtvaA636hKA7f5LwfkV6yK8UofTax7wDFQrZKBMH8z9DaiB+AStAC/EfA==;EndpointSuffix=core.windows.net"
input_container_name = "test"
output_container_name = "outputdata"

# Blob names
input_blob_name = "1.mp3"
output_blob_name = "mp3Output2.srt"

# Azure Speech Service subscription details
key = "f8e22ee1df5449a69ef8395b0173c4f4>"
region = "eastus"
phrases = "Contoso;Jessie;Rehaan"

# Initialize BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Get clients for both the input and output containers
input_container_client = blob_service_client.get_container_client(input_container_name)
output_container_client = blob_service_client.get_container_client(output_container_name)

# Read audio file from blob in the input container
blob_client = input_container_client.get_blob_client(input_blob_name)
print("\nReading blob from Azure Storage as blob object...")
download_file_path = input_blob_name
with open(download_file_path, "wb") as download_file:
    download_file.write(blob_client.download_blob().readall())

# Run captioning script on downloaded audio file
output_file = 'output.srt'

command = [
    "python3", "captioning/captioning.py",
    "--input", input_blob_name,
    "--format", "any",
    "--output", output_file,
    "--srt",
    "--offline",
    "--threshold", "5",
    "--delay", "100",
    "--profanity", "mask",
    "--language", "ar-EG",
    "--key", "f8e22ee1df5449a69ef8395b0173c4f4",
    "--region", "eastus",
    "--phrases", "Contoso;Jessie;Rehaan"
]

result = subprocess.run(command, capture_output=True, text=True)

print(result.stdout)

textgrid_file = 'output_file.TextGrid'

srt_to_textGrid(output_file , textgrid_file)

# Write the output to a new blob in the output container
print("\nWriting output textgrid to a new blob...")
with open(textgrid_file, "rb") as data:
    blob_client = output_container_client.get_blob_client(output_blob_name)
    blob_client.upload_blob(data, overwrite=True)

print("\nDone.")

