import os
import subprocess
from azure.storage.blob import BlobServiceClient
from SrtToTextgrid import srt_to_textGrid
import threading


def deleteAllSRTFiles():
        # Specify the directory path
    output_directory = "output"

    # Check if the directory exists
    if os.path.exists(output_directory) and os.path.isdir(output_directory):
        # List all files in the directory
        files = os.listdir(output_directory)

        # Loop through the files and delete .srt files
        for file_name in files:
            if file_name.endswith(".srt"):
                file_path = os.path.join(output_directory, file_name)
                
                try:
                    # Delete the .srt file
                    os.remove(file_path)
                    print(f"Deleted .srt file: {file_path}")
                except Exception as e:
                    print(f"Error deleting .srt file: {file_path}, {e}")
    else:
        print(f"Directory not found: {output_directory}")

def deleteAllMP3Files():
    # Specify the directory path
    directory_path = "input"

    # Check if the directory exists
    if os.path.exists(directory_path) and os.path.isdir(directory_path):
        # Get a list of all files in the directory
        files = os.listdir(directory_path)

        # Loop through the files and delete them
        for file_name in files:
            file_path = os.path.join(directory_path, file_name)
            
            try:
                if os.path.isfile(file_path):
                    # Delete the file
                    os.remove(file_path)
                    print(f"Deleted file: {file_path}")
                else:
                    print(f"Skipped non-file item: {file_path}")
            except Exception as e:
                print(f"Error deleting file: {file_path}, {e}")
    else:
        print(f"Directory not found: {directory_path}")

# Function to process each MP3 file
def process_mp3(input_blob_name):
    print(f"\nDownloading {input_blob_name} from Azure Storage...")
    blob_client = input_container_client.get_blob_client(input_blob_name)
    download_file_path = "input/"+input_blob_name
    with open(download_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())
    # Run captioning script on downloaded audio file
    output_file = "output/"+input_blob_name.replace(".mp3", ".srt")
    command = [
        "python3", "captioning/captioning.py",
        "--input", download_file_path,
        "--format", "any",
        "--output", output_file,
        "--srt",
        "--offline",
        "--threshold", "5",
        "--delay", "100",
        "--profanity", "mask",
        "--language", "ar-EG",
        "--key", key,
        "--region", region,
        "--phrases", phrases
    ]

    result = subprocess.run(command, capture_output=True, text=True)

    print(result.stdout)
    
    textgrid_file = output_file.replace(".srt", "_file.TextGrid")

    srt_to_textGrid(output_file , textgrid_file)

    # Write the output to a new blob in the output container
    print(f"\nWriting output textgrid for {input_blob_name} to a new blob...")
    with open(textgrid_file, "rb") as data:
        blob_client = output_container_client.get_blob_client(textgrid_file)
        blob_client.upload_blob(data, overwrite=True)
    
    print(f"\nDone processing {input_blob_name}.")

# Azure storage account details
connection_string = "DefaultEndpointsProtocol=https;AccountName=egabistorage;AccountKey=gcXX6kMcy7XQqAd1MT2CfGqKiauMAdtvaA636hKA7f5LwfkV6yK8UofTax7wDFQrZKBMH8z9DaiB+AStAC/EfA==;EndpointSuffix=core.windows.net"
input_container_name = "test2"
output_container_name = "outputdata"

# Azure Speech Service subscription details
key = "f8e22ee1df5449a69ef8395b0173c4f4"
region = "eastus"
phrases = "Contoso;Jessie;Rehaan"

# Initialize BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Get clients for both the input and output containers
input_container_client = blob_service_client.get_container_client(input_container_name)
output_container_client = blob_service_client.get_container_client(output_container_name)

# List all MP3 files in the input container
blobs = input_container_client.list_blobs()
mp3_files = [blob.name for blob in blobs if blob.name.endswith(".mp3")]

# Create and start threads for processing MP3 files
threads = []
print(mp3_files)

for mp3_file in mp3_files:
    thread = threading.Thread(target=process_mp3, args=(mp3_file,))
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()

print("\nAll MP3 files processed.")
deleteAllMP3Files()
deleteAllSRTFiles()





