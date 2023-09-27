import os
import subprocess
from azure.storage.blob import BlobServiceClient
from SrtToTextgrid import srt_to_textGrid
import threading
import shutil
import zipfile

def moveFailedFiles():
    input_directory = "input"
    output_directory = "output"
    failed_directory = "failed"

    # Ensure the directories exist
    for directory in [input_directory, output_directory, failed_directory]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    # Get a list of MP3 files in the input directory
    mp3_files = [f for f in os.listdir(input_directory) if f.endswith(".mp3")]

    # Check each MP3 file
    for mp3_file in mp3_files:
        mp3_path = os.path.join(input_directory, mp3_file)
        textgrid_file = mp3_file.replace(".mp3", ".TextGrid")
        textgrid_path = os.path.join(output_directory, textgrid_file)

        # If the corresponding TextGrid file doesn't exist in the output directory
        if not os.path.exists(textgrid_path):
            print(f"Moving {mp3_file} to the 'failed' directory.")
            shutil.move(mp3_path, os.path.join(failed_directory, mp3_file))
def deleteAllZipFiles():
    # Specify the directory path
    output_directory = "zipFiles"
    # Check if the directory exists
    if os.path.exists(output_directory) and os.path.isdir(output_directory):
        # List all files in the directory
        files = os.listdir(output_directory)

        # Loop through the files and delete .srt files
        for file_name in files:
            
                file_path = os.path.join(output_directory, file_name)
                
                try:
                    # Delete the .srt file
                    os.remove(file_path)
                    print(f"Deleted .srt file: {file_path}")
                except Exception as e:
                    print(f"Error deleting .srt file: {file_path}, {e}")
    else:
        print(f"Directory not found: {output_directory}")

def deleteAllOutputFiles():
        # Specify the directory path
    output_directory = "output"

    # Check if the directory exists
    if os.path.exists(output_directory) and os.path.isdir(output_directory):
        # List all files in the directory
        files = os.listdir(output_directory)

        # Loop through the files and delete .srt files
        for file_name in files:
            
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

def downloadMP3FilesFromBlob():
    # List all MP3 files in the input container
    blobs = input_container_client.list_blobs()
    mp3_files = [blob.name for blob in blobs if blob.name.endswith(".mp3")]
    for file in mp3_files:
        print(f"\nDownloading {file} from Azure Storage...")
        blob_client = input_container_client.get_blob_client(file)
        download_file_path = "input/"+file
        with open(download_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())

def downloadZIPFilesFromBlob():
    # List all MP3 files in the input container
    blobs = input_container_client.list_blobs()
    zip_files = [blob.name for blob in blobs if blob.name.endswith(".zip")]
    for file in zip_files:
        print(f"\nDownloading {file} from Azure Storage...")
        blob_client = input_container_client.get_blob_client(file)
        download_file_path = "zipFiles/"+file
        with open(download_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
def unzip():
    zipPath = "zipFiles"
    destinationPath = "input"
    for file in os.listdir(zipPath):
        zip_file = zipfile.ZipFile(os.path.join(zipPath,file), 'r')
        # Extract the contents to the destination folder
        zip_file.extractall(destinationPath)
        # Close the zip file
        zip_file.close()
# Function to process each MP3 file
def process_mp3(file):   
    # Run captioning script on downloaded audio file
    output_file = "output/"+file.replace(".mp3", ".srt")
    command = [
        "python3", "captioning/captioning.py",
        "--input", os.path.join("input",file),
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

    subprocess.run(command, capture_output=True, text=True)

    #print(result.stdout)
    #with open("output.txt", "a") as file:
    #    file.write(result.stdout)

    textgrid_file = output_file.replace(".srt", ".TextGrid")

    srt_to_textGrid(output_file , textgrid_file)

    # Write the output to a new blob in the output container
    print(f"\nWriting output textgrid for {file} to a new blob...")
    with open(textgrid_file, "rb") as data:
        blob_client = output_container_client.get_blob_client(textgrid_file)
        blob_client.upload_blob(data, overwrite=True)
    
    print(f"\nDone processing {file}.")

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
downloadZIPFilesFromBlob()
downloadMP3FilesFromBlob()
unzip()

directory_path = 'input'
mp3_files = [os.path.relpath(os.path.join(root, file), directory_path) for root, dirs, files in os.walk(directory_path) for file in files if file.endswith('.mp3')]
print(mp3_files)


# Create and start threads for processing MP3 files
maxThreads = 100
for i in range(len(mp3_files)):
    threads = []
    for mp3_file in mp3_files[i*maxThreads : min(len(mp3_files),(i+1)*maxThreads)]:
        thread = threading.Thread(target=process_mp3, args=(mp3_file,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

moveFailedFiles()
print("\nAll MP3 files processed.")
deleteAllMP3Files()
deleteAllOutputFiles()
deleteAllZipFiles()


