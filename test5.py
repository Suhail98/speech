import os
import subprocess
from azure.storage.blob import BlobServiceClient
from SrtToTextgrid import srt_to_textGrid
import threading
import shutil
import zipfile
import sys
if len(sys.argv) < 1:
        print("Usage: python script.py <arg1> [arg2] [arg3] ...")
        sys.exit(1)
# Initialize a boolean flag with a default value
resume = False

# Check if the '--flag' argument is present in sys.argv
if '--resume' in sys.argv:
        resume = True
        sys.argv.remove('--resume')  # Remove the flag from sys.argv
def create_batch_folders(input_container_name):
    # Define the main folder name based on the input_container_name variable
    input_container_name = input_container_name  # Replace with your actual container name

    # Check if the main folder exists, and create it if it doesn't
    if not os.path.exists(input_container_name):
            os.makedirs(input_container_name)

            # Create subfolders inside the main folder
    subfolders = ["input", "output", "zipFiles", "failed"]

    for folder in subfolders:
                    folder_path = os.path.join(input_container_name, folder)
                    if not os.path.exists(folder_path):
                                    os.makedirs(folder_path)
                                    print(f"Folders created in '{input_container_name}' with subfolders: {', '.join(subfolders)}")
def process_mp3_file(input_container_name,mp3_file):
        source_folder = os.path.join(input_container_name,"input")
        failed_folder = os.path.join(input_container_name,"failed")
        output_folder = os.path.join(input_container_name,"output")
        mp3_path = os.path.join(source_folder, mp3_file)
        textgrid_path = os.path.join(output_folder, mp3_file.replace(".mp3", ".TextGrid"))
        if not os.path.exists(textgrid_path):
            shutil.move(mp3_path, os.path.join(failed_folder, mp3_file))
            print(f"Moved '{mp3_file}' to 'failed' folder.")
        else:
            os.remove(mp3_path)
            print(f"Deleted '{mp3_file}' because a matching TextGrid file was found.")
                                                                                                                        

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
def deleteAllZipFiles(input_container_name):
    # Specify the directory path
    output_directory = os.path.join(input_container_name,"zipFiles")
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

def downloadZIPFilesFromBlob(input_container_name):
    # List all MP3 files in the input container
    blobs = input_container_client.list_blobs()
    zip_files = [blob.name for blob in blobs if blob.name.endswith(".zip")]
    zip_files = ["missing batch 1B mp3.zip"]
    for file in zip_files:
        print(f"\nDownloading {file} from Azure Storage...")
        blob_client = input_container_client.get_blob_client(file)
        download_file_path = input_container_name+"/zipFiles/"+file
        with open(download_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
def unzip(input_container_name):
    zipPath = os.path.join(input_container_name,"zipFiles")
    destinationPath = os.path.join(input_container_name,"input")
    for file in os.listdir(zipPath):
        zip_file = zipfile.ZipFile(os.path.join(zipPath,file), 'r')
        # Extract the contents to the destination folder
        zip_file.extractall(destinationPath)
        # Close the zip file
        zip_file.close()
# Function to process each MP3 file
def process_mp3(input_container_name,file):
   try:
    # Run captioning script on downloaded audio file
    output_file = input_container_name+"/output/"+file.replace(".mp3", ".srt")
    command = [
        "python3", "captioning/captioning.py",
        "--input", os.path.join(input_container_name+"/input",file),
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
    print("starting processing {file} ")
    subprocess.run(command)
    
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
    print("uploaded successfullu")
   except:
         process_mp3_file(input_container_name,file)
         return
   process_mp3_file(input_container_name,file)    
   print(f"\nDone processing {file}.")

# Azure storage account details
connection_string = "DefaultEndpointsProtocol=https;AccountName=egabistorage;AccountKey=gcXX6kMcy7XQqAd1MT2CfGqKiauMAdtvaA636hKA7f5LwfkV6yK8UofTax7wDFQrZKBMH8z9DaiB+AStAC/EfA==;EndpointSuffix=core.windows.net"
input_container_name = sys.argv[1]
#output_container_name = "out1"

# Azure Speech Service subscription details
key = "f8e22ee1df5449a69ef8395b0173c4f4"
region = "eastus"
phrases = "Contoso;Jessie;Rehaan"

# Initialize BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
# Get clients for both the input and output containers
input_container_client = blob_service_client.get_container_client(input_container_name)
create_batch_folders(input_container_name)
#output_container_client = blob_service_client.get_container_client(output_container_name)
if resume is False:
    downloadZIPFilesFromBlob(input_container_name)
    #downloadMP3FilesFromBlob()
    unzip(input_container_name)

directory_path = os.path.join(input_container_name,'input')
mp3_files = [os.path.relpath(os.path.join(root, file), directory_path) for root, dirs, files in os.walk(directory_path) for file in files if file.endswith('.mp3')]
print(mp3_files)
'''
maxThreads = 18
i = 0
while i * maxThreads < len(mp3_files):
    print(i)
    threads = []
    print("start threadings")
    for mp3_file in mp3_files[i*maxThreads : min(len(mp3_files),(i+1)*maxThreads)]:
        print("inside mp3_file")
        thread = threading.Thread(target=process_mp3, args=(mp3_file,))
        threads.append(thread)
        thread.start()
    print("then")
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    i += 1
    print("end")
'''
from concurrent.futures import ThreadPoolExecutor
import concurrent
import logging
import time
# Record the start time
start_time = time.time()

# create a ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=18) as executor:
        # start your threads
        futures = {executor.submit(process_mp3, input_container_name,filename) for filename in mp3_files}

        # handle exceptions
        for future in concurrent.futures.as_completed(futures):
            try:
                data = future.result()
            except Exception as e:
                logging.error(f'Error in thread: {e}')

#moveFailedFiles()
print("\nAll MP3 files processed.")
#deleteAllMP3Files()
#deleteAllOutputFiles()
deleteAllZipFile(input_container_name)
# Record the end time
end_time = time.time()
# Calculate and print the total time
total_time = (end_time - start_time) // 60
print("===================================================")
print(f"The script took {total_time} minutes to complete.")
print("===================================================")

