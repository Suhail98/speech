import os
import subprocess
import threading
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

python_path = "python"
script_path = "captioning/captioning.py"
key = "f8e22ee1df5449a69ef8395b0173c4f4"
region = "eastus"
phrases = "Contoso;Jessie;Rehaan"
source_dir = "downloaded_mp3_files"
connection_string = "DefaultEndpointsProtocol=https;AccountName=egabistorage;AccountKey=gcXX6kMcy7XQqAd1MT2CfGqKiauMAdtvaA636hKA7f5LwfkV6yK8UofTax7wDFQrZKBMH8z9DaiB+AStAC/EfA==;EndpointSuffix=core.windows.net"
output_container_name = "outputdata"  # Container where you want to upload the MP3 files
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
output_container_client = blob_service_client.get_container_client(output_container_name)


def run_python(source, output):
    command = [
        python_path, script_path,
        "--input", source,
        "--format", "any",
        "--output", output,
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
    subprocess.run(command, check=True)
i = 0
def process_files(filename):
            source = os.path.join(source_dir, filename)
            output = "output\\" + os.path.splitext(filename)[0] + ".srt"
            print("=" * 60)
            print("Source:", source)
            print("Output:", output)            
            global i
            i += 1
            print("i= ",i)
            print("=" * 60)
            run_python(source, output)
if __name__ == "__main__":
    all_files = os.listdir(source_dir)
    
    mp3_files = [filename for filename in all_files if filename.endswith(".mp3")]
    print(mp3_files)
    # Split the list of MP3 files into chunks for each thread
    #chunk_size = len(mp3_files) // num_threads
    #file_chunks = [mp3_files[i:i + chunk_size] for i in range(0, len(mp3_files), chunk_size)]

    threads = []

    # Define a lock to prevent multiple threads from starting at the same time
    #thread_start_lock = threading.Lock()
    print("len",len(mp3_files))
    for file in mp3_files:
        thread = threading.Thread(target=process_files, args=(file,))
        threads.append(thread)
        thread.start()
        #thread.start()
    #with thread_start_lock:
    #    for thread in threads:
    #        thread.start()
    # Wait for all threads to finish
    for thread in threads:
        thread.join()
    for file in os.listdir("output\\"):
        blob_client = output_container_client.get_blob_client(file)  # Use the container client to get the blob client
        with open("output\\" + file, "rb") as srt:
            blob_client.upload_blob(srt)