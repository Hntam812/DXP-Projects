from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import subprocess
import gzip
import shutil
import os
from decouple import config
from datetime import datetime

# Define env
AZURE_STORAGE_CONNECTION_STRING_A = config('AZURE_STORAGE_CONNECTION_STRING_A')  # Connection to Blob A : ENCRYPT
AZURE_STORAGE_CONNECTION_STRING_B = config('AZURE_STORAGE_CONNECTION_STRING_B')  # Connection to Blob B : DECRYPT
CONTAINER_NAME_A = config('CONTAINER_NAME_A')  # Container in Blob A
CONTAINER_NAME_B = config('CONTAINER_NAME_B')  # Container in Blob B

# Create connection string to Blob A và Blob B
blob_service_client_A = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING_A)
blob_service_client_B = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING_B)

container_client_A = blob_service_client_A.get_container_client(CONTAINER_NAME_A)
container_client_B = blob_service_client_B.get_container_client(CONTAINER_NAME_B)


# Function to get current date in format 'yyyymmdd'
def get_yyyymmdd():
    current_date = datetime.now()
    formatted_date = current_date.strftime('%Y%m%d')
    return formatted_date

# Define the log path with current date
log_path=config('LOG_PATH')
log_path = log_path+f"/log_{get_yyyymmdd()}.txt"

# Function to log a message to a file
def log(message, log_path):
    log_text = f"{message}\n"

    with open(log_path, 'a') as log_file:
        log_file.write(log_text)

# Function to decrypt a GPG file
def decrypt_gpg_file(input_file, output_file, passphrase):

    command=f'echo "{passphrase}" | gpg --pinentry-mode loopback --passphrase-fd 0 --output "{output_file}" --decrypt "{input_file}"'
    try:
        subprocess.run(command, shell=True, check=True)
        print('Decrypt file successful.')
        return True
    except subprocess.CalledProcessError as e:
        print('Cannot decrypt file')
        return False

# Function to decompress a GZ file
def decompress_gz_file(input_file, output_file):
    with gzip.open(input_file, 'rb') as f_in, open(output_file, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

# Function to delete files in a folder with a specific extension
def delete_files_in_folder_with_extension(folder_path, file_extension=None):
    if not os.path.exists(folder_path):
        print(f"The folder '{folder_path}' does not exist.")
        return

    files = os.listdir(folder_path)

    if file_extension is None:
        for file in files:
            file_path = os.path.join(folder_path, file)
            os.remove(file_path)
    else:
        filtered_files = [file for file in files if file.endswith(f".{file_extension}")]

        for filtered_file in filtered_files:
            file_path = os.path.join(folder_path, filtered_file)
            os.remove(file_path)


# Function to download blob from Azure Storage
def download_blob(container_client, blob_name, local_file_name):
    blob_client = container_client.get_blob_client(blob_name)
    with open(local_file_name, "wb") as my_blob:
        blob_data = blob_client.download_blob()
        blob_data.readinto(my_blob)

    print(f"Downloaded {blob_name} to local success.")


# Function to upload blob to Azure Storage
def upload_blob(container_client, local_file_name, input_txt_file):
    blob_name=input_txt_file
    blob_client = container_client.get_blob_client(blob_name)
    with open( local_file_name, "rb") as data:
        blob_client.upload_blob(data)
    print(f"Uploaded { local_file_name} to blob container {container_client}")


# Function to move files gpg to archive in Azure Blob Storage
def move_gpg_file(container_name, blob_name):
    timestamp = get_yyyymmdd()
    destination_folder = f"archive/encrypt/{timestamp}"

    if not any(item.name == destination_folder for item in container_name.list_blobs()):
        blob_client = container_name.get_blob_client(destination_folder)
        blob_client.upload_blob('')

    source_blob_client = container_name.get_blob_client(blob_name)
    destination_blob_name = f"{destination_folder}/{blob_name}"
    destination_blob_client = container_name.get_blob_client(destination_blob_name)

    source_blob_properties = source_blob_client.get_blob_properties()
    destination_blob_client.upload_blob(source_blob_client.download_blob().readall(), blob_type=source_blob_properties.blob_type)

    source_blob_client.delete_blob()

    print(f"Moved {blob_name} to {destination_blob_name}")


