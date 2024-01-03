from func import *

# Main function to execute the ETL process
def main():
    local_dir = config('LOCAL_DIR')

    blobs = container_client_A.list_blobs()

    for blob in blobs:

        if '/' not in blob.name:
            blob_name = blob.name
            local_file_name = os.path.join(local_dir, blob_name)

            try:
                print(f"File in blob : {blob_name} ----------------------------------------------------------------------------------------------------")
                # Download blob from container A to local
                download_blob(container_client_A, blob_name, local_file_name)

                #cDecrypt and decompress
                decrypted_file_name = local_file_name[:-7] + '.gz'
                decrypt_gpg_file(local_file_name, decrypted_file_name, "CV&zwpu%GU3Te6")

                output_txt_file = local_file_name[:-7] + '.txt'
                decompress_gz_file(decrypted_file_name, output_txt_file)

                # Remove all files at local
                delete_files_in_folder_with_extension(local_dir,'gz')
                delete_files_in_folder_with_extension(local_dir,'gpg')

                # Upload to container B
                input_txt_file=blob_name[:-7]+'.txt'
                upload_blob(container_client_B, output_txt_file, input_txt_file)

                # Move file from Blob A to archive/encrypt
                move_gpg_file(container_client_A, blob_name)

                delete_files_in_folder_with_extension(local_dir,'txt')

                # Log success

                log(f"File in blob: {blob_name} - Success\n", log_path)

            except Exception as e:

                # Log failure and specific step where it failed
                log(f"File name: {blob_name} - Failed at step: {str(e)}\n", log_path)

# Run the main function
if __name__ == '__main__':
    main()