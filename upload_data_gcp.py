import pandas as pd
import glob
import os
from google.cloud import storage

# create a client 
storage_client = storage.Client("mp-capstone-1")
bucket = storage_client.bucket("equity_market_raw_data_mp")


# This works for me. Copy all content from a local directory to a specific bucket-name/full-path (recursive) in google cloud storage:
def upload_local_directory_to_gcs(local_path, bucket, gcs_path):
    assert os.path.isdir(local_path)
    for local_file in glob.glob(local_path + '/**'):
        if not os.path.isfile(local_file):
           upload_local_directory_to_gcs(local_file, bucket, gcs_path + "/" + os.path.basename(local_file))
        else:
           remote_path = os.path.join(gcs_path, local_file[1 + len(local_path):])
           blob = bucket.blob(remote_path)
           blob.upload_from_filename(local_file)
    return True


upload_local_directory_to_gcs("data", bucket, "input_data")
