import pandas as pd
import gcsfs
from google.cloud import storage

# create a client 
storage_client = storage.Client("mp-capstone-1")
bucket = storage_client.create_bucket("equity_market_raw_data_mp")
loc = "us-central1"
bucket.location(loc)
