import duckdb
import requests
from requests_kerberos import HTTPKerberosAuth
import pyarrow.parquet as pq

# Connect to DuckDB
con = duckdb.connect(database='your_database')

# Enable WebHDFS and specify the HDFS endpoint
webhdfs_endpoint = 'http://your_hdfs_endpoint/webhdfs/v1'

# Specify the path to the Parquet file in HDFS
parquet_file_path = '/path/to/your/parquet_file.parquet'

# Authenticate with Kerberos
auth = HTTPKerberosAuth()

# Retrieve Parquet file metadata using WebHDFS
metadata_url = f'{webhdfs_endpoint}{parquet_file_path}?op=GETFILESTATUS'
response = requests.get(metadata_url, auth=auth)
metadata = response.json()

# Retrieve the Parquet file size
file_size = metadata['FileStatus']['length']

# Read Parquet data using WebHDFS
read_url = f'{webhdfs_endpoint}{parquet_file_path}?op=OPEN'
response = requests.get(read_url, stream=True, auth=auth)

# Save Parquet data locally
local_parquet_file = '/path/to/local/parquet_file.parquet'
with open(local_parquet_file, 'wb') as f:
    for chunk in response.iter_content(chunk_size=8192):
        if chunk:
            f.write(chunk)

# Load Parquet data into DuckDB
con.execute(f'CREATE TABLE parquet_data AS SELECT * FROM PARQUET(\'{local_parquet_file}\')')

# Query the data using DuckDB
result = con.execute('SELECT * FROM parquet_data')
for row in result.fetchall():
    print(row)

# Close the DuckDB connection
con.close()
