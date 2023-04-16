import os
import boto3
import tempfile
import s3fs
import polars as pl


os.environ['AWS_ACCESS_KEY_ID']=''
os.environ['AWS_SECRET_ACCESS_KEY']=''

def main():
    s3 = boto3.client('s3')
    
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp_file:
        # Download the CSV file from S3
        s3.download_fileobj("confessions-of-a-data-guy", 
                            "202303-divvy-tripdata.csv", 
                            temp_file)
        
        df = pl.read_csv(temp_file.name, try_parse_dates=True)

    df = df.groupby([pl.col('started_at').dt.date().alias('dt'), 
                     'rideable_type']).agg([pl.count('ride_id')]
                                           )
    fs = s3fs.S3FileSystem()
    with fs.open('confessions-of-a-data-guy/polars.parquet', mode='wb') as f:
        df.write_parquet(f)


if __name__ == '__main__':
    main()