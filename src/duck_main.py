import duckdb


def main():
    duckdb.sql("""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_region='us-east-1';
        SET s3_access_key_id='';
        SET s3_secret_access_key='';
        CREATE VIEW metrics AS 
        SELECT CAST(started_at as DATE) as dt, 
                rideable_type, 
                COUNT(ride_id) as cnt_per_day
        FROM read_csv_auto('s3://confessions-of-a-data-guy/202303-divvy-tripdata.csv')
        GROUP BY CAST(started_at as DATE), rideable_type;
    """)

    duckdb.sql("""
        COPY metrics TO 's3://confessions-of-a-data-guy/results.parquet';
    """)


if __name__ == '__main__':
    main()