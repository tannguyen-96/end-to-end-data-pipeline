import os
import duckdb

def get_db_connection():
    conn = duckdb.connect()
    conn.execute("INSTALL postgres; LOAD postgres; INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"""
        ATTACH 'host={os.environ["POSTGRES_HOST"]}
                port={os.environ["POSTGRES_PORT"]}
                dbname={os.environ["POSTGRES_DB"]}
                user={os.environ["POSTGRES_USER"]}
                password={os.environ["POSTGRES_PASSWORD"]}
                sslmode=require'
        AS pg (TYPE postgres, READ_ONLY)
    """)
    return conn

def stream_data(table: str, ds: str):
    conn = get_db_connection()
    conn.execute(f"""
        CREATE SECRET (
            TYPE s3,
            KEY_ID '{os.environ["AWS_ACCESS_KEY_ID"]}',
            SECRET '{os.environ["AWS_SECRET_ACCESS_KEY"]}',
            REGION '{os.environ["AWS_DEFAULT_REGION"]}'
        )
    """)
    bucket = os.environ["S3_BUCKET"]
    conn.execute(f"""
        COPY (
            SELECT * FROM postgres_query('pg',
              'SELECT t.*, md5(t::text) AS row_hash FROM public."{table}" t')
        )
        TO 's3://{bucket}/{table}/load_date={ds}'
        (FORMAT PARQUET, FILE_SIZE_BYTES '256MB', OVERWRITE_OR_IGNORE)
    """)