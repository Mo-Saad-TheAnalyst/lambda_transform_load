import pandas as pd
from schema import schema
from datetime import date
from datetime import datetime
import awswrangler as wr
import os



out_bucket = 'stock-parquet-transformed'

def extract_bucket_name(event):
    name = event["Records"][0]["s3"]["bucket"]["name"]
    return name

def extract_object_key(event):
    key = event["Records"][0]["s3"]["object"]["key"]
    return key 


def read_s3_json(bucket,key):
    path = "s3://{AWS_S3_BUCKET}/{key}".format(AWS_S3_BUCKET = bucket,key = key)
    df = wr.s3.read_json(path = path, lines=True)
    return df

def impose_schema(df):
    out_columns = []
    for column in df.columns:
        if column in schema.keys():
            out_columns.append(column)
            if df[column].dtype != schema[column]:
                df[column] = df[column].astype(schema[column])
                print(column + "transformed to " + schema[column])
    return df[out_columns]

def generate_output_path(out_bucket,df):
    today = date.today()
    now = datetime.now()
    timestamp = now.strftime("%H:%M:%S")
    type = str(df["type"][0])
    stock_name = df["stock_name"][0]
    path = "s3://{AWS_S3_BUCKET}/{date}/{type}/{stock_name}/{stock_name}-{timestamp}.parquet".format(AWS_S3_BUCKET = out_bucket,date = today,stock_name = stock_name,type=type,timestamp=timestamp)
    return path

def save_df_parquet_s3(df:pd.DataFrame,path):
    wr.s3.to_parquet(df=df,path=path,)
    return "saved to path" + path

def main(event,out_bucket):
    bucket = extract_bucket_name(event=event)
    key = extract_object_key(event=event)
    df = read_s3_json(bucket,key)
    df = impose_schema(df)
    out_path = generate_output_path(out_bucket,df)
    response = save_df_parquet_s3(df,out_path)
    return response

def lambda_handler(event, context):
    main(event,out_bucket = os.environ['out_bucket'])