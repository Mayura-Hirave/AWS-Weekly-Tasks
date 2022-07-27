import sys
import pyarrow
from io import BytesIO
import pandas as pd
import boto3
from awsglue.utils import getResolvedOptions



argProvidedByCaller = ['InputBucket', 'FileNameWithoutFiletype', 'OutputBucket']
args = getResolvedOptions(sys.argv, argProvidedByCaller)
s3 = boto3.resource('s3')

streaming_body = s3.Object(args['InputBucket'], args['FileNameWithoutFiletype']+'.csv').get()['Body']
df = pd.read_csv(streaming_body)

buffer = BytesIO() #_io.BytesIO object
df.to_parquet(buffer)
output_file_name = args['FileNameWithoutFiletype'] + '/' + 'data.parquet'
buffer.seek(0)
s3.Object(args['OutputBucket'], output_file_name).put(Body=buffer.read())


    
