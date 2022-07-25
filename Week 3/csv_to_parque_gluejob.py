import sys
import pyarrow
from io import BytesIO
import pandas as pd
import boto3
from awsglue.utils import getResolvedOptions


argProvidedByCaller = ['InputBucket', 'FileNameWithoutFiletype', 'OutputBucket']
args = getResolvedOptions(sys.argv, argProvidedByCaller)
s3 = boto3.resource('s3')

byte_like_obj = s3.Object(args['InputBucket'], args['FileNameWithoutFiletype']+'.csv').get()['Body'].read()
df = pd.read_csv(BytesIO(byte_like_obj))

buffer = BytesIO() #_io.BytesIO object
df.to_parquet(buffer, index=False)
output_file_name = args['FileNameWithoutFiletype'] + '/' + 'data.parquet'
s3.Object(args['OutputBucket'], output_file_name).put(Body=buffer.getvalue())

