import boto3
import urllib.parse
from custom_waiters import Waiter
import json


# json.dump(): json_object to string, json.loads(): string to json_object

class S3Object:
    output_bucket = 'output-data-s3bucket'
    gluejob = {'csv': 'GlueJobForCSVFiles'}  # mapping file_format with glue_job_name

    def __init__(self, event):
        self.bucket_name = urllib.parse.unquote(event['Records'][0]['s3']['bucket']['name'])
        self.key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        self.size = event['Records'][0]['s3']['object']["size"]
        self.filename_without_filetype, self.type = self.key.split('.')

    def run_glue_job(self, glue):
        if S3Object.gluejob[self.type] not in glue.list_jobs()['JobNames']:
            response = {"JobRunState": "FAILED",
                        "ErrorMessage": S3Object.gluejob[self.type] + " - This GlueJob doesn't exist"}
            return response

        args = {'--InputBucket': self.bucket_name, '--FileNameWithoutFiletype': self.filename_without_filetype,
                '--OutputBucket': S3Object.output_bucket}
        job_run_id = glue.start_job_run(JobName=S3Object.gluejob[self.type], Arguments=args)['JobRunId']
        job_run_details = Waiter.job_run_completed(Client=glue, JobName=S3Object.gluejob[self.type], RunId=job_run_id)
        return job_run_details

    def glue_job_success(self, lambdaa):
        output_path = "s3://" + S3Object.output_bucket + "/" + self.filename_without_filetype + "/"
        payload = {'Path': output_path, 'TableName': self.filename_without_filetype}
        response = lambdaa.invoke(FunctionName='LambdaFuncForGlueJobSuccess', InvocationType='RequestResponse',
                                  Payload=json.dumps(payload))
        return response['StatusCode']

    def glue_job_failure(self, lambdaa, error_details):
        payload = {"FileName": self.key, "GlueJobName": S3Object.gluejob[self.type], "ErrorDetails": error_details}
        response = lambdaa.invoke(FunctionName='LambdaFuncForGlueJobFailure', InvocationType='RequestResponse',
                                  Payload=json.dumps(payload))
        return response['StatusCode']


def lambda_handler(event, context):
    s3event = event['Records'][0]['Sns']['Message'].replace("'",'"')
    obj = S3Object(json.loads(s3event))

    try:
        if obj.type not in S3Object.gluejob.keys():
            print("File format not supported.")
            return
        else:
            response = obj.run_glue_job(glue=boto3.client('glue'))
            if response['JobRunState'] == 'SUCCEEDED':
                response = obj.glue_job_success(boto3.client('lambda', 'us-west-1'))
            else:
                response = obj.glue_job_failure(boto3.client('lambda', 'us-west-1'), response['ErrorMessage'])
            if response != 200:
                print("Error in Lambda Invocation")
    except Exception as err:
        print("Error -" + str(err))
