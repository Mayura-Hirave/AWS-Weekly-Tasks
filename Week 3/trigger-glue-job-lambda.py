import boto3
import urllib.parse
from custom_waiters import Waiter
# import json
# json.dump(): json_object to string, json.loads(): string to json_object


class Table:
    def __init__(self, data_store, db_name='db-for-crawler'):
        try:
            self.path = data_store  # -- path of data store
            self.db_name = db_name
            self.table_name = None
        except Exception as e:
            raise Exception("Error in Table.__init__():\n" + str(e))

    def create_table(self, glue, crawler_name='myCrawler'):
        try:
            if crawler_name not in glue.list_crawlers()['CrawlerNames']:
                print(crawler_name + "- This crawler doesn't exist")
                return False

            # Waiter.crawler_run_completed(Client=glue, CrawlerName=crawler_name)
            glue.update_crawler(Name=crawler_name, Targets={'S3Targets': [{'Path': self.path}]})
            glue.start_crawler(Name=crawler_name)
            crawl = Waiter.crawler_run_completed(Client=glue, CrawlerName=crawler_name)
            if crawl['Status'] == 'SUCCEEDED':
                return True
            print("Crawler Failed ", crawl['ErrorMessage'])
        except Exception as e:
            print("Error in Table.create_table():\n", str(e))
        return False

    def get_table_name(self, glue):
        try:
            next_token = ""
            while True:
                response = glue.get_tables(DatabaseName=self.db_name, NextToken=next_token)
                for table_details in response['TableList']:
                    if table_details["StorageDescriptor"]["Location"] == self.path:
                        self.table_name = table_details["Name"]
                        break
                next_token = response.get('NextToken')
                if next_token is None:
                    break

            if self.table_name:
                return True
            print("Crawler failed to create table for inputFile")
        except Exception as e:
            print("Error in Table.get_table_name():\n", str(e))
        return False

    def run_query(self, athena, query_result_bucket='s3://athena-query-results-buckett/'):
        try:
            query_str = 'CREATE VIEW ' + self.table_name + '_view AS SELECT * FROM ' + self.table_name
            execution_id = athena.start_query_execution(QueryString=query_str,
                                                        QueryExecutionContext={'Database': self.db_name,
                                                                               'Catalog': 'AwsDataCatalog'},
                                                        ResultConfiguration={'OutputLocation': query_result_bucket})[
                'QueryExecutionId']
            status = Waiter.query_run_completed(Client=athena, QueryExecutionID=execution_id)
            if status['State'] == 'SUCCEEDED':
                return True
            print("Query Failed. Reason: ", status['StateChangeReason'])
        except Exception as e:
            print("Error in Table.run_query():\n", str(e))
        return False


class S3Object:
    output_bucket = 'output-data-s3bucket'
    # gluejob = {'csv': 'GlueJobForCSVFiles'}  # mapping file_format with glue_job_name
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('FileSpecification_To_Gluejob_Mapping')

    def __init__(self, event):
        try:
            self.bucket_name = urllib.parse.unquote(event['Records'][0]['s3']['bucket']['name'])
            self.key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
            self.size = event['Records'][0]['s3']['object']["size"]
            self.filename_without_filetype, self.type = self.key.split('.')
        except Exception as e:
            raise Exception("Error in S3Object.__init__():\n" + str(e))

    def get_glue_job(self):
        query_result = S3Object.table.query(Select="SPECIFIC_ATTRIBUTES", AttributesToGet=["Gluejob_Name"], Limit=1,
                                            KeyConditions={
                                                'Media_Type': {'AttributeValueList': [self.type],
                                                               'ComparisonOperator': 'EQ'},
                                                'File_Size_Limit': {'AttributeValueList': [self.size],
                                                                    'ComparisonOperator': 'GE'}
                                            })
        return query_result

    def run_glue_job(self, glue, job_name):
        try:
            if job_name not in glue.list_jobs()['JobNames']:
                print(job_name, " - This GlueJob doesn't exist")
                return {'JobRunState': 'Failed'}

            args = {'--InputBucket': self.bucket_name, '--FileNameWithoutFiletype': self.filename_without_filetype,
                    '--OutputBucket': S3Object.output_bucket}
            job_run_id = glue.start_job_run(JobName=job_name, Arguments=args)['JobRunId']
            job_run_details = Waiter.job_run_completed(Client=glue, JobName=job_name,
                                                       RunId=job_run_id)
            return job_run_details
        except Exception as e:
            print("Error in S3Object.run_glue_job():\n ", str(e))
            return {'JobRunState': 'Failed'}

    def glue_job_success(self):
        try:
            output_path = "s3://" + S3Object.output_bucket + "/" + self.filename_without_filetype + "/"
            table = Table(output_path)
            glue = boto3.client("glue")
            if table.create_table(glue) and table.get_table_name(glue):
                if table.run_query(boto3.client("athena")):
                    return True
        except Exception as e:
            print("Error in S3Object.glue_job_success():\n", str(e))
        return False

    def send_failure_alert(self, error_details, job_name):
        try:
            sns = boto3.client('sns')
            topic_arn = 'arn:aws:sns:ap-south-1:478055296570:GlueJobFailureEmailNotification'
            print("GlueJob ", job_name, " Failed!")
            print("Error: ", error_details)
            message = "Failed to convert " + self.key + " into parque file."
            sns.publish(TopicArn=topic_arn, Message=message, Subject='File conversion failed')
            return True
        except Exception as e:
            print("Error in S3Object.send_failure_alert():\n", str(e))
            return False


def lambda_handler(event, context):
    try:
        obj = S3Object(event)

        query_result = obj.get_glue_job()
        if query_result['Count'] == 0:
            print("File format not supported.")
            return

        glue_response = obj.run_glue_job(glue=boto3.client('glue'), job_name=query_result['Items'][0]['Gluejob_Name'])
        if glue_response['JobRunState'] == 'SUCCEEDED':
            response = obj.glue_job_success()
        else:
            response = obj.send_failure_alert(glue_response['ErrorMessage'], job_name=query_result['Items'][0]['Gluejob_Name'])

        if response:
            print("Successfully completed task!")
    except Exception as err:
        print("Error: " + str(err))
